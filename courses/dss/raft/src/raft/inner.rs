use crate::proto::raftpb::raft::Client as RaftClient;
use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use std::fmt::{Debug, Formatter};
use std::ops::Range;

use crate::raft::persister::Persister;
use crate::raft::role::Role;
use crate::raft::{ApplyMsg, NodeId, TermId};
use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use num::integer::div_ceil;
use rand::RngCore;

use crate::raft::logs::Logs;
use assert2::assert;
use tokio::sync::{mpsc, oneshot};
use tracing::instrument;

pub struct Config {
    pub heartbeat_cycle: u64,
    // NOTE: both follower and candidate need random timeout
    // followers need random timeout to prevent multiple followers to become candidates simultaneously
    // candidates need random timeout to prevent multiple candidates to restart votes collection simultaneously
    pub election_timeout: Range<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_cycle: 100,
            election_timeout: 300..500,
        }
    }
}

pub struct Inner {
    role: Option<Role>,
    handle: Handle,
}

impl Inner {
    pub const fn new(role: Option<Role>, handle: Handle) -> Self {
        Self { role, handle }
    }

    pub async fn raft_main(&mut self) {
        while let role = self.role.take().unwrap() && !matches!(role, Role::Stop) {
            let new_role = role.progress(&mut self.handle).await;
            self.role = Some(new_role);
        }
    }
}

pub struct Handle {
    pub node_id: usize,                // this peer's index into peers[]
    pub persister: Box<dyn Persister>, // Object to hold this peer's persisted state
    pub election: Election,
    pub logs: Logs,
    pub apply_ch: UnboundedSender<ApplyMsg>,
    pub peers: Vec<RaftClient>, // RPC end points of all peers
    pub remote_task_receiver: mpsc::Receiver<RemoteTask>,
    pub local_task_receiver: mpsc::Receiver<LocalTask>,
    pub random_generator: Box<dyn RngCore + Send>,
    pub config: Config,
}

impl Debug for Handle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("node_id", &self.node_id)
            .field("election", &self.election)
            .field("logs", &self.logs)
            .finish()
    }
}

impl Handle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: usize,
        persister: Box<dyn Persister>,
        election: Election,
        logs: Logs,
        apply_ch: UnboundedSender<ApplyMsg>,
        peers: Vec<RaftClient>,
        remote_task_receiver: mpsc::Receiver<RemoteTask>,
        local_task_receiver: mpsc::Receiver<LocalTask>,
        random_generator: Box<dyn RngCore + Send>,
        config: Config,
    ) -> Self {
        Self {
            node_id,
            persister,
            election,
            logs,
            apply_ch,
            peers,
            remote_task_receiver,
            local_task_receiver,
            random_generator,
            config,
        }
    }

    pub fn get_node_ids_except_mine(&self) -> impl Iterator<Item = NodeId> {
        let me = self.node_id;
        (0..self.peers.len()).filter(move |node_id| *node_id != me)
    }

    /// In the Raft paper, there is `lastApplied` field.
    /// But here, we don't don't make the execution of this function a transaction.
    #[instrument(skip_all, level = "debug")]
    pub async fn apply_messages<I, M>(apply_ch: &mut UnboundedSender<ApplyMsg>, messages: I)
    where
        I: Iterator<Item = M> + Send,
        M: Into<ApplyMsg>,
    {
        for entry in messages {
            let apply_msg = entry.into();
            apply_ch.send(apply_msg).await.unwrap();
        }
    }

    pub fn get_majority_threshold(&self) -> usize {
        div_ceil(self.peers.len() + 1, 2)
    }
}

pub enum RemoteTask {
    RequestVote {
        args: RequestVoteArgs,
        sender: oneshot::Sender<RequestVoteReply>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        sender: oneshot::Sender<AppendEntriesReply>,
    },
}

impl Debug for RemoteTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("RemoteTask");
        match self {
            Self::RequestVote { args, .. } => debug.field("RequestVoteArgs", args),
            Self::AppendEntries { args, .. } => debug.field("AppendEntiresArgs", args),
        };
        debug.finish()
    }
}

pub struct RemoteTaskResult {
    pub success: bool,
    pub new_role: Role,
}

impl RemoteTaskResult {
    pub const fn new(success: bool, new_role: Role) -> Self {
        Self { success, new_role }
    }
}

impl RemoteTask {
    pub async fn handle(self, role: Role, handle: &mut Handle) -> RemoteTaskResult {
        match self {
            RemoteTask::RequestVote { args, sender } => {
                let (reply, new_role) = role.request_vote(handle, &args);
                let result = if reply.vote_granted {
                    RemoteTaskResult::new(true, new_role)
                } else {
                    RemoteTaskResult::new(false, new_role)
                };
                sender.send(reply).unwrap();
                result
            }
            RemoteTask::AppendEntries { args, sender } => {
                let (reply, new_role) = role.append_entries(handle, args).await;
                let result = if reply.match_length.is_some() {
                    RemoteTaskResult::new(true, new_role)
                } else {
                    RemoteTaskResult::new(false, new_role)
                };
                sender.send(reply).unwrap();
                result
            }
        }
    }
}

pub enum LocalTask {
    AppendEntries {
        data: Vec<u8>,
        sender: oneshot::Sender<Option<(u64, u64)>>, // None if not leader
    },
    GetTerm(oneshot::Sender<TermId>),
    CheckLeader(oneshot::Sender<bool>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Default, Debug)]
pub struct Election {
    term: TermId,
    pub voted_for: Option<NodeId>,
}

impl Election {
    pub fn get_current_term(&self) -> TermId {
        self.term
    }

    pub fn update_current_term(&mut self, new_term: TermId) {
        assert!(new_term >= self.term);
        self.term = new_term;
    }

    pub fn increment_term(&mut self) {
        self.term += 1;
    }
}
