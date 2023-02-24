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
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, instrument};

pub struct Config {
    pub heartbeat_failure_random_range: Range<u64>,
    pub election_timeout: u64,
}

impl Default for Config {
    fn default() -> Self {
        todo!()
    }
}

pub struct RaftInner {
    role: Option<Role>,
    handle: Handle,
}

impl RaftInner {
    pub fn new(role: Option<Role>, handle: Handle) -> Self {
        Self { role, handle }
    }

    fn append_entries(&mut self, _args: &AppendEntriesArgs) -> AppendEntriesReply {
        todo!()
    }

    pub async fn raft_main(&mut self) {
        loop {
            let role = self.role.take().unwrap();
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
    pub random_generator: Box<dyn RngCore>,
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
    pub fn new(
        node_id: usize,
        persister: Box<dyn Persister>,
        election: Election,
        logs: Logs,
        apply_ch: UnboundedSender<ApplyMsg>,
        peers: Vec<RaftClient>,
        remote_task_receiver: mpsc::Receiver<RemoteTask>,
        local_task_receiver: mpsc::Receiver<LocalTask>,
        random_generator: Box<dyn RngCore>,
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
    #[instrument(skip_all)]
    pub async fn apply_messages<I, M>(apply_ch: &mut UnboundedSender<ApplyMsg>, messages: I)
    where
        I: Iterator<Item = M>,
        M: Into<ApplyMsg>,
    {
        for entry in messages {
            let apply_msg = entry.into();
            debug!(?apply_msg);
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

impl RemoteTask {
    pub fn get_term(&self) -> TermId {
        match self {
            RemoteTask::RequestVote { args, sender: _ } => args.term,
            RemoteTask::AppendEntries { args, sender: _ } => args.term,
        }
    }

    pub async fn handle(self, role: Role, handle: &mut Handle) -> Role {
        match self {
            RemoteTask::RequestVote { args, sender } => {
                let (reply, new_role) = role.request_vote(handle, &args);
                sender.send(reply).unwrap();
                new_role
            }
            RemoteTask::AppendEntries { args, sender } => {
                let (reply, new_role) = role.append_entries(handle, args).await;
                sender.send(reply).unwrap();
                new_role
            }
        }
    }

    pub fn may_interrupt_election(self, current_term: TermId) -> Option<RemoteTask> {
        if self.get_term() > current_term {
            Some(self)
        } else {
            None
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
    current_term: TermId,
    pub voted_for: Option<NodeId>,
}

impl Election {
    pub fn get_current_term(&self) -> TermId {
        self.current_term
    }

    pub fn update_current_term(&mut self, current_term: TermId) {
        assert!(current_term >= self.current_term);
        self.current_term = current_term;
    }

    pub fn increment_term(&mut self) {
        self.current_term += 1;
    }
}
