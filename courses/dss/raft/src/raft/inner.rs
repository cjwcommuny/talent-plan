use crate::proto::raftpb::raft::Client as RaftClient;
use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use crate::raft::leader::{LogEntry, LogKind, LogState};
use crate::raft::persister::Persister;
use crate::raft::role::Role;
use crate::raft::{ApplyMsg, NodeId, TermId};
use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use std::cmp::{max, min};

use crate::raft::logs::Logs;
use tokio::sync::{mpsc, oneshot};

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
            let new_role = role.transit(&mut self.handle).await;
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
        }
    }

    pub fn get_node_ids_except_mine(&self) -> impl Iterator<Item = NodeId> {
        let me = self.node_id;
        (0..self.peers.len()).filter(move |node_id| *node_id != me)
    }

    /// In the Raft paper, there is `lastApplied` field.
    /// But here, we don't don't make the execution of this function a transaction.
    pub async fn apply_messages<I, M>(apply_ch: &mut UnboundedSender<ApplyMsg>, messages: I)
    where
        I: Iterator<Item = M>,
        M: Into<ApplyMsg>,
    {
        for entry in messages {
            apply_ch.send(entry.into()).await.unwrap();
        }
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
}

pub enum LocalTask {
    AppendEntries {
        data: Vec<u8>,
        sender: oneshot::Sender<Option<(u64, u64)>>, // None if not leader
    },
    GetTerm(oneshot::Sender<TermId>),
    CheckLeader(oneshot::Sender<bool>),
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
        assert!(current_term > self.current_term);
        self.current_term = current_term;
    }

    pub fn increment_term(&mut self) {
        self.current_term += 1;
    }
}
