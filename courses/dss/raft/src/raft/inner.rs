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
    pub async fn apply_messages<I, M>(apply_ch: &mut UnboundedSender<ApplyMsg>, messages: I) where I: Iterator<Item = M>, M: Into<ApplyMsg> {
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
    IsLeader(oneshot::Sender<bool>),
}


/// ```plot
/// log = |--- committed --- | --- uncommitted --- |
///       0            commit_length            log.len()
/// ```
///
#[derive(Default, Debug)]
pub struct Logs {
    log: Vec<LogEntry>,
    commit_length: usize,
}

impl Logs {
    pub fn get_log_len(&self) -> usize {
        self.log.len()
    }

    pub fn get_commit_length(&self) -> usize {
        self.commit_length
    }

    pub fn add_log(&mut self, log_kind: LogKind, data: Vec<u8>, term: TermId) -> usize {
        let index = self.log.len();
        self.log.push(LogEntry::new(log_kind, data, index, term));
        index
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.log.get(index)
    }

    pub fn get_tail(&self, tail_begin: usize) -> impl Iterator<Item = &LogEntry> {
        self.log[tail_begin..].iter()
    }

    pub fn get_log_state(&self) -> Option<LogState> {
        self.log.last().map(|entry| entry.log_state)
    }

    /// return `LogState` of `self.log[..length]`
    pub fn get_log_state_front(&self, length: usize) -> Option<LogState> {
        if length > 0 {
            Some(self.log[length - 1].log_state)
        } else {
            None
        }
    }

    pub fn update_log_tail(&mut self, tail_begin: usize, mut entries: Vec<LogEntry>) {
        assert!(tail_begin >= self.commit_length); // must not modify committed logs
        let limit = min(entries.len(), self.log.len() - tail_begin);
        let mutation_offset = (0..limit)
            .find(|offset| {
                entries[*offset].log_state.term != self.log[tail_begin + *offset].log_state.term
            })
            .unwrap_or(limit);
        self.log.splice(
            tail_begin + mutation_offset..,
            entries.drain(mutation_offset..),
        );
    }

    /// returns the logs just committed
    pub fn commit_logs(&mut self, new_commit_length: usize) -> impl Iterator<Item = &LogEntry> {
        assert!(new_commit_length <= self.log.len());
        let just_committed = self.log[self.commit_length..new_commit_length].iter();
        // `self.commit_length` can be incremented only
        self.commit_length = max(self.commit_length, new_commit_length);
        just_committed
    }
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
