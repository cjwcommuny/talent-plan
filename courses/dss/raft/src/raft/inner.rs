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
        peers: Vec<RaftClient>,
        remote_task_receiver: mpsc::Receiver<RemoteTask>,
        local_task_receiver: mpsc::Receiver<LocalTask>,
    ) -> Self {
        Self {
            node_id,
            persister,
            election,
            logs,
            peers,
            remote_task_receiver,
            local_task_receiver,
        }
    }

    pub fn get_node_ids_except_mine(&self) -> impl Iterator<Item = NodeId> {
        let me = self.node_id;
        (0..self.peers.len()).filter(move |node_id| *node_id != me)
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

pub struct Logs {
    log: Vec<LogEntry>,
    apply_ch: UnboundedSender<ApplyMsg>,
    commit_length: usize,
    last_applied: Option<usize>,
}

impl Logs {
    pub fn new(apply_ch: UnboundedSender<ApplyMsg>) -> Self {
        Logs {
            log: Vec::new(),
            apply_ch,
            commit_length: 0,
            last_applied: None,
        }
    }

    pub fn get_commit_length(&self) -> usize {
        self.commit_length
    }

    pub fn add_log(&mut self, log_kind: LogKind, data: Vec<u8>, term: TermId) -> usize {
        let index = self.log.len();
        self.log.push(LogEntry::new(log_kind, data, index, term));
        index
    }

    pub fn get_entries(&self) -> &[LogEntry] {
        &self.log
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

    pub fn update_log(&mut self, log_begin: usize, mut entries: Vec<LogEntry>) {
        let mutation_offset = (0..min(entries.len(), self.log.len() - log_begin))
            .find(|offset| {
                entries[*offset].log_state.term != self.log[log_begin + *offset].log_state.term
            })
            .unwrap_or(entries.len());
        self.log.splice(
            log_begin + mutation_offset..,
            entries.drain(mutation_offset..),
        );
    }

    pub async fn commit_log(&mut self, leader_commit_length: usize) {
        for entry in &self.log[self.commit_length..leader_commit_length] {
            self.apply_ch.send(entry.clone().into()).await.unwrap(); // TODO: handle error
        }
        self.commit_length = max(self.commit_length, leader_commit_length); // FIXME
    }
}

#[derive(Default)]
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
