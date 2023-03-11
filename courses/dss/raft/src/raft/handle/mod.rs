pub mod election;
pub mod logs;

pub use logs::Logs;

use crate::proto::raftpb::{decode, encode};
use crate::raft::handle::election::Election;
use crate::raft::inner::Config;
use crate::raft::leader::{LogEntry, LogKind};
use crate::raft::persister::Persister;
use crate::raft::{ApplyMsg, NodeId, TermId};
use derive_new::new;
use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;

use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

use tracing::instrument;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct Handle {
    pub node_id: NodeId,               // this peer's index into peers[]
    pub persister: Box<dyn Persister>, // Object to hold this peer's persisted state
    pub election: Election,
    pub logs: Logs,
    pub apply_ch: UnboundedSender<ApplyMsg>,
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

/// persistent relevant methods
/// We persist the states immediately after the state is mutated
/// An alternative method of it is to persist until making an RPC call
/// By batching the RPC call, we can save time of persisting
impl Handle {
    pub fn update_current_term(&mut self, new_term: TermId) {
        self.election.update_current_term(new_term);
        self.persist();
    }

    pub fn increment_term(&mut self) {
        self.election.increment_term();
        self.persist();
    }

    pub fn add_log(&mut self, log_kind: LogKind, data: Vec<u8>, term: TermId) -> usize {
        let result = self.logs.add_log(log_kind, data, term);
        self.persist();
        result
    }

    pub fn update_log_tail(&mut self, tail_begin: usize, entries: Vec<LogEntry>) {
        self.logs.update_log_tail(tail_begin, entries);
        self.persist();
    }

    pub fn restore(data: Vec<u8>) -> Option<PersistentState> {
        if data.is_empty() {
            None
        } else {
            Some(decode(&data))
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&self) {
        let state = self.get_persistent_state();
        let data = encode(&state);
        self.persister.save_raft_state(data)
    }

    fn get_persistent_state(&self) -> PersistentState {
        PersistentState::new(
            self.election.current_term(),
            self.election.voted_for.map(|id| id as u32),
            self.logs
                .tail(0)
                .map(Clone::clone)
                .map(Into::into)
                .collect(),
        )
    }
}

impl Handle {
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
}

#[derive(new, Serialize, Deserialize, Debug)]
pub struct PersistentState {
    pub term: u64,
    pub voted_for: Option<u32>,
    pub log: Vec<LogEntry>,
}
