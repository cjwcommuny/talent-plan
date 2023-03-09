use crate::proto::raftpb::raft::Client as RaftClient;
use crate::raft::election::Election;
use crate::raft::inner::{Config, LocalTask, PersistentState, RemoteTask};
use crate::raft::logs::Logs;
use crate::raft::persister::Persister;
use crate::raft::{ApplyMsg, NodeId};
use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use num::integer::div_ceil;
use rand::RngCore;
use std::fmt::{Debug, Formatter};
use tokio::sync::mpsc;
use tracing::instrument;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
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
    pub fn restore(data: Vec<u8>) -> Option<PersistentState> {
        if data.is_empty() {
            None
        } else {
            Some(labcodec::decode(&data).expect("decoding persistent state error"))
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&self) {
        let state = self.get_persistent_state();
        let mut buffer = Vec::new();
        labcodec::encode(&state, &mut buffer).expect("decoding persistent state error");
        self.persister.save_raft_state(buffer)
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

    pub fn node_ids_except_mine(&self) -> impl Iterator<Item = NodeId> {
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

    pub fn majority_threshold(&self) -> usize {
        div_ceil(self.peers.len() + 1, 2)
    }
}
