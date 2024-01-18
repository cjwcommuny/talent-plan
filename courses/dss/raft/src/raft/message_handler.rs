use crate::raft::inner::{LocalTask, PeerEndPoint, RemoteTask};
use crate::raft::NodeId;
use derive_new::new;
use num::integer::div_ceil;
use std::fmt::{Debug, Formatter};
use tokio::sync::mpsc;

#[derive(new)]
pub struct MessageHandler {
    pub peers: Vec<Box<dyn PeerEndPoint + Send>>, // RPC end points of all peers
    pub remote_task_receiver: mpsc::Receiver<RemoteTask>,
    pub local_task_receiver: mpsc::Receiver<LocalTask>,
}

impl Debug for MessageHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageHandler").finish()
    }
}

impl MessageHandler {
    pub fn node_ids_except(&self, me: NodeId) -> impl Iterator<Item = NodeId> {
        (0..self.peers.len()).filter(move |node_id| *node_id != me)
    }

    pub fn majority_threshold(&self) -> usize {
        div_ceil(self.peers.len() + 1, 2)
    }
}
