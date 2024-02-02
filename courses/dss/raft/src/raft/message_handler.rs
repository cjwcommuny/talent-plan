use crate::raft::inner::{LocalTask, PeerEndPoint, RemoteTask};
use crate::raft::NodeId;
use derive_new::new;
use num::integer::div_ceil;
use std::fmt::{Debug, Formatter};

use tokio_stream::wrappers::ReceiverStream;

#[derive(new)]
pub struct MessageHandler {
    pub peers: Peers, // RPC end points of all peers
    pub remote_tasks: ReceiverStream<RemoteTask>,
    pub local_tasks: ReceiverStream<LocalTask>,
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
}

#[derive(new)]
pub struct Peers {
    pub inner: Vec<Box<dyn PeerEndPoint + Send>>,
}

impl From<Vec<Box<dyn PeerEndPoint + Send>>> for Peers {
    fn from(value: Vec<Box<dyn PeerEndPoint + Send>>) -> Self {
        Self::new(value)
    }
}

impl Peers {
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn node_ids_except(&self, me: NodeId) -> impl Iterator<Item = NodeId> {
        (0..self.inner.len()).filter(move |node_id| *node_id != me)
    }

    pub fn majority_threshold(&self) -> usize {
        div_ceil(self.inner.len() + 1, 2)
    }
}
