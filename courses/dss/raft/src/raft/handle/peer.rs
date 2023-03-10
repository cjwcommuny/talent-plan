use crate::raft::inner::PeerEndPoint;
use crate::raft::leader::{MatchLength, NextIndex};
use crate::raft::NodeId;
use num::integer::div_ceil;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter, Pointer};
use std::iter::FromIterator;

pub struct Peers<T> {
    me: NodeId,
    peers: HashMap<NodeId, T>,
}

impl<T> Peers<T> {
    pub fn me(&self) -> NodeId {
        self.me
    }

    pub fn new<I>(me: NodeId, iter: I) -> Self
    where
        I: IntoIterator<Item = (NodeId, T)>,
    {
        Self {
            me,
            peers: HashMap::from_iter(iter),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeId, &T)> {
        self.peers.iter()
    }

    pub fn iter_except_me(&self) -> impl Iterator<Item = (&NodeId, &T)> {
        self.iter().filter(move |(node_id, _)| **node_id != self.me)
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&T> {
        self.peers.get(node_id)
    }

    pub fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut T> {
        self.peers.get_mut(node_id)
    }

    pub fn majority_threshold(&self) -> usize {
        div_ceil(self.peers.len() + 1, 2)
    }
}

impl Debug for Peers<(Box<dyn PeerEndPoint>, NextIndex, MatchLength)> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peers")
            .field(
                "[NodeId: (NextIndex, MatchIndex)]",
                &HashMap::from_iter(self.iter().map(|(node_id, (_, next_index, match_index))| {
                    (node_id, (next_index, match_index))
                })),
            )
            .finish()
    }
}

impl Debug for Peers<Box<dyn PeerEndPoint>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peers").finish()
    }
}

impl<T> IntoIterator for Peers<T> {
    type Item = <HashMap<NodeId, T> as IntoIterator>::Item;
    type IntoIter = <HashMap<NodeId, T> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.peers.into_iter()
    }
}
