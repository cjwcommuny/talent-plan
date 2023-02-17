use std::future::Future;
use std::result;
use std::time::Duration;

use crate::proto::raftpb::LogStateMessage;
use crate::proto::raftpb::{AppendEntriesArgs, AppendEntriesReply};
use crate::raft::errors::{Error, Result};
use crate::raft::role::{Follower, Role};
use crate::raft::{add_message_to_queue, receive_task, ApplyMsg, Handle, NodeId, TermId};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::select;
use tokio::time::interval;

#[derive(Debug)]
pub struct Leader {
    state: VolatileLeaderState,
}

#[derive(Debug)]
pub struct VolatileLeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

impl VolatileLeaderState {
    pub fn new(log_length: usize, num_servers: usize) -> Self {
        VolatileLeaderState {
            next_index: vec![log_length; num_servers],
            match_index: vec![0; num_servers],
        }
    }
}

impl Leader {
    pub fn new(state: VolatileLeaderState) -> Self {
        Leader { state }
    }

    pub(crate) async fn add_entry<M>(&self, handle: &Handle, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut buffer = Vec::new();
        command.encode(&mut buffer).map_err(|e| Error::Encode(e))?;
        add_message_to_queue(&handle.entry_task_sender, |sender| (buffer, sender))
            .await
            .map_err(Error::Rpc)
    }

    pub(crate) async fn transit(mut self, handle: &mut Handle) -> Role {
        let mut rpc_replies: FuturesUnordered<_> =
            replicate_logs_followers(&self, handle).collect();
        let mut heartbeat_timer = interval(Duration::from_millis(100));
        // TODO: move to config
        loop {
            let current_term = handle.persistent_state.current_term;
            select! {
                _ = heartbeat_timer.tick() => {
                    for future in replicate_logs_followers(&self, handle) {
                        rpc_replies.push(future);
                    }
                }
                Some(result) = rpc_replies.next() => {
                    let reply: AppendEntriesReply = result.unwrap(); // TODO: add logging
                    match on_receive_append_entries_reply(reply) {
                        AppendEntriesResult::Commit => {
                            commit_log_entries()
                        }
                        AppendEntriesResult::Retry(node_id) => {
                            self.state.next_index[node_id] -= 1; // TODO: independent retry strategy
                            let future = replicate_log(&self, handle, node_id);
                            rpc_replies.push(future);
                        }
                        AppendEntriesResult::FoundLargerTerm(new_term) => {
                            handle.persistent_state.current_term = new_term;
                            return Role::Follower(Follower::default());
                        }
                    }
                }
                Some((data, sender)) = handle.entry_task_receiver.recv() => {
                    let index = handle.persistent_state.log.len() as u64;
                    let current_term = handle.persistent_state.current_term;
                    sender.send((index, current_term)).unwrap();
                    handle.persistent_state.log.push(Log::new(ApplyMsg::Command { data, index }, current_term));
                    for future in replicate_logs_followers(&self, handle) {
                        rpc_replies.push(future);
                    }
                }
                Some(task) = receive_task(&mut handle.task_receiver, current_term) => {
                    handle.persistent_state.current_term = task.get_term();
                    handle.persistent_state.voted_for = None;
                    let new_role = task.handle(Role::Leader(self), handle);
                    if let Role::Leader(new_role) = new_role {
                        self = new_role;
                    } else {
                        return new_role;
                    }
                }
                else => { // no more replies
                    todo!()
                }
            }
        }
    }
}

enum AppendEntriesResult {
    FoundLargerTerm(TermId),
    Commit,
    Retry(NodeId),
}

fn on_receive_append_entries_reply(_reply: AppendEntriesReply) -> AppendEntriesResult {
    todo!()
}

type ReplicateLogFuture = impl Future<Output = result::Result<AppendEntriesReply, labrpc::Error>>;

fn replicate_log(leader: &Leader, handle: &Handle, node_id: NodeId) -> ReplicateLogFuture {
    let prev_log_index = leader.state.next_index[node_id];
    let prev_log_term = if prev_log_index > 0 {
        handle.persistent_state.log[prev_log_index - 1].term
    } else {
        0
    };
    let args = AppendEntriesArgs {
        term: handle.persistent_state.current_term,
        leader_id: handle.node_id as u64,
        prev_log_index: prev_log_index as u32,
        prev_log_term,
    };
    let peer = handle.peers[node_id].clone();
    peer.append_entries(&args)
}

pub fn replicate_logs_followers<'a>(
    leader: &'a Leader,
    handle: &'a Handle,
) -> impl Iterator<Item = ReplicateLogFuture> + 'a {
    (0..handle.peers.len())
        .filter(move |node_id| *node_id != handle.node_id)
        .map(move |node_id| replicate_log(leader, handle, node_id))
}

#[derive(Debug)]
pub struct Log<T> {
    content: T,
    pub term: TermId,
}

impl<T> Log<T> {
    fn new(content: T, term: TermId) -> Self {
        Log { content, term }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct LogState {
    term: TermId,
    index: usize,
}

impl LogState {
    pub fn new(term: TermId, index: usize) -> Self {
        LogState { term, index }
    }
}

impl From<&LogStateMessage> for LogState {
    fn from(x: &LogStateMessage) -> Self {
        LogState::new(x.last_log_term, x.last_log_index as usize)
    }
}

fn commit_log_entries() {
    todo!()
}
