use std::cmp::{max, min};
use std::future::Future;
use std::result;
use std::time::Duration;

use crate::proto::raftpb::{AppendEntriesArgs, AppendEntriesReply};
use crate::proto::raftpb::{LogEntryProst, LogStateProst};
use crate::raft::errors::{Error, Result};
use crate::raft::role::{Follower, Role};
use crate::raft::{add_message_to_queue, receive_task, ApplyMsg, Handle, NodeId, TermId};
use futures::{stream::FuturesUnordered, FutureExt, SinkExt, StreamExt};
use tokio::select;
use tokio::time::interval;

#[derive(Debug, Clone)]
pub enum LogKind {
    Command,
    Snapshot { term: TermId },
}

#[derive(Debug)]
pub struct Leader {
    state: VolatileLeaderState,
}

#[derive(Debug)]
pub struct VolatileLeaderState {
    next_index: Vec<usize>, // for each server, index of the next log entry to send to that server
    match_index: Vec<usize>, // for each server, index of highest log entry known to be replicated on server
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
                    let index = handle.persistent_state.log.len();
                    let current_term = handle.persistent_state.current_term;
                    sender.send((index as u64, current_term)).unwrap();
                    handle.persistent_state.log.push(LogEntry::new(LogKind::Command, data, index, current_term));
                    for future in replicate_logs_followers(&self, handle) {
                        rpc_replies.push(future);
                    }
                }
                Some(task) = receive_task(&mut handle.task_receiver, current_term) => {
                    handle.persistent_state.current_term = task.get_term();
                    handle.persistent_state.voted_for = None;
                    let new_role = task.handle(Role::Leader(self), handle).await;
                    if let Role::Leader(new_role) = new_role {
                        self = new_role;
                    } else {
                        return new_role;
                    }
                }
                else => {} // no more replies
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
    let log_length = leader.state.next_index[node_id];
    let log_state = if log_length > 0 {
        let last_log_index = log_length - 1;
        Some(
            LogState::new(
                handle.persistent_state.log[last_log_index].log_state.term,
                last_log_index,
            )
            .into(),
        )
    } else {
        None
    };
    let entries: Vec<LogEntryProst> = handle.persistent_state.log[log_length..]
        .iter()
        .map(|entry| entry.clone().into())
        .collect();
    let args = AppendEntriesArgs {
        term: handle.persistent_state.current_term,
        leader_id: handle.node_id as u64,
        log_state,
        entries,
        leader_commit_index: handle.volatile_state.commit_index as u64,
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

#[derive(Debug, Clone)]
pub struct LogEntry {
    log_kind: LogKind,
    data: Vec<u8>,
    pub(crate) log_state: LogState,
}

impl LogEntry {
    fn new(log_kind: LogKind, data: Vec<u8>, index: usize, term: TermId) -> Self {
        LogEntry {
            log_kind,
            data,
            log_state: LogState::new(term, index),
        }
    }
}

impl Into<ApplyMsg> for LogEntry {
    fn into(self) -> ApplyMsg {
        let LogEntry {
            log_kind,
            data,
            log_state,
        } = self;
        match log_kind {
            LogKind::Command => ApplyMsg::Command {
                data,
                index: log_state.index as u64,
            },
            LogKind::Snapshot { term } => ApplyMsg::Snapshot {
                data,
                term,
                index: log_state.index as u64,
            },
        }
    }
}

impl From<LogEntryProst> for LogEntry {
    fn from(value: LogEntryProst) -> Self {
        let log_kind = if value.is_command {
            LogKind::Command
        } else {
            LogKind::Snapshot { term: value.term }
        };
        LogEntry::new(
            log_kind,
            value.data,
            value.last_log_index as usize,
            value.last_log_term,
        )
    }
}

impl Into<LogEntryProst> for LogEntry {
    fn into(self) -> LogEntryProst {
        let LogEntry {
            log_kind: apply_msg,
            data,
            log_state,
        } = self;
        match apply_msg {
            LogKind::Command => LogEntryProst {
                is_command: true,
                term: 0,
                data,
                last_log_index: log_state.index as u32,
                last_log_term: log_state.term,
            },
            LogKind::Snapshot { term } => LogEntryProst {
                is_command: false,
                term,
                data,
                last_log_index: log_state.index as u32,
                last_log_term: log_state.term,
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Copy, Clone)]
pub(crate) struct LogState {
    pub term: TermId,
    pub index: usize,
}

impl LogState {
    pub fn new(term: TermId, index: usize) -> Self {
        LogState { term, index }
    }
}

impl From<LogStateProst> for LogState {
    fn from(value: LogStateProst) -> Self {
        LogState::new(value.last_log_term, value.last_log_index as usize)
    }
}

impl Into<LogStateProst> for LogState {
    fn into(self) -> LogStateProst {
        LogStateProst {
            last_log_term: self.term,
            last_log_index: self.index as u32,
        }
    }
}

fn commit_log_entries() {
    todo!()
}

pub async fn append_entries_in_powerpoint(
    handle: &mut Handle,
    log_begin: usize,
    mut entries: Vec<LogEntry>,
    leader_commit: usize,
) {
    let log = &mut handle.persistent_state.log;
    let commit_index = &mut handle.volatile_state.commit_index;

    // update log
    let mutation_offset = (0..min(entries.len(), log.len() - log_begin))
        .find(|offset| entries[*offset].log_state.term != log[log_begin + *offset].log_state.term)
        .unwrap_or(entries.len());
    log.splice(
        log_begin + mutation_offset..,
        entries.drain(mutation_offset..),
    );

    // commit
    *commit_index = max(*commit_index, leader_commit);
    for entry in &log[*commit_index..leader_commit] {
        handle.apply_ch.send(entry.clone().into()).await.unwrap(); // TODO: handle error
    }
}
