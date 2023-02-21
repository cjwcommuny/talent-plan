use crate::proto::raftpb::{AppendEntriesArgs, AppendEntriesReply, RaftClient};
use crate::proto::raftpb::{LogEntryProst, LogStateProst};
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::AppendEntriesResult::FoundLargerTerm;
use crate::raft::role::{Follower, Role};
use crate::raft::{receive_task, ApplyMsg, NodeId, TermId};
use futures::{stream::FuturesUnordered, FutureExt, SinkExt, StreamExt};
use std::future::Future;
use std::result;
use std::time::Duration;
use tokio::select;
use tokio::time::interval;

/// inner structure for `ApplyMsg`
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

    pub(crate) async fn transit(mut self, handle: &mut Handle) -> Role {
        let mut rpc_replies: FuturesUnordered<_> = handle
            .get_node_ids_except_mine()
            .map(replicate_log(&self, handle))
            .collect();
        let mut heartbeat_timer = interval(Duration::from_millis(100));
        // TODO: move to config
        loop {
            let current_term = handle.election.get_current_term();
            select! {
                _ = heartbeat_timer.tick() => {
                    rpc_replies.extend(handle.get_node_ids_except_mine().map(check_heartbeat(&self, handle)))
                }
                Some(result) = rpc_replies.next() => {
                    let reply: AppendEntriesReply = result.unwrap(); // TODO: add logging
                    match on_receive_append_entries_reply(reply, current_term) {
                        AppendEntriesResult::Commit => {
                            commit_log_entries()
                        }
                        AppendEntriesResult::Retry(node_id) => {
                            self.state.next_index[node_id] -= 1; // TODO: independent retry strategy
                            let future = replicate_log(&self, handle)(node_id);
                            rpc_replies.push(future);
                        }
                        AppendEntriesResult::FoundLargerTerm(new_term) => {
                            handle.election.update_current_term(new_term);
                            return Role::Follower(Follower::default());
                        }
                    }
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    match task {
                        LocalTask::AppendEntries { data, sender } => {
                            let current_term = handle.election.get_current_term();
                            let index = handle.logs.add_log(LogKind::Command, data, current_term);
                            self.state.match_index[handle.node_id] = index + 1;
                            sender.send(Some((index as u64, current_term))).unwrap();
                            rpc_replies.extend(handle.get_node_ids_except_mine().map(replicate_log(&self, handle)));
                        }
                        LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).unwrap(),
                        LocalTask::IsLeader(sender) => sender.send(true).unwrap()
                    }
                }
                Some(task) = receive_task(&mut handle.remote_task_receiver, current_term) => {
                    handle.election.update_current_term(task.get_term());
                    handle.election.voted_for = None;
                    let new_role = task.handle(Role::Leader(self), handle).await;
                    if let Role::Leader(new_role) = new_role {
                        self = new_role;
                    } else {
                        return new_role;
                    }
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

fn on_receive_append_entries_reply(
    reply: AppendEntriesReply,
    current_term: TermId,
) -> AppendEntriesResult {
    if reply.term > current_term {
        FoundLargerTerm(reply.term)
    } else if reply.term == current_term {
        todo!()
    } else {
        panic!("corrupted data");
    }
}

type ReplicateLogFuture = impl Future<Output = Result<AppendEntriesReply, labrpc::Error>>;

fn send_append_entries(
    leader: &Leader,
    handle: &Handle,
    node_id: NodeId,
    entries: Vec<LogEntryProst>,
) -> ReplicateLogFuture {
    let log_length = leader.state.next_index[node_id];
    let log_state = handle.logs.get_log_state_front(log_length).map(Into::into);
    let args = AppendEntriesArgs {
        term: handle.election.get_current_term(),
        leader_id: handle.node_id as u64,
        log_state,
        entries,
        leader_commit_length: handle.logs.get_commit_length() as u64,
    };
    let peer = handle.peers[node_id].clone();
    peer.append_entries(&args)
}

fn replicate_log<'a>(
    leader: &'a Leader,
    handle: &'a Handle,
) -> impl Fn(NodeId) -> ReplicateLogFuture + 'a {
    move |node_id| {
        let log_length = leader.state.next_index[node_id];
        let entries: Vec<LogEntryProst> = handle.logs.get_entries()[log_length..]
            .iter()
            .map(|entry| entry.clone().into())
            .collect();
        send_append_entries(leader, handle, node_id, entries)
    }
}

fn check_heartbeat<'a>(
    leader: &'a Leader,
    handle: &'a Handle,
) -> impl Fn(NodeId) -> ReplicateLogFuture + 'a {
    move |node_id| send_append_entries(leader, handle, node_id, Vec::new())
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    log_kind: LogKind,
    data: Vec<u8>,
    pub(crate) log_state: LogState,
}

impl LogEntry {
    pub fn new(log_kind: LogKind, data: Vec<u8>, index: usize, term: TermId) -> Self {
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
pub struct LogState {
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
