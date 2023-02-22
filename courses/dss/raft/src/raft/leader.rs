use crate::proto::raftpb::{AppendEntriesArgs, AppendEntriesReply};
use crate::proto::raftpb::{LogEntryProst, LogStateProst};
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::AppendEntriesResult::{Commit, FoundLargerTerm, Retry};
use crate::raft::role::{Follower, Role};
use crate::raft::{receive_task, ApplyMsg, NodeId, TermId};
use futures::{stream::FuturesUnordered, StreamExt};
use std::future::Future;

use num::integer::div_ceil;
use std::time::Duration;
use tokio::select;
use tokio::time::interval;

/// inner structure for `ApplyMsg`
#[derive(Debug, Clone)]
pub enum LogKind {
    Command,
    Snapshot { term: TermId },
}

/// It's impossible that there exists multiple leaders with the same term.
#[derive(Debug)]
pub struct Leader {
    state: VolatileLeaderState,
}

#[derive(Debug)]
pub struct VolatileLeaderState {
    next_index: Vec<usize>, // for each server, index of the next log entry to send to that server
    /// different from the Raft paper, `match_length = match_index + 1`
    match_length: Vec<usize>,
}

impl VolatileLeaderState {
    pub fn new(log_length: usize, num_servers: usize) -> Self {
        VolatileLeaderState {
            next_index: vec![log_length; num_servers],
            match_length: vec![0; num_servers],
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
                    match self.on_receive_append_entries_reply(reply, current_term) {
                        AppendEntriesResult::Commit{ follower_id, match_length } => {
                            self.state.next_index[follower_id] = match_length;
                            self.state.match_length[follower_id] = match_length;
                            try_commit_logs(&self, handle).await
                        }
                        AppendEntriesResult::Retry(node_id) => {
                            // TODO: independent retry strategy
                            // if `self.state.next_index[node_id] == 0`, the follower is out of date
                            // we still need to retry
                            if self.state.next_index[node_id] > 0 {
                                self.state.next_index[node_id] -= 1;
                            }
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
                            self.state.match_length[handle.node_id] = index + 1;
                            sender.send(Some((index as u64, current_term))).unwrap();
                            rpc_replies.extend(handle.get_node_ids_except_mine().map(replicate_log(&self, handle)));
                        }
                        LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).unwrap(),
                        LocalTask::CheckLeader(sender) => sender.send(true).unwrap()
                    }
                }
                Some(task) = receive_task(&mut handle.remote_task_receiver, current_term) => {
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

    fn on_receive_append_entries_reply(
        &mut self,
        reply: AppendEntriesReply,
        current_term: TermId,
    ) -> AppendEntriesResult {
        let follower_id = reply.node_id as usize;
        if reply.term > current_term {
            FoundLargerTerm(reply.term)
        } else if reply.term == current_term && let Some(match_length) = reply.match_length {
            let match_length = match_length as usize;
            assert!(match_length > self.state.match_length[follower_id]);
            Commit {
                follower_id,
                match_length,
            }
        } else {
            Retry(follower_id)
        }
    }
}

enum AppendEntriesResult {
    FoundLargerTerm(TermId),
    Commit {
        follower_id: NodeId,
        match_length: usize,
    },

    /// There are two possibilities:
    /// 1. the previous `AppendEntries` didn't success, `reply.match_length == None`
    /// 2. `reply.term < leader.term`, which means the follower is out-of-date
    ///         (the reply is too late)
    Retry(NodeId),
}

async fn try_commit_logs(leader: &Leader, handle: &mut Handle) {
    let commit_threshold = div_ceil(handle.peers.len() + 1, 2);
    let compute_acks = |length_threshold| {
        leader
            .state
            .match_length
            .iter()
            .filter(|match_length| **match_length > length_threshold)
            .count()
    };
    // find the largest length which satisfies the commit threshold
    let ready = (0..handle.logs.get_log_len())
        .find(|match_length| compute_acks(*match_length) < commit_threshold)
        .unwrap_or(handle.logs.get_log_len())
        .checked_sub(1)
        .unwrap();
    let messages = handle.logs.commit_logs(ready).map(Clone::clone);
    Handle::apply_messages(&mut handle.apply_ch, messages).await
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
    handle.peers[node_id].append_entries(&args)
}

fn replicate_log<'a>(
    leader: &'a Leader,
    handle: &'a Handle,
) -> impl Fn(NodeId) -> ReplicateLogFuture + 'a {
    move |node_id| {
        let log_length = leader.state.next_index[node_id];
        let entries: Vec<LogEntryProst> = handle
            .logs
            .get_tail(log_length)
            .map(Clone::clone)
            .map(Into::into)
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
