use crate::proto::raftpb::{AppendEntriesArgs, AppendEntriesReply};
use crate::proto::raftpb::{LogEntryProst, LogStateProst};
use crate::raft::inner::LocalTask;
use crate::raft::inner::RemoteTaskResult;
use crate::raft::leader::AppendEntriesResult::{Commit, FoundLargerTerm, Retry};
use crate::raft::role::Role;
use crate::raft::{ApplyMsg, NodeId, TermId};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::future::Future;

use derive_new::new;
use std::time::Duration;

use crate::raft;
use crate::raft::candidate::Candidate;
use crate::raft::follower::Follower;
use crate::raft::handle::Handle;
use tokio::select;
use tokio::time::interval;
use tracing::{info, instrument, trace, trace_span, warn};

/// inner structure for `ApplyMsg`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogKind {
    Command,
    Snapshot,
}

/// It's impossible that there exists multiple leaders with the same term.
#[derive(Debug)]
pub struct Leader {
    next_index: Vec<usize>, // for each server, index of the next log entry to send to that server
    /// different from the Raft paper, `match_length = match_index + 1`
    match_length: Vec<usize>,
}

impl Leader {
    pub fn from(_: Candidate, log_length: usize, num_servers: usize) -> Self {
        Leader {
            next_index: vec![log_length; num_servers],
            match_length: vec![0; num_servers],
        }
    }

    pub(crate) async fn progress(mut self, handle: &mut Handle) -> Role {
        let _span = trace_span!("Leader", node_id = handle.node_id).entered();
        let mut rpc_replies: FuturesUnordered<_> = handle
            .node_ids_except_mine()
            .map(replicate_log(&self, handle))
            .collect();
        let mut heartbeat_timer = interval(Duration::from_millis(handle.config.heartbeat_cycle));
        let new_role: Role = loop {
            select! {
                _ = heartbeat_timer.tick() => {
                    trace!("term={}, send heartbeat", handle.election.current_term());
                    rpc_replies
                        .extend(handle.node_ids_except_mine().map(replicate_log(&self, handle)))
                }
                Some(result) = rpc_replies.next() => {
                    match result {
                        Ok(reply) => {
                            match Self::on_receive_append_entries_reply(reply, handle.election.current_term()) {
                                AppendEntriesResult::Commit{ follower_id, match_length } => {
                                    self.next_index[follower_id] = match_length;
                                    self.match_length[follower_id] = match_length;
                                    try_commit_logs(&self, handle).await
                                }
                                AppendEntriesResult::Retry(node_id) => {
                                    // TODO: independent retry strategy
                                    // if `self.state.next_index[node_id] == 0`, the follower is out of date
                                    // we still need to retry
                                    self.next_index[node_id] = self.next_index[node_id].saturating_sub(1);
                                    rpc_replies.push(replicate_log(&self, handle)(node_id));
                                }
                                AppendEntriesResult::FoundLargerTerm(new_term) => {
                                    handle.election.update_current_term(new_term);
                                    break Role::Follower(Follower::default())
                                }
                            }
                        }
                        Err(e) => warn!(rpc_error = e.to_string()),
                    }
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    match task {
                        LocalTask::AppendEntries { data, sender } => {
                            trace!("term={}, local task append entries", handle.election.current_term());
                            let current_term = handle.election.current_term();
                            let index = handle.logs.add_log(LogKind::Command, data, current_term);
                            self.match_length[handle.node_id] = index + 1;
                            sender.send(Some((index as u64, current_term))).unwrap();
                            rpc_replies.extend(handle.node_ids_except_mine().map(replicate_log(&self, handle)));
                        }
                        LocalTask::GetTerm(sender) => sender.send(handle.election.current_term()).unwrap(),
                        LocalTask::CheckLeader(sender) => sender.send(true).unwrap(),
                        LocalTask::Shutdown(sender) => {
                            info!("term={}, shutdown", handle.election.current_term());
                            sender.send(()).unwrap();
                            break Role::Stop;
                        }
                    }
                }
                Some(task) = handle.remote_task_receiver.recv() => {
                    trace!("handle remote task");
                    let RemoteTaskResult { success: _, new_role } = task.handle(Role::Leader(self), handle).await;
                    if let Role::Leader(new_role) = new_role {
                        self = new_role;
                    } else {
                        break new_role;
                    }
                }
            }
        };
        new_role
    }

    #[instrument(ret, level = "trace")]
    fn on_receive_append_entries_reply(
        reply: AppendEntriesReply,
        current_term: TermId,
    ) -> AppendEntriesResult {
        let follower_id = reply.node_id as usize;
        if reply.term > current_term {
            FoundLargerTerm(reply.term)
        } else if reply.term == current_term && let Some(match_length) = reply.match_length {
            let match_length = match_length as usize;
            Commit {
                follower_id,
                match_length,
            }
        } else {
            Retry(follower_id)
        }
    }
}

#[derive(Debug)]
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

#[instrument(level = "debug")]
async fn try_commit_logs(leader: &Leader, handle: &mut Handle) {
    let commit_threshold = handle.majority_threshold();
    let compute_acks = |length_threshold| {
        let acks = leader
            .match_length
            .iter()
            .filter(|match_length| **match_length > length_threshold)
            .count();
        acks
    };
    // find the largest length which satisfies the commit threshold
    let new_commit_length = (handle.logs.commit_len()..handle.logs.len())
        .find(|match_length| compute_acks(*match_length) < commit_threshold)
        .unwrap_or(handle.logs.len());
    let messages = handle.logs.commit_logs(new_commit_length);
    Handle::apply_messages(&mut handle.apply_ch, messages).await
}

type ReplicateLogFuture = impl Future<Output = raft::Result<AppendEntriesReply>>;

#[instrument(skip(leader, handle), level = "trace")]
fn send_append_entries(
    node_id: NodeId,
    leader: &Leader,
    handle: &Handle,
    entries: Vec<LogEntryProst>,
) -> ReplicateLogFuture {
    trace!("term={}, {:?}", handle.election.current_term(), handle.logs);
    let log_length = leader.next_index[node_id];
    let log_state = handle.logs.log_state_before(log_length).map(Into::into);
    let args = AppendEntriesArgs {
        term: handle.election.current_term(),
        leader_id: handle.node_id as u64,
        log_state,
        entries,
        leader_commit_length: handle.logs.commit_len() as u64,
    };
    handle.peers[node_id]
        .append_entries(&args)
        .map(move |result| result.map_err(|e| raft::Error::Rpc(e, node_id)))
}

fn replicate_log<'a>(
    leader: &'a Leader,
    handle: &'a Handle,
) -> impl Fn(NodeId) -> ReplicateLogFuture + 'a {
    move |node_id| {
        let log_length = leader.next_index[node_id];
        let entries: Vec<LogEntryProst> = handle
            .logs
            .tail(log_length)
            .map(Clone::clone)
            .map(Into::into)
            .collect();
        send_append_entries(node_id, leader, handle, entries)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, new)]
pub struct LogEntry {
    log_kind: LogKind,
    data: Vec<u8>,
    pub term: TermId,
}

impl From<(usize, LogEntry)> for ApplyMsg {
    fn from(value: (usize, LogEntry)) -> Self {
        let (index, entry) = value;
        let LogEntry {
            log_kind,
            data,
            term,
        } = entry;

        // The test code uses indices starting from 1,
        // while the implementation uses indices starting from 0.
        let one_based_index = index as u64 + 1;
        match log_kind {
            LogKind::Command => ApplyMsg::Command {
                data,
                index: one_based_index,
            },
            LogKind::Snapshot => ApplyMsg::Snapshot {
                data,
                term,
                index: one_based_index,
            },
        }
    }
}

impl From<LogEntryProst> for LogEntry {
    fn from(value: LogEntryProst) -> Self {
        let log_kind = if value.is_command {
            LogKind::Command
        } else {
            LogKind::Snapshot
        };
        LogEntry::new(log_kind, value.data, value.term)
    }
}

impl From<LogEntry> for LogEntryProst {
    fn from(value: LogEntry) -> Self {
        let LogEntry {
            log_kind,
            data,
            term,
        } = value;
        match log_kind {
            LogKind::Command => LogEntryProst {
                is_command: true,
                data,
                term,
            },
            LogKind::Snapshot => LogEntryProst {
                is_command: false,
                data,
                term,
            },
        }
    }
}

/// NOTE: `term` should come first, since we implement `PartialOrd` and `Ord` here.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Copy, Clone, new)]
pub struct LogState {
    pub term: TermId,
    pub index: usize,
}

impl From<LogStateProst> for LogState {
    fn from(value: LogStateProst) -> Self {
        LogState::new(value.term, value.index as usize)
    }
}

impl From<LogState> for LogStateProst {
    fn from(value: LogState) -> Self {
        LogStateProst {
            term: value.term,
            index: value.index as u32,
        }
    }
}
