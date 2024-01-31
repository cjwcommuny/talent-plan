use crate::raft::inner::{AppendEntriesFuture, LocalTask, PeerEndPoint};
use crate::raft::inner::RemoteTaskResult;
use crate::raft::leader::AppendEntriesResult::{Commit, Retry, UpdateTermAndTransitToFollower};
use crate::raft::role::Role;
use crate::raft::{ApplyMsg, Node, NodeId, TermId};
use futures::{stream::FuturesUnordered, StreamExt};
use std::cmp::Ordering::{Equal, Greater, Less};
use std::future::Future;

use derive_new::new;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::raft::candidate::Candidate;
use crate::raft::common::{with_context, FutureOutput, FutureWithContext};
use crate::raft::follower::Follower;
use crate::raft::handle::election::Election;
use crate::raft::handle::{Handle, Logs};

use crate::raft::message_handler::MessageHandler;
use crate::raft::rpc::AppendEntriesReplyResult::{
    LogNotContainThisEntry, LogNotMatch, Success, TermCheckFail,
};
use crate::raft::rpc::{AppendEntriesArgs, AppendEntriesReply};
use tokio::select;
use tokio::time::interval;
use tracing::{info, instrument, trace, trace_span, warn};

/// inner structure for `ApplyMsg`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogKind {
    Command,
    Snapshot,
}

pub type NextIndex = usize;
pub type MatchLength = usize;

/// It's impossible that there exists multiple leaders with the same term.
#[derive(Debug)]
pub struct Leader {
    /// for each server, index of the next log entry to send to that server
    next_index: Vec<NextIndex>,
    /// different from the Raft paper, `match_length = match_index + 1`
    match_length: Vec<MatchLength>,
}

impl Leader {
    pub fn from(_: Candidate, log_length: usize, num_servers: usize) -> Self {
        Leader {
            next_index: vec![log_length; num_servers],
            match_length: vec![0; num_servers],
        }
    }

    pub(crate) async fn progress(
        mut self,
        handle: &mut Handle,
        message_handler: &mut MessageHandler,
    ) -> Role {
        let _span = trace_span!("Leader", node_id = handle.node_id).entered();
        let me = handle.node_id;
        let mut heartbeat_timer = interval(Duration::from_millis(handle.config.heartbeat_cycle));
        use LoopResult::{Shutdown, TransitToFollower};
        let peers = &message_handler.peers;
        let loop_result: LoopResult = {
            let mut rpc_replies: FuturesUnordered<_> = {
                let me = handle.node_id;
                let params: Vec<_> = message_handler
                    .node_ids_except(me)
                    .map(|peer_id| {
                        let next_index = self.next_index[peer_id];
                        let args = build_append_entries_args(
                            me,
                            &handle.election,
                            &handle.logs,
                            next_index,
                        );
                        (peer_id, next_index, args)
                    })
                    .collect();
                params
                    .into_iter()
                    .map(|(peer_id, next_index, args)| {
                        let future = peers[peer_id].append_entries(args);
                        let context = ReplyContext::new(peer_id, next_index);
                        with_context(future, context)
                    })
                    .collect()
            };
            loop {
                select! {
                    _ = heartbeat_timer.tick() => {
                        trace!("term={}, send heartbeat", handle.election.current_term());
                        let futures = self.send_append_entries(&handle, message_handler, peers, me);
                        rpc_replies.extend(futures)
                    }
                    Some(FutureOutput { output: result, context: ReplyContext { follower_id, old_next_index }}) = rpc_replies.next() => {
                        let _span = trace_span!("append entries receive reply", follower_id, old_next_index).entered();
                        match result {
                            Ok(reply) => {
                                match on_receive_append_entries_reply(
                                    &handle.logs,
                                    old_next_index,
                                    reply,
                                    follower_id,
                                    handle.election.current_term()
                                ) {
                                    Commit{ follower_id, match_length } => {
                                        trace!("commit");
                                        self.next_index[follower_id] = match_length;
                                        self.match_length[follower_id] = match_length;
                                        try_commit_logs(&self, handle, message_handler).await
                                    }
                                    Retry { follower_id, new_next_index } => {
                                        trace!("retry");
                                        self.next_index[follower_id] = new_next_index;
                                        let args = build_append_entries_args(me, &handle.election, &handle.logs, new_next_index);
                                        let future = message_handler.peers[follower_id].append_entries(args);
                                        let context = ReplyContext::new(follower_id, new_next_index);
                                        rpc_replies.push(with_context(future, context));
                                    }
                                    UpdateTermAndTransitToFollower(new_term) => {
                                        trace!("update terem and transit to follower");
                                        handle.update_current_term(new_term);
                                        break TransitToFollower;
                                    }
                                }
                            }
                            Err(e) => warn!(%e, "rpc error"),
                        }
                    }
                    Some(task) = message_handler.local_tasks.next() => {
                        match task {
                            LocalTask::AppendEntries { data, sender } => {
                                // trace!("term={}, local task append entries", handle.election.current_term());
                                let me = handle.node_id;
                                let current_term = handle.election.current_term();
                                let index = handle.add_log(LogKind::Command, data, current_term);
                                self.match_length[me] = index + 1;
                                sender.send(Some((index as u64, current_term))).unwrap();
                                let futures = self.send_append_entries(&handle, message_handler, peers, me);
                                rpc_replies.extend(futures);
                            }
                            LocalTask::GetTerm(sender) => sender.send(handle.election.current_term()).unwrap(),
                            LocalTask::CheckLeader(sender) => sender.send(true).unwrap(),
                            LocalTask::Shutdown(sender) => {
                                sender.send(()).unwrap();
                                break Shutdown;
                            }
                        }
                    }
                    Some(task) = message_handler.remote_tasks.next() => {
                        trace!("handle remote task");
                        let RemoteTaskResult { transit_to_follower } = task.handle(handle).await;
                        if transit_to_follower {
                            break TransitToFollower;
                        }
                    }
                }
            }
        };
        info!(
            "term={}, loop_result={:?}",
            handle.election.current_term(),
            loop_result
        );
        match loop_result {
            TransitToFollower => Role::Follower(Follower::from(self)),
            Shutdown => Role::Shutdown,
        }
    }

    fn send_append_entries<'peer: 'iter, 'iter>(
        &'iter self,
        handle: &'iter Handle,
        message_handler: &'iter MessageHandler,
        peers: &'peer [Box<dyn PeerEndPoint + Send>],
        me: NodeId,
    ) -> impl Iterator<Item = FutureWithContext<AppendEntriesFuture<'peer>, ReplyContext>> + 'iter {
        message_handler
            .node_ids_except(me)
            .map(move |peer_id| {
                let next_index = self.next_index[peer_id];
                let args = build_append_entries_args(me, &handle.election, &handle.logs, next_index);
                let future: AppendEntriesFuture = peers[peer_id].append_entries(args);
                let context = ReplyContext::new(peer_id, next_index);
                with_context(future, context)
            })
    }
}



#[derive(Debug)]
enum LoopResult {
    TransitToFollower,
    Shutdown,
}

#[instrument(skip_all, ret, level = "trace")]
fn on_receive_append_entries_reply(
    logs: &Logs,
    old_next_index: usize,
    reply: AppendEntriesReply,
    follower_id: NodeId,
    current_term: TermId,
) -> AppendEntriesResult {
    trace!(%old_next_index, follower_id, current_term);
    trace!(?reply);
    trace!(?logs);
    match reply.term.cmp(&current_term) {
        Greater => UpdateTermAndTransitToFollower(reply.term),
        Less => Retry {
            follower_id,
            new_next_index: old_next_index,
        },
        Equal => match reply.result {
            Success { match_length } => Commit {
                follower_id,
                match_length,
            },
            LogNotMatch {
                term_conflicted,
                first_index_of_term_conflicted,
            } => {
                let new_next_index = (first_index_of_term_conflicted..old_next_index)
                    .rev()
                    .find(|index| {
                        logs.get(*index)
                            .unwrap_or_else(|| {
                                panic!("index {} out of range {}", *index, logs.len())
                            })
                            .term
                            == term_conflicted
                    })
                    .map_or(0, |index| index + 1);
                Retry {
                    follower_id,
                    new_next_index,
                }
            }
            LogNotContainThisEntry { log_len } => Retry {
                follower_id,
                new_next_index: log_len,
            },
            TermCheckFail => {
                // TODO: make this state unrepresentable
                panic!(
                    "reply.term ({}) should larger than current_term ({current_term})",
                    reply.term
                )
            }
        },
    }
}

struct ReplyContext {
    follower_id: NodeId,
    old_next_index: usize,
}

impl ReplyContext {
    fn new(follower_id: NodeId, old_next_index: usize) -> Self {
        Self {
            follower_id,
            old_next_index,
        }
    }
}

#[derive(Debug)]
enum AppendEntriesResult {
    UpdateTermAndTransitToFollower(TermId),
    Commit {
        follower_id: NodeId,
        match_length: usize,
    },
    /// the previous `AppendEntries` didn't success, `reply.match_length == None`
    /// or the reply is too late
    Retry {
        follower_id: NodeId,
        new_next_index: usize,
    },
}

/// A log can be committed
/// iff
/// 1. the log is replicated on the majority of servers
/// 2. the log with max term that is replicated on the majority of servers is from current term
#[instrument(level = "debug")]
async fn try_commit_logs(leader: &Leader, handle: &mut Handle, message_handle: &MessageHandler) {
    let commit_threshold = message_handle.majority_threshold();
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
    let nonempty_commits = new_commit_length > handle.logs.commit_len();
    // lazy condition
    let from_current_term = || {
        handle
            .logs
            .get(new_commit_length - 1) // can overflow if new_commit_length == handle.logs.commit_len()
            .expect("should never fail")
            .term
            == handle.election.current_term()
    };
    // Only log entries from the leader’s current term are committed by counting replicas
    if nonempty_commits && from_current_term() {
        let messages = handle.logs.commit_logs(new_commit_length);
        Handle::apply_messages(&mut handle.apply_ch, messages).await
    }
}

fn build_append_entries_args(
    leader_id: NodeId,
    election: &Election,
    logs: &Logs,
    next_index: NextIndex,
) -> AppendEntriesArgs {
    AppendEntriesArgs {
        term: election.current_term(),
        leader_id,
        log_state: logs.log_state_before(next_index),
        entries: logs.tail(next_index).map(Clone::clone).collect(),
        leader_commit_length: logs.commit_len(),
    }
}

#[derive(Debug, Clone, Eq, PartialEq, new, Serialize, Deserialize)]
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

/// NOTE: `term` should come first, since we implement `PartialOrd` and `Ord` here.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Copy, Clone, new, Serialize, Deserialize)]
pub struct LogState {
    pub term: TermId,
    pub index: usize,
}
