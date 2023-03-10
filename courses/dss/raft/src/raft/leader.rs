use crate::proto::raftpb::{AppendEntriesArgsProst, RaftClient};
use crate::raft::inner::RemoteTaskResult;
use crate::raft::inner::{LocalTask, PeerEndPoint};
use crate::raft::leader::AppendEntriesResult::{Commit, Retry, UpdateTermAndTransitToFollower};
use crate::raft::role::Role;
use crate::raft::{ApplyMsg, NodeId, TermId};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::cmp::max;
use std::future::Future;

use derive_new::new;
use std::time::Duration;

use crate::raft;
use crate::raft::candidate::Candidate;
use crate::raft::common::{with_context, FutureOutput};
use crate::raft::follower::Follower;
use crate::raft::handle::election::Election;
use crate::raft::handle::peer::Peers;
use crate::raft::handle::{Handle, Logs};
use crate::raft::leader::LoopResult::TransitToFollower;
use crate::raft::rpc::AppendEntriesReplyResult::{
    LogNotContainThisEntry, LogNotMatch, Success, TermCheckFail,
};
use crate::raft::rpc::{AppendEntriesArgs, AppendEntriesReply, AppendEntriesReplyResult};
use tokio::select;
use tokio::time::interval;
use tracing::{info, instrument, trace, trace_span, warn};

/// inner structure for `ApplyMsg`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogKind {
    Command,
    Snapshot,
}

pub type NextIndex = usize;
pub type MatchLength = usize;

/// It's impossible that there exists multiple leaders with the same term.
#[derive(Debug)]
pub struct Leader {
    /// for each server, `next_index` is the index of the next log entry to send to that server
    /// different from the Raft paper, `match_length = match_index + 1`
    pub peers: Peers<(Box<dyn PeerEndPoint>, NextIndex, MatchLength)>,
}

impl Leader {
    pub fn from(candidate: Candidate, log_length: usize) -> Self {
        const INITIAL_MATCH_LENGTH: usize = 0;
        let peers = Peers::new(
            candidate.peers.me(),
            candidate
                .peers
                .into_iter()
                .map(|(node_id, peer)| (node_id, (peer, log_length, INITIAL_MATCH_LENGTH)))
        );
        Leader { peers }
    }

    pub(crate) async fn progress(mut self, handle: &mut Handle) -> Role {
        let _span = trace_span!("Leader", node_id = handle.node_id).entered();
        let mut heartbeat_timer = interval(Duration::from_millis(handle.config.heartbeat_cycle));
        use LoopResult::{Shutdown, TransitToFollower};
        let loop_result: LoopResult = {
            let mut rpc_replies: FuturesUnordered<_> = {
                self.peers
                    .iter_except_me()
                    .map(|(peer_id, (peer, next_index, _))| {
                        with_context(
                            replicate_log(handle.node_id, &handle.logs, &handle.election, peer, *next_index),
                            ReplyContext::new(*peer_id, *next_index),
                        )
                    })
                    .collect()
            };
            loop {
                select! {
                _ = heartbeat_timer.tick() => {
                    trace!("term={}, send heartbeat", handle.election.current_term());
                    let futures = self
                        .peers
                        .iter_except_me()
                        .map(|(peer_id, (peer, next_index, _))|
                            with_context(replicate_log(handle.node_id, &handle.logs, &handle.election, peer, *next_index), ReplyContext::new(*peer_id, *next_index))
                        );
                    rpc_replies.extend(futures)
                }
                Some(FutureOutput { output: result, context: ReplyContext { follower_id, old_next_index }}) = rpc_replies.next() => {
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
                                    let (_, next_index, match_index) = self.peers.get_mut(&follower_id)
                                        .expect(&format!("follower {follower_id} not in peers"));
                                    *next_index = match_length;
                                    *match_index = match_length;
                                    try_commit_logs(&self, handle).await
                                }
                                Retry { follower_id, new_next_index } => {
                                    let (ref peer, next_index, _) = self.peers
                                        .get_mut(&follower_id)
                                        .expect(&format!("{follower_id} not in peers"));
                                    *next_index = new_next_index;
                                    rpc_replies.push(with_context(replicate_log(handle.node_id, &handle.logs, &handle.election, peer, *next_index), ReplyContext::new(follower_id, *next_index)));
                                }
                                UpdateTermAndTransitToFollower(new_term) => {
                                    handle.update_current_term(new_term);
                                    break TransitToFollower;
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
                            let index = handle.add_log(LogKind::Command, data, current_term);
                            let (_, _, match_length) = self.peers.get_mut(&handle.node_id).expect(&format!("node {} not in peers", handle.node_id));
                            *match_length = index + 1;
                            sender.send(Some((index as u64, current_term))).unwrap();
                            let futures = self
                                .peers
                                .iter_except_me()
                                .map(|(follower_id, (peer, next_index, _))| with_context(replicate_log(handle.node_id, &handle.logs, &handle.election, peer, *next_index), ReplyContext::new(*follower_id, *next_index)));
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
                Some(task) = handle.remote_task_receiver.recv() => {
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
}

#[derive(Debug)]
enum LoopResult {
    TransitToFollower,
    Shutdown,
}

#[instrument(ret, level = "trace")]
fn on_receive_append_entries_reply(
    logs: &Logs,
    old_next_index: usize,
    reply: AppendEntriesReply,
    follower_id: NodeId,
    current_term: TermId,
) -> AppendEntriesResult {
    if reply.term > current_term {
        UpdateTermAndTransitToFollower(reply.term)
    } else if reply.term == current_term {
        match reply.result {
            Success { match_length } => Commit {
                follower_id,
                match_length,
            },
            LogNotMatch {
                term_conflicted,
                first_index_of_term_conflicted,
            } => Retry {
                follower_id,
                new_next_index: max(
                    first_index_of_term_conflicted,
                    logs.first_index_with_same_term_with(
                        old_next_index
                            .checked_sub(1)
                            .expect("the log_state in AppendEntriesArgs should not be None"),
                    ),
                ), // TODO: make this state unrepresentable
            },
            LogNotContainThisEntry { log_len } => Retry {
                follower_id,
                new_next_index: log_len,
            },
            TermCheckFail => {
                // TODO: make this state unrepresentable
                panic!(format!(
                    "reply.term ({}) should larger than current_term ({current_term})",
                    reply.term
                ))
            }
        }
    } else {
        Retry {
            follower_id,
            new_next_index: old_next_index,
        }
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

#[instrument(level = "debug")]
async fn try_commit_logs(leader: &Leader, handle: &mut Handle) {
    let commit_threshold = leader.peers.majority_threshold();
    let compute_acks = |length_threshold| {
        let acks = leader
            .peers
            .iter()
            .map(|(_, (_, _, match_index))| *match_index)
            .filter(|match_length| *match_length > length_threshold)
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

fn replicate_log<'a, P: PeerEndPoint>(
    leader_id: NodeId,
    logs: &'a Logs,
    election: &'a Election,
    peer: &'a P,
    next_index: NextIndex
) -> impl Future<Output = raft::Result<AppendEntriesReply>> + 'a {
    let _span = trace_span!("replicate_log").entered();
    trace!("term={}, {:?}", election.current_term(), logs);
    let args = AppendEntriesArgs {
        term: election.current_term(),
        leader_id,
        log_state: logs.log_state_before(next_index),
        entries: logs.tail(next_index).map(Clone::clone).collect(),
        leader_commit_length: logs.commit_len(),
    };
    peer.append_entries(&args)
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

/// NOTE: `term` should come first, since we implement `PartialOrd` and `Ord` here.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Copy, Clone, new)]
pub struct LogState {
    pub term: TermId,
    pub index: usize,
}
