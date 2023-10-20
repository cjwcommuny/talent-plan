use crate::raft::inner::LocalTask;
use crate::raft::inner::RemoteTaskResult;
use crate::raft::leader::Leader;
use crate::raft::role::Role;
use futures::{stream, stream::FuturesUnordered, StreamExt};
use std::collections::HashSet;
use std::fmt::Debug;

use crate::raft::common::{with_context, FutureOutput};
use crate::raft::follower::Follower;
use crate::raft::handle::Handle;
use crate::raft::message_handler::MessageHandler;
use crate::raft::rpc::RequestVoteArgs;
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{info, trace, trace_span, warn};

/// make the constructor private
#[derive(Debug)]
pub struct Candidate(());

impl From<Follower> for Candidate {
    fn from(_: Follower) -> Self {
        Self(())
    }
}

impl Candidate {
    pub(crate) async fn progress(
        self,
        handle: &mut Handle,
        message_handler: &mut MessageHandler,
    ) -> Role {
        let _span = trace_span!("Candidate", node_id = handle.node_id).entered();
        let me = handle.node_id;
        handle.increment_term();
        handle.election.voted_for = Some(handle.node_id);
        let args = RequestVoteArgs {
            log_state: handle.logs.log_state(),
            term: handle.election.current_term(),
            candidate_id: handle.node_id,
        };
        let election_timeout = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )));
        let mut votes_received = HashSet::from([handle.node_id]);
        futures::pin_mut!(election_timeout);
        let majority_threshold = message_handler.majority_threshold();
        use LoopResult::{Elected, RestartAsCandidate, Shutdown, TransitToFollower};
        let peers = &message_handler.peers;
        let vote_result = 'collect_vote: {
            let mut replies: FuturesUnordered<_> = {
                trace!(
                    "term={}, send vote requests",
                    handle.election.current_term()
                );
                message_handler
                    .node_ids_except(me)
                    .map(|peer_id| with_context(peers[peer_id].request_vote(args.clone()), peer_id))
                    .collect()
            };
            while votes_received.len() < majority_threshold {
                select! {
                    _ = election_timeout.next() => {
                        break 'collect_vote RestartAsCandidate;
                    }
                    Some(task) = message_handler.remote_task_receiver.recv() => {
                        let RemoteTaskResult { transit_to_follower } = task.handle(handle).await;
                        if transit_to_follower {
                            break 'collect_vote TransitToFollower;
                        }
                    }
                    Some(task) = message_handler.local_task_receiver.recv() => {
                        match task {
                            LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                            LocalTask::GetTerm(sender) => sender.send(handle.election.current_term()).unwrap(),
                            LocalTask::CheckLeader(sender) => sender.send(false).unwrap(),
                            LocalTask::Shutdown(sender) => {
                                sender.send(()).unwrap();
                                break 'collect_vote Shutdown;
                            }
                        }
                    }
                    Some(FutureOutput { output: reply_result, context: peer_id }) = replies.next() => {
                        let current_term = handle.election.current_term();
                        match reply_result {
                            Ok(reply) => {
                                if reply.term == current_term && reply.vote_granted {
                                    trace!("receive vote granted from {peer_id}");
                                    votes_received.insert(peer_id);
                                } else if reply.term > current_term {
                                    handle.update_current_term(reply.term);
                                    handle.election.voted_for = None;
                                    break 'collect_vote TransitToFollower;
                                } else {
                                    trace!("term={}, received outdated votes or non-granted votes, reply.term: {}", current_term, reply.term);
                                }
                            }
                            Err(e) => warn!("term={}, {}", handle.election.current_term(), e.to_string())
                        }
                    }
                }
            }
            break 'collect_vote Elected;
        };
        info!(
            "term={}, vote_result={:?}",
            handle.election.current_term(),
            vote_result
        );
        match vote_result {
            RestartAsCandidate => Role::Candidate(self),
            TransitToFollower => Role::Follower(Follower::from(self)),
            Shutdown => Role::Shutdown,
            Elected => Role::Leader(Leader::from(
                self,
                handle.logs.len(),
                message_handler.peers.len(),
            )),
        }
    }
}

#[derive(Debug)]
enum LoopResult {
    RestartAsCandidate,
    TransitToFollower,
    Shutdown,
    Elected,
}
