use crate::proto::raftpb::RequestVoteArgs;
use crate::raft::errors::Error;
use crate::raft::inner::LocalTask;
use crate::raft::inner::RemoteTaskResult;
use crate::raft::leader::Leader;
use crate::raft::role::Role;
use futures::{stream, stream::FuturesUnordered, FutureExt, StreamExt};
use std::collections::HashSet;
use std::fmt::Debug;

use crate::raft::follower::Follower;
use crate::raft::handle::Handle;
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{info, trace, trace_span, warn};

/// make the constructor private
#[derive(Debug, Default)]
pub struct Candidate(());

impl Candidate {
    fn new() -> Self {
        Self(())
    }

    pub(crate) async fn progress(self, handle: &mut Handle) -> Role {
        let _span = trace_span!("Candidate", node_id = handle.node_id).entered();
        handle.increment_term();
        handle.election.voted_for = Some(handle.node_id);
        let peers = &handle.peers;
        let mut replies: FuturesUnordered<_> = {
            let args = RequestVoteArgs {
                log_state: handle.logs.log_state().map(Into::into),
                term: handle.election.current_term(),
                candidate_id: handle.node_id as u32,
            };
            trace!(
                "term={}, send vote requests",
                handle.election.current_term()
            );
            handle
                .node_ids_except_mine()
                .map(|node_id| {
                    peers[node_id]
                        .request_vote(&args)
                        .map(move |r| r.map_err(|e| Error::Rpc(e, node_id)))
                })
                .collect()
        };
        let election_timeout = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )));
        let mut votes_received = HashSet::from([handle.node_id as u32]);
        futures::pin_mut!(election_timeout);
        while votes_received.len() < handle.majority_threshold() {
            select! {
                _ = election_timeout.next() => {
                    info!("term={}, election timeout", handle.election.current_term());
                    return Role::Candidate(self)
                }
                Some(task) = handle.remote_task_receiver.recv() => {
                    let RemoteTaskResult { success, new_role } = task.handle(Role::Candidate(Candidate::new()), handle).await;
                    if success {
                        info!("term={}, transit to {:?}", handle.election.current_term(), new_role);
                        return new_role;
                    }
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                        LocalTask::GetTerm(sender) => sender.send(handle.election.current_term()).unwrap(),
                        LocalTask::CheckLeader(sender) => sender.send(false).unwrap(),
                        LocalTask::Shutdown(sender) => {
                            info!("term={}, shutdown", handle.election.current_term());
                            sender.send(()).unwrap();
                            return Role::Stop;
                        }
                    }
                }
                Some(reply_result) = replies.next() => {
                    let current_term = handle.election.current_term();
                    match reply_result {
                        Ok(reply) => {
                            if reply.term == current_term && reply.vote_granted {
                                votes_received.insert(reply.node_id);
                            } else if reply.term > current_term {
                                handle.update_current_term(reply.term);
                                handle.election.voted_for = None;
                                return Role::Follower(Follower::default());
                            } else {
                                trace!("term={}, received outdated votes or non-granted votes, reply.term: {}", current_term, reply.term);
                            }
                        }
                        Err(e) => warn!("term={}, {}", handle.election.current_term(), e.to_string())
                    }
                }
            }
        }
        info!("term={}, become leader", handle.election.current_term());
        Role::Leader(Leader::from(self, handle.logs.len(), handle.peers.len()))
    }
}

impl From<Follower> for Candidate {
    fn from(_: Follower) -> Self {
        Self::new()
    }
}
