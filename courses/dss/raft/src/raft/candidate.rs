use crate::proto::raftpb::RequestVoteArgs;
use crate::raft::errors::Error;
use crate::raft::inner::RemoteTaskResult;
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::Leader;
use crate::raft::role::Role;
use futures::{stream, stream::FuturesUnordered, FutureExt, StreamExt};
use std::collections::HashSet;
use std::fmt::Debug;

use crate::raft::follower::Follower;
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error, instrument};

#[derive(Debug, Default)]
pub struct Candidate;

impl Candidate {
    #[instrument(name = "Candidate::progress", skip_all, ret, fields(node_id = handle.node_id, term = handle.election.get_current_term()), level = "debug")]
    pub(crate) async fn progress(self, handle: &mut Handle) -> Role {
        handle.election.increment_term();
        handle.election.voted_for = Some(handle.node_id);
        let peers = &handle.peers;
        let mut replies: FuturesUnordered<_> = {
            let args = RequestVoteArgs {
                log_state: handle.logs.get_log_state().map(Into::into),
                term: handle.election.get_current_term(),
                candidate_id: handle.node_id as u32,
            };
            handle
                .get_node_ids_except_mine()
                .map(|node_id| {
                    peers[node_id]
                        .request_vote(&args)
                        .map(|r| r.map_err(Error::Rpc))
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
        while votes_received.len() < handle.get_majority_threshold() {
            select! {
                _ = election_timeout.next() => return Role::Candidate(Candidate),
                Some(task) = handle.remote_task_receiver.recv() => {
                    let RemoteTaskResult { success, new_role } = task.handle(Role::Candidate(Candidate), handle).await;
                    if success {
                        return new_role;
                    }
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                        LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).unwrap(),
                        LocalTask::CheckLeader(sender) => sender.send(false).unwrap(),
                        LocalTask::Shutdown(sender) => {
                            info!("shutdown");
                            sender.send(()).unwrap();
                            return Role::Stop;
                        }
                    }
                }
                Some(reply_result) = replies.next() => {
                    let current_term = handle.election.get_current_term();
                    match reply_result {
                        Ok(reply) => {
                            if reply.term == current_term && reply.vote_granted {
                                votes_received.insert(reply.node_id);
                            } else if reply.term > current_term {
                                handle.election.update_current_term(reply.term);
                                handle.election.voted_for = None;
                                return Role::Follower(Follower::default());
                            } else {
                                debug!("received outdated votes or non-granted votes, current_term: {}, reply.term: {}", current_term, reply.term);
                            }
                        }
                        Err(e) => error!("{}", e.to_string())
                    }
                }
            }
        }
        Role::Leader(Leader::new(handle.logs.get_log_len(), handle.peers.len()))
    }
}
