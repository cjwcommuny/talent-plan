use crate::proto::raftpb::{RequestVoteArgs, RequestVoteReply};
use crate::raft::errors::{Error, Result};
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::Leader;
use crate::raft::role::{Follower, Role};
use crate::raft::TermId;
use futures::{stream, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use std::collections::HashSet;
use std::fmt::Debug;

use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{error, instrument};

#[derive(Debug, Default)]
pub struct Candidate;

impl Candidate {
    #[instrument(ret)]
    pub(crate) async fn progress(self, handle: &mut Handle) -> Role {
        handle.election.increment_term();
        handle.election.voted_for = Some(handle.node_id);
        let peers = &handle.peers;
        let vote_result = {
            let replies: FuturesUnordered<_> = {
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

            stream::once(collect_vote(
                replies,
                handle.get_majority_threshold(),
                handle.election.get_current_term(),
            ))
        };
        let election_timeout =
            stream::once(sleep(Duration::from_millis(handle.config.election_timeout)));
        futures::pin_mut!(vote_result);
        futures::pin_mut!(election_timeout);
        let new_role: Role = loop {
            select! {
                _ = election_timeout.next() => break Role::Candidate(Candidate),
                Some(task) = handle.remote_task_receiver.recv() => {
                    let new_role = task.handle(Role::Candidate(Candidate), handle).await;
                    if !matches!(new_role, Role::Candidate(_)) {
                        break new_role;
                    }
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                        LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).unwrap(),
                        LocalTask::CheckLeader(sender) => sender.send(false).unwrap(),
                        LocalTask::Shutdown(sender) => sender.send(()).unwrap(),
                    }
                }
                Some(vote_result) = vote_result.next() => match vote_result {
                    VoteResult::Elected => break Role::Leader(Leader::new(handle.logs.get_log_len(), handle.peers.len())),
                    VoteResult::Lost => break Role::Candidate(Candidate), // split vote
                    VoteResult::FoundLargerTerm(new_term) => {
                        handle.election.update_current_term(new_term);
                        handle.election.voted_for = None;
                        break Role::Follower(Follower::default());
                    }
                }
            }
        };
        new_role
    }
}

enum VoteResult {
    Elected,
    Lost, // all peers reply but none of the reply is legal
    FoundLargerTerm(TermId),
}

#[instrument]
async fn collect_vote(
    mut replies: impl Stream<Item = Result<RequestVoteReply>> + Unpin + Debug,
    electoral_threshold: usize,
    current_term: TermId,
) -> VoteResult {
    let mut votes_received = HashSet::new();
    while let Some(reply) = replies
        .next()
        .await
        .and_then(|r: Result<RequestVoteReply>| {
            r.inspect_err(|e| error!(reply_error = e.to_string())).ok()
        })
    {
        if reply.term == current_term && reply.vote_granted {
            votes_received.insert(reply.node_id);
            if votes_received.len() >= electoral_threshold {
                return VoteResult::Elected;
            }
        } else if reply.term > current_term {
            return VoteResult::FoundLargerTerm(reply.term);
        }
    }
    return VoteResult::Lost;
}
