use crate::proto::raftpb::{RequestVoteArgs, RequestVoteReply};
use crate::raft::errors::{Error, Result};
use crate::raft::inner::RemoteTaskResult;
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::Leader;
use crate::raft::role::Role;
use crate::raft::{NodeId, TermId};
use futures::{stream, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
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
            debug!(?handle);
            stream::once(collect_vote(
                handle.node_id,
                replies,
                handle.get_majority_threshold(),
                handle.election.get_current_term(),
            ))
        };
        let election_timeout = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )));
        futures::pin_mut!(vote_result);
        futures::pin_mut!(election_timeout);
        let new_role: Role = loop {
            select! {
                _ = election_timeout.next() => break Role::Candidate(Candidate),
                Some(task) = handle.remote_task_receiver.recv() => {
                    let RemoteTaskResult { success, new_role } = task.handle(Role::Candidate(Candidate), handle).await;
                    if success {
                        break new_role;
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
                            break Role::Stop;
                        }
                    }
                }
                Some(vote_result) = vote_result.next() => match vote_result {
                    VoteResult::Elected => break Role::Leader(Leader::new(handle.logs.get_log_len(), handle.peers.len())),
                    // split vote, do nothing, wait for the election timeout
                    // if we break the loop immediately, the candidate is likely to retry requesting vote immediately
                    // and increment its term rapidly
                    VoteResult::Lost => debug!("vote lost"),
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

#[derive(Debug)]
enum VoteResult {
    Elected,
    Lost, // all peers reply but none of the reply is legal
    FoundLargerTerm(TermId),
}

#[instrument(skip(replies), ret, level = "debug")]
async fn collect_vote(
    node_id: NodeId,
    mut replies: impl Stream<Item = Result<RequestVoteReply>> + Unpin + Debug,
    electoral_threshold: usize,
    current_term: TermId,
) -> VoteResult {
    let mut votes_received = HashSet::from([node_id as u32]);
    // we need to first check the number of received votes, then await the replies
    // the order matters
    // if we await the replies first, there can be a situation where the condition is satisfied,
    // but still awating for the replies
    while votes_received.len() < electoral_threshold && let Some(reply) = replies
        .next()
        .await
    {
        match reply {
            Ok(reply) => {
                if reply.term == current_term && reply.vote_granted {
                    votes_received.insert(reply.node_id);
                } else if reply.term > current_term {
                    return VoteResult::FoundLargerTerm(reply.term);
                } else {
                    debug!("received outdated votes or non-granted votes, current_term: {}, reply.term: {}", current_term, reply.term);
                }
            }
            Err(e) => error!("{}", e.to_string())
        }
    }
    if votes_received.len() >= electoral_threshold {
        VoteResult::Elected
    } else {
        VoteResult::Lost
    }
}
