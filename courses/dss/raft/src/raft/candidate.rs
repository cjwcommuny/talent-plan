use crate::proto::raftpb::{LogStateProst, RequestVoteArgs, RequestVoteReply};
use crate::raft::errors::{Error, Result};
use crate::raft::leader::{Leader, VolatileLeaderState};
use crate::raft::role::{Follower, Role};
use crate::raft::{receive_task, Handle, TermId};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use num::integer::div_ceil;
use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;
use tokio::select;
use tokio::time::{sleep_until, Instant};

#[derive(Debug)]
pub struct Candidate {}

impl Candidate {
    pub(crate) async fn transit(self, handle: &mut Handle) -> Role {
        let me = handle.node_id;
        handle.persistent_state.current_term += 1;
        handle.persistent_state.voted_for = Some(me);
        let log_state = handle.persistent_state.log.last().map(|log| LogStateProst {
            last_log_index: (handle.persistent_state.log.len() - 1) as u32,
            last_log_term: log.log_state.term,
        });
        let args = RequestVoteArgs {
            log_state,
            term: handle.persistent_state.current_term,
            candidate_id: me as u32,
        };
        let peers = &handle.peers;
        let replies = (0..handle.peers.len())
            .filter(|node_id| *node_id != me)
            .map(|node_id| {
                peers[node_id]
                    .request_vote(&args)
                    .map(|r| r.map_err(Error::Rpc))
            });
        // TODO: timer parameters as config
        let election_timer = sleep_until(Instant::now() + Duration::from_millis(200));
        let electoral_threshold = div_ceil(handle.peers.len() + 1, 2);
        let current_term = handle.persistent_state.current_term;
        select! {
            _ = election_timer => {
                Role::Candidate(self)
            }
            Some(task) = receive_task(&mut handle.task_receiver, current_term) => {
                handle.persistent_state.current_term = task.get_term();
                handle.persistent_state.voted_for = None;
                task.handle(Role::Candidate(self), handle).await
            }
            vote_result = collect_vote(replies, electoral_threshold, handle.persistent_state.current_term) => match vote_result {
                VoteResult::Elected => {
                    Role::Leader(Leader::new(VolatileLeaderState::new(handle.persistent_state.log.len(), handle.peers.len())))
                }
                VoteResult::Unsuccess => {
                    Role::Candidate(self)
                }
                VoteResult::FoundLargerTerm(new_term) => {
                    handle.persistent_state.current_term = new_term;
                    handle.persistent_state.voted_for = None;
                    Role::Follower(Follower::default())
                }
            }
        }
    }
}

enum VoteResult {
    Elected,
    Unsuccess, // all peers reply but none of the reply is legal
    FoundLargerTerm(TermId),
}

async fn collect_vote<F>(
    replies: impl Iterator<Item = F>,
    electoral_threshold: usize,
    current_term: TermId,
) -> VoteResult
where
    F: Future<Output = Result<RequestVoteReply>>,
{
    let mut replies: FuturesUnordered<_> = replies.collect();
    let mut votes_received = HashSet::new();
    while let Some(reply) = replies.next().await.map(|r| r.unwrap()) {
        // TODO: remove unwrap
        if reply.term == current_term && reply.vote_granted {
            votes_received.insert(reply.node_id);
            if votes_received.len() >= electoral_threshold {
                return VoteResult::Elected;
            }
        } else if reply.term > current_term {
            return VoteResult::FoundLargerTerm(reply.term);
        }
    }
    return VoteResult::Unsuccess;
}
