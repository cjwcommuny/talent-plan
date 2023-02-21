use crate::proto::raftpb::{RequestVoteArgs, RequestVoteReply};
use crate::raft::errors::{Error, Result};
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::{Leader, VolatileLeaderState};
use crate::raft::role::{Follower, Role};
use crate::raft::{receive_task, TermId};
use futures::{stream::FuturesUnordered, FutureExt, Stream, StreamExt};
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
        handle.election.increment_term();
        handle.election.voted_for = Some(me);
        let log_state = handle.logs.get_log_state().map(Into::into);
        let args = RequestVoteArgs {
            log_state,
            term: handle.election.get_current_term(),
            candidate_id: me as u32,
        };
        let peers = &handle.peers;
        let replies: FuturesUnordered<_> = handle
            .get_node_ids_except_mine()
            .map(|node_id| {
                peers[node_id]
                    .request_vote(&args)
                    .map(|r| r.map_err(Error::Rpc))
            })
            .collect();
        // TODO: timer parameters as config
        let election_timer = sleep_until(Instant::now() + Duration::from_millis(200));
        let electoral_threshold = div_ceil(handle.peers.len() + 1, 2);
        let current_term = handle.election.get_current_term();
        select! {
            _ = election_timer => {
                Role::Candidate(self)
            }
            Some(task) = receive_task(&mut handle.remote_task_receiver, current_term) => {
                handle.election.update_current_term(task.get_term());
                handle.election.voted_for = None;
                task.handle(Role::Candidate(self), handle).await
            }
            Some(task) = handle.local_task_receiver.recv() => {
                match task {
                    LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                    LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).unwrap(),
                    LocalTask::IsLeader(sender) => sender.send(false).unwrap(),
                }
                Role::Candidate(self)
            }
            vote_result = collect_vote(replies, electoral_threshold, handle.election.get_current_term()) => match vote_result {
                VoteResult::Elected => {
                    Role::Leader(Leader::new(VolatileLeaderState::new(handle.logs.get_entries().len(), handle.peers.len())))
                }
                VoteResult::Unsuccess => {
                    Role::Candidate(self)
                }
                VoteResult::FoundLargerTerm(new_term) => {
                    handle.election.update_current_term(new_term);
                    handle.election.voted_for = None;
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

async fn collect_vote(
    mut replies: impl Stream<Item = Result<RequestVoteReply>> + Unpin,
    electoral_threshold: usize,
    current_term: TermId,
) -> VoteResult {
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
