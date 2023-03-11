use derive_more::IsVariant;
use std::cmp::max;

use tracing::{instrument, trace};

use crate::raft::candidate::Candidate;
use crate::raft::common::{async_side_effect, side_effect};
use crate::raft::follower::Follower;

use crate::raft::handle::Handle;
use crate::raft::leader::{Leader, LogEntry, LogState};
use crate::raft::message_handler::MessageHandler;
use crate::raft::rpc::AppendEntriesReplyResult::{
    LogNotContainThisEntry, LogNotMatch, Success, TermCheckFail,
};
use crate::raft::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::raft::NodeId;

#[derive(Debug, IsVariant)]
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
    Shutdown,
}

pub type TransitToFollower = bool;

impl Role {
    pub(crate) async fn progress(
        self,
        handle: &mut Handle,
        message_handler: &mut MessageHandler,
    ) -> Role {
        match self {
            Role::Follower(follower) => follower.progress(handle, message_handler).await,
            Role::Candidate(candidate) => candidate.progress(handle, message_handler).await,
            Role::Leader(leader) => leader.progress(handle, message_handler).await,
            Role::Shutdown => Role::Shutdown,
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower(Follower::default())
    }
}

#[instrument(skip_all, fields(node_id = handle.node_id), level = "trace")]
pub fn request_vote(
    args: &RequestVoteArgs,
    handle: &mut Handle,
) -> (RequestVoteReply, TransitToFollower) {
    trace!(?args);
    trace!(?handle);
    let log_ok = {
        let self_log_state = handle.logs.log_state();
        let candidate_log_state: Option<LogState> = args.log_state.map(Into::into);
        trace!(?candidate_log_state, ?self_log_state);
        candidate_log_state >= self_log_state
    };
    let term_ok = {
        let current_term = handle.election.current_term();
        let voted_for = handle.election.voted_for;
        args.term > current_term
            || (args.term == current_term
                && (voted_for.is_none() || voted_for == Some(args.candidate_id)))
    };
    let vote_granted = log_ok && term_ok;
    trace!(log_ok, term_ok, vote_granted);
    let transit_to_follower = vote_granted || args.term > handle.election.current_term();
    let reply = RequestVoteReply {
        term: max(handle.election.current_term(), args.term),
        vote_granted,
    };
    let result = (reply, transit_to_follower);
    side_effect(|| {
        if vote_granted {
            handle.election.voted_for = Some(args.candidate_id);
        }
        if args.term > handle.election.current_term() {
            handle.update_current_term(args.term);
        }
    });
    result
}

#[instrument(skip_all, level = "trace")]
pub async fn append_entries(
    args: AppendEntriesArgs,
    handle: &mut Handle,
) -> (AppendEntriesReply, TransitToFollower) {
    trace!(?args);
    trace!(?handle);
    if args.term < handle.election.current_term() {
        trace!("term_ok = false");
        handle.election.voted_for = None;
        let reply = AppendEntriesReply {
            term: handle.election.current_term(),
            result: TermCheckFail,
        };
        (reply, false)
    } else {
        trace!("term_ok = true");
        handle.election.voted_for = Some(args.leader_id as NodeId);
        let index_remote_term_and_local_term =
            args.log_state
                .map(Into::<LogState>::into)
                .map(|remote_state| {
                    let index = remote_state.index;
                    (
                        index,
                        remote_state.term,
                        handle.logs.get(remote_state.index).map(|entry| entry.term),
                    )
                });
        let (result, new_log_begin) = match index_remote_term_and_local_term {
            None => {
                let new_log_begin = 0;
                let match_length = new_log_begin + args.entries.len();
                (Success { match_length }, Some(new_log_begin))
            }
            Some((index, remote_term, Some(local_term))) if remote_term == local_term => {
                let new_log_begin = index + 1;
                let match_length = new_log_begin + args.entries.len();
                (Success { match_length }, Some(new_log_begin))
            }
            Some((index, _, Some(local_term))) => (
                LogNotMatch {
                    term_conflicted: local_term,
                    first_index_of_term_conflicted: handle
                        .logs
                        .first_index_with_same_term_with(index),
                },
                None,
            ),
            Some((index, _, None)) => {
                assert!(index >= handle.logs.len());
                (
                    LogNotContainThisEntry {
                        log_len: handle.logs.len(),
                    },
                    None,
                )
            }
        };
        let reply = AppendEntriesReply {
            term: max(handle.election.current_term(), args.term),
            result,
        };
        // side effect
        async_side_effect(async {
            if let Some(new_log_begin) = new_log_begin {
                trace!("commit logs from {new_log_begin}");
                let entries: Vec<LogEntry> = args.entries.into_iter().map(Into::into).collect();
                handle.update_log_tail(new_log_begin, entries);
                let logs = handle.logs.commit_logs(args.leader_commit_length);
                Handle::apply_messages(&mut handle.apply_ch, logs).await;
            }
            handle.update_current_term(args.term);
        })
        .await;
        (reply, true)
    }
}
