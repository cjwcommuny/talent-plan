use std::cmp::max;

use tracing::{instrument, trace};

use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use crate::raft::candidate::Candidate;
use crate::raft::common::side_effect;
use crate::raft::follower::Follower;

use crate::raft::handle::Handle;
use crate::raft::leader::{Leader, LogEntry, LogState};
use crate::raft::NodeId;

#[derive(Debug)]
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
    Stop,
}

impl Role {
    pub(crate) async fn progress(self, handle: &mut Handle) -> Role {
        match self {
            Role::Follower(follower) => follower.progress(handle).await,
            Role::Candidate(candidate) => candidate.progress(handle).await,
            Role::Leader(leader) => leader.progress(handle).await,
            Role::Stop => Role::Stop,
        }
    }

    #[instrument(skip_all, fields(node_id = handle.node_id), level = "trace")]
    pub fn request_vote(
        self,
        args: &RequestVoteArgs,
        handle: &mut Handle,
    ) -> (RequestVoteReply, Role) {
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
                    && (voted_for.is_none() || voted_for == Some(args.candidate_id as usize)))
        };
        let vote_granted = log_ok && term_ok;
        trace!(log_ok, term_ok, vote_granted);
        let new_role = if vote_granted || args.term > handle.election.current_term() {
            Role::Follower(Follower::default())
        } else {
            self
        };
        let reply = RequestVoteReply {
            term: max(handle.election.current_term(), args.term),
            node_id: handle.node_id as u32,
            vote_granted,
        };
        let result = (reply, new_role);
        side_effect(|| {
            if vote_granted {
                handle.election.voted_for = Some(args.candidate_id as usize);
            }
            if args.term > handle.election.current_term() {
                handle.update_current_term(args.term);
            }
        });
        result
    }

    #[instrument(skip_all, level = "trace")]
    pub async fn append_entries(
        self,
        args: AppendEntriesArgs,
        handle: &mut Handle,
    ) -> (AppendEntriesReply, Role) {
        trace!(?args);
        trace!(?handle);
        if args.term < handle.election.current_term() {
            trace!("term_ok = false");
            handle.election.voted_for = None;
            let reply = AppendEntriesReply {
                term: handle.election.current_term(),
                match_length: None,
                node_id: handle.node_id as u32,
            };
            (reply, self)
        } else {
            trace!("term_ok = true");
            handle.election.voted_for = Some(args.leader_id as NodeId);
            let remote_log_state: Option<LogState> = args.log_state.map(Into::into);
            let local_log_state = remote_log_state.and_then(|remote_state| {
                handle
                    .logs
                    .get(remote_state.index)
                    .map(|entry| LogState::new(entry.term, remote_state.index))
            });
            let log_ok = remote_log_state == local_log_state;
            trace!(log_ok, ?remote_log_state, ?local_log_state);
            let match_length = if log_ok {
                let new_log_begin = local_log_state.map_or(0, |state| state.index + 1);
                let entries: Vec<LogEntry> = args.entries.into_iter().map(Into::into).collect();
                let match_length = (new_log_begin + entries.len()) as u64;
                handle.update_log_tail(new_log_begin, entries);
                let logs = handle.logs.commit_logs(args.leader_commit_length as usize);
                Handle::apply_messages(&mut handle.apply_ch, logs).await;
                Some(match_length)
            } else {
                None
            };
            handle.update_current_term(args.term);
            let reply = AppendEntriesReply {
                term: handle.election.current_term(),
                match_length,
                node_id: handle.node_id as u32,
            };
            let new_role = Role::Follower(Follower::default());
            (reply, new_role)
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower(Follower::default())
    }
}
