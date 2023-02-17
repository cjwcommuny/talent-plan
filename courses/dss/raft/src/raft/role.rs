use rand::{thread_rng, Rng};
use std::time::Duration;
use tokio::select;
use tokio::time::{sleep_until, Instant};

use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use futures::FutureExt;

use crate::raft::{receive_task, Handle};

use crate::raft::candidate::Candidate;
use crate::raft::leader::{append_entries_in_powerpoint, Leader, LogEntry, LogState};

#[derive(Debug)]
pub enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl Role {
    pub(crate) async fn transit(self, handle: &mut Handle) -> Role {
        match self {
            Role::Follower(follower) => follower.transit(handle).await,
            Role::Candidate(candidate) => candidate.transit(handle).await,
            Role::Leader(leader) => leader.transit(handle).await,
        }
    }

    pub fn request_vote(
        self,
        handle: &mut Handle,
        args: &RequestVoteArgs,
    ) -> (RequestVoteReply, Role) {
        let log_ok = {
            let self_log_state = handle.persistent_state.get_log_state();
            let other_log_state: Option<LogState> = args.log_state.map(Into::into);
            other_log_state >= self_log_state
        };
        let term_ok = {
            let current_term = handle.persistent_state.current_term;
            let voted_for = handle.persistent_state.voted_for;
            current_term > args.term
                || (current_term == args.term
                    && (voted_for.is_none() || voted_for == Some(args.candidate_id as usize)))
        };
        if log_ok && term_ok {
            handle.persistent_state.current_term += 1;
            let new_role = Role::Follower(Follower::default());
            handle.persistent_state.voted_for = Some(args.candidate_id as usize);
            let response = RequestVoteReply {
                term: handle.persistent_state.current_term,
                node_id: handle.node_id as u32,
                vote_granted: true,
            };
            (response, new_role)
        } else {
            let response = RequestVoteReply {
                term: handle.persistent_state.current_term,
                node_id: handle.node_id as u32,
                vote_granted: false,
            };
            (response, self)
        }
    }

    pub async fn append_entries(
        self,
        handle: &mut Handle,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Role) {
        if args.term < handle.persistent_state.current_term {
            handle.persistent_state.voted_for = None;
            let reply = AppendEntriesReply {
                term: handle.persistent_state.current_term,
                success: false,
            };
            (reply, self)
        } else {
            let new_role = if matches!(self, Role::Follower { .. }) {
                Role::Follower(Follower::default())
            } else {
                self
            };
            let remote_log_state = args.log_state.map(Into::<LogState>::into);
            let local_log_state = remote_log_state
                .map(|remote_state| handle.persistent_state.log[remote_state.index].log_state);
            let log_ok = remote_log_state == local_log_state;
            if log_ok {
                let log_begin = remote_log_state.map_or(0, |state| state.index + 1);
                let entries: Vec<LogEntry> = args.entries.into_iter().map(Into::into).collect();
                append_entries_in_powerpoint(
                    handle,
                    log_begin,
                    entries,
                    args.leader_commit_index as usize,
                )
                .await;
            }
            handle.persistent_state.current_term = args.term;
            let reply = AppendEntriesReply {
                term: handle.persistent_state.current_term,
                success: log_ok,
            };
            (reply, new_role)
        }
    }
}

#[derive(Debug, Default)]
pub struct Follower {}

impl Follower {
    pub async fn transit(self, handle: &mut Handle) -> Role {
        // TODO: random timeout, time function as dependency
        let failure_timer =
            sleep_until(Instant::now() + Duration::from_millis(thread_rng().gen_range(100, 200)));
        select! {
            _ = failure_timer => {
                Role::Candidate(Candidate {})
            }
            Some(task) = receive_task(&mut handle.task_receiver, handle.persistent_state.current_term) => task.handle(Role::Follower(self), handle).await
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower(Follower::default())
    }
}
