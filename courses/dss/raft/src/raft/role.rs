use rand::{thread_rng, Rng};
use std::time::Duration;
use tokio::select;
use tokio::time::{sleep_until, Instant};

use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use crate::raft::candidate::Candidate;
use crate::raft::inner::{Handle, LocalTask};
use crate::raft::leader::{Leader, LogEntry, LogState};
use crate::raft::receive_task;

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
            let self_log_state = handle.logs.get_log_state();
            let other_log_state: Option<LogState> = args.log_state.map(Into::into);
            other_log_state >= self_log_state
        };
        let term_ok = {
            let current_term = handle.election.get_current_term();
            let voted_for = handle.election.voted_for;
            args.term > current_term
                || (args.term == args.term
                    && (voted_for.is_none() || voted_for == Some(args.candidate_id as usize)))
        };
        if log_ok && term_ok {
            handle.election.update_current_term(args.term);
            let new_role = Role::Follower(Follower::default());
            handle.election.voted_for = Some(args.candidate_id as usize);
            let response = RequestVoteReply {
                term: handle.election.get_current_term(),
                node_id: handle.node_id as u32,
                vote_granted: true,
            };
            (response, new_role)
        } else {
            let response = RequestVoteReply {
                term: handle.election.get_current_term(),
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
        if args.term < handle.election.get_current_term() {
            handle.election.voted_for = None;
            let reply = AppendEntriesReply {
                term: handle.election.get_current_term(),
                match_length: None,
                node_id: handle.node_id as u32,
            };
            (reply, self)
        } else {
            let new_role = if matches!(self, Role::Follower { .. }) {
                self
            } else {
                Role::Follower(Follower::default())
            };
            let remote_log_state = args.log_state.map(Into::<LogState>::into);
            let local_log_state = remote_log_state.and_then(|remote_state| {
                handle
                    .logs
                    .get(remote_state.index)
                    .map(|entry| entry.log_state)
            });
            let log_ok = remote_log_state == local_log_state;
            let match_length = if log_ok {
                let log_begin = remote_log_state.map_or(0, |state| state.index + 1);
                let entries: Vec<LogEntry> = args.entries.into_iter().map(Into::into).collect();
                let match_length = Some((log_begin + entries.len()) as u64);
                handle.logs.update_log_tail(log_begin, entries);
                let logs = handle
                    .logs
                    .commit_logs(args.leader_commit_length as usize)
                    .map(Clone::clone);
                Handle::apply_messages(&mut handle.apply_ch, logs).await;
                match_length
            } else {
                None
            };
            handle.election.update_current_term(args.term);
            let reply = AppendEntriesReply {
                term: handle.election.get_current_term(),
                match_length,
                node_id: handle.node_id as u32,
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
            sleep_until(Instant::now() + Duration::from_millis(thread_rng().gen_range(100..200)));
        select! {
            _ = failure_timer => {
                Role::Candidate(Candidate {})
            }
            Some(task) = handle.local_task_receiver.recv() => {
                match task {
                    LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                    LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).unwrap(),
                    LocalTask::CheckLeader(sender) => sender.send(false).unwrap(),
                }
                Role::Follower(self)
            }
            Some(task) = receive_task(&mut handle.remote_task_receiver, handle.election.get_current_term()) => task.handle(Role::Follower(self), handle).await
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower(Follower::default())
    }
}
