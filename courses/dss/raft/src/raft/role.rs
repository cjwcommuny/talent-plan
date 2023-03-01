use futures::{pin_mut, stream, StreamExt};
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error, instrument};

use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use crate::raft::candidate::Candidate;
use crate::raft::inner::RemoteTaskResult;
use crate::raft::inner::{Handle, LocalTask};
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

    #[instrument(ret, level = "debug")]
    pub fn request_vote(
        self,
        handle: &mut Handle,
        args: &RequestVoteArgs,
    ) -> (RequestVoteReply, Role) {
        let log_ok = {
            let self_log_state = handle.logs.get_log_state();
            let candidate_log_state: Option<LogState> = args.log_state.map(Into::into);
            candidate_log_state >= self_log_state
        };
        let term_ok = {
            let current_term = handle.election.get_current_term();
            let voted_for = handle.election.voted_for;
            args.term > current_term
                || (args.term == current_term
                    && (voted_for.is_none() || voted_for == Some(args.candidate_id as usize)))
        };
        if log_ok && term_ok {
            handle.election.update_current_term(args.term);
            handle.election.voted_for = Some(args.candidate_id as usize);
            let response = RequestVoteReply {
                term: handle.election.get_current_term(),
                node_id: handle.node_id as u32,
                vote_granted: true,
            };
            let new_role = Role::Follower(Follower::default());
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

    #[instrument(skip(self), ret, level = "debug")]
    pub async fn append_entries(
        self,
        handle: &mut Handle,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Role) {
        assert!(args.leader_commit_length as usize >= handle.logs.get_commit_length());
        if args.term < handle.election.get_current_term() {
            handle.election.voted_for = None;
            let reply = AppendEntriesReply {
                term: handle.election.get_current_term(),
                match_length: None,
                node_id: handle.node_id as u32,
            };
            (reply, self)
        } else {
            handle.election.voted_for = Some(args.leader_id as NodeId);
            let remote_log_state: Option<LogState> = args.log_state.map(Into::into);
            let local_log_state = remote_log_state.and_then(|remote_state| {
                handle
                    .logs
                    .get(remote_state.index)
                    .map(|entry| entry.log_state)
            });
            let log_ok = remote_log_state == local_log_state;
            let match_length = if log_ok {
                let new_log_begin = local_log_state.map_or(0, |state| state.index + 1);
                let entries: Vec<LogEntry> = args.entries.into_iter().map(Into::into).collect();
                let match_length = (new_log_begin + entries.len()) as u64;
                handle.logs.update_log_tail(new_log_begin, entries);
                let logs = handle
                    .logs
                    .commit_logs(args.leader_commit_length as usize)
                    .map(Clone::clone);
                Handle::apply_messages(&mut handle.apply_ch, logs).await;
                Some(match_length)
            } else {
                None
            };
            handle.election.update_current_term(args.term);
            let reply = AppendEntriesReply {
                term: handle.election.get_current_term(),
                match_length,
                node_id: handle.node_id as u32,
            };
            let new_role = Role::Follower(Follower);
            (reply, new_role)
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower(Follower::default())
    }
}

#[derive(Debug, Default)]
pub struct Follower;

impl Follower {
    #[instrument(name = "Follower::progress", skip_all, ret, fields(node_id = handle.node_id), level = "debug")]
    pub async fn progress(self, handle: &mut Handle) -> Role {
        let failure_timer = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.heartbeat_failure_random_range.clone()),
        )));
        pin_mut!(failure_timer);
        let new_role: Role = loop {
            select! {
                _ = failure_timer.next() => {
                    debug!("failure timer timeout, transit to Candidate");
                    break Role::Candidate(Candidate);
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    if let None = match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).ok(), // not leader
                        LocalTask::GetTerm(sender) => sender.send(handle.election.get_current_term()).ok(),
                        LocalTask::CheckLeader(sender) => sender.send(false).ok(),
                        LocalTask::Shutdown(sender) => {
                            info!("shutdown");
                            sender.send(()).unwrap();
                            break Role::Stop;
                        }
                    } {
                        error!("local task response error");
                    }
                }
                Some(task) = handle.remote_task_receiver.recv() => {
                    let RemoteTaskResult { success, new_role } = task.handle(Role::Follower(Follower), handle).await;
                    if success {
                        break new_role; // restart failure timer
                    }
                }
            }
        };
        new_role
    }
}
