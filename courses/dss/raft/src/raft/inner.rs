use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, LogEntryProst, RequestVoteArgs, RequestVoteReply,
};
use std::fmt::{Debug, Formatter};
use derive_new::new;
use std::ops::Range;
use crate::raft::role::Role;
use crate::raft::TermId;
use tokio::sync::oneshot;
use crate::raft::handle::Handle;

pub struct Config {
    pub heartbeat_cycle: u64,
    // NOTE: both follower and candidate need random timeout
    // followers need random timeout to prevent multiple followers to become candidates simultaneously
    // candidates need random timeout to prevent multiple candidates to restart votes collection simultaneously
    pub election_timeout: Range<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_cycle: 100,
            election_timeout: 200..500,
        }
    }
}

#[derive(new)]
pub struct Inner {
    role: Option<Role>,
    handle: Handle,
}

impl Inner {
    pub async fn raft_main(&mut self) {
        while let role = self.role.take().unwrap() && !matches!(role, Role::Stop) {
            let new_role = role.progress(&mut self.handle).await;
            self.role = Some(new_role);
        }
    }
}

pub enum RemoteTask {
    RequestVote {
        args: RequestVoteArgs,
        sender: oneshot::Sender<RequestVoteReply>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        sender: oneshot::Sender<AppendEntriesReply>,
    },
}

impl Debug for RemoteTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("RemoteTask");
        match self {
            Self::RequestVote { args, .. } => debug.field("RequestVoteArgs", args),
            Self::AppendEntries { args, .. } => debug.field("AppendEntiresArgs", args),
        };
        debug.finish()
    }
}

#[derive(new)]
pub struct RemoteTaskResult {
    pub success: bool,
    pub new_role: Role,
}

impl RemoteTask {
    pub async fn handle(self, role: Role, handle: &mut Handle) -> RemoteTaskResult {
        match self {
            RemoteTask::RequestVote { args, sender } => {
                let (reply, new_role) = role.request_vote(&args, handle);
                let result = if reply.vote_granted {
                    RemoteTaskResult::new(true, new_role)
                } else {
                    RemoteTaskResult::new(false, new_role)
                };
                sender.send(reply).unwrap();
                result
            }
            RemoteTask::AppendEntries { args, sender } => {
                let (reply, new_role) = role.append_entries(args, handle).await;
                let result = if reply.match_length.is_some() {
                    RemoteTaskResult::new(true, new_role)
                } else {
                    RemoteTaskResult::new(false, new_role)
                };
                sender.send(reply).unwrap();
                result
            }
        }
    }
}

pub enum LocalTask {
    AppendEntries {
        data: Vec<u8>,
        sender: oneshot::Sender<Option<(u64, u64)>>, // None if not leader
    },
    GetTerm(oneshot::Sender<TermId>),
    CheckLeader(oneshot::Sender<bool>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(prost::Message, new)]
pub struct PersistentState {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint32, optional, tag = "2")]
    pub voted_for: Option<u32>,
    #[prost(message, repeated, tag = "3")]
    pub log: Vec<LogEntryProst>,
}
