use crate::raft::leader::{LogEntry, LogState};
use crate::raft::{NodeId, TermId};
use derive_more::IsVariant;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub log_state: Option<LogState>,
    pub term: TermId,
    pub candidate_id: NodeId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: TermId,
    pub vote_granted: bool,
}

/// if `log_length == 0`, then `prev_log_term == 0`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: TermId,
    pub leader_id: NodeId,

    /// None if logs is empty
    pub log_state: Option<LogState>,
    pub entries: Vec<LogEntry>,
    pub leader_commit_length: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: TermId,
    pub result: AppendEntriesReplyResult,
}

#[derive(Serialize, Deserialize, Debug, IsVariant)]
pub enum AppendEntriesReplyResult {
    Success {
        match_length: usize,
    },
    LogNotMatch {
        term_conflicted: TermId,
        first_index_of_term_conflicted: usize,
    },
    LogNotContainThisEntry {
        log_len: usize,
    },
    TermCheckFail,
}
