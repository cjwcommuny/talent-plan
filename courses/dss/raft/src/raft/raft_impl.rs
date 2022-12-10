use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, LogStateMessage, RequestVoteArgs, RequestVoteReply,
};
use futures::channel::oneshot;

type NodeId = usize;
type TermId = u64;

#[derive(Debug)]
struct Log<T> {
    content: T,
    term: TermId,
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
struct LogState {
    term: TermId,
    index: usize,
}

pub enum Task {
    RequestVote {
        args: RequestVoteArgs,
        sender: oneshot::Sender<RequestVoteReply>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        sender: oneshot::Sender<AppendEntriesReply>,
    },
}

impl Into<LogState> for &LogStateMessage {
    fn into(self) -> LogState {
        LogState {
            term: self.last_log_term,
            index: self.last_log_index as usize,
        }
    }
}

#[derive(Debug, Default)]
struct PersistentState<A> {
    current_term: TermId,
    voted_for: Option<NodeId>,
    log: Vec<Log<A>>,
}

impl<A> PersistentState<A> {
    fn get_log_state(&self) -> Option<LogState> {
        self.log.last().map(|log| LogState {
            term: log.term,
            index: self.log.len() - 1,
        })
    }
}

#[derive(Debug, Default)]
struct VolatileServerState {
    commit_index: usize,
    last_applied: usize,
    role: Role,
}

#[derive(Debug)]
struct VolatileLeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

#[derive(Debug)]
enum Role {
    Follower,
    Candidate,
    Leader { leader_state: VolatileLeaderState },
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

#[derive(Debug, Default)]
pub struct RaftState<A> {
    persistent_state: PersistentState<A>,
    volatile_state: VolatileServerState,
}

impl<A> RaftState<A> {
    pub(crate) fn is_leader(&self) -> bool {
        match self.volatile_state.role {
            Role::Leader { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn get_term(&self) -> TermId {
        self.persistent_state.current_term
    }
}

fn request_vote<A>(
    mut state: RaftState<A>,
    node_id: NodeId,
    args: &RequestVoteArgs,
) -> (RequestVoteReply, RaftState<A>) {
    let log_ok = {
        let self_log_state = state.persistent_state.get_log_state();
        let other_log_state: Option<LogState> = args.log_state.as_ref().map(Into::into);
        other_log_state >= self_log_state
    };
    let term_ok = {
        let current_term = state.persistent_state.current_term;
        let voted_for = state.persistent_state.voted_for;
        current_term > args.term
            || (current_term == args.term
                && (voted_for.is_none() || voted_for == Some(args.candidate_id as usize)))
    };
    if log_ok && term_ok {
        state.persistent_state.current_term += 1;
        state.volatile_state.role = Role::Follower;
        state.persistent_state.voted_for = Some(args.candidate_id as usize);
        let response = RequestVoteReply {
            term: state.persistent_state.current_term,
            node_id: node_id as u32,
            vote_granted: true,
        };
        (response, state)
    } else {
        let response = RequestVoteReply {
            term: state.persistent_state.current_term,
            node_id: node_id as u32,
            vote_granted: false,
        };
        (response, state)
    }
}

fn append_entries<A>(
    mut state: RaftState<A>,
    args: &AppendEntriesArgs,
) -> (AppendEntriesReply, RaftState<A>) {
    todo!()
}
