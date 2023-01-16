use crate::proto::raftpb::{
    AppendEntriesArgs, AppendEntriesReply, LogStateMessage, RequestVoteArgs, RequestVoteReply,
};
use crate::raft::ApplyMsg;
use futures::channel::oneshot;
use rand::{thread_rng, Rng};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::time::{sleep_until, Instant};

type NodeId = usize;
type TermId = u64;

struct Config;

impl Config {
    const ELECTION_TIMEOUT: Duration = Duration::from_millis(0); // TODO
}

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

impl From<&LogStateMessage> for LogState {
    fn from(x: &LogStateMessage) -> Self {
        LogState {
            term: x.last_log_term,
            index: x.last_log_index as usize,
        }
    }
}

#[derive(Debug, Default)]
struct PersistentState {
    current_term: TermId,
    voted_for: Option<NodeId>,
    log: Vec<Log<ApplyMsg>>,
}

impl PersistentState {
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
}

#[derive(Debug)]
struct VolatileLeaderState {
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

#[derive(Debug)]
enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

#[derive(Debug)]
struct Leader {
    state: VolatileLeaderState,
}

#[derive(Debug, Default)]
struct Follower {}

#[derive(Debug)]
struct Candidate {}

impl Default for Role {
    fn default() -> Self {
        Role::Follower(Follower::default())
    }
}

#[derive(Debug)]
pub struct RaftI {
    role: Role,
    except_role: ExceptRole,
}

#[derive(Debug)]
struct ExceptRole {
    node_id: NodeId,
    persistent_state: PersistentState,
    volatile_state: VolatileServerState,
    task_receiver: Receiver<Task>,
}

impl RaftI {
    fn new(role: Role, except_role: ExceptRole) -> RaftI {
        RaftI { role, except_role }
    }

    pub fn init(node_id: NodeId, task_receiver: Receiver<Task>) -> RaftI {
        RaftI::new(
            Role::default(),
            ExceptRole {
                node_id,
                persistent_state: PersistentState::default(),
                volatile_state: VolatileServerState::default(),
                task_receiver,
            },
        )
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    pub(crate) fn get_term(&self) -> TermId {
        self.except_role.persistent_state.current_term
    }
}

fn request_vote(state: RaftI, args: &RequestVoteArgs) -> (RequestVoteReply, RaftI) {
    let RaftI {
        role,
        mut except_role,
    } = state;
    let log_ok = {
        let self_log_state = except_role.persistent_state.get_log_state();
        let other_log_state: Option<LogState> = args.log_state.as_ref().map(Into::into);
        other_log_state >= self_log_state
    };
    let term_ok = {
        let current_term = except_role.persistent_state.current_term;
        let voted_for = except_role.persistent_state.voted_for;
        current_term > args.term
            || (current_term == args.term
                && (voted_for.is_none() || voted_for == Some(args.candidate_id as usize)))
    };
    if log_ok && term_ok {
        except_role.persistent_state.current_term += 1;
        let new_role = Role::Follower(Follower::default());
        except_role.persistent_state.voted_for = Some(args.candidate_id as usize);
        let response = RequestVoteReply {
            term: except_role.persistent_state.current_term,
            node_id: except_role.node_id as u32,
            vote_granted: true,
        };
        (response, RaftI::new(new_role, except_role))
    } else {
        let response = RequestVoteReply {
            term: except_role.persistent_state.current_term,
            node_id: except_role.node_id as u32,
            vote_granted: false,
        };
        (response, RaftI::new(role, except_role))
    }
}

fn append_entries(_state: RaftI, _args: &AppendEntriesArgs) -> (AppendEntriesReply, RaftI) {
    todo!()
}

async fn handle_follower(state: Follower, mut except_role: ExceptRole) -> RaftI {
    // TODO: random timeout, time function as dependency
    let failure_timer =
        sleep_until(Instant::now() + Duration::from_millis(thread_rng().gen_range(100, 200)));
    select! {
        _ = failure_timer => RaftI::new(Role::Candidate(Candidate {}), except_role),
        Some(message) = except_role.task_receiver.recv() => match message {
            Task::RequestVote { args, sender } => {
                let (reply, new_state) = request_vote(RaftI::new(Role::Follower(state), except_role), &args);
                sender.send(reply).unwrap();
                new_state
            }
            Task::AppendEntries { args, sender } => {
                sender.send(todo!()).unwrap();
                todo!()
            }
        }
    }
}

async fn handle_candidate(state: Candidate, mut except_role: ExceptRole) -> RaftI {
    todo!()
}

async fn handle_leader(state: Leader, mut except_role: ExceptRole) -> RaftI {
    todo!()
}

async fn raft_handle(state: RaftI) -> RaftI {
    let RaftI { role, except_role } = state;
    match role {
        Role::Follower(follower) => handle_follower(follower, except_role).await,
        Role::Candidate(candidate) => handle_candidate(candidate, except_role).await,
        Role::Leader(leader) => handle_leader(leader, except_role).await,
    }
}

async fn raft_main(mut state: RaftI) {
    loop {
        state = raft_handle(state).await;
    }
}
