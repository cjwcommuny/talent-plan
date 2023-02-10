use futures::channel::oneshot::Canceled;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use std::collections::HashSet;
use std::convert::identity;
use std::future::Future;
use std::result;
use std::sync::Arc;
use std::time::Duration;

use futures::TryFutureExt;
use labrpc::Error::{Other, Recv};
use num::integer::div_ceil;
use rand::{thread_rng, Rng};
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_blocking;
use tokio::time::{interval, sleep_until, Instant};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::errors::Error::NotLeader;
use crate::raft::Task::AppendEntries;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Debug)]
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

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

impl<T> Log<T> {
    fn new(content: T, term: TermId) -> Self {
        Log { content, term }
    }
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord)]
struct LogState {
    term: TermId,
    index: usize,
}

pub enum Task {
    RequestVote {
        args: RequestVoteArgs,
        sender: futures::channel::oneshot::Sender<RequestVoteReply>,
    },
    AppendEntries {
        args: AppendEntriesArgs,
        sender: futures::channel::oneshot::Sender<AppendEntriesReply>,
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

impl VolatileLeaderState {
    fn new(log_length: usize, num_servers: usize) -> Self {
        VolatileLeaderState {
            next_index: vec![log_length; num_servers],
            match_index: vec![0; num_servers],
        }
    }
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

impl Leader {
    fn new(state: VolatileLeaderState) -> Self {
        Leader { state }
    }
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

struct Handle {
    // RPC end points of all peers
    peers: Vec<RaftClient>,

    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,

    // this peer's index into peers[]
    node_id: usize,

    // states
    volatile_state: VolatileServerState,
    persistent_state: PersistentState,

    // async task queue
    task_sender: mpsc::Sender<Task>,
    task_receiver: mpsc::Receiver<Task>,

    // entry task queue
    entry_task_sender: mpsc::Sender<(Vec<u8>, futures::channel::oneshot::Sender<(u64, u64)>)>,
    entry_task_receiver: mpsc::Receiver<(Vec<u8>, futures::channel::oneshot::Sender<(u64, u64)>)>,
}

// A single Raft peer.
pub struct Raft {
    role: Option<Role>,
    handle: Handle,
}

impl Raft {
    const BUFFER_SIZE: usize = 32; // TODO: put it in the config

    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        node_id: NodeId,
        persister: Box<dyn Persister>,
        apply_ch: futures::channel::mpsc::UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let (task_sender, task_receiver) = mpsc::channel(Self::BUFFER_SIZE);
        let (entry_task_sender, entry_task_receiver) = mpsc::channel(Self::BUFFER_SIZE);
        let mut rf = Raft {
            role: Some(Role::default()),
            handle: Handle {
                peers,
                persister,
                node_id,
                volatile_state: VolatileServerState::default(),
                persistent_state: PersistentState::default(),
                task_sender,
                task_receiver,
                entry_task_sender,
                entry_task_receiver
            },
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        crate::your_code_here((rf, apply_ch)) // TODO
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.cond_install_snapshot(0, 0, &[]);
        self.snapshot(0, &[]);
        self.persist();
        let _ = &self.handle.persister;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Raft>,
    runtime: Arc<Runtime>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            raft: Arc::new(raft),
            runtime: Arc::new(Runtime::new().unwrap()),
        };
        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if let Some(Role::Leader(leader)) = &self.raft.role {
            self.runtime.block_on(leader.add_entry(&self.raft.handle, command))
            // TODO: is block_on corret?
            // TODO: is `(u64, u64)` returned by Raft or returned directly
        } else {
            Err(NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.handle.persistent_state.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        matches!(self.raft.role, Some(Role::Leader { .. }))
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        add_message_to_queue(&self.raft.handle.task_sender, |sender| Task::RequestVote {
            args,
            sender,
        })
        .await
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        add_message_to_queue(&self.raft.handle.task_sender, |sender| {
            Task::AppendEntries { args, sender }
        })
        .await
    }
}

async fn add_message_to_queue<F, R, M>(
    message_sender: &mpsc::Sender<M>,
    message_constructor: F,
) -> labrpc::Result<R>
where
    F: FnOnce(futures::channel::oneshot::Sender<R>) -> M,
{
    let (sender, receiver) = futures::channel::oneshot::channel();
    message_sender
        .send(message_constructor(sender))
        .map_err(|_| Other(String::from("sender error")))
        .await?;
    receiver.await.map_err(Recv)
}

impl Raft {
    fn append_entries(&mut self, _args: &AppendEntriesArgs) -> AppendEntriesReply {
        todo!()
    }

    async fn raft_main(&mut self) {
        loop {
            let role = self.role.take().unwrap();
            let new_role = Self::handle(role, &mut self.handle).await;
            self.role = Some(new_role);
        }
    }

    async fn handle(role: Role, handle: &mut Handle) -> Role {
        match role {
            Role::Follower(follower) => handle_follower(follower, handle).await,
            Role::Candidate(candidate) => handle_candidate(candidate, handle).await,
            Role::Leader(leader) => handle_leader(leader, handle).await,
        }
    }
}

fn request_vote(
    role: Role,
    handle: &mut Handle,
    args: &RequestVoteArgs,
) -> (RequestVoteReply, Role) {
    let log_ok = {
        let self_log_state = handle.persistent_state.get_log_state();
        let other_log_state: Option<LogState> = args.log_state.as_ref().map(Into::into);
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
        (response, role)
    }
}

async fn handle_follower(role: Follower, handle: &mut Handle) -> Role {
    // TODO: random timeout, time function as dependency
    let failure_timer =
        sleep_until(Instant::now() + Duration::from_millis(thread_rng().gen_range(100, 200)));
    select! {
        _ = failure_timer => {
            Role::Candidate(Candidate {})
        }
        Some(task) = handle.task_receiver.recv() => handle_task(task, Role::Follower(role), handle)
    }
}

fn handle_task(task: Task, role: Role, handle: &mut Handle) -> Role {
    match task {
        Task::RequestVote { args, sender } => {
            let (reply, new_role) = request_vote(role, handle, &args);
            sender.send(reply).unwrap();
            new_role
        }
        Task::AppendEntries { args, sender } => {
            sender.send(todo!()).unwrap();
        }
    }
}

enum ValidatedTask {
    Illegal, // term less than or equal to the current term
    Legal { term: TermId, task: Task },
}

fn validate_task(current_term: TermId) -> impl FnOnce(Task) -> ValidatedTask {
    move |task| match task {
        Task::RequestVote { args, sender } if args.term > current_term => ValidatedTask::Legal {
            term: args.term,
            task: Task::RequestVote { args, sender },
        },
        Task::AppendEntries { args, sender } if args.term > current_term => ValidatedTask::Legal {
            term: args.term,
            task: Task::AppendEntries { args, sender },
        },
        _ => ValidatedTask::Illegal,
    }
}

async fn handle_candidate(role: Candidate, handle: &mut Handle) -> Role {
    handle.persistent_state.current_term += 1;
    handle.persistent_state.voted_for = Some(handle.node_id);
    let log_state = handle
        .persistent_state
        .log
        .last()
        .map(|log| LogStateMessage {
            last_log_index: (handle.persistent_state.log.len() - 1) as u32,
            last_log_term: log.term,
        });
    let args = RequestVoteArgs {
        log_state,
        term: handle.persistent_state.current_term,
        candidate_id: handle.node_id as u32,
    };
    let replies = handle.peers.iter().map(|peer| {
        send_task(peer.clone(), args, RaftClient::request_vote).map(|result| {
            result
                .map_err(|_| Error::Rpc(Recv(Canceled)))
                .and_then(identity)
        })
    });
    // TODO: timer parameters as config
    let election_timer = sleep_until(Instant::now() + Duration::from_millis(200));
    let electoral_threshold = div_ceil(handle.peers.len() + 1, 2);
    let current_term = handle.persistent_state.current_term;
    select! {
        _ = election_timer => {
            Role::Candidate(role)
        }
        Some(ValidatedTask::Legal { term ,task }) = handle.task_receiver.recv().map(|option| option.map(validate_task(current_term))) => {
            handle.persistent_state.current_term = term;
            handle.persistent_state.voted_for = None;
            handle_task(task, Role::Candidate(role), handle)
        }
        vote_result = collect_vote(replies, electoral_threshold, handle.persistent_state.current_term) => match vote_result {
            VoteResult::Elected => {
                Role::Leader(Leader::new(VolatileLeaderState::new(handle.persistent_state.log.len(), handle.peers.len())))
            }
            VoteResult::Unsuccess => {
                Role::Candidate(role)
            }
            VoteResult::FoundLargerTerm(new_term) => {
                handle.persistent_state.current_term = new_term;
                handle.persistent_state.voted_for = None;
                Role::Follower(Follower::default())
            }
        }

    }
}

enum VoteResult {
    Elected,
    Unsuccess, // all peers reply but none of the reply is legal
    FoundLargerTerm(TermId),
}

async fn collect_vote<F>(
    replies: impl Iterator<Item = F>,
    electoral_threshold: usize,
    current_term: TermId,
) -> VoteResult
where
    F: Future<Output = Result<RequestVoteReply>>,
{
    let mut replies: FuturesUnordered<_> = replies.collect();
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

async fn handle_leader(mut role: Leader, handle: &mut Handle) -> Role {
    let mut rpc_replies: FuturesUnordered<_> = replicate_logs_followers(&role, handle).collect();
    let mut heartbeat_timer = interval(Duration::from_millis(100)); // TODO: move to config
    // let entry_stream = &handle.entry_task_receiver;
    // let rpc_task_stream = &handle.task_receiver;
    // let append_entries_rpc_reply_stream = ;
    // TODO: merge heartbeat stream and broadcast request stream
    loop {
        let current_term = handle.persistent_state.current_term;
        select! {
            _ = heartbeat_timer.tick() => {
                for future in replicate_logs_followers(&role, handle) {
                    rpc_replies.push(future);
                }
            }
            Some(result) = rpc_replies.next() => {
                let reply: AppendEntriesReply = result.unwrap().unwrap(); // TODO: add logging
                match on_receive_append_entries_reply(reply) {
                    AppendEntriesResult::Commit => {
                        commit_log_entries()
                    }
                    AppendEntriesResult::Retry(node_id) => {
                        role.state.next_index[node_id] -= 1; // TODO: independent retry strategy
                        let future = replicate_log(&role, handle, node_id);
                        rpc_replies.push(future);
                    }
                    AppendEntriesResult::FoundLargerTerm(new_term) => {
                        handle.persistent_state.current_term = new_term;
                        return Role::Follower(Follower::default());
                    }
                }
            }
            Some((data, sender)) = handle.entry_task_receiver.recv() => {
                let index = handle.persistent_state.log.len() as u64;
                let current_term = handle.persistent_state.current_term;
                sender.send((index, current_term)).unwrap();
                handle.persistent_state.log.push(Log::new(ApplyMsg::Command { data, index }, current_term));
                for future in replicate_logs_followers(&role, handle) {
                    rpc_replies.push(future);
                }
            }
            Some(ValidatedTask::Legal { term ,task }) = handle.task_receiver.recv().map(|option| option.map(validate_task(current_term))) => {
                handle.persistent_state.current_term = term;
                handle.persistent_state.voted_for = None;
                let new_role = handle_task(task, Role::Leader(role), handle);
                if let Role::Leader(new_role) = new_role {
                    role = new_role;
                } else {
                    return new_role;
                }
            }
            else => { // no more replies
                todo!()
            }
        }
    }
    todo!()
}

fn commit_log_entries() {
    todo!()
}

enum AppendEntriesResult {
    FoundLargerTerm(TermId),
    Commit,
    Retry(NodeId),
}

fn on_receive_append_entries_reply(reply: AppendEntriesReply) -> AppendEntriesResult {
    todo!()
}

/// example code to send a RequestVote RPC to a server.
/// server is the index of the target server in peers.
/// expects RPC arguments in args.
///
/// The labrpc package simulates a lossy network, in which servers
/// may be unreachable, and in which requests and replies may be lost.
/// This method sends a request and waits for a reply. If a reply arrives
/// within a timeout interval, This method returns Ok(_); otherwise
/// this method returns Err(_). Thus this method may not return for a while.
/// An Err(_) return can be caused by a dead server, a live server that
/// can't be reached, a lost request, or a lost reply.
///
/// This method is guaranteed to return (perhaps after a delay) *except* if
/// the handler function on the server side does not return.  Thus there
/// is no need to implement your own timeouts around this method.
///
/// look at the comments in ../labrpc/src/lib.rs for more details.
fn send_task<A, R, Call, F>(
    peer: RaftClient,
    args: A,
    rpc_call: Call,
) -> oneshot::Receiver<Result<R>>
where
    Call: FnOnce(&RaftClient, &A) -> F + Send + 'static,
    F: Future<Output = labrpc::Result<R>> + Send,
    A: Send + Sync + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let peer_clone = peer.clone();
    peer.spawn(async move {
        let result = rpc_call(&peer_clone, &args).await.map_err(Error::Rpc);
        let _ = tx.send(result);
    });
    rx
}

fn replicate_log(
    leader: &Leader,
    handle: &Handle,
    node_id: NodeId,
) -> oneshot::Receiver<Result<AppendEntriesReply>> {
    let prev_log_index = leader.state.next_index[node_id];
    let prev_log_term = if prev_log_index > 0 {
        handle.persistent_state.log[prev_log_index - 1].term
    } else {
        0
    };
    let args = AppendEntriesArgs {
        term: handle.persistent_state.current_term,
        leader_id: handle.node_id as u64,
        prev_log_index: prev_log_index as u32,
        prev_log_term,
    };
    let peer = handle.peers[node_id].clone();
    send_task(peer, args, RaftClient::append_entries)
}

fn replicate_logs_followers<'a>(
    leader: &'a Leader,
    handle: &'a Handle,
) -> impl Iterator<Item = oneshot::Receiver<Result<AppendEntriesReply>>> + 'a {
    (0..handle.peers.len())
        .filter(move |node_id| *node_id != handle.node_id)
        .map(move |node_id| replicate_log(leader, handle, node_id))
}

impl Leader {
    async fn add_entry<M>(&self, handle: &Handle, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut buffer = Vec::new();
        command.encode(&mut buffer).map_err(|e| Error::Encode(e))?;
        add_message_to_queue(&handle.entry_task_sender, |sender| (buffer, sender))
            .await
            .map_err(Error::Rpc)
    }
}
