use futures::FutureExt;

use std::sync::Arc;
use std::time::Duration;

use futures::TryFutureExt;
use labrpc::Error::{Other, Recv};

use tokio::runtime::Runtime;

use tokio::sync::mpsc;

use crate::raft::role::Role;

mod candidate;
#[cfg(test)]
pub mod config;
pub mod errors;
mod leader;
pub mod persister;
mod role;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::errors::Error::NotLeader;
use crate::raft::leader::{LogEntry, LogState};

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
#[derive(Debug, Clone)]
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

impl Task {
    pub fn get_term(&self) -> TermId {
        match self {
            Task::RequestVote { args, sender: _ } => args.term,
            Task::AppendEntries { args, sender: _ } => args.term,
        }
    }
}

#[derive(Debug, Default)]
struct PersistentState {
    current_term: TermId,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
}

impl PersistentState {
    fn get_log_state(&self) -> Option<LogState> {
        self.log
            .last()
            .map(|log| LogState::new(log.log_state.term, self.log.len() - 1))
    }
}

#[derive(Debug, Default)]
struct VolatileServerState {
    commit_index: usize,
    last_applied: usize,
}

pub struct Handle {
    // RPC end points of all peers
    peers: Vec<RaftClient>,

    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,

    // application
    apply_ch: futures::channel::mpsc::UnboundedSender<ApplyMsg>,

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
                apply_ch,
                volatile_state: VolatileServerState::default(),
                persistent_state: PersistentState::default(),
                task_sender,
                task_receiver,
                entry_task_sender,
                entry_task_receiver,
            },
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
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
            self.runtime
                .block_on(leader.add_entry(&self.raft.handle, command))
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
            let new_role = role.transit(&mut self.handle).await;
            self.role = Some(new_role);
        }
    }
}

///a task is legal iff the term is larger than the current term
pub struct LegalTask(Task);

impl LegalTask {
    pub fn get_term(&self) -> TermId {
        self.0.get_term()
    }

    pub fn validate(current_term: TermId) -> impl FnOnce(Task) -> Option<LegalTask> {
        move |task| {
            if task.get_term() > current_term {
                Some(LegalTask(task))
            } else {
                None
            }
        }
    }

    pub async fn handle(self, role: Role, handle: &mut Handle) -> Role {
        match self.0 {
            Task::RequestVote { args, sender } => {
                let (reply, new_role) = role.request_vote(handle, &args);
                sender.send(reply).unwrap();
                new_role
            }
            Task::AppendEntries { args, sender } => {
                let (reply, new_role) = role.append_entries(handle, args).await;
                sender.send(reply).unwrap();
                new_role
            }
        }
    }
}

pub async fn receive_task(
    receiver: &mut mpsc::Receiver<Task>,
    current_term: TermId,
) -> Option<LegalTask> {
    receiver
        .recv()
        .map(|option| option.and_then(LegalTask::validate(current_term)))
        .await
}
