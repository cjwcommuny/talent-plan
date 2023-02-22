use crate::raft::role::Role;
use futures::channel::oneshot::Canceled;
use futures::FutureExt;
use futures::TryFutureExt;
use inner::{Handle, RemoteTask};
use labrpc::Error::{Other, Recv};
use logs::Logs;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

mod candidate;
mod common;
#[cfg(test)]
pub mod config;
pub mod errors;
mod inner;
mod leader;
mod logs;
pub mod persister;
mod role;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

use crate::raft::inner::{Election, LocalTask, RaftInner};
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

#[derive(Debug, Default)]
pub struct PersistentState {
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
pub struct VolatileServerState {
    commit_index: usize,
    last_applied: usize,
}

// A single Raft peer.
pub struct Raft {
    remote_task_sender: mpsc::Sender<RemoteTask>,
    local_task_sender: mpsc::Sender<LocalTask>,
    handle: Option<std::thread::JoinHandle<()>>,
    runtime: Runtime,
}

impl Raft {
    pub(crate) async fn add_entry<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut buffer = Vec::new();
        command.encode(&mut buffer).map_err(|e| Error::Encode(e))?;
        pass_message(&self.local_task_sender, |sender| LocalTask::AppendEntries {
            data: buffer,
            sender,
        })
        .await
        .map_err(Error::Rpc)
        .and_then(|result| result.ok_or(Error::NotLeader))
    }
}

impl Drop for Raft {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap()
        }
    }
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
        let (remote_task_sender, remote_task_receiver) = mpsc::channel(Self::BUFFER_SIZE);
        let (local_task_sender, local_task_receiver) = mpsc::channel(Self::BUFFER_SIZE);

        let mut raft_inner = RaftInner::new(
            Some(Role::default()),
            Handle::new(
                node_id,
                persister,
                Election::default(),
                Logs::default(),
                apply_ch,
                peers,
                remote_task_receiver,
                local_task_receiver,
            ),
        );

        let raft_runtime = Runtime::new().unwrap();
        let handle = std::thread::spawn(move || {
            raft_runtime.block_on(raft_inner.raft_main());
        });

        let mut raft = Raft {
            remote_task_sender,
            local_task_sender,
            handle: Some(handle),
            runtime: Runtime::new().unwrap(),
        };

        // initialize from state persisted before a crash
        raft.restore(&raft_state);
        raft
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
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        Node {
            raft: Arc::new(raft),
        }
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
        self.raft.runtime.block_on(self.raft.add_entry(command))
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft
            .runtime
            .block_on(pass_message(
                &self.raft.local_task_sender,
                LocalTask::GetTerm,
            ))
            .unwrap()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft
            .runtime
            .block_on(pass_message(
                &self.raft.local_task_sender,
                LocalTask::CheckLeader,
            ))
            .unwrap()
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
        pass_message(&self.raft.remote_task_sender, |sender| {
            RemoteTask::RequestVote { args, sender }
        })
        .await
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        pass_message(&self.raft.remote_task_sender, |sender| {
            RemoteTask::AppendEntries { args, sender }
        })
        .await
    }
}

async fn pass_message<F, R, M>(
    message_sender: &mpsc::Sender<M>,
    message_constructor: F,
) -> labrpc::Result<R>
where
    F: FnOnce(tokio::sync::oneshot::Sender<R>) -> M,
{
    let (sender, receiver) = tokio::sync::oneshot::channel();
    message_sender
        .send(message_constructor(sender))
        .map_err(|_| Other(String::from("sender error")))
        .await?;
    receiver.await.map_err(|_| Recv(Canceled))
}

///a task is legal iff the term is larger than the current term
pub struct LegalTask(RemoteTask);

impl LegalTask {
    pub fn get_term(&self) -> TermId {
        self.0.get_term()
    }

    pub fn validate(current_term: TermId) -> impl FnOnce(RemoteTask) -> Option<LegalTask> {
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
            RemoteTask::RequestVote { args, sender } => {
                let (reply, new_role) = role.request_vote(handle, &args);
                sender.send(reply).unwrap();
                new_role
            }
            RemoteTask::AppendEntries { args, sender } => {
                let (reply, new_role) = role.append_entries(handle, args).await;
                sender.send(reply).unwrap();
                new_role
            }
        }
    }
}

pub async fn receive_task(
    receiver: &mut mpsc::Receiver<RemoteTask>,
    current_term: TermId,
) -> Option<LegalTask> {
    receiver
        .recv()
        .map(|option| option.and_then(LegalTask::validate(current_term)))
        .await
}
