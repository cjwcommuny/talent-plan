use crate::raft::role::Role;
use futures::channel::oneshot::Canceled;
use futures::TryFutureExt;
use handle::election::Election;
use handle::Handle;
use handle::Logs;
use inner::RemoteTask;
use labrpc::Error::{Other, Recv};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, instrument};

mod candidate;
mod common;
pub mod errors;
mod follower;
mod handle;
mod inner;
mod leader;
mod message_handler;
pub mod persister;
mod role;
mod rpc;
mod sender;
mod sink;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::common::set_panic_with_log;

use crate::raft::inner::{Config, Inner, LocalTask};
use crate::raft::message_handler::MessageHandler;

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

pub type NodeId = usize;
type TermId = u64;

// A single Raft peer.
pub struct Raft {
    /// for RPC calls
    remote_task_sender: mpsc::Sender<RemoteTask>,
    local_task_sender: mpsc::Sender<LocalTask>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
    node_id: NodeId,
}

impl Drop for Raft {
    #[instrument(skip_all)]
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            handle.join().map_err(|_| error!("thread panic")).ok();
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
        info!("start Raft {node_id}");

        // Your initialization code here (2A, 2B, 2C).
        let (remote_task_sender, remote_task_receiver) = mpsc::channel(Self::BUFFER_SIZE);
        let (local_task_sender, local_task_receiver) = mpsc::channel(Self::BUFFER_SIZE);

        let raft_runtime = Runtime::new().unwrap();

        let persistent_state = Handle::restore(persister.raft_state());
        let handle_builder = Handle::builder()
            .node_id(node_id)
            .persister(persister)
            .apply_ch(apply_ch)
            .random_generator(Box::new(StdRng::from_entropy()))
            .config(Config::default());
        let handle = if let Some(persistent_state) = persistent_state {
            handle_builder
                .election(Election::new(
                    persistent_state.term,
                    persistent_state.voted_for.map(|id| id as NodeId),
                ))
                .logs(Logs::with_logs(persistent_state.log.into_iter().collect()))
                .build()
        } else {
            handle_builder
                .election(Election::default())
                .logs(Logs::default())
                .build()
        };
        let message_handler = MessageHandler::new(
            peers
                .into_iter()
                .map(|client| Box::new(client) as _)
                .collect::<Vec<_>>()
                .into(),
            remote_task_receiver.into(),
            local_task_receiver.into(),
        );

        // different node should has different seeds
        let join_handle = std::thread::spawn(move || {
            set_panic_with_log();
            let raft_inner = Inner::new(Role::default(), handle, message_handler);
            raft_runtime.block_on(raft_inner.raft_main());
        });

        Raft {
            remote_task_sender,
            local_task_sender,
            thread_handle: Some(join_handle),
            node_id,
        }
    }

    #[allow(dead_code)]
    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    #[allow(dead_code)]
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
        let mut buffer = Vec::new();
        command.encode(&mut buffer).map_err(Error::Encode)?;
        blocking_pass_message(&self.raft.local_task_sender, |sender| {
            LocalTask::AppendEntries {
                data: buffer,
                sender,
            }
        })
        // The test code uses indices starting from 1, while the implementation uses indices starting from 0.
        .map(|option_tuple| option_tuple.map(|(index, term)| (index + 1, term)))
        .map_err(Error::Rpc)
        .and_then(|result| result.ok_or(Error::NotLeader))
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        blocking_pass_message(&self.raft.local_task_sender, LocalTask::GetTerm)
            .unwrap_or_else(|_| panic!("get term failed in node {}", self.raft.node_id))
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        blocking_pass_message(&self.raft.local_task_sender, LocalTask::CheckLeader)
            .unwrap_or_else(|_| panic!("check is leader failed in node {}", self.raft.node_id))
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
    #[instrument(skip_all)]
    pub fn kill(&self) {
        // Your code here, if desired.
        blocking_pass_message(&self.raft.local_task_sender, LocalTask::Shutdown)
            .map_err(|e| error!("in node {}: {}", self.raft.node_id, e.to_string()))
            .ok();
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
    #[allow(dead_code)]
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
    async fn request_vote(
        &self,
        args: RequestVoteArgsProst,
    ) -> labrpc::Result<RequestVoteReplyProst> {
        pass_message(&self.raft.remote_task_sender, |sender| {
            RemoteTask::RequestVote {
                args: decode(&args.data),
                sender,
            }
        })
        .await
        .map(|reply| RequestVoteReplyProst {
            data: encode(&reply),
        })
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgsProst,
    ) -> labrpc::Result<AppendEntriesReplyProst> {
        pass_message(&self.raft.remote_task_sender, |sender| {
            RemoteTask::AppendEntries {
                args: decode(&args.data),
                sender,
            }
        })
        .await
        .map(|reply| AppendEntriesReplyProst {
            data: encode(&reply),
        })
    }
}

/// if we have algebraic effects we can unify
///`blocking_pass_message` and `pass_message`
fn blocking_pass_message<F, R, M>(
    message_sender: &mpsc::Sender<M>,
    message_constructor: F,
) -> labrpc::Result<R>
where
    F: FnOnce(oneshot::Sender<R>) -> M,
    R: Send,
{
    let (sender, receiver) = oneshot::channel();
    message_sender
        .blocking_send(message_constructor(sender))
        .map_err(|_| Other(String::from("sender error")))?;
    receiver.blocking_recv().map_err(|_| Recv(Canceled))
}

async fn pass_message<F, R, M>(
    message_sender: &mpsc::Sender<M>,
    message_constructor: F,
) -> labrpc::Result<R>
where
    F: FnOnce(oneshot::Sender<R>) -> M,
    R: Send,
{
    let (sender, receiver) = oneshot::channel();
    message_sender
        .send(message_constructor(sender))
        .map_err(|_| Other(String::from("sender error")))
        .await?;
    receiver.await.map_err(|_| Recv(Canceled))
}
