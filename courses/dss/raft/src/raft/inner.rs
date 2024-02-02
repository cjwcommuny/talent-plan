use crate::proto::raftpb::{
    decode, encode, AppendEntriesArgsProst, RaftClient, RequestVoteArgsProst,
};
use crate::raft;
use crate::raft::handle::Handle;
use crate::raft::role::{append_entries, request_vote, Role};
use crate::raft::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::raft::{NodeId, TermId};
use async_trait::async_trait;

use derive_new::new;
use futures::SinkExt;
use futures::{FutureExt, TryFutureExt};
use futures::{Sink, Stream};

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use crate::raft::message_handler::MessageHandler;
use tokio::sync::oneshot;

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
            heartbeat_cycle: 50,
            election_timeout: 200..500,
        }
    }
}

#[derive(new)]
pub struct Inner {
    role: Role,
    handle: Handle,
    message_handler: MessageHandler,
}

impl Inner {
    pub async fn raft_main(self) {
        let Inner {
            mut role,
            mut handle,
            mut message_handler,
        } = self;

        while !matches!(role, Role::Shutdown) {
            role = role.progress(&mut handle, &mut message_handler).await;
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
    pub transit_to_follower: bool,
}

impl RemoteTask {
    pub async fn handle(self, handle: &mut Handle) -> RemoteTaskResult {
        match self {
            RemoteTask::RequestVote { args, sender } => {
                let (reply, transit_to_follower) = request_vote(&args, handle);
                sender.send(reply).unwrap();
                RemoteTaskResult::new(transit_to_follower)
            }
            RemoteTask::AppendEntries { args, sender } => {
                let (reply, transit_to_follower) = append_entries(args, handle).await;
                sender.send(reply).unwrap();
                RemoteTaskResult::new(transit_to_follower)
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

/// RPC end points of all peers
#[async_trait]
pub trait PeerEndPoint {
    async fn request_vote(&self, args: RequestVoteArgs) -> raft::Result<RequestVoteReply>;
    async fn append_entries(&self, args: AppendEntriesArgs) -> raft::Result<AppendEntriesReply>;
}

#[derive(Debug, new)]
pub struct AppendEntriesContext {
    pub follower_id: NodeId,
    pub old_next_index: usize,
}

pub type RequestVote<T> = (T, NodeId);
pub type AppendEntries<T> = (T, AppendEntriesContext);

pub trait ClientChannel {
    type RequestVoteSink<'a, S>: Sink<RequestVote<RequestVoteArgs>, Error = raft::Error> + 'a
    where
        Self: 'a,
        S: Sink<raft::Result<RequestVote<RequestVoteReply>>, Error = raft::Error> + 'a;

    type AppendEntriesSink<'a, S>: Sink<AppendEntries<AppendEntriesArgs>, Error = raft::Error> + 'a
    where
        Self: 'a,
        S: Sink<raft::Result<AppendEntries<AppendEntriesReply>>, Error = raft::Error> + 'a;

    fn register_request_vote_sink<'a, S>(&'a self, sink: S) -> Self::RequestVoteSink<'a, S>
    where
        S: Sink<raft::Result<RequestVote<RequestVoteReply>>, Error = raft::Error> + 'a;

    fn register_append_entries_sink<'a, S>(&'a self, sink: S) -> Self::AppendEntriesSink<'a, S>
    where
        S: Sink<raft::Result<AppendEntries<AppendEntriesReply>>, Error = raft::Error> + 'a;
}

pub trait AppendEntriesChannel {
    type AppendEntriesArgsSink: Sink<AppendEntriesArgs, Error = raft::Error>;
    type AppendEntriesReplyStream: Stream<Item = AppendEntriesReply>;

    fn append_entries_channel(
        &self,
    ) -> (Self::AppendEntriesArgsSink, Self::AppendEntriesReplyStream);
}

pub type AppendEntriesFuture<'a> =
    Pin<Box<dyn Future<Output = raft::Result<AppendEntriesReply>> + Send + 'a>>;

#[async_trait]
impl<T> PeerEndPoint for Box<T>
where
    T: PeerEndPoint + ?Sized + Sync + Send,
{
    async fn request_vote(&self, args: RequestVoteArgs) -> raft::errors::Result<RequestVoteReply> {
        (**self).request_vote(args).await
    }

    async fn append_entries(
        &self,
        args: AppendEntriesArgs,
    ) -> raft::errors::Result<AppendEntriesReply> {
        (**self).append_entries(args).await
    }
}

#[async_trait]
impl PeerEndPoint for RaftClient {
    async fn request_vote(&self, args: RequestVoteArgs) -> raft::Result<RequestVoteReply> {
        let args = RequestVoteArgsProst {
            data: encode(&args),
        };
        self.request_vote(&args)
            .await
            .map(|prost| decode(&prost.data))
            .map_err(raft::Error::Rpc)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> raft::Result<AppendEntriesReply> {
        let args = AppendEntriesArgsProst {
            data: encode(&args),
        };
        self.append_entries(&args)
            .await
            .map(|prost| decode(&prost.data))
            .map_err(raft::Error::Rpc)
    }
}

impl<T: ?Sized> ClientChannel for T
where
    T: PeerEndPoint,
{
    type RequestVoteSink<'a, S> = impl Sink<RequestVote<RequestVoteArgs>, Error = raft::Error> + 'a
        where
            Self: 'a,
            S: Sink<raft::Result<RequestVote<RequestVoteReply>>, Error = raft::Error> + 'a;

    type AppendEntriesSink<'a, S> = impl Sink<AppendEntries<AppendEntriesArgs>, Error = raft::Error> + 'a
        where
            Self: 'a,
            S: Sink<raft::Result<AppendEntries<AppendEntriesReply>>, Error = raft::Error> + 'a;

    fn register_request_vote_sink<'a, S>(&'a self, sink: S) -> Self::RequestVoteSink<'a, S>
    where
        S: Sink<raft::Result<RequestVote<RequestVoteReply>>, Error = raft::Error> + 'a,
    {
        sink.with(move |(payload, node_id)| {
            PeerEndPoint::request_vote(self, payload)
                .map(move |r| Ok::<_, raft::Error>(r.map(|r| (r, node_id))))
        })
        .sink_map_err(Into::into)
    }

    fn register_append_entries_sink<'a, S>(&'a self, sink: S) -> Self::AppendEntriesSink<'a, S>
    where
        S: Sink<raft::Result<AppendEntries<AppendEntriesReply>>, Error = raft::Error> + 'a,
    {
        sink.with(move |(payload, node_id)| {
            PeerEndPoint::append_entries(self, payload)
                .map(move |r| Ok::<_, raft::Error>(r.map(|r| (r, node_id))))
        })
        .sink_map_err(Into::into)
    }
}
