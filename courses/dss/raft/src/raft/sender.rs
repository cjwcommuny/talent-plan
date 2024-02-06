use std::fmt::Debug;
use std::future::Future;

use crate::raft;
use crate::raft::inner::{AppendEntries, PeerEndPoint, RequestVote};
use crate::raft::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};

use derive_new::new;
use futures::FutureExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;

pub trait Sender<T> {
    type Error;

    fn send(&self, data: T) -> Result<(), Self::Error>;
}

impl<T> Sender<T> for UnboundedSender<T> {
    type Error = mpsc::error::SendError<T>;

    fn send(&self, data: T) -> Result<(), Self::Error> {
        self.send(data)
    }
}

// pub struct RequestVoteSender {
//     client: Arc<dyn PeerEndPoint>,
//     sender: UnboundedSender<>
// }

#[derive(new)]
pub struct ClientSender<F, S> {
    f: F,
    sender: S,
}

impl<F, S, A, B, Fut, E, Context> Sender<(A, Context)> for ClientSender<F, S>
where
    S: Sender<(B, Context), Error = E> + Clone + Sync + Send + 'static,
    F: Fn(A) -> Fut,
    Fut: Future<Output = B> + Send + 'static,
    E: Into<raft::Error> + Debug,
    Context: Send + 'static,
{
    type Error = !;

    fn send(&self, (data, context): (A, Context)) -> Result<(), Self::Error> {
        let sender = self.sender.clone();
        let fut = (self.f)(data).map(move |b| sender.send((b, context)));
        tokio::spawn(async {
            fut.await.unwrap();
        });
        Ok(())
    }
}

pub fn request_vote_sender(
    client: Arc<dyn PeerEndPoint + Send + Sync>,
    sender: UnboundedSender<RequestVote<raft::Result<RequestVoteReply>>>,
) -> impl Sender<RequestVote<RequestVoteArgs>> {
    let f = move |a| {
        let client = client.clone();
        async move { client.request_vote(a).await }
    };
    ClientSender::new(f, sender)
}

pub fn append_entries_sender(
    client: Arc<dyn PeerEndPoint + Send + Sync>,
    sender: UnboundedSender<AppendEntries<raft::Result<AppendEntriesReply>>>,
) -> impl Sender<AppendEntries<AppendEntriesArgs>> {
    let f = move |a| {
        let client = client.clone();
        async move { client.append_entries(a).await }
    };
    ClientSender::new(f, sender)
}
