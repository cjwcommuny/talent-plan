use crate::raft::inner::{LocalTask, RemoteTask, RemoteTaskResult};

use crate::raft::candidate::Candidate;

use crate::raft::handle::Handle;
use crate::raft::leader::Leader;
use crate::raft::message_handler::MessageHandler;
use crate::raft::role::Role;
use futures::{pin_mut, stream, StreamExt};
use futures_concurrency::stream::Merge;
use rand::Rng;
use std::time::Duration;

use tokio::time::sleep;
use tracing::{info, trace_span};

#[derive(Debug, Default)]
pub struct Follower(());

impl Follower {
    pub async fn progress(self, handle: &mut Handle, message_handler: &mut MessageHandler) -> Role {
        let _span = trace_span!("Follower", node_id = handle.node_id).entered();
        let election_timer = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )))
        .map(|_| Message::Timeout);

        let local_tasks = message_handler.local_tasks.by_ref().map(Message::LocalTask);
        let remote_tasks = message_handler
            .remote_tasks
            .by_ref()
            .map(Message::RemoteTask);
        let messages = (election_timer, local_tasks, remote_tasks).merge();

        pin_mut!(messages);
        use LoopResult::{RestartFollower, Shutdown, TransitToCandidate};
        let loop_result = loop {
            if let Some(message) = messages.next().await {
                match message {
                    Message::Timeout => break TransitToCandidate,
                    Message::LocalTask(task) => match task {
                        LocalTask::AppendEntries { sender, .. } => {
                            sender.send(None).unwrap_or_else(|_| {
                                panic!("{}", "term={term}, local task AppendEntries response error")
                            })
                        }
                        LocalTask::GetTerm(sender) => sender
                            .send(handle.election.current_term())
                            .unwrap_or_else(|_| {
                                panic!("{}", "term={term}, local task GetTerm response error")
                            }),
                        LocalTask::CheckLeader(sender) => sender.send(false).unwrap_or_else(|_| {
                            panic!("{}", "term={term}, local task CheckLeader response error")
                        }),
                        LocalTask::Shutdown(sender) => {
                            sender.send(()).unwrap_or_else(|_| {
                                panic!("{}", "term={term}, local task Shutdown response error")
                            });
                            break Shutdown;
                        }
                    },
                    Message::RemoteTask(task) => {
                        trace!(
                            "term={}, handle remote task",
                            handle.election.current_term()
                        );
                        let RemoteTaskResult {
                            transit_to_follower,
                        } = task.handle(handle).await;
                        if transit_to_follower {
                            break RestartFollower; // restart failure timer
                        }
                    }
                }
            }
        };
        info!(
            "term={}, loop_result={loop_result:?}",
            handle.election.current_term()
        );
        match loop_result {
            TransitToCandidate => Role::Candidate(Candidate::from(self)),
            RestartFollower => Role::Follower(self),
            Shutdown => Role::Shutdown,
        }
    }
}

#[derive(Debug)]
enum LoopResult {
    TransitToCandidate,
    RestartFollower,
    Shutdown,
}

impl From<Leader> for Follower {
    fn from(_: Leader) -> Self {
        Self(())
    }
}

impl From<Candidate> for Follower {
    fn from(_: Candidate) -> Self {
        Self(())
    }
}

enum Message {
    Timeout,
    RemoteTask(RemoteTask),
    LocalTask(LocalTask),
}
