use crate::raft::inner::{LocalTask, RemoteTask};
use crate::raft::inner::{RemoteTaskResult, RequestVote};
use crate::raft::leader::Leader;
use crate::raft::role::Role;
use crate::raft::sender::Sender;
use futures::{stream, SinkExt, StreamExt};
use std::collections::HashSet;
use std::fmt::Debug;

use crate::raft::follower::Follower;
use crate::raft::handle::Handle;
use crate::raft::message_handler::MessageHandler;
use crate::raft::rpc::{RequestVoteArgs, RequestVoteReply};

use futures_concurrency::stream::Merge;
use rand::Rng;
use std::time::Duration;

use tokio::sync::mpsc::unbounded_channel;
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::raft::sender::request_vote_sender;
use tracing::{info, trace, trace_span, warn};

/// make the constructor private
#[derive(Debug)]
pub struct Candidate(());

impl From<Follower> for Candidate {
    fn from(_: Follower) -> Self {
        Self(())
    }
}

impl Candidate {
    pub(crate) async fn progress(
        self,
        handle: &mut Handle,
        message_handler: &mut MessageHandler,
    ) -> Role {
        let _span = trace_span!("Candidate", node_id = handle.node_id).entered();
        let me = handle.node_id;
        handle.increment_term();
        handle.election.voted_for = Some(handle.node_id);
        let args = RequestVoteArgs {
            log_state: handle.logs.log_state(),
            term: handle.election.current_term(),
            candidate_id: handle.node_id,
        };
        let peers = &message_handler.peers;
        let node_ids = message_handler.peers.node_ids_except(me);
        let majority_threshold = message_handler.peers.majority_threshold();

        let election_timeout = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )))
        .map(|_| Message::Timeout);
        let client_tasks = message_handler
            .local_tasks
            .by_ref()
            .map(Message::ClientTask);
        let server_tasks = message_handler
            .remote_tasks
            .by_ref()
            .map(Message::ServerTask);

        let replies = {
            let (sender, receiver) = unbounded_channel();
            for peer_id in node_ids {
                trace!(
                    "term={}, send vote requests",
                    handle.election.current_term()
                );
                request_vote_sender(peers.inner[peer_id].clone(), sender.clone())
                    .send((args.clone(), peer_id))
                    .unwrap();
            }
            UnboundedReceiverStream::new(receiver).map(Message::RequestVoteResponse)
        };

        let messages = (election_timeout, client_tasks, server_tasks, replies).merge();
        futures::pin_mut!(messages);

        let mut votes_received = HashSet::from([handle.node_id]);
        use LoopResult::{Elected, RestartAsCandidate, Shutdown, TransitToFollower};

        let vote_result = 'collect_vote: {
            while votes_received.len() < majority_threshold {
                let Some(message) = messages.next().await else {
                    continue;
                };
                match message {
                    Message::Timeout => break 'collect_vote RestartAsCandidate,
                    Message::ServerTask(task) => {
                        let RemoteTaskResult {
                            transit_to_follower,
                        } = task.handle(handle).await;
                        if transit_to_follower {
                            break 'collect_vote TransitToFollower;
                        }
                    }
                    Message::ClientTask(task) => match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).unwrap(),
                        LocalTask::GetTerm(sender) => {
                            sender.send(handle.election.current_term()).unwrap()
                        }
                        LocalTask::CheckLeader(sender) => sender.send(false).unwrap(),
                        LocalTask::Shutdown(sender) => {
                            sender.send(()).unwrap();
                            break 'collect_vote Shutdown;
                        }
                    },
                    Message::RequestVoteResponse((reply_result, peer_id)) => {
                        let current_term = handle.election.current_term();
                        match reply_result {
                            Ok(reply) => {
                                if reply.term == current_term && reply.vote_granted {
                                    trace!("receive vote granted from {peer_id}");
                                    votes_received.insert(peer_id);
                                } else if reply.term > current_term {
                                    handle.update_current_term(reply.term);
                                    handle.election.voted_for = None;
                                    break 'collect_vote TransitToFollower;
                                } else {
                                    trace!("term={}, received outdated votes or non-granted votes, reply.term: {}", current_term, reply.term);
                                }
                            }
                            Err(e) => {
                                warn!("term={}, {}", handle.election.current_term(), e.to_string())
                            }
                        }
                    }
                }
            }
            break 'collect_vote Elected;
        };
        info!(
            "term={}, vote_result={:?}",
            handle.election.current_term(),
            vote_result
        );
        match vote_result {
            RestartAsCandidate => Role::Candidate(self),
            TransitToFollower => Role::Follower(Follower::from(self)),
            Shutdown => Role::Shutdown,
            Elected => Role::Leader(Leader::from(
                self,
                handle.logs.len(),
                message_handler.peers.len(),
            )),
        }
    }
}

enum Message {
    Timeout,
    ServerTask(RemoteTask),
    ClientTask(LocalTask),
    RequestVoteResponse(RequestVote<crate::raft::Result<RequestVoteReply>>),
}

#[derive(Debug)]
enum LoopResult {
    RestartAsCandidate,
    TransitToFollower,
    Shutdown,
    Elected,
}
