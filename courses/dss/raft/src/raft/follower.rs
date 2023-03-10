use crate::raft::inner::{LocalTask, PeerEndPoint, RemoteTaskResult};

use crate::raft::candidate::Candidate;
use crate::raft::follower::LoopResult::Shutdown;
use crate::raft::handle::peer::Peers;
use crate::raft::handle::Handle;
use crate::raft::leader::Leader;
use crate::raft::role::Role;
use futures::{pin_mut, stream, StreamExt};
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{info, trace_span, warn};

#[derive(Debug)]
pub struct Follower {
    pub peers: Peers<Box<dyn PeerEndPoint>>,
}

impl Follower {
    pub async fn progress(self, handle: &mut Handle) -> Role {
        let _span = trace_span!("Follower", node_id = handle.node_id).entered();
        let election_timer = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )));
        pin_mut!(election_timer);
        use LoopResult::{RestartFollower, Shutdown, TransitToCandidate};
        let loop_result: LoopResult = loop {
            select! {
                _ = election_timer.next() => {
                    break TransitToCandidate;
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    let term = handle.election.current_term();
                    match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None)
                            .expect(&format!("term={term}, local task AppendEntries response error")),
                        LocalTask::GetTerm(sender) => sender.send(handle.election.current_term())
                            .expect(&format!("term={term}, local task GetTerm response error")),
                        LocalTask::CheckLeader(sender) => sender.send(false)
                            .expect(&format!("term={term}, local task CheckLeader response error")),
                        LocalTask::Shutdown(sender) => {
                            sender.send(()).expect(&format!("term={term}, local task Shutdown response error"));
                            break Shutdown;
                        }
                    }
                }
                Some(task) = handle.remote_task_receiver.recv() => {
                    trace!("term={}, handle remote task", handle.election.current_term());
                    let RemoteTaskResult { transit_to_follower } = task.handle(handle).await;
                    if transit_to_follower {
                        break RestartFollower; // restart failure timer
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

impl From<Candidate> for Follower {
    fn from(candidate: Candidate) -> Self {
        Follower {
            peers: candidate.peers,
        }
    }
}

impl From<Leader> for Follower {
    fn from(leader: Leader) -> Self {
        Follower {
            peers: leader
                .peers
                .into_iter()
                .map(|(endpoint, ..)| endpoint)
                .collect(),
        }
    }
}
