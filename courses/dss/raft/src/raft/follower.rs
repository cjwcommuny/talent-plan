use crate::raft::inner::{LocalTask, RemoteTaskResult};

use crate::raft::handle::Handle;
use crate::raft::role::Role;
use futures::{pin_mut, stream, StreamExt};
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::{info, trace_span, warn};

#[derive(Debug, Default)]
pub struct Follower(());

impl Follower {
    pub async fn progress(self, handle: &mut Handle) -> Role {
        let _span = trace_span!("Follower", node_id = handle.node_id).entered();
        let election_timer = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )));
        pin_mut!(election_timer);
        let new_role: Role = loop {
            select! {
                _ = election_timer.next() => {
                    info!("term={}, election timeout", handle.election.current_term());
                    break Role::Candidate(self.into());
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    if match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).ok(), // not leader
                        LocalTask::GetTerm(sender) => sender.send(handle.election.current_term()).ok(),
                        LocalTask::CheckLeader(sender) => sender.send(false).ok(),
                        LocalTask::Shutdown(sender) => {
                            info!("term={}, shutdown", handle.election.current_term());
                            sender.send(()).unwrap();
                            break Role::Stop;
                        }
                    }.is_none() {
                        warn!("term={}, local task response error", handle.election.current_term());
                    }
                }
                Some(task) = handle.remote_task_receiver.recv() => {
                    trace!("term={}, handle remote task", handle.election.current_term());
                    let RemoteTaskResult { success, new_role } = task.handle(Role::Follower(Follower::default()), handle).await;
                    if success {
                        info!("term={}, restart failure timer", handle.election.current_term());
                        break new_role; // restart failure timer
                    }
                }
            }
        };
        new_role
    }
}
