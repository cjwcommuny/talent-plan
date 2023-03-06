use crate::raft::inner::{Handle, LocalTask, RemoteTaskResult};

use crate::raft::role::Role;
use futures::{pin_mut, stream, StreamExt};
use rand::Rng;
use std::time::Duration;
use tokio::select;
use tokio::time::sleep;
use tracing::instrument;

#[derive(Debug, Default)]
pub struct Follower(());

impl Follower {
    #[instrument(name = "Follower::progress", skip_all, ret, fields(node_id = handle.node_id, term = handle.election.get_current_term()), level = "debug")]
    pub async fn progress(self, handle: &mut Handle) -> Role {
        let failure_timer = stream::once(sleep(Duration::from_millis(
            handle
                .random_generator
                .gen_range(handle.config.election_timeout.clone()),
        )));
        pin_mut!(failure_timer);
        let new_role: Role = loop {
            select! {
                _ = failure_timer.next() => {
                    debug!("failure timer timeout, transit to Candidate");
                    break Role::Candidate(self.into());
                }
                Some(task) = handle.local_task_receiver.recv() => {
                    if match task {
                        LocalTask::AppendEntries { sender, .. } => sender.send(None).ok(), // not leader
                        LocalTask::GetTerm(sender) => sender.send(handle.election.current_term()).ok(),
                        LocalTask::CheckLeader(sender) => sender.send(false).ok(),
                        LocalTask::Shutdown(sender) => {
                            info!("shutdown");
                            sender.send(()).unwrap();
                            break Role::Stop;
                        }
                    }.is_none() {
                        error!("local task response error");
                    }
                }
                Some(task) = handle.remote_task_receiver.recv() => {
                    debug!("{:?}", handle);
                    let RemoteTaskResult { success, new_role } = task.handle(Role::Follower(Follower::default()), handle).await;
                    if success {
                        break new_role; // restart failure timer
                    }
                }
            }
        };
        new_role
    }
}
