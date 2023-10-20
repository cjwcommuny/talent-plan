use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use raft::raft::config::Config;
use std::thread;
use std::time::Duration;
use tracing::warn;

mod common;

#[test]
#[named]
fn test_initial_election_2a() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2A): initial election");

    // is a leader elected?
    cfg.check_one_leader();

    // sleep a bit to avoid racing with followers learning of the
    // election, then check that all peers agree on the term.
    thread::sleep(Duration::from_millis(50));
    let term1 = cfg.check_terms();

    // does the leader+term stay the same if there is no network failure?
    thread::sleep(2 * RAFT_ELECTION_TIMEOUT);
    let term2 = cfg.check_terms();
    if term1 != term2 {
        warn!(
            "warning: term changed even though there were no failures, term1: {}, term2: {}",
            term1, term2
        )
    }

    // there should still be a leader.
    cfg.check_one_leader();

    cfg.end();
}
