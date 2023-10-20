use crate::common::config::Config;
use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use std::thread;
use tracing::info;

pub mod common;

#[test]
#[named]
fn test_reelection_2a() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);
    cfg.begin("Test (2A): election after network failure");

    let leader1 = cfg.check_one_leader();
    // if the leader disconnects, a new one should be elected.
    cfg.disconnect(leader1);
    info!("disconnect leader1: {leader1}");
    cfg.check_one_leader();

    // if the old leader rejoins, that shouldn't
    // disturb the new leader.
    cfg.connect(leader1);
    info!("connect leader1: {leader1}");
    let leader2 = cfg.check_one_leader();

    // if there's no quorum, no leader should
    // be elected.
    info!(
        "disconnect leader2: {}, leader2 next {}",
        leader2,
        (leader2 + 1) % servers
    );
    cfg.disconnect(leader2);
    cfg.disconnect((leader2 + 1) % servers);
    thread::sleep(2 * RAFT_ELECTION_TIMEOUT);
    cfg.check_no_leader();

    // if a quorum arises, it should elect a leader.
    info!("connect leader2 next: {}", (leader2 + 1) % servers);
    cfg.connect((leader2 + 1) % servers);
    cfg.check_one_leader();

    // re-join of last node shouldn't prevent leader from existing.
    info!("connect leader2: {leader2}");
    cfg.connect(leader2);
    cfg.check_one_leader();

    cfg.end();
}
