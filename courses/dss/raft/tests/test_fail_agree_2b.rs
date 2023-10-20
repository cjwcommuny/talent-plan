use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use raft::raft::config::{Config, Entry};
use std::thread;
use tracing::info;

mod common;

#[test]
#[named]
fn test_fail_agree_2b() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2B): agreement despite follower disconnection");

    info!("one Entry {{x: 101 }}");
    cfg.one(Entry { x: 101 }, servers, false);

    // follower network disconnection
    let leader = cfg.check_one_leader();
    info!("follower network disconnection");
    cfg.disconnect((leader + 1) % servers);

    // agree despite one disconnected server?
    info!("one Entry {{x: 102 }}");
    cfg.one(Entry { x: 102 }, servers - 1, false);
    info!("one Entry {{x: 103 }}");
    cfg.one(Entry { x: 103 }, servers - 1, false);
    thread::sleep(RAFT_ELECTION_TIMEOUT);
    info!("one Entry {{x: 104 }}");
    cfg.one(Entry { x: 104 }, servers - 1, false);
    info!("one Entry {{x: 105 }}");
    cfg.one(Entry { x: 105 }, servers - 1, false);

    // re-connect
    info!("re-connect");
    cfg.connect((leader + 1) % servers);

    // agree with full set of servers?
    info!("one Entry {{x: 106 }}");
    cfg.one(Entry { x: 106 }, servers, true);
    thread::sleep(RAFT_ELECTION_TIMEOUT);
    info!("one Entry {{x: 107 }}");
    cfg.one(Entry { x: 107 }, servers, true);

    cfg.end();
}
