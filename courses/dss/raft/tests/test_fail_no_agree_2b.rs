use crate::common::config::{Config, Entry};
use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use std::thread;
use tracing::info;

pub mod common;

#[test]
#[named]
fn test_fail_no_agree_2b() {
    init_logger(function_name!());
    let servers = 5;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2B): no agreement if too many followers disconnect");

    cfg.one(Entry { x: 10 }, servers, false);

    // 3 of 5 followers disconnect
    let leader = cfg.check_one_leader();
    info!("3 of 5 followers disconnect");
    cfg.disconnect((leader + 1) % servers);
    cfg.disconnect((leader + 2) % servers);
    cfg.disconnect((leader + 3) % servers);
    let (index, _) = cfg.rafts.lock().unwrap()[leader]
        .as_ref()
        .unwrap()
        .start(&Entry { x: 20 })
        .expect("leader rejected start");
    if index != 2 {
        panic!("expected index 2, got {}", index);
    }

    thread::sleep(2 * RAFT_ELECTION_TIMEOUT);

    let (n, _) = cfg.n_committed(index);
    if n > 0 {
        panic!("{} committed but no majority", n);
    }

    // repair
    info!("repair");
    cfg.connect((leader + 1) % servers);
    cfg.connect((leader + 2) % servers);
    cfg.connect((leader + 3) % servers);

    // the disconnected majority may have chosen a leader from
    // among their own ranks, forgetting index 2.
    let leader2 = cfg.check_one_leader();
    let (index2, _) = cfg.rafts.lock().unwrap()[leader2]
        .as_ref()
        .unwrap()
        .start(&Entry { x: 30 })
        .expect("leader2 rejected start");
    if !(2..=3).contains(&index2) {
        panic!("unexpected index {}", index2);
    }

    cfg.one(Entry { x: 1000 }, servers, true);

    cfg.end();
}
