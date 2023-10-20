use crate::common::config::{Config, Entry};
use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use std::thread;

pub mod common;

#[test]
#[named]
fn test_persist_2c_2() {
    init_logger(function_name!());
    let servers = 5;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2C): more persistence");

    let mut index = 1;
    for _ in 0..5 {
        cfg.one(Entry { x: 10 + index }, servers, true);
        index += 1;

        let leader1 = cfg.check_one_leader();

        cfg.disconnect((leader1 + 1) % servers);
        cfg.disconnect((leader1 + 2) % servers);

        cfg.one(Entry { x: 10 + index }, servers - 2, true);
        index += 1;

        cfg.disconnect(leader1 % servers);
        cfg.disconnect((leader1 + 3) % servers);
        cfg.disconnect((leader1 + 4) % servers);

        cfg.start1((leader1 + 1) % servers);
        cfg.start1((leader1 + 2) % servers);
        cfg.connect((leader1 + 1) % servers);
        cfg.connect((leader1 + 2) % servers);

        thread::sleep(RAFT_ELECTION_TIMEOUT);

        cfg.start1((leader1 + 3) % servers);
        cfg.connect((leader1 + 3) % servers);

        cfg.one(Entry { x: 10 + index }, servers - 2, true);
        index += 1;

        cfg.connect((leader1 + 4) % servers);
        cfg.connect(leader1 % servers);
    }

    cfg.one(Entry { x: 1000 }, servers, true);

    cfg.end();
}
