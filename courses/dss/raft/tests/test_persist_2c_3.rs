use crate::common::config::{Config, Entry};
use crate::common::init_logger;
use function_name::named;

pub mod common;

#[test]
#[named]
fn test_persist_2c_3() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts");

    cfg.one(Entry { x: 101 }, 3, true);

    let leader = cfg.check_one_leader();
    cfg.disconnect((leader + 2) % servers);

    cfg.one(Entry { x: 102 }, 2, true);

    cfg.crash1(leader % servers);
    cfg.crash1((leader + 1) % servers);
    cfg.connect((leader + 2) % servers);
    cfg.start1(leader % servers);
    cfg.connect(leader % servers);

    cfg.one(Entry { x: 103 }, 2, true);

    cfg.start1((leader + 1) % servers);
    cfg.connect((leader + 1) % servers);

    cfg.one(Entry { x: 104 }, servers, true);

    cfg.end();
}
