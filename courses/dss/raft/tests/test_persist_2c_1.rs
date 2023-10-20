use crate::common::init_logger;
use function_name::named;
use raft::raft::config::{Config, Entry};

mod common;

#[test]
#[named]
fn test_persist_2c_1() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2C): basic persistence");

    cfg.one(Entry { x: 11 }, servers, true);

    // crash and re-start all
    for i in 0..servers {
        cfg.start1(i);
    }
    for i in 0..servers {
        cfg.disconnect(i);
        cfg.connect(i);
    }

    cfg.one(Entry { x: 12 }, servers, true);

    let leader1 = cfg.check_one_leader();
    cfg.disconnect(leader1);
    cfg.start1(leader1);
    cfg.connect(leader1);

    cfg.one(Entry { x: 13 }, servers, true);

    let leader2 = cfg.check_one_leader();
    cfg.disconnect(leader2);
    cfg.one(Entry { x: 14 }, servers - 1, true);
    cfg.start1(leader2);
    cfg.connect(leader2);

    cfg.wait(4, servers, None); // wait for leader2 to join before killing i3

    let i3 = (cfg.check_one_leader() + 1) % servers;
    cfg.disconnect(i3);
    cfg.one(Entry { x: 15 }, servers - 1, true);
    cfg.start1(i3);
    cfg.connect(i3);

    cfg.one(Entry { x: 16 }, servers, true);

    cfg.end();
}
