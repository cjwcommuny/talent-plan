use crate::common::init_logger;
use function_name::named;
use raft::raft::config::Config;
use rand::Rng;

mod common;

#[test]
#[named]
fn test_many_election_2a() {
    init_logger(function_name!());
    let servers = 7;
    let iters = 10;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2A): multiple elections");

    cfg.check_one_leader();

    let mut random = rand::thread_rng();
    for _ in 0..iters {
        // disconnect three nodes
        let i1 = random.gen::<usize>() % servers;
        let i2 = random.gen::<usize>() % servers;
        let i3 = random.gen::<usize>() % servers;
        cfg.disconnect(i1);
        cfg.disconnect(i2);
        cfg.disconnect(i3);

        // either the current leader should still be alive,
        // or the remaining four should elect a new one.
        cfg.check_one_leader();

        cfg.connect(i1);
        cfg.connect(i2);
        cfg.connect(i3);
    }

    cfg.check_one_leader();

    cfg.end();
}
