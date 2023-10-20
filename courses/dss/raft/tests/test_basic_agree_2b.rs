use crate::common::init_logger;
use function_name::named;
use raft::raft::config::{Config, Entry};
use tracing::instrument;

mod common;

#[test]
#[instrument]
#[named]
fn test_basic_agree_2b() {
    init_logger(function_name!());
    let servers = 5;
    let mut cfg = Config::new(servers);
    cfg.begin("Test (2B): basic agreement");

    let iters = 3;
    for index in 1..=iters {
        let (nd, _) = cfg.n_committed(index);
        if nd > 0 {
            panic!("some have committed before start()");
        }
        let xindex = cfg.one(Entry { x: index * 100 }, servers, false);
        if xindex != index {
            panic!("got index {} but expected {}", xindex, index);
        }
    }

    cfg.end()
}
