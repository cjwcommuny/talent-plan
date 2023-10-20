use crate::common::config::{Config, Entry};
use crate::common::init_logger;
use function_name::named;
use tracing::info;

pub mod common;

#[test]
#[named]
fn test_rejoin_2b() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2B): rejoin of partitioned leader");

    cfg.one(Entry { x: 101 }, servers, true);

    // leader network failure
    let leader1 = cfg.check_one_leader();
    info!("leader {leader1} network failure");
    cfg.disconnect(leader1);

    // make old leader try to agree on some entries
    info!("leader {leader1} start 102");
    let _ = cfg.rafts.lock().unwrap()[leader1]
        .as_ref()
        .unwrap()
        .start(&Entry { x: 102 });
    info!("leader {leader1} start 103");
    let _ = cfg.rafts.lock().unwrap()[leader1]
        .as_ref()
        .unwrap()
        .start(&Entry { x: 103 });
    info!("leader {leader1} start 104");
    let _ = cfg.rafts.lock().unwrap()[leader1]
        .as_ref()
        .unwrap()
        .start(&Entry { x: 104 });

    // new leader commits, also for index=2
    cfg.one(Entry { x: 105 }, 2, true);

    // new leader network failure
    let leader2 = cfg.check_one_leader();
    info!("new leader network failure");
    cfg.disconnect(leader2);

    // old leader connected again
    info!("old leader connected again");
    cfg.connect(leader1);

    cfg.one(Entry { x: 106 }, 2, true);

    // all together now
    info!("all together now");
    cfg.connect(leader2);

    cfg.one(Entry { x: 107 }, servers, true);

    cfg.end();
}
