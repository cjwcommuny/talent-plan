use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use raft::raft::config::{Config, Entry};
use std::thread;
use tracing::info;

mod common;

#[test]
#[named]
fn test_backup_2b() {
    const N: usize = 50;
    let mut entry_index = 0..;
    init_logger(function_name!());
    let servers = 5;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2B): leader backs up quickly over incorrect follower logs");

    let entry = Entry {
        x: entry_index.next().unwrap(),
    };
    info!("add initial entry {:?}", entry);
    cfg.one(entry, servers, true);

    // put leader and one follower in a partition
    let leader1 = cfg.check_one_leader();
    info!(
        "put leader {} and one follower {} in a partition",
        leader1,
        (leader1 + 1) % servers
    );
    cfg.disconnect((leader1 + 2) % servers);
    cfg.disconnect((leader1 + 3) % servers);
    cfg.disconnect((leader1 + 4) % servers);

    // submit lots of commands that won't commit
    for i in 0..N {
        let entry = Entry {
            x: entry_index.next().unwrap(),
        };
        info!("{}: commands won't commit: {:?}", i, entry);
        let _ = cfg.rafts.lock().unwrap()[leader1]
            .as_ref()
            .unwrap()
            .start(&entry);
    }

    thread::sleep(RAFT_ELECTION_TIMEOUT / 2);
    info!("disconnect");
    cfg.disconnect(leader1 % servers);
    cfg.disconnect((leader1 + 1) % servers);

    // allow other partition to recover
    info!(
        "allow other {}, {}, {} partition to recover",
        (leader1 + 2) % servers,
        (leader1 + 3) % servers,
        (leader1 + 4) % servers
    );
    cfg.connect((leader1 + 2) % servers);
    cfg.connect((leader1 + 3) % servers);
    cfg.connect((leader1 + 4) % servers);

    // lots of successful commands to new group.
    for i in 0..N {
        let entry = Entry {
            x: entry_index.next().unwrap(),
        };
        info!("{}: successful commands to new group: {:?}", i, entry);
        cfg.one(entry, 3, true);
    }

    // now another partitioned leader and one follower
    let leader2 = cfg.check_one_leader();
    let mut other = (leader1 + 2) % servers;
    if leader2 == other {
        other = (leader2 + 1) % servers;
    }
    info!("disconnect other {}", other);
    cfg.disconnect(other);

    // lots more commands that won't commit
    for i in 0..N {
        let entry = Entry {
            x: entry_index.next().unwrap(),
        };
        info!("{}: more commands won't commit: {:?}", i, entry);
        let _ = cfg.rafts.lock().unwrap()[leader2]
            .as_ref()
            .unwrap()
            .start(&entry);
    }

    thread::sleep(RAFT_ELECTION_TIMEOUT / 2);

    // bring original leader back to life,
    info!(
        "bring original leader back to life, {}, {}, {}",
        leader1,
        (leader1 + 1) % servers,
        other
    );
    for i in 0..servers {
        cfg.disconnect(i);
    }
    cfg.connect(leader1 % servers);
    cfg.connect((leader1 + 1) % servers);
    cfg.connect(other);

    // lots of successful commands to new group.
    for i in 0..N {
        let entry = Entry {
            x: entry_index.next().unwrap(),
        };
        info!("{}: successful commands to new group-2: {:?}", i, entry);
        cfg.one(entry, 3, true);
    }

    // now everyone
    info!("now everyone");
    for i in 0..servers {
        cfg.connect(i);
    }

    let entry = Entry {
        x: entry_index.next().unwrap(),
    };
    info!("add last entry {:?}", entry);
    cfg.one(entry, servers, true);

    cfg.end();
}
