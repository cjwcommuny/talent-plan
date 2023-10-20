use crate::common::{init_logger, random_entry, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use raft::raft::config::Config;
use rand::Rng;
use std::thread;
use std::time::Duration;
use tracing::info;

mod common;

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
#[test]
#[named]
fn test_figure_8_2c() {
    init_logger(function_name!());
    let servers = 5;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2C): Figure 8");

    let mut random = rand::thread_rng();
    info!("one random entry");
    cfg.one(random_entry(&mut random), 1, true);

    let mut nup = servers;
    info!("2");
    for _iters in 0..1000 {
        let mut leader = None;
        info!("2.1");
        for i in 0..servers {
            let mut rafts = cfg.rafts.lock().unwrap();
            if let Some(Some(raft)) = rafts.get_mut(i) {
                if raft.start(&random_entry(&mut random)).is_ok() {
                    leader = Some(i);
                }
            }
        }

        info!("2.2");
        if (random.gen::<usize>() % 1000) < 100 {
            let ms = random.gen::<u64>() % ((RAFT_ELECTION_TIMEOUT.as_millis() / 2) as u64);
            thread::sleep(Duration::from_millis(ms));
        } else {
            let ms = random.gen::<u64>() % 13;
            thread::sleep(Duration::from_millis(ms));
        }

        info!("2.3");
        if let Some(leader) = leader {
            cfg.crash1(leader);
            nup -= 1;
        }

        info!("2.4");
        if nup < 3 {
            let s = random.gen::<usize>() % servers;
            if cfg.rafts.lock().unwrap().get(s).unwrap().is_none() {
                cfg.start1(s);
                cfg.connect(s);
                nup += 1;
            }
        }
    }

    info!("3");
    for i in 0..servers {
        if cfg.rafts.lock().unwrap().get(i).unwrap().is_none() {
            cfg.start1(i);
            cfg.connect(i);
        }
    }

    info!("one final random entry");
    cfg.one(random_entry(&mut random), servers, true);

    cfg.end();
}
