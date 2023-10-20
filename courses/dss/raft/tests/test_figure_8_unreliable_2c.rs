use crate::common::config::{Config, Entry};
use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use rand::Rng;
use std::thread;
use std::time::Duration;
use tracing::info;

pub mod common;

#[test]
#[named]
fn test_figure_8_unreliable_2c() {
    init_logger(function_name!());
    let servers = 5;
    let mut cfg = Config::new_with(servers, true, false);

    cfg.begin("Test (2C): Figure 8 (unreliable)");
    let mut random = rand::thread_rng();
    cfg.one(
        Entry {
            x: random.gen::<u64>() % 10000,
        },
        1,
        true,
    );

    let mut nup = servers;
    for iters in 0..1000 {
        if iters == 200 {
            cfg.net.set_long_reordering(true);
        }
        let mut leader = None;
        for i in 0..servers {
            if cfg.rafts.lock().unwrap()[i]
                .as_ref()
                .unwrap()
                .start(&Entry {
                    x: random.gen::<u64>() % 10000,
                })
                .is_ok()
                && cfg.connected[i]
            {
                leader = Some(i);
            }
        }

        if (random.gen::<usize>() % 1000) < 100 {
            let ms = random.gen::<u64>() % (RAFT_ELECTION_TIMEOUT.as_millis() as u64 / 2);
            thread::sleep(Duration::from_millis(ms));
        } else {
            let ms = random.gen::<u64>() % 13;
            thread::sleep(Duration::from_millis(ms));
        }

        if let Some(leader) = leader {
            if (random.gen::<usize>() % 1000) < (RAFT_ELECTION_TIMEOUT.as_millis() as usize) / 2 {
                cfg.disconnect(leader);
                nup -= 1;
            }
        }

        if nup < 3 {
            let s = random.gen::<usize>() % servers;
            if !cfg.connected[s] {
                cfg.connect(s);
                nup += 1;
            }
        }
    }

    for i in 0..servers {
        if !cfg.connected[i] {
            cfg.connect(i);
        }
    }

    cfg.one(
        Entry {
            x: random.gen::<u64>() % 10000,
        },
        servers,
        true,
    );

    info!("end");
    cfg.end();
}
