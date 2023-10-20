use crate::common::init_logger;
use function_name::named;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future;
use raft::raft::config::{Config, Entry};
use std::thread;
use std::time::Duration;
use tracing::warn;

mod common;

#[test]
#[named]
fn test_concurrent_starts_2b() {
    init_logger(function_name!());
    let servers = 3;
    let mut cfg = Config::new(servers);

    cfg.begin("Test (2B): concurrent start()s");
    let mut success = false;
    'outer: for tried in 0..5 {
        if tried > 0 {
            // give solution some time to settle
            thread::sleep(Duration::from_secs(3));
        }

        let leader = cfg.check_one_leader();
        let term = match cfg.rafts.lock().unwrap()[leader]
            .as_ref()
            .unwrap()
            .start(&Entry { x: 1 })
        {
            Err(err) => {
                warn!("start leader {} meet error {:?}", leader, err);
                continue;
            }
            Ok((_, term)) => term,
        };

        let mut idx_rxs = vec![];
        for ii in 0..5 {
            let (tx, rx) = oneshot::channel();
            idx_rxs.push(rx);
            let node = cfg.rafts.lock().unwrap()[leader].clone().unwrap();
            cfg.net.spawn(future::lazy(move |_| {
                let idx = match node.start(&Entry { x: 100 + ii }) {
                    Err(err) => {
                        warn!("start leader {} meet error {:?}", leader, err);
                        None
                    }
                    Ok((idx, term1)) => {
                        if term1 != term {
                            None
                        } else {
                            Some(idx)
                        }
                    }
                };
                tx.send(idx)
                    .map_err(|e| panic!("send failed: {:?}", e))
                    .unwrap();
            }));
        }
        let idxes = block_on(async {
            future::join_all(idx_rxs)
                .await
                .into_iter()
                .map(|idx_rx| idx_rx.unwrap())
                .collect::<Vec<_>>()
        });

        for j in 0..servers {
            let t = cfg.rafts.lock().unwrap()[j].as_ref().unwrap().term();
            if t != term {
                // term changed -- can't expect low RPC counts
                continue 'outer;
            }
        }

        let mut cmds = vec![];
        for index in idxes.into_iter().flatten() {
            if let Some(cmd) = cfg.wait(index, servers, Some(term)) {
                cmds.push(cmd.x);
            } else {
                // peers have moved on to later terms
                // so we can't expect all Start()s to
                // have succeeded
                continue;
            }
        }

        for ii in 0..5 {
            let x = 100 + ii;
            let mut ok = false;
            for cmd in &cmds {
                if *cmd == x {
                    ok = true;
                }
            }
            assert!(ok, "cmd {} missing in {:?}", x, cmds)
        }

        success = true;
        break;
    }

    assert!(success, "term changed too often");

    cfg.end();
}
