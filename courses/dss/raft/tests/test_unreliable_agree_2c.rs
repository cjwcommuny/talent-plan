use crate::common::config::{Config, Entry};
use crate::common::init_logger;
use function_name::named;
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::future;
use std::sync::Arc;
use std::thread;

pub mod common;

#[test]
#[named]
fn test_unreliable_agree_2c() {
    init_logger(function_name!());
    let servers = 5;

    let cfg = {
        let mut cfg = Config::new_with(servers, true, false);
        cfg.begin("Test (2C): unreliable agreement");
        Arc::new(cfg)
    };

    let mut dones = vec![];
    for iters in 1..50 {
        for j in 0..4 {
            let c = cfg.clone();
            let (tx, rx) = oneshot::channel();
            thread::spawn(move || {
                c.one(
                    Entry {
                        x: (100 * iters) + j,
                    },
                    1,
                    true,
                );
                tx.send(()).map_err(|e| panic!("send failed: {:?}", e))
            });
            dones.push(rx);
        }
        cfg.one(Entry { x: iters }, 1, true);
    }

    cfg.net.set_reliable(true);

    block_on(async {
        future::join_all(dones)
            .await
            .into_iter()
            .for_each(|done| done.unwrap());
    });

    cfg.one(Entry { x: 100 }, servers, true);

    cfg.end();
}
