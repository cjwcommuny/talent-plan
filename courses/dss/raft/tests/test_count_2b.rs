use crate::common::{init_logger, RAFT_ELECTION_TIMEOUT};
use function_name::named;
use raft::raft::config::{Config, Entry};
use rand::Rng;
use std::thread;
use std::time::Duration;
use tracing::warn;

mod common;

#[test]
#[named]
fn test_count_2b() {
    init_logger(function_name!());
    const SERVERS: usize = 3;
    fn rpcs(cfg: &Config) -> usize {
        (0..SERVERS).map(|j| cfg.rpc_count(j)).sum()
    }

    let mut cfg = Config::new(SERVERS);

    cfg.begin("Test (2B): RPC counts aren't too high");

    cfg.check_one_leader();
    let mut total1 = rpcs(&cfg);

    if !(1..=30).contains(&total1) {
        panic!("too many or few RPCs ({}) to elect initial leader", total1);
    }

    let mut total2 = 0;
    let mut success = false;
    'outer: for tried in 0..5 {
        if tried > 0 {
            // give solution some time to settle
            thread::sleep(Duration::from_secs(3));
        }

        let leader = cfg.check_one_leader();
        total1 = rpcs(&cfg);

        let iters = 10;
        let (starti, term) = match cfg.rafts.lock().unwrap()[leader]
            .as_ref()
            .unwrap()
            .start(&Entry { x: 1 })
        {
            Ok((starti, term)) => (starti, term),
            Err(err) => {
                warn!("start leader {} meet error {:?}", leader, err);
                continue;
            }
        };

        let mut cmds = vec![];
        let mut random = rand::thread_rng();
        for i in 1..iters + 2 {
            let x = random.gen::<u64>();
            cmds.push(x);
            match cfg.rafts.lock().unwrap()[leader]
                .as_ref()
                .unwrap()
                .start(&Entry { x })
            {
                Ok((index1, term1)) => {
                    if term1 != term {
                        // Term changed while starting
                        continue 'outer;
                    }
                    if starti + i != index1 {
                        panic!("start failed");
                    }
                }
                Err(err) => {
                    warn!("start leader {} meet error {:?}", leader, err);
                    continue 'outer;
                }
            }
        }

        for i in 1..=iters {
            if let Some(ix) = cfg.wait(starti + i, SERVERS, Some(term)) {
                if ix.x != cmds[(i - 1) as usize] {
                    panic!(
                        "wrong value {:?} committed for index {}; expected {:?}",
                        ix,
                        starti + i,
                        cmds
                    );
                }
            }
        }

        let mut failed = false;
        total2 = 0;
        for j in 0..SERVERS {
            let t = cfg.rafts.lock().unwrap()[j].as_ref().unwrap().term();
            if t != term {
                // term changed -- can't expect low RPC counts
                // need to keep going to update total2
                failed = true;
            }
            total2 += cfg.rpc_count(j);
        }

        if failed {
            continue 'outer;
        }

        if total2 - total1 > (iters as usize + 1 + 3) * 3 {
            panic!("too many RPCs ({}) for {} entries", total2 - total1, iters);
        }

        success = true;
        break;
    }

    if !success {
        panic!("term changed too often");
    }

    thread::sleep(RAFT_ELECTION_TIMEOUT);

    let mut total3 = 0;
    for j in 0..SERVERS {
        total3 += cfg.rpc_count(j);
    }

    if total3 - total2 > 3 * 20 {
        panic!(
            "too many RPCs ({}) for 1 second of idleness",
            total3 - total2
        );
    }
    cfg.end();
}
