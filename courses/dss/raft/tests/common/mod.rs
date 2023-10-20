use raft::raft::config::{Config, Entry, Storage, SNAPSHOT_INTERVAL};
use raft::raft::Node;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

pub fn init_logger(test_name: &str) {
    let file = File::create(Path::new("../logs").join(format!("{}.log", test_name))).unwrap();
    let subscriber = tracing_subscriber::fmt()
        .with_file(false)
        .with_line_number(false)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(Arc::new(file))
        .with_target(false)
        .without_time()
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap()
}

pub const RAFT_ELECTION_TIMEOUT: Duration = Duration::from_millis(1000);

pub fn random_entry(rnd: &mut ThreadRng) -> Entry {
    Entry {
        x: rnd.gen::<u64>(),
    }
}

pub fn internal_churn(unreliable: bool) {
    let servers = 5;
    let mut cfg = Config::new_with(servers, unreliable, false);
    if unreliable {
        cfg.begin("Test (2C): unreliable churn")
    } else {
        cfg.begin("Test (2C): churn")
    }

    let stop = Arc::new(AtomicUsize::new(0));

    // create concurrent clients
    // TODO: change it a future
    fn cfn(
        me: usize,
        stop_clone: Arc<AtomicUsize>,
        tx: Sender<Option<Vec<u64>>>,
        rafts: Arc<Mutex<Box<[Option<Node>]>>>,
        storage: Arc<Mutex<Storage>>,
    ) {
        let mut values = vec![];
        while stop_clone.load(Ordering::SeqCst) == 0 {
            let mut random = rand::thread_rng();
            let x = random.gen::<u64>();
            let mut index: i64 = -1;
            let mut ok = false;
            // try them all, maybe one of them is a leader
            let rafts: Vec<_> = rafts.lock().unwrap().iter().cloned().collect();
            for raft in &rafts {
                match raft {
                    Some(rf) => {
                        match rf.start(&Entry { x }) {
                            Ok((index1, _)) => {
                                index = index1 as i64;
                                ok = true;
                            }
                            Err(_) => continue,
                        };
                    }
                    None => continue,
                }
            }
            if ok {
                // maybe leader will commit our value, maybe not.
                // but don't wait forever.
                for to in &[10, 20, 50, 100, 200] {
                    let (nd, cmd) = storage.lock().unwrap().n_committed(index as u64);
                    if nd > 0 {
                        match cmd {
                            Some(xx) => {
                                if xx.x == x {
                                    values.push(xx.x);
                                }
                            }
                            None => panic!("wrong command type"),
                        }
                        break;
                    }
                    thread::sleep(Duration::from_millis(*to));
                }
            } else {
                thread::sleep(Duration::from_millis((79 + me * 17) as u64));
            }
        }
        if !values.is_empty() {
            tx.send(Some(values)).unwrap();
        } else {
            tx.send(None).unwrap();
        }
    }

    let ncli = 3;
    let mut nrec = vec![];
    for i in 0..ncli {
        let stop_clone = stop.clone();
        let (tx, rx) = channel();
        let storage = cfg.storage.clone();
        let rafts = cfg.rafts.clone();
        thread::spawn(move || {
            cfn(i, stop_clone, tx, rafts, storage);
        });
        nrec.push(rx);
    }
    let mut random = rand::thread_rng();
    for _iters in 0..20 {
        if (random.gen::<usize>() % 1000) < 200 {
            let i = random.gen::<usize>() % servers;
            cfg.disconnect(i);
        }

        if (random.gen::<usize>() % 1000) < 500 {
            let i = random.gen::<usize>() % servers;
            if cfg.rafts.lock().unwrap().get(i).unwrap().is_none() {
                cfg.start1(i);
            }
            cfg.connect(i);
        }

        if (random.gen::<usize>() % 1000) < 200 {
            let i = random.gen::<usize>() % servers;
            if cfg.rafts.lock().unwrap().get(i).unwrap().is_some() {
                cfg.crash1(i);
            }
        }

        // Make crash/restart infrequent enough that the peers can often
        // keep up, but not so infrequent that everything has settled
        // down from one change to the next. Pick a value smaller than
        // the election timeout, but not hugely smaller.
        thread::sleep((RAFT_ELECTION_TIMEOUT * 7) / 10)
    }

    thread::sleep(RAFT_ELECTION_TIMEOUT);
    cfg.net.set_reliable(true);
    for i in 0..servers {
        if cfg.rafts.lock().unwrap().get(i).unwrap().is_none() {
            cfg.start1(i);
        }
        cfg.connect(i);
    }

    stop.store(1, Ordering::SeqCst);

    let mut values = vec![];
    for rx in &nrec {
        let mut vv = rx.recv().unwrap().unwrap();
        values.append(&mut vv);
    }

    thread::sleep(RAFT_ELECTION_TIMEOUT);

    let last_index = cfg.one(random_entry(&mut random), servers, true);

    let mut really = vec![];
    for index in 1..=last_index {
        let v = cfg.wait(index, servers, None).unwrap();
        really.push(v.x);
    }

    for v1 in &values {
        let mut ok = false;
        for v2 in &really {
            if v1 == v2 {
                ok = true;
            }
        }
        assert!(ok, "didn't find a value");
    }

    cfg.end()
}

pub fn snap_common(name: &str, disconnect: bool, reliable: bool, crash: bool) {
    const MAX_LOG_SIZE: usize = 2000;

    let iters = 30;
    let servers = 3;
    let mut cfg = Config::new_with(servers, !reliable, true);

    cfg.begin(name);

    let mut random = rand::thread_rng();
    cfg.one(random_entry(&mut random), servers, true);
    let mut leader1 = cfg.check_one_leader();

    for i in 0..iters {
        let mut victim = (leader1 + 1) % servers;
        let mut sender = leader1;
        if i % 3 == 1 {
            sender = (leader1 + 1) % servers;
            victim = leader1;
        }

        if disconnect {
            cfg.disconnect(victim);
            cfg.one(random_entry(&mut random), servers - 1, true);
        }
        if crash {
            cfg.crash1(victim);
            cfg.one(random_entry(&mut random), servers - 1, true);
        }
        // send enough to get a snapshot
        for _ in 0..=SNAPSHOT_INTERVAL {
            let _ = cfg.rafts.lock().unwrap()[sender]
                .as_ref()
                .unwrap()
                .start(&random_entry(&mut random));
        }
        // let applier threads catch up with the Start()'s
        cfg.one(random_entry(&mut random), servers - 1, true);

        assert!(cfg.log_size() < MAX_LOG_SIZE, "log size too large");

        if disconnect {
            // reconnect a follower, who maybe behind and
            // needs to receive a snapshot to catch up.
            cfg.connect(victim);
            cfg.one(random_entry(&mut random), servers, true);
            leader1 = cfg.check_one_leader();
        }
        if crash {
            cfg.start1_snapshot(victim);
            cfg.connect(victim);
            cfg.one(random_entry(&mut random), servers, true);
            leader1 = cfg.check_one_leader();
        }
    }
    cfg.end();
}
