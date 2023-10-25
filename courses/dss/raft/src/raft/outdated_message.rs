use derive_new::new;
use serde::{Deserialize, Serialize};

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::trace;
use uuid::Uuid;

pub struct OutdatedMessageDiscarder<T> {
    recent_ids: HashMap<String, u64>,
    receiver: mpsc::Receiver<WithIdAndSerial<T>>,
}

impl<T> OutdatedMessageDiscarder<T> {
    pub fn new(receiver: mpsc::Receiver<WithIdAndSerial<T>>) -> Self {
        Self {
            recent_ids: HashMap::new(),
            receiver,
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        while let Some(WithIdAndSerial { id, serial, data }) = self.receiver.recv().await {
            match self.recent_ids.entry(id) {
                Entry::Occupied(mut recent_serial) => {
                    if *recent_serial.get() < serial {
                        recent_serial.insert(serial);
                        return Some(data);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(serial);
                    return Some(data);
                }
            }
        }
        None
    }
}

#[derive(new)]
pub struct WithIdAndSerialManager<T> {
    manager: Arc<IdAndSerialManager>,
    inner: T,
}

impl<T> WithIdAndSerialManager<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new_args<U: Debug>(&self, arg: U) -> WithIdAndSerial<U> {
        self.manager.new_args(arg)
    }
}

pub struct IdAndSerialManager {
    id: String,
    serial: AtomicU64,
}

impl IdAndSerialManager {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            serial: AtomicU64::new(1),
        }
    }

    pub fn new_args<U: Debug>(&self, arg: U) -> WithIdAndSerial<U> {
        let serial = self.serial.fetch_add(1, Ordering::SeqCst);
        let id = self.id.clone();
        trace!(serial, id, ?arg, "send message");
        WithIdAndSerial::new(self.id.clone(), serial, arg)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, new)]
pub struct WithIdAndSerial<T> {
    pub id: String,
    pub serial: u64,
    pub data: T,
}
