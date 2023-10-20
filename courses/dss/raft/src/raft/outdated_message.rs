use dashmap::DashMap;
use derive_new::new;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

pub struct OutdatedMessageDiscarder {
    recent_ids: DashMap<String, u64>,
}

impl OutdatedMessageDiscarder {
    pub fn new() -> Self {
        Self {
            recent_ids: DashMap::new(),
        }
    }

    pub fn may_discard<T>(&self, message: WithIdAndSerial<T>) -> labrpc::Result<T> {
        let WithIdAndSerial {
            id,
            serial: message_serial,
            data,
        } = message;
        let output = if let Some(serial) = self.recent_ids.get(&id) {
            if message_serial > *serial {
                Ok(data)
            } else {
                Err(labrpc::Error::Outdated {
                    local_serial: *serial,
                    remote_serial: message_serial,
                })
            }
        } else {
            Ok(data)
        };
        self.recent_ids
            .alter(&id, |_key, serial| max(serial, message_serial));
        output
    }
}

pub struct IdAndSerialManager<T> {
    id: String,
    serial: AtomicU64,
    inner: T,
}

impl<T> IdAndSerialManager<T> {
    pub fn new(inner: T) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            serial: AtomicU64::new(1),
            inner,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new_args<U>(&self, arg: U) -> WithIdAndSerial<U> {
        let serial = self.serial.fetch_add(1, Ordering::SeqCst);
        WithIdAndSerial::new(self.id.clone(), serial, arg)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, new)]
pub struct WithIdAndSerial<T> {
    pub id: String,
    pub serial: u64,
    pub data: T,
}
