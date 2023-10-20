use dashmap::DashMap;
use derive_new::new;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{instrument, trace};
use uuid::Uuid;

pub struct OutdatedMessageDiscarder {
    recent_ids: DashMap<String, u64>,
}

// leader 可能连续发送两个 append entries，但是后一个 append entries 先到，导致乱序
// FIXME: 在这种情况下是不是乱序不会改变结果？
impl OutdatedMessageDiscarder {
    pub fn new() -> Self {
        Self {
            recent_ids: DashMap::new(),
        }
    }

    #[instrument(skip_all, level = "trace")]
    pub fn may_discard<T: Debug>(&self, message: WithIdAndSerial<T>) -> labrpc::Result<T> {
        let WithIdAndSerial {
            id,
            serial: message_serial,
            data,
        } = message;
        let output = if let Some(serial) = self.recent_ids.get(&id) {
            let serial = *serial;
            if message_serial > serial {
                trace!(serial, message_serial, ?data, "receive fresh message");
                Ok(data)
            } else {
                trace!(serial, message_serial, ?data, "receive stale message");
                Err(labrpc::Error::Outdated {
                    local_serial: serial,
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
