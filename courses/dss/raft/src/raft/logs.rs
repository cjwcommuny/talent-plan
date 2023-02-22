use crate::raft::leader::{LogEntry, LogKind, LogState};
use crate::raft::TermId;
use std::cmp::{max, min};

/// `commit_length` split the logs to the two sections:
///
/// ```plot
/// log = |--- committed --- | --- uncommitted --- |
///       0            commit_length            log.len()
/// ```
///
#[derive(Default, Debug)]
pub struct Logs {
    logs: Vec<LogEntry>,
    commit_length: usize,
}

impl Logs {
    pub fn get_log_len(&self) -> usize {
        self.logs.len()
    }

    pub fn get_commit_length(&self) -> usize {
        self.commit_length
    }

    pub fn add_log(&mut self, log_kind: LogKind, data: Vec<u8>, term: TermId) -> usize {
        let index = self.logs.len();
        self.logs.push(LogEntry::new(log_kind, data, index, term));
        index
    }

    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        self.logs.get(index)
    }

    pub fn get_tail(&self, tail_begin: usize) -> impl Iterator<Item = &LogEntry> {
        self.logs[tail_begin..].iter()
    }

    pub fn get_log_state(&self) -> Option<LogState> {
        self.logs.last().map(|entry| entry.log_state)
    }

    /// return `LogState` of `self.log[..length]`
    pub fn get_log_state_front(&self, length: usize) -> Option<LogState> {
        if length > 0 {
            Some(self.logs[length - 1].log_state)
        } else {
            None
        }
    }

    pub fn update_log_tail(&mut self, tail_begin: usize, mut entries: Vec<LogEntry>) {
        assert!(tail_begin >= self.commit_length); // must not modify committed logs
        let limit = min(entries.len(), self.logs.len() - tail_begin);
        let mutation_offset = (0..limit)
            .find(|offset| {
                entries[*offset].log_state.term != self.logs[tail_begin + *offset].log_state.term
            })
            .unwrap_or(limit);
        self.logs.splice(
            tail_begin + mutation_offset..,
            entries.drain(mutation_offset..),
        );
    }

    /// returns the logs just committed
    pub fn commit_logs(&mut self, new_commit_length: usize) -> impl Iterator<Item = &LogEntry> {
        assert!(new_commit_length <= self.logs.len());
        let just_committed = self.logs[self.commit_length..new_commit_length].iter();
        // `self.commit_length` can be incremented only
        self.commit_length = max(self.commit_length, new_commit_length);
        just_committed
    }
}
