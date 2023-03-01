use crate::raft::leader::{LogEntry, LogKind, LogState};
use crate::raft::TermId;
use std::cmp::min;
use tracing::instrument;

/// `commit_length` split the logs to the two sections:
///
/// ```plot
/// log = |--- committed --- | --- uncommitted --- |
///       0            commit_length            log.len()
/// ```
///
#[derive(Default, Debug, Eq, PartialEq)]
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
        assert!(tail_begin <= self.logs.len());
        self.logs[tail_begin..].iter()
    }

    pub fn get_log_state(&self) -> Option<LogState> {
        self.logs.last().map(|entry| entry.log_state)
    }

    /// return `LogState` of `self.log[..length]`
    pub fn get_log_state_front(&self, length: usize) -> Option<LogState> {
        if length == 0 {
            None
        } else {
            Some(self.logs[length - 1].log_state)
        }
    }

    // TODO: test
    pub fn update_log_tail(&mut self, tail_begin: usize, mut entries: Vec<LogEntry>) {
        assert!(self.commit_length <= tail_begin && tail_begin <= self.logs.len(), "{}, {}, {}", tail_begin, self.commit_length, self.logs.len()); // must not modify committed logs
        let max_offset = min(entries.len(), self.logs.len() - tail_begin);
        let mutation_offset = (0..max_offset)
            .find(|offset| {
                entries[*offset].log_state.term != self.logs[tail_begin + *offset].log_state.term
            })
            .unwrap_or(max_offset);
        self.logs.splice(
            tail_begin + mutation_offset..,
            entries.drain(mutation_offset..),
        );
    }

    /// returns the logs just committed
    #[instrument(skip(self), fields(logs_len = self.logs.len()))]
    pub fn commit_logs(&mut self, new_commit_length: usize) -> impl Iterator<Item = &LogEntry> {
        assert!(new_commit_length <= self.logs.len(), "{}, {}", new_commit_length, self.logs.len());
        let newly_committed = self.logs[self.commit_length..new_commit_length].iter();
        // `self.commit_length` can be incremented only
        assert!(self.commit_length <= new_commit_length);
        self.commit_length = new_commit_length;
        newly_committed
    }
}

#[cfg(test)]
mod test_log {
    use super::*;

    fn build_log_entry(index: usize) -> LogEntry {
        LogEntry::new(LogKind::Command, Vec::new(), index, 1)
    }

    #[test]
    fn test_update_log_tail() {
        let mut log = Logs { logs: Vec::new(), commit_length: 0 };
        let entries = vec![build_log_entry(0), build_log_entry(1)];
        log.update_log_tail(0, entries.clone());
        assert_eq!(log, Logs { logs: entries, commit_length: 0});

        let original_entries = vec![build_log_entry(0), build_log_entry(1)];
        let mut log = Logs { logs: original_entries.clone(), commit_length: 0 };
        let new_entries = vec![build_log_entry(2), build_log_entry(3)];
        log.update_log_tail(2, new_entries.clone());
        assert_eq!(log.logs, original_entries.into_iter().chain(new_entries.into_iter()).collect::<Vec<_>>());

        let original_entries = vec![
            build_log_entry(0),
            build_log_entry(1),
            build_log_entry(2),
            build_log_entry(3)
        ];
        let mut log = Logs { logs: original_entries.clone(), commit_length: 0 };
        let new_entries = vec![build_log_entry(1), build_log_entry(2)];
        log.update_log_tail(1, new_entries.clone());
        assert_eq!(log.logs, original_entries.into_iter().take(1).chain(new_entries).collect::<Vec<_>>());
    }
}