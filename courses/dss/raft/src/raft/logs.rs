use crate::raft::common::last_index_and_element;
use crate::raft::leader::{LogEntry, LogKind, LogState};
use crate::raft::TermId;
use assert2::assert;
use either::Either;
use std::cmp::min;
use std::fmt::{Debug, Formatter};
use std::iter::empty;
use tracing::instrument;

/// `commit_length` split the logs to the two sections:
///
/// ```plot
/// log = |--- committed --- | --- uncommitted --- |
///       0            commit_length            log.len()
/// ```
///
#[derive(Default, Eq, PartialEq)]
pub struct Logs {
    commit_length: usize,
    logs: Vec<LogEntry>,
}

impl Debug for Logs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logs")
            .field("commit_length", &self.commit_length)
            // add index for each `LogEntry`
            .field("logs", &self.logs.iter().enumerate().collect::<Vec<_>>())
            .finish()
    }
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
        self.logs.push(LogEntry::new(log_kind, data, term));
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
        last_index_and_element(self.logs.as_slice())
            .map(|(index, entry)| LogState::new(index, entry.term))
    }

    /// return `LogState` of `self.log[..length]`
    pub fn get_log_state_before(&self, index: usize) -> Option<LogState> {
        assert!(index <= self.logs.len());
        index
            .checked_sub(1)
            .map(|index| LogState::new(index, self.logs[index].term))
    }

    pub fn update_log_tail(&mut self, tail_begin: usize, mut entries: Vec<LogEntry>) {
        assert!(
            tail_begin <= self.logs.len(),
            "{}, {}",
            tail_begin,
            self.logs.len()
        );
        let max_offset = min(entries.len(), self.logs.len() - tail_begin);
        let mutation_offset = (0..max_offset)
            .find(|offset| entries[*offset].term != self.logs[tail_begin + *offset].term)
            .unwrap_or(max_offset);
        let mutation_begin = tail_begin + mutation_offset;
        // must not modify committed logs
        assert!(
            self.commit_length <= mutation_begin,
            "{}, {}",
            self.commit_length,
            mutation_begin
        );
        self.logs
            .splice(mutation_begin.., entries.drain(mutation_offset..));
    }

    /// returns the logs just committed
    #[instrument(skip(self), fields(logs_len = self.logs.len()))]
    pub fn commit_logs<'a>(
        &'a mut self,
        new_commit_length: usize,
    ) -> impl Iterator<Item = (usize, LogEntry)> + 'a {
        assert!(
            new_commit_length <= self.logs.len(),
            "{}, {}",
            new_commit_length,
            self.logs.len()
        );
        // `self.commit_length` can be incremented only
        if self.commit_length < new_commit_length {
            let newly_committed = self
                .logs
                .iter()
                .enumerate()
                .skip(self.commit_length)
                .take(new_commit_length)
                .map(|(index, entry)| (index, entry.clone()));
            self.commit_length = new_commit_length;
            Either::Left(newly_committed)
        } else {
            // `self.logs[new_commit_length..self.commit_length] has been committed
            // this situation occurs when leader has smaller `commit_length` than follower
            // if `node1` is leader at first and `node1.commit_length > node2.commit_length`
            // and `node1.log.len() == node2.log.len()`, then `node1` no longer be leader
            // due to some reasons, and node2 becoms leader
            Either::Right(empty())
        }
    }
}

#[cfg(test)]
mod test_log {
    use super::*;

    fn build_log_entry() -> LogEntry {
        LogEntry::new(LogKind::Command, Vec::new(), 1)
    }

    #[test]
    fn test_update_log_tail() {
        let mut log = Logs {
            logs: Vec::new(),
            commit_length: 0,
        };
        let entries = vec![build_log_entry(), build_log_entry()];
        log.update_log_tail(0, entries.clone());
        assert_eq!(
            log,
            Logs {
                logs: entries,
                commit_length: 0
            }
        );

        let original_entries = vec![build_log_entry(), build_log_entry()];
        let mut log = Logs {
            logs: original_entries.clone(),
            commit_length: 0,
        };
        let new_entries = vec![build_log_entry(), build_log_entry()];
        log.update_log_tail(2, new_entries.clone());
        assert_eq!(
            log.logs,
            original_entries
                .into_iter()
                .chain(new_entries.into_iter())
                .collect::<Vec<_>>()
        );

        let original_entries = vec![
            build_log_entry(),
            build_log_entry(),
            build_log_entry(),
            build_log_entry(),
        ];
        let mut log = Logs {
            logs: original_entries.clone(),
            commit_length: 0,
        };
        let new_entries = vec![build_log_entry(), build_log_entry()];
        log.update_log_tail(1, new_entries.clone());
        assert_eq!(
            log.logs,
            original_entries
                .into_iter()
                .take(1)
                .chain(new_entries)
                .collect::<Vec<_>>()
        );
    }
}
