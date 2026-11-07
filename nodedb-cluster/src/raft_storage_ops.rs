// SPDX-License-Identifier: BUSL-1.1

//! Writes to one Raft group's redb log, applied inside a single transaction.
//!
//! A run of writes shares one commit, so one group of fsyncs makes all of
//! them durable. Each write runs in order inside the transaction, so a run
//! leaves the same state as the same writes committed one by one.

use std::ops::Bound;

use redb::{
    CommitError, DatabaseError, ReadableTable, StorageError, TableError, TransactionError,
    WriteTransaction,
};

use nodedb_raft::error::RaftError;
use nodedb_raft::message::LogEntry;
use nodedb_raft::state::HardState;

use crate::wire_version::envelope::encode_versioned;

use crate::raft_storage::{
    ENTRIES, KEY_APPLIED_INDEX, KEY_HARD_STATE, KEY_SNAPSHOT_INDEX, KEY_SNAPSHOT_TERM, META,
    index_key,
};

/// One write to a group's log, applied by `RedbLogStorage::commit_ops`.
#[derive(Debug, Clone, Copy)]
pub enum LogOp<'a> {
    /// Store these entries, replacing any at the same indexes.
    Append(&'a [LogEntry]),
    /// Remove every entry at or above this index.
    Truncate(u64),
    /// Remove every entry at or below `index` and record it as the snapshot
    /// boundary.
    Compact { index: u64, term: u64 },
    /// Record the term and the vote.
    HardState(&'a HardState),
    /// Record the applied floor.
    AppliedIndex(u64),
}

/// Why a run of writes to a group's log did not commit. The run left nothing
/// on disk either way: its transaction aborted.
#[derive(Debug, thiserror::Error)]
pub enum LogWriteError {
    /// An I/O error. The next attempt opens the log again, so it can succeed.
    #[error("raft log write: {context}: {detail}. Retry after the disk recovers.")]
    Transient {
        context: &'static str,
        detail: String,
    },
    /// Every attempt fails the same way: the bytes do not encode, the log
    /// file is corrupt, or a value exceeds what redb stores.
    #[error(
        "raft log write: {context}: {detail}. A retry fails the same way. \
         Fix the cause and restart the node."
    )]
    Deterministic {
        context: &'static str,
        detail: String,
    },
}

impl From<LogWriteError> for RaftError {
    fn from(error: LogWriteError) -> Self {
        RaftError::Storage {
            detail: error.to_string(),
        }
    }
}

impl From<LogWriteError> for crate::error::ClusterError {
    fn from(error: LogWriteError) -> Self {
        Self::Raft(error.into())
    }
}

fn deterministic(context: &'static str, e: impl std::fmt::Display) -> LogWriteError {
    LogWriteError::Deterministic {
        context,
        detail: e.to_string(),
    }
}

/// Classify a redb storage error. redb latches a failed write as `PreviousIo`
/// on the open handle until it reopens, so that error is an I/O error too.
pub(crate) fn storage_error(context: &'static str, e: StorageError) -> LogWriteError {
    match e {
        StorageError::Io(_) | StorageError::PreviousIo => LogWriteError::Transient {
            context,
            detail: e.to_string(),
        },
        other => deterministic(context, other),
    }
}

fn table_error(context: &'static str, e: TableError) -> LogWriteError {
    match e {
        TableError::Storage(e) => storage_error(context, e),
        other => deterministic(context, other),
    }
}

pub(crate) fn transaction_error(context: &'static str, e: TransactionError) -> LogWriteError {
    match e {
        TransactionError::Storage(e) => storage_error(context, e),
        other => deterministic(context, other),
    }
}

pub(crate) fn commit_error(context: &'static str, e: CommitError) -> LogWriteError {
    match e {
        CommitError::Storage(e) => storage_error(context, e),
        other => deterministic(context, other),
    }
}

pub(crate) fn database_error(context: &'static str, e: DatabaseError) -> LogWriteError {
    match e {
        DatabaseError::Storage(e) => storage_error(context, e),
        other => deterministic(context, other),
    }
}

fn put_meta<T: zerompk::ToMessagePack>(
    txn: &WriteTransaction,
    key: &str,
    value: &T,
) -> Result<(), LogWriteError> {
    let bytes = zerompk::to_msgpack_vec(value).map_err(|e| deterministic("serialize meta", e))?;
    let mut meta = txn
        .open_table(META)
        .map_err(|e| table_error("open meta", e))?;
    meta.insert(key, bytes.as_slice())
        .map_err(|e| storage_error("insert meta", e))?;
    Ok(())
}

/// Remove the entries between `lower` and `upper`, as log index keys. An
/// error reading the range fails the write: a commit with entries left behind
/// makes a restart restore them.
fn remove_entries(
    txn: &WriteTransaction,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> Result<(), LogWriteError> {
    let mut table = txn
        .open_table(ENTRIES)
        .map_err(|e| table_error("open entries", e))?;
    let mut keys: Vec<[u8; 8]> = Vec::new();
    for item in table
        .range((lower, upper))
        .map_err(|e| storage_error("range", e))?
    {
        let (key, _) = item.map_err(|e| storage_error("read range entry", e))?;
        let key: [u8; 8] = key
            .value()
            .try_into()
            .map_err(|e| deterministic("read range key", e))?;
        keys.push(key);
    }
    for key in &keys {
        table
            .remove(key.as_slice())
            .map_err(|e| storage_error("remove", e))?;
    }
    Ok(())
}

/// Apply `op` inside `txn`.
pub(crate) fn apply_in_txn(txn: &WriteTransaction, op: &LogOp<'_>) -> Result<(), LogWriteError> {
    match op {
        LogOp::Append(entries) => {
            let mut table = txn
                .open_table(ENTRIES)
                .map_err(|e| table_error("open entries", e))?;
            for entry in *entries {
                let key = index_key(entry.index);
                let value =
                    encode_versioned(entry).map_err(|e| deterministic("serialize entry", e))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| storage_error("insert entry", e))?;
            }
            Ok(())
        }
        LogOp::Truncate(index) => remove_entries(
            txn,
            Bound::Included(index_key(*index).as_slice()),
            Bound::Unbounded,
        ),
        LogOp::Compact { index, term } => {
            remove_entries(
                txn,
                Bound::Unbounded,
                Bound::Excluded(index_key(index + 1).as_slice()),
            )?;
            put_meta(txn, KEY_SNAPSHOT_INDEX, index)?;
            put_meta(txn, KEY_SNAPSHOT_TERM, term)
        }
        LogOp::HardState(state) => put_meta(txn, KEY_HARD_STATE, *state),
        LogOp::AppliedIndex(index) => put_meta(txn, KEY_APPLIED_INDEX, index),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn an_io_error_and_a_latched_io_error_are_transient() {
        let io_error = StorageError::Io(io::Error::other("disk gone"));
        assert!(matches!(
            storage_error("commit", io_error),
            LogWriteError::Transient { .. }
        ));
        assert!(matches!(
            storage_error("commit", StorageError::PreviousIo),
            LogWriteError::Transient { .. }
        ));
        assert!(matches!(
            commit_error(
                "commit",
                CommitError::Storage(StorageError::Io(io::Error::other("disk gone")))
            ),
            LogWriteError::Transient { .. }
        ));
    }

    #[test]
    fn corruption_and_an_oversized_value_are_deterministic() {
        assert!(matches!(
            storage_error("commit", StorageError::Corrupted("bad page".into())),
            LogWriteError::Deterministic { .. }
        ));
        assert!(matches!(
            storage_error("insert entry", StorageError::ValueTooLarge(1 << 40)),
            LogWriteError::Deterministic { .. }
        ));
        assert!(matches!(
            commit_error("commit", CommitError::TransactionPoisoned),
            LogWriteError::Deterministic { .. }
        ));
    }

    /// The error reaches both central errors with its text.
    #[test]
    fn a_log_write_error_wires_into_the_raft_and_cluster_errors() {
        let raft: RaftError = deterministic("serialize entry", "bad bytes").into();
        assert!(matches!(&raft, RaftError::Storage { detail } if detail.contains("bad bytes")));
        let cluster: crate::error::ClusterError = deterministic("serialize entry", "x").into();
        assert!(matches!(
            cluster,
            crate::error::ClusterError::Raft(RaftError::Storage { .. })
        ));
    }
}
