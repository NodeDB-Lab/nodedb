// SPDX-License-Identifier: BUSL-1.1

//! Writes to one Raft group's redb log, applied inside a single transaction.
//!
//! A run of writes shares one commit, so one group of fsyncs makes all of
//! them durable. Each write runs in order inside the transaction, so a run
//! leaves the same state as the same writes committed one by one.

use std::ops::Bound;

use redb::{ReadableTable, WriteTransaction};

use nodedb_raft::error::{RaftError, Result};
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

fn storage_err(context: &str, e: impl std::fmt::Display) -> RaftError {
    RaftError::Storage {
        detail: format!("{context}: {e}"),
    }
}

fn put_meta<T: zerompk::ToMessagePack>(txn: &WriteTransaction, key: &str, value: &T) -> Result<()> {
    let bytes = zerompk::to_msgpack_vec(value).map_err(|e| storage_err("serialize", e))?;
    let mut meta = txn
        .open_table(META)
        .map_err(|e| storage_err("open meta", e))?;
    meta.insert(key, bytes.as_slice())
        .map_err(|e| storage_err("insert meta", e))?;
    Ok(())
}

/// Remove the entries between `lower` and `upper`, as log index keys.
fn remove_entries(txn: &WriteTransaction, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<()> {
    let mut table = txn
        .open_table(ENTRIES)
        .map_err(|e| storage_err("open entries", e))?;
    let keys: Vec<[u8; 8]> = table
        .range((lower, upper))
        .map_err(|e| storage_err("range", e))?
        .filter_map(|r| {
            r.ok().map(|(k, _)| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(k.value());
                buf
            })
        })
        .collect();
    for key in &keys {
        table
            .remove(key.as_slice())
            .map_err(|e| storage_err("remove", e))?;
    }
    Ok(())
}

/// Apply `op` inside `txn`.
pub(crate) fn apply_in_txn(txn: &WriteTransaction, op: &LogOp<'_>) -> Result<()> {
    match op {
        LogOp::Append(entries) => {
            let mut table = txn
                .open_table(ENTRIES)
                .map_err(|e| storage_err("open entries", e))?;
            for entry in *entries {
                let key = index_key(entry.index);
                let value =
                    encode_versioned(entry).map_err(|e| storage_err("serialize entry", e))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| storage_err("insert entry", e))?;
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
