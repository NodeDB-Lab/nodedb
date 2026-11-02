// SPDX-License-Identifier: BUSL-1.1

//! The KV section of a tenant snapshot: every live row with the ordinal its
//! version starts at, and every ended version of every key.
//!
//! A snapshot install restores each version at the ordinal the source
//! stamped, so a read at any cut answers on the installed node exactly as it
//! answered on the source.

use nodedb_types::Surrogate;

use super::super::hash_helpers::extract_value_from;
use super::super::history::HistoryVersion;
use super::types::KvHashTable;

/// One live row of a tenant snapshot's KV section.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvSnapshotRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms, `0` for a row with no TTL.
    pub expire_at_ms: u64,
    /// The row's bound identity in the source database.
    pub surrogate: u32,
    /// Ordinal of the write that set the row's value.
    pub sys_from: i64,
}

/// One ended version of a key, visible at every ordinal `S` with
/// `sys_from <= S < sys_to`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvSnapshotVersion {
    pub sys_from: i64,
    pub sys_to: i64,
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms the version held, `0` for none.
    pub expire_at_ms: u64,
    /// Bound row identity the version held.
    pub surrogate: u32,
}

/// Every ended version of one key, oldest first.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvSnapshotKeyHistory {
    pub key: Vec<u8>,
    pub versions: Vec<KvSnapshotVersion>,
}

/// One KV table of a tenant snapshot.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct KvSnapshotTable {
    /// Every live row.
    pub rows: Vec<KvSnapshotRow>,
    /// Every ended version of every key, including keys no live row holds.
    pub history: Vec<KvSnapshotKeyHistory>,
}

impl KvSnapshotVersion {
    /// The engine form of this version.
    pub fn into_history(self) -> HistoryVersion {
        HistoryVersion {
            sys_from: self.sys_from,
            sys_to: self.sys_to,
            value: self.value,
            expire_at_ms: self.expire_at_ms,
            surrogate: Surrogate::new(self.surrogate),
        }
    }
}

impl KvHashTable {
    /// Export the table as a tenant-snapshot table, [`KvSnapshotTable`]:
    /// every live row with its bound surrogate and its version's ordinal, and
    /// every ended version. Keys sort, so unchanged state encodes alike.
    pub fn export_snapshot(&self) -> KvSnapshotTable {
        let mut rows: Vec<KvSnapshotRow> = self
            .live_entries()
            .map(|entry| KvSnapshotRow {
                key: entry.key.clone(),
                value: extract_value_from(&entry.value, &self.overflow),
                expire_at_ms: entry.expire_at_ms,
                surrogate: entry.surrogate.as_u32(),
                sys_from: entry.sys_from,
            })
            .collect();
        rows.sort_by(|a, b| a.key.cmp(&b.key));
        let mut history: Vec<KvSnapshotKeyHistory> = self
            .history
            .iter()
            .map(|(key, versions)| KvSnapshotKeyHistory {
                key: key.to_vec(),
                versions: versions
                    .iter()
                    .map(|v| KvSnapshotVersion {
                        sys_from: v.sys_from,
                        sys_to: v.sys_to,
                        value: v.value.clone(),
                        expire_at_ms: v.expire_at_ms,
                        surrogate: v.surrogate.as_u32(),
                    })
                    .collect(),
            })
            .collect();
        history.sort_by(|a, b| a.key.cmp(&b.key));
        KvSnapshotTable { rows, history }
    }
}
