// SPDX-License-Identifier: BUSL-1.1

//! Conversion between a table's ended versions and their checkpoint form.

use nodedb_types::Surrogate;

use super::format::{KvCheckpointKeyHistory, KvCheckpointVersion};
use crate::engine::kv::hash_table::KvHashTable;
use crate::engine::kv::history::HistoryVersion;

/// Every ended version of every key of `table`, sorted by key so a
/// checkpoint of unchanged state encodes to the same bytes.
pub(super) fn export_history(table: &KvHashTable) -> Vec<KvCheckpointKeyHistory> {
    let mut history: Vec<KvCheckpointKeyHistory> = table
        .history()
        .iter()
        .map(|(key, versions)| KvCheckpointKeyHistory {
            key: key.to_vec(),
            versions: versions
                .iter()
                .map(|v| KvCheckpointVersion {
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
    history
}

/// The engine form of one key's checkpointed ended versions.
pub(super) fn restore_versions(key_history: &KvCheckpointKeyHistory) -> Vec<HistoryVersion> {
    key_history
        .versions
        .iter()
        .map(|v| HistoryVersion {
            sys_from: v.sys_from,
            sys_to: v.sys_to,
            value: v.value.clone(),
            expire_at_ms: v.expire_at_ms,
            surrogate: Surrogate::new(v.surrogate),
        })
        .collect()
}
