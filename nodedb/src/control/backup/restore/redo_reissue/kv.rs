// SPDX-License-Identifier: BUSL-1.1

//! Restored KV keys as redo units.
//!
//! A backup's KV section holds each table in the shape the KV snapshot
//! captures: every live row with the ordinal its value was set at and its
//! absolute expiry, and every ended version of every key with its interval.
//! Each key re-issues as one unit: a `kv_restore` sub-record carrying every
//! version of the key. Every replica installs each version at exactly its
//! backed-up ordinal, so an `AS OF` read after the restore sees what it saw
//! before the backup. A live row whose TTL elapsed installs too: every read
//! hides it from its expiry on, as on the source.
//!
//! Each key binds a destination surrogate. The captured surrogate belongs to
//! the source database, so RESTORE ignores it, and every version of the key
//! installs under the destination's.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::RestoredKvKey;
use nodedb_wal::record::RecordType;

use crate::Error;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::engine::kv::hash_table::KvSnapshotTable;
use crate::types::{TenantId, TraceId};
use crate::wal::{KvRestoreLive, KvRestoreRedo, KvRestoreVersion, RedoSubRecord};

use super::super::target::{DatabaseTarget, RestoredName};
use super::commit::commit_collection;
use super::units::{CollectionUnits, RowUnit};

/// Every version of one key a backed-up table holds.
#[derive(Debug, Default, PartialEq, Eq)]
struct KeyVersions {
    live: Option<KvRestoreLive>,
    history: Vec<KvRestoreVersion>,
}

/// Group a table's live rows and ended versions by key, in key order.
fn key_versions(table: KvSnapshotTable) -> BTreeMap<Vec<u8>, KeyVersions> {
    let KvSnapshotTable { rows, history } = table;
    let mut keys: BTreeMap<Vec<u8>, KeyVersions> = BTreeMap::new();
    for row in rows {
        keys.entry(row.key).or_default().live = Some(KvRestoreLive {
            value: row.value,
            expire_at_ms: row.expire_at_ms,
            sys_from: row.sys_from,
        });
    }
    for key_history in history {
        let entry = keys.entry(key_history.key).or_default();
        entry
            .history
            .extend(key_history.versions.into_iter().map(|v| KvRestoreVersion {
                sys_from: v.sys_from,
                sys_to: v.sys_to,
                value: v.value,
                expire_at_ms: v.expire_at_ms,
            }));
        entry.history.sort_by_key(|v| v.sys_from);
    }
    keys
}

/// The unit one restored key re-issues as.
fn key_unit(
    name: &RestoredName,
    key: Vec<u8>,
    versions: KeyVersions,
    surrogate: nodedb_types::Surrogate,
) -> crate::Result<RowUnit> {
    let stored = name.stored.as_str().to_string();
    let payload = KvRestoreRedo {
        collection: stored.clone(),
        key: key.clone(),
        surrogate: surrogate.as_u32(),
        live: versions.live,
        history: versions.history,
        // The resolve reads the destination at the transaction's turn and
        // names the ordinal, so every replica installs the same one.
        reassert_at: None,
        // A restore installs every backed-up version as it stands.
        clone_copy: false,
    }
    .encode()?;
    Ok(RowUnit {
        ops: vec![RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload,
        }],
        identities: vec![CarriedIdentity {
            collection: name.bare.clone(),
            pk_bytes: key.clone(),
            surrogate,
        }],
        // A restored key publishes its live row as a KV put event.
        changes: Vec::new(),
        rows: Vec::new(),
        kv_keys: vec![RestoredKvKey {
            collection: stored,
            key,
        }],
        columnar_keys: Vec::new(),
        vector_rows: Vec::new(),
        timeseries: Vec::new(),
    })
}

/// Decode and durably re-issue every restored KV table of one database.
/// Each table key is `"{db}:{tid}:{collection}"`, the collection named as the
/// source KV engine stored it. Returns the number of keys re-issued.
pub(in crate::control::backup::restore) async fn reissue_kv_tables(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    tables: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for (table_key, bytes) in tables {
        let name = target.resolve_scoped(&table_key, tenant_id)?;
        let table: KvSnapshotTable =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("restore reissue: deserialize KV table '{table_key}': {e}"),
            })?;
        let keys = key_versions(table);
        if keys.is_empty() {
            continue;
        }
        // Every key's surrogate in one batch at the table's home.
        let key_refs: Vec<&[u8]> = keys.keys().map(Vec::as_slice).collect();
        let surrogates = crate::control::server::surrogate_exchange::assign_surrogates_routed(
            state,
            name.key(target.dest),
            tenant,
            &key_refs,
            TraceId::ZERO,
        )
        .await?;
        if surrogates.len() != keys.len() {
            return Err(Error::Internal {
                detail: format!(
                    "restore of KV table '{}' bound {} of {} keys",
                    name.bare,
                    surrogates.len(),
                    keys.len()
                ),
            });
        }
        let mut units = Vec::with_capacity(keys.len());
        for ((key, versions), surrogate) in keys.into_iter().zip(surrogates) {
            units.push(key_unit(&name, key, versions, surrogate)?);
        }
        reissued += units.len();
        let collection = CollectionUnits::rows(target.dest, name.bare.clone(), units);
        commit_collection(state, tenant, target.mark(), collection).await?;
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::kv::hash_table::{KvSnapshotKeyHistory, KvSnapshotRow, KvSnapshotVersion};

    fn table() -> KvSnapshotTable {
        KvSnapshotTable {
            rows: vec![KvSnapshotRow {
                key: b"a".to_vec(),
                value: b"a2".to_vec(),
                expire_at_ms: 0,
                surrogate: 3,
                sys_from: 20,
            }],
            history: vec![
                KvSnapshotKeyHistory {
                    key: b"a".to_vec(),
                    versions: vec![KvSnapshotVersion {
                        sys_from: 10,
                        sys_to: 20,
                        value: b"a1".to_vec(),
                        expire_at_ms: 0,
                        surrogate: 3,
                    }],
                },
                KvSnapshotKeyHistory {
                    key: b"gone".to_vec(),
                    versions: vec![KvSnapshotVersion {
                        sys_from: 5,
                        sys_to: 8,
                        value: b"g".to_vec(),
                        expire_at_ms: 0,
                        surrogate: 4,
                    }],
                },
            ],
        }
    }

    /// Every key keeps its live row and its ended versions with their
    /// ordinals. A key that ends deleted keeps its history.
    #[test]
    fn every_version_keeps_its_ordinal() {
        let keys = key_versions(table());
        assert_eq!(keys.len(), 2);
        let a = &keys[b"a".as_slice()];
        assert_eq!(a.live.as_ref().map(|l| l.sys_from), Some(20));
        assert_eq!(
            a.history
                .iter()
                .map(|v| (v.sys_from, v.sys_to))
                .collect::<Vec<_>>(),
            vec![(10, 20)]
        );
        let gone = &keys[b"gone".as_slice()];
        assert!(gone.live.is_none());
        assert_eq!(gone.history[0].sys_from, 5);
    }

    /// A key's unit carries one `kv_restore` sub-record under the
    /// destination surrogate and locks the key.
    #[test]
    fn a_key_unit_carries_every_version() {
        let target = DatabaseTarget {
            source: crate::types::DatabaseId::DEFAULT,
            dest: crate::types::DatabaseId::DEFAULT,
            restore_id: 1,
            restore_cut: i64::MAX,
            event_source: crate::event::EventSource::Restore,
        };
        let name = target.bare("sessions");
        let mut keys = key_versions(table());
        let versions = keys.remove(b"a".as_slice()).expect("key a");
        let unit = key_unit(
            &name,
            b"a".to_vec(),
            versions,
            nodedb_types::Surrogate::new(9),
        )
        .expect("unit");
        assert_eq!(unit.ops.len(), 1);
        let redo = KvRestoreRedo::decode(&unit.ops[0].payload).expect("kv_restore");
        assert_eq!(redo.surrogate, 9);
        assert_eq!(redo.live.map(|l| l.sys_from), Some(20));
        assert_eq!(redo.history.len(), 1);
        assert_eq!(unit.kv_keys[0].key, b"a");
        assert_eq!(
            unit.identities[0].surrogate,
            nodedb_types::Surrogate::new(9)
        );
    }
}
