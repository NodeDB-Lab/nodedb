// SPDX-License-Identifier: BUSL-1.1

//! Restored columnar keys as redo units.
//!
//! A backup's columnar section holds each collection's engine snapshot:
//! every version of every primary key with its interval. Each key re-issues
//! as one unit: a `columnar_restore` sub-record carrying every version of
//! the key, ascending by `sys_from`. The resolve keeps the versions the cut
//! rule chooses (`restore_key_plan`), and every replica installs each at its
//! interval. A RESTORE never truncates the destination: a key the backup
//! does not hold keeps its rows.
//!
//! Every version of a key carries the key's surrogate. A key the backup
//! binds to a primary key re-issues under the destination's surrogate of
//! that primary key. One it does not bind keeps the backup's surrogate.

use std::collections::{BTreeMap, HashMap};

use nodedb_physical::physical_plan::RestoredColumnarKey;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

use crate::Error;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::types::{SurrogateBindEntry, TenantId, TraceId};
use crate::wal::{ColumnarRestoreRedo, ColumnarRestoreVersion, RedoSubRecord};

use super::super::columnar_reissue::{DecodedColumnarRows, decode_snapshot_versions};
use super::super::target::{DatabaseTarget, RestoredName};
use super::commit::commit_collection;
use super::units::{CollectionUnits, RowUnit};

/// Every version of one key, ascending by `sys_from`, under its backed-up
/// surrogate.
type KeyVersions = BTreeMap<Surrogate, Vec<ColumnarRestoreVersion>>;

/// Group the decoded versions by key. `decoded` is ascending by `sys_from`,
/// so each key's versions stay in that order.
fn key_versions(collection: &str, decoded: DecodedColumnarRows) -> crate::Result<KeyVersions> {
    let mut keys: KeyVersions = BTreeMap::new();
    let DecodedColumnarRows {
        rows,
        surrogates,
        versions,
        ..
    } = decoded;
    for ((row, surrogate), version) in rows.iter().zip(surrogates).zip(versions) {
        let row = nodedb_types::value_to_msgpack(row).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: encode a row of columnar '{collection}': {e}"),
        })?;
        keys.entry(surrogate)
            .or_default()
            .push(ColumnarRestoreVersion {
                row,
                sys_from: version.sys_from,
                sys_to: version.sys_to,
            });
    }
    Ok(keys)
}

/// The unit one restored key re-issues as, under `surrogate` in the
/// destination. `pk` is the primary key the backup binds the key to.
fn key_unit(
    name: &RestoredName,
    schema_bytes: &[u8],
    surrogate: Surrogate,
    pk: Option<&[u8]>,
    versions: Vec<ColumnarRestoreVersion>,
) -> crate::Result<RowUnit> {
    let stored = name.stored.as_str().to_string();
    let payload = ColumnarRestoreRedo {
        collection: stored.clone(),
        schema_bytes: schema_bytes.to_vec(),
        surrogate: surrogate.as_u32(),
        versions,
    }
    .encode()?;
    Ok(RowUnit {
        ops: vec![RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload,
        }],
        identities: pk
            .map(|pk| CarriedIdentity {
                collection: name.bare.clone(),
                pk_bytes: pk.to_vec(),
                surrogate,
            })
            .into_iter()
            .collect(),
        changes: Vec::new(),
        rows: Vec::new(),
        kv_keys: Vec::new(),
        columnar_keys: vec![RestoredColumnarKey {
            collection: stored,
            surrogate: surrogate.as_u32(),
        }],
        vector_rows: Vec::new(),
        timeseries: Vec::new(),
    })
}

/// Decode and re-issue every restored plain-columnar collection of one
/// database, one Calvin transaction per batch of keys. Each entry key is
/// `"{db}:{tid}:{collection}"`. `binds` are the backup's surrogate binds.
/// Returns the number of collections that re-issued a key.
pub(in crate::control::backup::restore) async fn reissue_columnar_keys(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    entries: Vec<(String, Vec<u8>)>,
    binds: &[SurrogateBindEntry],
) -> Result<usize, Error> {
    // Columnar segments are written with the WAL encryption key. Absent
    // when at-rest encryption is not configured.
    let kek = state.wal.encryption_key().cloned();
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for (entry_key, bytes) in entries {
        let name = target.resolve_scoped(&entry_key, tenant_id)?;
        let snap: nodedb_columnar::ColumnarEngineSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize ColumnarEngineSnapshot for '{}': {e}",
                    name.bare
                ),
            })?;
        let decoded = decode_snapshot_versions(&name.bare, snap, kek.as_ref())?;
        if decoded.rows.is_empty() {
            continue;
        }
        let schema_bytes = decoded.schema_bytes.clone();
        let keys = key_versions(&name.bare, decoded)?;

        // The primary key each backed-up surrogate binds, and the
        // destination's surrogate of each, in one batch at the home.
        let pks: HashMap<u32, &[u8]> = binds
            .iter()
            .filter(|bind| bind.tenant_id == tenant_id && bind.collection == name.bare)
            .map(|bind| (bind.surrogate, bind.pk.as_slice()))
            .collect();
        let bound: Vec<&[u8]> = keys
            .keys()
            .filter_map(|surrogate| pks.get(&surrogate.as_u32()).copied())
            .collect();
        let assigned = if bound.is_empty() {
            Vec::new()
        } else {
            crate::control::server::surrogate_exchange::assign_surrogates_routed(
                state,
                name.key(target.dest),
                tenant,
                &bound,
                TraceId::ZERO,
            )
            .await?
        };
        if assigned.len() != bound.len() {
            return Err(Error::Internal {
                detail: format!(
                    "restore of columnar '{}' bound {} of {} keys",
                    name.bare,
                    assigned.len(),
                    bound.len()
                ),
            });
        }
        let dest: HashMap<&[u8], Surrogate> = bound.into_iter().zip(assigned).collect();

        let mut units = Vec::with_capacity(keys.len());
        for (backed_up, versions) in keys {
            let pk = pks.get(&backed_up.as_u32()).copied();
            let surrogate = pk.and_then(|pk| dest.get(pk).copied()).unwrap_or(backed_up);
            units.push(key_unit(&name, &schema_bytes, surrogate, pk, versions)?);
        }
        let collection = CollectionUnits::rows(target.dest, name.bare.clone(), units);
        commit_collection(state, tenant, target.mark(), collection).await?;
        reissued += 1;
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::RestoredVersion;
    use nodedb_types::value::Value;

    use super::*;

    fn decoded() -> DecodedColumnarRows {
        let row = |id: i64, v: i64| {
            Value::Object(
                [
                    ("id".to_string(), Value::Integer(id)),
                    ("v".to_string(), Value::Integer(v)),
                ]
                .into_iter()
                .collect(),
            )
        };
        let version = |sys_from, sys_to| RestoredVersion {
            sys_from,
            sys_to,
            clone_copy: false,
        };
        DecodedColumnarRows {
            rows: vec![row(1, 10), row(2, 20), row(1, 11)],
            surrogates: vec![Surrogate::new(5), Surrogate::new(6), Surrogate::new(5)],
            versions: vec![version(10, Some(30)), version(20, None), version(30, None)],
            schema_bytes: Vec::new(),
        }
    }

    /// Each key keeps every version, ascending by `sys_from`.
    #[test]
    fn versions_group_by_key_in_order() {
        let keys = key_versions("m", decoded()).expect("group");
        assert_eq!(keys.len(), 2);
        let first = &keys[&Surrogate::new(5)];
        assert_eq!(
            first
                .iter()
                .map(|v| (v.sys_from, v.sys_to))
                .collect::<Vec<_>>(),
            vec![(10, Some(30)), (30, None)]
        );
    }

    /// A key's unit carries one `columnar_restore` sub-record and locks the
    /// key's surrogate.
    #[test]
    fn a_key_unit_locks_its_surrogate() {
        let target = DatabaseTarget {
            source: crate::types::DatabaseId::DEFAULT,
            dest: crate::types::DatabaseId::DEFAULT,
            restore_id: 1,
            restore_cut: i64::MAX,
        };
        let name = target.bare("metrics");
        let mut keys = key_versions("metrics", decoded()).expect("group");
        let versions = keys.remove(&Surrogate::new(5)).expect("key 5");
        let unit = key_unit(&name, &[], Surrogate::new(9), Some(b"1"), versions).expect("unit");
        let redo = ColumnarRestoreRedo::decode(&unit.ops[0].payload).expect("columnar_restore");
        assert_eq!(redo.surrogate, 9);
        assert_eq!(redo.versions.len(), 2);
        assert_eq!(unit.columnar_keys[0].surrogate, 9);
        assert_eq!(unit.identities[0].surrogate, Surrogate::new(9));
    }
}
