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
//! Every version of a key carries the key's surrogate. A key re-issues
//! under the destination's surrogate of its primary key: the one the backup
//! binds its surrogate to, else the one its rows name. A key with neither
//! takes a fresh surrogate (`super::row_keys`).

use std::collections::{BTreeMap, HashMap};

use nodedb_physical::physical_plan::RestoredColumnarKey;
use nodedb_types::value::Value;
use nodedb_types::{DEFAULT_IDENTITY_COLUMN, Surrogate, value_to_pk_string};
use nodedb_wal::record::RecordType;

use crate::Error;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::types::TenantId;
use crate::wal::{ColumnarRestoreRedo, ColumnarRestoreVersion, RedoSubRecord};

use super::super::bind_conflicts::CarriedBind;
use super::super::columnar_reissue::{DecodedColumnarRows, decode_snapshot_versions};
use super::super::target::{DatabaseTarget, RestoredName};
use super::commit::commit_collection;
use super::row_keys::{RowKey, destination_keys};
use super::units::{CollectionUnits, RowUnit};

/// Every version of one key, ascending by `sys_from`, and the primary key
/// its rows name.
#[derive(Debug, Default)]
struct KeyRows {
    versions: Vec<ColumnarRestoreVersion>,
    /// The value of the identity column in the key's rows: the declared
    /// primary key, else `id`. `None` for a keyless row.
    row_key: Option<String>,
}

/// Every key's rows, under its backed-up surrogate.
type KeyVersions = BTreeMap<Surrogate, KeyRows>;

/// The value a row holds in its identity column, as INSERT reads it: the
/// declared primary key, else [`DEFAULT_IDENTITY_COLUMN`]. A row keyed by
/// `_rowid` alone names no key.
fn row_key(row: &Value, declared_primary_key: Option<&str>) -> Option<String> {
    let Value::Object(columns) = row else {
        return None;
    };
    columns
        .get(declared_primary_key.unwrap_or(DEFAULT_IDENTITY_COLUMN))
        .and_then(value_to_pk_string)
}

/// Group the decoded versions by key. `decoded` is ascending by `sys_from`,
/// so each key's versions stay in that order.
fn key_versions(
    collection: &str,
    decoded: DecodedColumnarRows,
    declared_primary_key: Option<&str>,
) -> crate::Result<KeyVersions> {
    let mut keys: KeyVersions = BTreeMap::new();
    let DecodedColumnarRows {
        rows,
        surrogates,
        versions,
        ..
    } = decoded;
    for ((row, surrogate), version) in rows.iter().zip(surrogates).zip(versions) {
        let entry = keys.entry(surrogate).or_default();
        if entry.row_key.is_none() {
            entry.row_key = row_key(row, declared_primary_key);
        }
        let row = nodedb_types::value_to_msgpack(row).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: encode a row of columnar '{collection}': {e}"),
        })?;
        entry.versions.push(ColumnarRestoreVersion {
            row,
            sys_from: version.sys_from,
            sys_to: version.sys_to,
        });
    }
    Ok(keys)
}

/// The key each restored key binds: the one the backup binds its surrogate
/// to, else the primary key its rows name, else [`RowKey::Keyless`]. Every
/// columnar key holds a row at some system time, so a keyless key still
/// re-issues.
fn key_bindings(
    keys: KeyVersions,
    pks: &HashMap<u32, &[u8]>,
) -> Vec<(RowKey, Vec<ColumnarRestoreVersion>)> {
    keys.into_iter()
        .map(|(backed_up, rows)| {
            let key = match (pks.get(&backed_up.as_u32()), rows.row_key) {
                (Some(pk), _) => RowKey::Carried(pk.to_vec()),
                (None, Some(row_key)) => RowKey::Carried(row_key.into_bytes()),
                (None, None) => RowKey::Keyless,
            };
            (key, rows.versions)
        })
        .collect()
}

/// The unit one restored key re-issues as, under `surrogate` in the
/// destination. `pk` is the key the unit binds to `surrogate`.
fn key_unit(
    name: &RestoredName,
    schema_bytes: &[u8],
    surrogate: Surrogate,
    pk: &[u8],
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
        identities: vec![CarriedIdentity {
            collection: name.bare.clone(),
            pk_bytes: pk.to_vec(),
            surrogate,
        }],
        changes: Vec::new(),
        rows: Vec::new(),
        kv_keys: Vec::new(),
        columnar_keys: vec![RestoredColumnarKey {
            collection: stored,
            surrogate: surrogate.as_u32(),
        }],
        vector_rows: Vec::new(),
        synced_text: Vec::new(),
        timeseries: Vec::new(),
    })
}

/// Decode and re-issue every restored plain-columnar collection of one
/// database, one Calvin transaction per batch of keys. Each entry key is
/// `"{db}:{tid}:{collection}"`. `carried` names the key of each backed-up
/// surrogate. Returns the number of collections that re-issued a key.
pub(in crate::control::backup::restore) async fn reissue_columnar_keys(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    entries: Vec<(String, Vec<u8>)>,
    carried: &[CarriedBind],
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
        let declared_primary_key = state
            .credentials
            .catalog()
            .get_collection(target.dest, tenant_id, &name.bare)?
            .and_then(|stored| stored.declared_primary_key);
        let keys = key_versions(&name.bare, decoded, declared_primary_key.as_deref())?;

        // The key of each restored key, and the destination's surrogate of
        // each.
        let pks: HashMap<u32, &[u8]> = carried
            .iter()
            .filter(|bind| bind.collection == name.bare)
            .map(|bind| (bind.surrogate, bind.pk.as_slice()))
            .collect();
        let (bindings, versions): (Vec<RowKey>, Vec<Vec<ColumnarRestoreVersion>>) =
            key_bindings(keys, &pks).into_iter().unzip();
        let bound =
            destination_keys(state, name.key(target.dest), tenant, &name.bare, &bindings).await?;

        let mut units = Vec::with_capacity(bound.len());
        for ((surrogate, pk), versions) in bound.into_iter().zip(versions) {
            units.push(key_unit(&name, &schema_bytes, surrogate, &pk, versions)?);
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
        let keys = key_versions("m", decoded(), None).expect("group");
        assert_eq!(keys.len(), 2);
        let first = &keys[&Surrogate::new(5)];
        assert_eq!(first.row_key.as_deref(), Some("1"));
        assert_eq!(
            first
                .versions
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
            event_source: crate::event::EventSource::Restore,
        };
        let name = target.bare("metrics");
        let mut keys = key_versions("metrics", decoded(), None).expect("group");
        let versions = keys.remove(&Surrogate::new(5)).expect("key 5").versions;
        let unit = key_unit(&name, &[], Surrogate::new(9), b"1", versions).expect("unit");
        let redo = ColumnarRestoreRedo::decode(&unit.ops[0].payload).expect("columnar_restore");
        assert_eq!(redo.surrogate, 9);
        assert_eq!(redo.versions.len(), 2);
        assert_eq!(unit.columnar_keys[0].surrogate, 9);
        assert_eq!(unit.identities[0].surrogate, Surrogate::new(9));
    }

    /// A key binds the key the backup binds its surrogate to, else the key
    /// its rows name, else none, and then it takes a fresh surrogate.
    #[test]
    fn a_key_binds_its_bind_then_its_row_key() {
        let mut keys = key_versions("m", decoded(), None).expect("group");
        let keyless = Value::Object([("v".to_string(), Value::Integer(1))].into_iter().collect());
        keys.insert(
            Surrogate::new(7),
            KeyRows {
                versions: Vec::new(),
                row_key: row_key(&keyless, None),
            },
        );
        let pks: HashMap<u32, &[u8]> = [(6, b"bound".as_slice())].into_iter().collect();
        let bindings: Vec<RowKey> = key_bindings(keys, &pks)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(
            bindings,
            [
                RowKey::Carried(b"1".to_vec()),
                RowKey::Carried(b"bound".to_vec()),
                RowKey::Keyless,
            ]
        );
    }

    /// The declared primary key names a row's key in place of `id`.
    #[test]
    fn the_declared_primary_key_names_the_row_key() {
        let row = Value::Object(
            [
                ("id".to_string(), Value::Integer(1)),
                ("sensor".to_string(), Value::String("s-9".into())),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(row_key(&row, Some("sensor")).as_deref(), Some("s-9"));
        assert_eq!(row_key(&row, None).as_deref(), Some("1"));
    }
}
