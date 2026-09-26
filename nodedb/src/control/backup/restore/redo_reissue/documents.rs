// SPDX-License-Identifier: BUSL-1.1

//! Restored document rows as redo units.
//!
//! A backup carries each row in its stored form, keyed by its storage key:
//!
//! * `documents` — `"{db}:{tid}:{collection}:{storage_key}"`, the current row
//!   of a collection that keeps no history;
//! * `documents_versioned` — `"{db}:{tid}:{collection}:{storage_key}\x00{sys:020}"`,
//!   every version of a `bitemporal=true` row.
//!
//! Each row becomes one unit: its sub-records in version order plus the
//! identity every replica binds before it installs them. A strict row's Binary
//! Tuple decodes back to MessagePack with the collection's schema, the same
//! conversion a transaction commit applies to a staged strict row. Every
//! replica re-derives the row's secondary index entries as it installs it.

use std::collections::{BTreeMap, HashMap};

use nodedb_types::columnar::StrictSchema;
use nodedb_types::{CollectionType, DocumentMode, RowIdentity, StorageKey};

use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::data::executor::strict_format::{binary_tuple_to_msgpack, undecodable_strict_row};
use crate::engine::sparse::btree_versioned::{TAG_LIVE, TAG_TOMBSTONE, decode_value};
use crate::types::{DatabaseId, SurrogateBindEntry, TenantId};

use super::sub_record::{VersionStamp, document_put, document_tombstone};
use super::units::{CollectionUnits, RowUnit};

/// A row as the backup stored it.
enum StoredRow {
    /// The one current body of a row with no history.
    Current(Vec<u8>),
    /// Every `(sys_from_ms, versioned value)` of a bitemporal row.
    Versions(Vec<(i64, Vec<u8>)>),
}

/// What decoding a collection's rows reads from its catalog entry.
struct CollectionShape {
    strict: Option<StrictSchema>,
    declared_primary_key: Option<String>,
}

fn malformed(key: &str) -> crate::Error {
    let prefix: String = key.chars().take(64).collect();
    crate::Error::Serialization {
        format: "backup".into(),
        detail: format!("restore: document key '{prefix}' is malformed"),
    }
}

/// Split `"{db}:{tid}:{collection}:{rest}"`, checking the tenant.
fn split_key(key: &str, tenant_id: u64) -> crate::Result<(u64, &str, &str)> {
    let mut parts = key.splitn(4, ':');
    let (Some(db), Some(tid), Some(collection), Some(rest)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(malformed(key));
    };
    let db = db.parse::<u64>().map_err(|_| malformed(key))?;
    if tid.parse::<u64>().ok() != Some(tenant_id) || collection.is_empty() {
        return Err(malformed(key));
    }
    Ok((db, collection, rest))
}

/// Group every restored row by `(database, collection)`, then by storage key.
fn group_rows(
    tenant_id: u64,
    documents: Vec<(String, Vec<u8>)>,
    documents_versioned: Vec<(String, Vec<u8>)>,
) -> crate::Result<BTreeMap<(u64, String), BTreeMap<StorageKey, StoredRow>>> {
    let mut grouped: BTreeMap<(u64, String), BTreeMap<StorageKey, StoredRow>> = BTreeMap::new();
    for (key, body) in documents {
        let (db, collection, rest) = split_key(&key, tenant_id)?;
        let storage_key = StorageKey::parse(rest).ok_or_else(|| malformed(&key))?;
        grouped
            .entry((db, collection.to_string()))
            .or_default()
            .insert(storage_key, StoredRow::Current(body));
    }
    for (key, value) in documents_versioned {
        let (db, collection, rest) = split_key(&key, tenant_id)?;
        let (hex, sys) = rest.split_once('\x00').ok_or_else(|| malformed(&key))?;
        let storage_key = StorageKey::parse(hex).ok_or_else(|| malformed(&key))?;
        let sys_from_ms = sys.parse::<i64>().map_err(|_| malformed(&key))?;
        let rows = grouped.entry((db, collection.to_string())).or_default();
        match rows
            .entry(storage_key)
            .or_insert_with(|| StoredRow::Versions(Vec::new()))
        {
            StoredRow::Versions(versions) => versions.push((sys_from_ms, value)),
            StoredRow::Current(_) => {
                return Err(crate::Error::Serialization {
                    format: "backup".into(),
                    detail: format!(
                        "restore: row {storage_key} of '{collection}' is both current-only \
                         and versioned"
                    ),
                });
            }
        }
    }
    for rows in grouped.values_mut() {
        for row in rows.values_mut() {
            if let StoredRow::Versions(versions) = row {
                versions.sort_by_key(|(sys, _)| *sys);
            }
        }
    }
    Ok(grouped)
}

fn collection_shape(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<CollectionShape> {
    let stored = state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, collection)?
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "restore: the backup holds rows of '{collection}' but restored no catalog \
                 entry for it"
            ),
        })?;
    // The storage mode the Data Plane registers for the collection, so a row
    // decodes with the schema it was encoded with.
    let strict = match stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => Some(schema),
        CollectionType::KeyValue(config) => Some(config.schema),
        CollectionType::Document(DocumentMode::Schemaless) | CollectionType::Columnar(_) => None,
    };
    Ok(CollectionShape {
        strict,
        declared_primary_key: stored.declared_primary_key,
    })
}

/// A stored body as the MessagePack a put carries.
fn body_msgpack(
    shape: &CollectionShape,
    collection: &str,
    key: StorageKey,
    body: &[u8],
) -> crate::Result<Vec<u8>> {
    match &shape.strict {
        Some(schema) => binary_tuple_to_msgpack(body, schema)
            .ok_or_else(|| undecodable_strict_row(collection, key.to_identity().as_str())),
        None => Ok(body.to_vec()),
    }
}

/// Builds each row's unit for one collection.
struct RowBuilder<'a> {
    state: &'a SharedState,
    database_id: DatabaseId,
    tenant: TenantId,
    collection: &'a str,
    shape: CollectionShape,
    /// `storage surrogate → primary key` the backup bound for this collection.
    binds: HashMap<u32, &'a [u8]>,
}

impl RowBuilder<'_> {
    /// The row's client identity: the backup's binding, else the identity
    /// INSERT derives from the row body.
    fn identity(&self, key: StorageKey, body: Option<&[u8]>) -> crate::Result<RowIdentity> {
        if let Some(pk) = self.binds.get(&key.surrogate().as_u32()) {
            let pk = std::str::from_utf8(pk).map_err(|_| crate::Error::Serialization {
                format: "backup".into(),
                detail: format!(
                    "restore: the backup binds row {key} of '{}' to a key that is not UTF-8",
                    self.collection
                ),
            })?;
            return Ok(RowIdentity::from_user_key(pk));
        }
        Ok(match body {
            Some(body) => {
                RowIdentity::of_stored_row(body, self.shape.declared_primary_key.as_deref(), key)
            }
            None => key.to_identity(),
        })
    }

    /// Bind the row's identity on this node. The backup's surrogate wins
    /// unless this node already binds the identity: the row then installs
    /// under that surrogate, over the row it names.
    fn bind(&self, identity: &RowIdentity, key: StorageKey) -> crate::Result<CarriedIdentity> {
        let surrogate = self.state.surrogate_assigner.bind(
            self.database_id,
            self.tenant,
            self.collection,
            identity.as_str().as_bytes(),
            key.surrogate(),
        )?;
        Ok(CarriedIdentity {
            collection: self.collection.to_string(),
            pk_bytes: identity.as_str().as_bytes().to_vec(),
            surrogate,
        })
    }

    fn current(&self, key: StorageKey, body: &[u8]) -> crate::Result<RowUnit> {
        let value = body_msgpack(&self.shape, self.collection, key, body)?;
        let identity = self.identity(key, Some(&value))?;
        let carried = self.bind(&identity, key)?;
        let op = document_put(
            self.collection,
            identity.as_str(),
            value,
            carried.surrogate.as_u32(),
            None,
        )?;
        Ok(RowUnit {
            ops: vec![op],
            identities: vec![carried],
        })
    }

    fn versions(&self, key: StorageKey, versions: &[(i64, Vec<u8>)]) -> crate::Result<RowUnit> {
        // Decode every version first: the identity comes from a live body.
        let mut decoded = Vec::with_capacity(versions.len());
        for (sys_from_ms, raw) in versions {
            let version = decode_value(raw)?;
            let body = match version.tag {
                TAG_LIVE => Some(body_msgpack(
                    &self.shape,
                    self.collection,
                    key,
                    version.body,
                )?),
                TAG_TOMBSTONE => None,
                tag => {
                    return Err(crate::Error::Serialization {
                        format: "versioned-doc".into(),
                        detail: format!(
                            "restore: version {sys_from_ms} of row {key} of '{}' carries tag \
                             {tag:#04x}, which no write path records",
                            self.collection
                        ),
                    });
                }
            };
            let stamp = VersionStamp {
                sys_from_ms: *sys_from_ms,
                valid_from_ms: version.valid_from_ms,
                valid_until_ms: version.valid_until_ms,
            };
            decoded.push((stamp, body));
        }
        let first_live = decoded.iter().find_map(|(_, body)| body.as_deref());
        let identity = self.identity(key, first_live)?;
        let carried = self.bind(&identity, key)?;
        let surrogate = carried.surrogate.as_u32();
        let mut ops = Vec::with_capacity(decoded.len());
        for (stamp, body) in decoded {
            ops.push(match body {
                Some(value) => document_put(
                    self.collection,
                    identity.as_str(),
                    value,
                    surrogate,
                    Some(stamp),
                )?,
                None => document_tombstone(
                    self.collection,
                    identity.as_str(),
                    surrogate,
                    stamp.sys_from_ms,
                )?,
            });
        }
        Ok(RowUnit {
            ops,
            identities: vec![carried],
        })
    }
}

/// Every restored row of `tenant_id`, one unit per row, grouped by
/// collection. `binds` is the backup's primary-key section.
pub(super) fn document_units(
    state: &SharedState,
    tenant_id: u64,
    documents: Vec<(String, Vec<u8>)>,
    documents_versioned: Vec<(String, Vec<u8>)>,
    binds: &[SurrogateBindEntry],
) -> crate::Result<Vec<CollectionUnits>> {
    let grouped = group_rows(tenant_id, documents, documents_versioned)?;
    let mut out = Vec::with_capacity(grouped.len());
    for ((db, collection), rows) in grouped {
        let database_id = DatabaseId::new(db);
        let builder = RowBuilder {
            state,
            database_id,
            tenant: TenantId::new(tenant_id),
            collection: &collection,
            shape: collection_shape(state, database_id, tenant_id, &collection)?,
            binds: binds
                .iter()
                .filter(|b| b.tenant_id == tenant_id && b.collection == collection)
                .map(|b| (b.surrogate, b.pk.as_slice()))
                .collect(),
        };
        let mut units = Vec::with_capacity(rows.len());
        for (key, row) in &rows {
            units.push(match row {
                StoredRow::Current(body) => builder.current(*key, body)?,
                StoredRow::Versions(versions) => builder.versions(*key, versions)?,
            });
        }
        out.push(CollectionUnits {
            database_id,
            collection: collection.clone(),
            units,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_split_into_collection_and_storage_key() {
        let (db, collection, rest) = split_key("0:7:users:0000002a", 7).unwrap();
        assert_eq!((db, collection, rest), (0, "users", "0000002a"));
        assert!(split_key("0:8:users:0000002a", 7).is_err());
        assert!(split_key("0:7:users", 7).is_err());
    }

    #[test]
    fn versions_group_under_their_row_in_system_time_order() {
        let grouped = group_rows(
            7,
            vec![("0:7:plain:00000001".into(), vec![1])],
            vec![
                (
                    "0:7:ledger:00000002\x0000000000000000000200".into(),
                    vec![2],
                ),
                (
                    "0:7:ledger:00000002\x0000000000000000000100".into(),
                    vec![1],
                ),
            ],
        )
        .unwrap();
        let ledger = &grouped[&(0, "ledger".to_string())];
        let key = StorageKey::parse("00000002").unwrap();
        let StoredRow::Versions(versions) = &ledger[&key] else {
            panic!("a versioned row groups as versions");
        };
        let order: Vec<i64> = versions.iter().map(|(sys, _)| *sys).collect();
        assert_eq!(order, vec![100, 200]);
        assert!(matches!(
            grouped[&(0, "plain".to_string())][&StorageKey::parse("00000001").unwrap()],
            StoredRow::Current(_)
        ));
    }
}
