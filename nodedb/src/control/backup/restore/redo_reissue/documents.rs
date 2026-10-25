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
//! `collection` is the name the source Data Plane stored the collection
//! under: database-qualified outside the default database. Each row
//! re-issues under the destination-qualified name, and binds its identity
//! under the bare name in the destination database.
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
use crate::data::executor::strict_format::{binary_tuple_to_msgpack, undecodable_strict_row};
use crate::engine::sparse::btree_versioned::{TAG_LIVE, TAG_TOMBSTONE, decode_value};
use crate::types::{DatabaseId, SurrogateBindEntry, TenantId};

use super::super::target::DatabaseTarget;
use super::prepared::{PendingBody, PendingRow, PreparedRows};
use super::sub_record::VersionStamp;

/// A row as the backup stored it.
enum StoredRow {
    /// The one current body of a row with no history.
    Current(Vec<u8>),
    /// Every `(sys_from, versioned value)` of a bitemporal row.
    Versions(Vec<(i64, Vec<u8>)>),
}

/// What decoding a collection's rows reads from its catalog entry.
struct CollectionShape {
    strict: Option<StrictSchema>,
    declared_primary_key: Option<String>,
    /// The collection declares `HASH_CHAIN`: its rows re-issue in their
    /// install order, so the destination relinks the source chain.
    hash_chain: bool,
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

/// The highest surrogate any restored row's storage key carries, `0` for
/// none. A row the backup carries no bind for keeps its key's surrogate.
pub(in crate::control::backup::restore) fn max_row_surrogate(
    tenant_id: u64,
    documents: &[(String, Vec<u8>)],
    documents_versioned: &[(String, Vec<u8>)],
) -> crate::Result<u32> {
    let mut highest = 0u32;
    for (key, _) in documents {
        let (_, _, rest) = split_key(key, tenant_id)?;
        let storage_key = StorageKey::parse(rest).ok_or_else(|| malformed(key))?;
        highest = highest.max(storage_key.surrogate().as_u32());
    }
    for (key, _) in documents_versioned {
        let (_, _, rest) = split_key(key, tenant_id)?;
        let (hex, _) = rest.split_once('\x00').ok_or_else(|| malformed(key))?;
        let storage_key = StorageKey::parse(hex).ok_or_else(|| malformed(key))?;
        highest = highest.max(storage_key.surrogate().as_u32());
    }
    Ok(highest)
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
        let sys_from = sys.parse::<i64>().map_err(|_| malformed(&key))?;
        let rows = grouped.entry((db, collection.to_string())).or_default();
        match rows
            .entry(storage_key)
            .or_insert_with(|| StoredRow::Versions(Vec::new()))
        {
            StoredRow::Versions(versions) => versions.push((sys_from, value)),
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
        hash_chain: stored.hash_chain,
    })
}

/// A `HASH_CHAIN` row's install-order position, read from its stored body:
/// the current body, or the first live version.
fn chain_seq(
    shape: &CollectionShape,
    collection: &str,
    key: StorageKey,
    row: &StoredRow,
) -> crate::Result<u64> {
    let body = match row {
        StoredRow::Current(body) => Some(body_msgpack(shape, collection, key, body)?),
        StoredRow::Versions(versions) => {
            let mut live = None;
            for (_, raw) in versions {
                let version = decode_value(raw)?;
                if version.tag == TAG_LIVE {
                    live = Some(body_msgpack(shape, collection, key, version.body)?);
                    break;
                }
            }
            live
        }
    };
    body.and_then(|body| nodedb_types::json_from_msgpack(&body).ok())
        .and_then(|doc| {
            doc.get(crate::types::hash_chain::CHAIN_SEQ_FIELD)
                .and_then(|seq| seq.as_u64())
        })
        .ok_or_else(|| crate::Error::Serialization {
            format: "backup".into(),
            detail: format!(
                "restore: row {key} of hash-chained '{collection}' carries no chain position"
            ),
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

/// Decodes each row of one collection and names its identity.
struct RowBuilder<'a> {
    /// Bare catalog name.
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

    fn current(&self, key: StorageKey, body: &[u8]) -> crate::Result<PendingRow> {
        let value = body_msgpack(&self.shape, self.collection, key, body)?;
        let identity = self.identity(key, Some(&value))?;
        Ok(PendingRow {
            key,
            identity,
            body: PendingBody::Current(value),
        })
    }

    fn versions(&self, key: StorageKey, versions: &[(i64, Vec<u8>)]) -> crate::Result<PendingRow> {
        // Decode every version first: the identity comes from a live body.
        let mut decoded = Vec::with_capacity(versions.len());
        for (sys_from, raw) in versions {
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
                            "restore: version {sys_from} of row {key} of '{}' carries tag \
                             {tag:#04x}, which no write path records",
                            self.collection
                        ),
                    });
                }
            };
            let stamp = VersionStamp {
                sys_from: *sys_from,
                valid_from_ms: version.valid_from_ms,
                valid_until_ms: version.valid_until_ms,
            };
            decoded.push((stamp, body));
        }
        let first_live = decoded.iter().find_map(|(_, body)| body.as_deref());
        let identity = self.identity(key, first_live)?;
        Ok(PendingRow {
            key,
            identity,
            body: PendingBody::Versions(decoded),
        })
    }
}

/// Every restored row of `tenant_id` in one database, decoded and identified,
/// grouped by collection in install order. Nothing is bound yet. Every row key
/// must name `target.source`. `binds` is the backup's primary-key section of
/// that database.
pub(in crate::control::backup::restore) fn prepare_documents(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    documents: Vec<(String, Vec<u8>)>,
    documents_versioned: Vec<(String, Vec<u8>)>,
    binds: &[SurrogateBindEntry],
) -> crate::Result<Vec<PreparedRows>> {
    let grouped = group_rows(tenant_id, documents, documents_versioned)?;
    let mut out = Vec::with_capacity(grouped.len());
    for ((db, stored), rows) in grouped {
        if db != target.source.as_u64() {
            return Err(crate::Error::Serialization {
                format: "backup".into(),
                detail: format!(
                    "restore: rows of '{stored}' name database {db}, but sit with database {}",
                    target.source.as_u64()
                ),
            });
        }
        let name = target.resolve(&stored)?;
        let database_id = target.dest;
        let builder = RowBuilder {
            collection: &name.bare,
            shape: collection_shape(state, database_id, tenant_id, &name.bare)?,
            binds: binds
                .iter()
                .filter(|b| b.tenant_id == tenant_id && b.collection == name.bare)
                .map(|b| (b.surrogate, b.pk.as_slice()))
                .collect(),
        };
        let mut ordered: Vec<(&StorageKey, &StoredRow)> = rows.iter().collect();
        if builder.shape.hash_chain {
            let mut positioned = Vec::with_capacity(ordered.len());
            for (key, row) in ordered {
                positioned.push((chain_seq(&builder.shape, &name.bare, *key, row)?, key, row));
            }
            positioned.sort_by_key(|(seq, _, _)| *seq);
            ordered = positioned
                .into_iter()
                .map(|(_, key, row)| (key, row))
                .collect();
        }
        let mut pending = Vec::with_capacity(rows.len());
        for (key, row) in ordered {
            pending.push(match row {
                StoredRow::Current(body) => builder.current(*key, body)?,
                StoredRow::Versions(versions) => builder.versions(*key, versions)?,
            });
        }
        out.push(PreparedRows {
            database_id,
            tenant: TenantId::new(tenant_id),
            bare: name.bare.clone(),
            stored: name.stored.clone(),
            rows: pending,
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
    fn the_highest_row_surrogate_spans_current_and_versioned_rows() {
        let highest = max_row_surrogate(
            7,
            &[("0:7:plain:0000002a".into(), vec![])],
            &[("0:7:ledger:000000ff\x0000000000000000000100".into(), vec![])],
        )
        .unwrap();
        assert_eq!(
            highest,
            StorageKey::parse("000000ff").unwrap().surrogate().as_u32()
        );
        assert_eq!(max_row_surrogate(7, &[], &[]).unwrap(), 0);
        assert!(max_row_surrogate(7, &[("0:7:plain:zz".into(), vec![])], &[]).is_err());
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
