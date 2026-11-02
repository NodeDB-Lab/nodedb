// SPDX-License-Identifier: BUSL-1.1

//! Canonical rows of the KV, vector, timeseries, columnar and CRDT sections.
//!
//! Timeseries and columnar rows decode with the same decoders RESTORE re-issues
//! them from, so a row restore writes hashes the same on both sides.

use std::collections::{BTreeMap, HashMap, HashSet};

use loro::{LoroDoc, LoroValue};
use nodedb_types::backup_envelope::VerifiedPart;
use nodedb_types::columnar::RestoredVersion;
use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::Error;
use crate::control::backup::restore::columnar_reissue::decode_snapshot_versions;
use crate::control::backup::restore::timeseries_reissue::decode_timeseries_live_rows;
use crate::control::backup::restore::vector_reissue::split_vector_coll_key;
use crate::types::TsFlushedCollectionBlob;

use super::canonical::{Row, RowHasher, key_hash};
use super::walk::Walk;

/// One KV table, in the KV snapshot shape.
type KvTable = crate::engine::kv::hash_table::KvSnapshotTable;

/// One vector index's export rows: `(node_id, vector, surrogate)`.
type VectorRows = Vec<(u32, Vec<f32>, Option<Surrogate>)>;

/// How a vector row is keyed.
const VECTOR_BOUND: u8 = b'p';
/// A vector with no surrogate. Every stored vector is bound, so a capture that
/// holds one is corrupt, and RESTORE refuses it.
const VECTOR_UNBOUND: u8 = b'u';
/// A vector whose surrogate binds no primary key.
const VECTOR_UNKEYED: u8 = b's';

fn decode_error(what: &str, collection: &str, e: impl std::fmt::Display) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup verification: decode {what} of '{collection}': {e}"),
    }
}

impl Walk<'_> {
    pub(super) fn kv_tables(
        &self,
        tables: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (table_key, bytes) in tables {
            let bare = self.bare(self.scoped_rest(table_key)?);
            let table: KvTable =
                zerompk::from_msgpack(bytes).map_err(|e| decode_error("KV table", &bare, e))?;
            // Every version of a key hashes under the key and its start
            // ordinal, live or ended alike: RESTORE installs each version at
            // its ordinal, and a version live in the backup can end in the
            // destination at a later write. The end ordinal and the TTL
            // deadline stay out of the hash. The surrogate does too: RESTORE
            // binds each key in the destination catalog. The key's current
            // state rides on its newest version: the live value, or absent.
            let live_keys: HashSet<Vec<u8>> = table.rows.iter().map(|r| r.key.clone()).collect();
            for row in table.rows {
                let crate::engine::kv::hash_table::KvSnapshotRow {
                    key,
                    value,
                    expire_at_ms,
                    sys_from,
                    ..
                } = row;
                let row_key: [&[u8]; 1] = [key.as_slice()];
                let mut state = RowHasher::state(VerifiedPart::KeyValue, &row_key);
                state.bytes(b'v', &value);
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::KeyValue,
                    key: Some(key_hash(VerifiedPart::KeyValue, &row_key)),
                    hash: kv_version_hash(&row_key, &value, sys_from),
                    expire_at_ms,
                    stamp: Some(sys_from),
                    state: Some(state.finish()),
                });
            }
            for key_history in table.history {
                let row_key: [&[u8]; 1] = [key_history.key.as_slice()];
                let absent = !live_keys.contains(&key_history.key);
                let newest = key_history.versions.iter().map(|v| v.sys_from).max();
                for version in key_history.versions {
                    let state = (absent && Some(version.sys_from) == newest).then(|| {
                        let mut state = RowHasher::state(VerifiedPart::KeyValue, &row_key);
                        state.bytes(b'x', &[]);
                        state.finish()
                    });
                    sink(Row {
                        collection: bare.clone(),
                        part: VerifiedPart::KeyValue,
                        key: Some(key_hash(VerifiedPart::KeyValue, &row_key)),
                        hash: kv_version_hash(&row_key, &version.value, version.sys_from),
                        expire_at_ms: 0,
                        stamp: Some(version.sys_from),
                        state,
                    });
                }
            }
        }
        Ok(())
    }

    /// Each index's live vectors. A vector whose surrogate binds a primary
    /// key is a version of that key, stamped with its node's start: RESTORE
    /// installs each node at its start. `history` carries the starts, keyed
    /// like `indexes`. A node no history names starts at `i64::MIN`.
    pub(super) fn vectors(
        &self,
        indexes: &[(String, Vec<u8>)],
        history: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (index_key, bytes) in indexes {
            let (stored, field) = split_vector_coll_key(self.scoped_rest(index_key)?);
            let bare = self.bare(stored);
            let rows: VectorRows =
                zerompk::from_msgpack(bytes).map_err(|e| decode_error("vector index", &bare, e))?;
            let starts: HashMap<u32, i64> = match history.iter().find(|(k, _)| k == index_key) {
                Some((_, bytes)) => {
                    let history: crate::types::VectorIndexHistory = zerompk::from_msgpack(bytes)
                        .map_err(|e| decode_error("vector history", &bare, e))?;
                    history.live_starts.into_iter().collect()
                }
                None => HashMap::new(),
            };
            for (node_id, vector, surrogate) in rows {
                let bound = surrogate
                    .filter(|s| *s != Surrogate::ZERO)
                    .map(|s| self.binds.pk(&bare, s.as_u32()));
                let (kind, pk): (u8, &[u8]) = match bound {
                    Some(Some(pk)) => (VECTOR_BOUND, pk),
                    None => (VECTOR_UNBOUND, &[]),
                    Some(None) => (VECTOR_UNKEYED, &[]),
                };
                let kind = [kind];
                let key: [&[u8]; 3] = [field.as_bytes(), &kind, pk];
                let mut hasher = RowHasher::new(VerifiedPart::Vectors, &key);
                let data: Vec<u8> = vector.iter().flat_map(|v| v.to_le_bytes()).collect();
                hasher.bytes(b'f', &data);
                // A bound row is a version of its key. An unbound or unkeyed
                // vector names no key of its own.
                let stamp = (kind[0] == VECTOR_BOUND)
                    .then(|| starts.get(&node_id).copied().unwrap_or(i64::MIN));
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::Vectors,
                    key: Some(key_hash(VerifiedPart::Vectors, &key)),
                    hash: hasher.finish(),
                    expire_at_ms: 0,
                    stamp,
                    state: None,
                });
            }
        }
        Ok(())
    }

    /// Each collection's memtable rows plus every flushed partition's rows.
    pub(super) fn timeseries(
        &self,
        memtables: &[(String, Vec<u8>)],
        flushed: &[TsFlushedCollectionBlob],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        let mut by_key: BTreeMap<&str, (Option<&[u8]>, Option<&TsFlushedCollectionBlob>)> =
            BTreeMap::new();
        for (key, bytes) in memtables {
            by_key.entry(key.as_str()).or_default().0 = Some(bytes.as_slice());
        }
        for blob in flushed {
            by_key.entry(blob.collection_key.as_str()).or_default().1 = Some(blob);
        }
        let empty = TsFlushedCollectionBlob::default();
        for (key, (memtable, flushed)) in by_key {
            let bare = self.bare(self.scoped_rest(key)?);
            let rows =
                decode_timeseries_live_rows(&bare, memtable, flushed.unwrap_or(&empty), self.kek)?;
            for row in rows {
                let mut hasher = RowHasher::new(VerifiedPart::Timeseries, &[]);
                hasher.value(&row)?;
                sink(keyless(bare.clone(), VerifiedPart::Timeseries, hasher));
            }
        }
        Ok(())
    }

    pub(super) fn columnar(
        &self,
        engines: &[(String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (key, bytes) in engines {
            let bare = self.bare(self.scoped_rest(key)?);
            let snap: nodedb_columnar::ColumnarEngineSnapshot = zerompk::from_msgpack(bytes)
                .map_err(|e| decode_error("columnar snapshot", &bare, e))?;
            let decoded = decode_snapshot_versions(&bare, snap, self.kek)?;
            // Each version is a version of its key, the surrogate its rows
            // carry: RESTORE installs every version of a key under one
            // surrogate. The key's current state rides on its newest
            // version: the open row, or absent.
            let mut newest: HashMap<Surrogate, (i64, usize)> = HashMap::new();
            for (index, (surrogate, version)) in
                decoded.surrogates.iter().zip(&decoded.versions).enumerate()
            {
                let held = newest
                    .entry(*surrogate)
                    .or_insert((version.sys_from, index));
                if version.sys_from >= held.0 {
                    *held = (version.sys_from, index);
                }
            }
            for (index, ((row, version), surrogate)) in decoded
                .rows
                .iter()
                .zip(&decoded.versions)
                .zip(&decoded.surrogates)
                .enumerate()
            {
                let surrogate_bytes = surrogate.as_u32().to_le_bytes();
                let row_key: [&[u8]; 1] = [&surrogate_bytes];
                let state = if newest.get(surrogate).map(|(_, at)| *at) == Some(index) {
                    let mut state = RowHasher::state(VerifiedPart::Columnar, &row_key);
                    match version.sys_to {
                        None => state.value(row)?,
                        Some(_) => state.bytes(b'x', &[]),
                    }
                    Some(state.finish())
                } else {
                    None
                };
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::Columnar,
                    key: Some(key_hash(VerifiedPart::Columnar, &row_key)),
                    hash: columnar_version_hasher(row, version)?.finish(),
                    expire_at_ms: 0,
                    stamp: Some(version.sys_from),
                    state,
                });
            }
        }
        Ok(())
    }

    /// Each collection's Loro document, one row per entry of each root map.
    pub(super) fn crdt(
        &self,
        states: &[(u64, u64, String, Vec<u8>)],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        for (_database_id, _tenant_id, stored, bytes) in states {
            let bare = self.bare(stored);
            let doc = LoroDoc::new();
            doc.import(bytes)
                .map_err(|e| decode_error("CRDT state", &bare, e))?;
            let mut emit = |key: &[&[u8]], value: &LoroValue| {
                let mut hasher = RowHasher::new(VerifiedPart::Crdt, key);
                hasher.loro(value);
                sink(Row {
                    collection: bare.clone(),
                    part: VerifiedPart::Crdt,
                    key: Some(key_hash(VerifiedPart::Crdt, key)),
                    hash: hasher.finish(),
                    expire_at_ms: 0,
                    stamp: None,
                    state: None,
                });
            };
            match doc.get_deep_value() {
                LoroValue::Map(roots) => {
                    for (root, value) in roots.iter() {
                        match value {
                            LoroValue::Map(rows) => {
                                for (id, row) in rows.iter() {
                                    emit(&[root.as_bytes(), id.as_bytes()][..], row);
                                }
                            }
                            other => emit(&[root.as_bytes()][..], other),
                        }
                    }
                }
                other => emit(&[][..], &other),
            }
        }
        Ok(())
    }
}

/// The hash of one KV version: its key, its value and its start ordinal.
fn kv_version_hash(key: &[&[u8]], value: &[u8], sys_from: i64) -> [u8; 32] {
    let mut hasher = RowHasher::new(VerifiedPart::KeyValue, key);
    hasher.bytes(b'v', value);
    hasher.int(b's', sys_from);
    hasher.finish()
}

/// The hash input of one columnar row version: its cells and its stamps.
/// RESTORE installs each version at its stamps, so a version restored at
/// another system time does not verify.
fn columnar_version_hasher(row: &Value, version: &RestoredVersion) -> Result<RowHasher, Error> {
    let mut hasher = RowHasher::new(VerifiedPart::Columnar, &[]);
    hasher.value(row)?;
    hasher.int(b'f', version.sys_from);
    match version.sys_to {
        Some(sys_to) => hasher.int(b't', sys_to),
        None => hasher.bytes(b'o', &[]),
    }
    Ok(hasher)
}

fn keyless(collection: String, part: VerifiedPart, hasher: RowHasher) -> Row {
    Row {
        collection,
        part,
        key: None,
        hash: hasher.finish(),
        expire_at_ms: 0,
        stamp: None,
        state: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn digest(row: &Value, sys_from: i64, sys_to: Option<i64>) -> [u8; 32] {
        columnar_version_hasher(
            row,
            &RestoredVersion {
                sys_from,
                sys_to,
                clone_copy: false,
            },
        )
        .expect("hash")
        .finish()
    }

    /// The digest of a columnar row covers its stamps: a version restored
    /// at another `sys_from` or `sys_to` does not verify.
    #[test]
    fn a_columnar_digest_covers_the_version_stamps() {
        let row = Value::Object(HashMap::from([
            ("id".to_string(), Value::Integer(1)),
            ("v".to_string(), Value::Integer(10)),
        ]));
        let base = digest(&row, 10, Some(20));
        assert_eq!(base, digest(&row, 10, Some(20)));
        assert_ne!(base, digest(&row, 11, Some(20)), "sys_from differs");
        assert_ne!(base, digest(&row, 10, Some(21)), "sys_to differs");
        assert_ne!(base, digest(&row, 10, None), "an open version differs");
        assert_ne!(digest(&row, 10, None), digest(&row, 11, None));
    }
}
