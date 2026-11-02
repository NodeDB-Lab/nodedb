// SPDX-License-Identifier: BUSL-1.1

//! Walk a `TenantDataSnapshot` of one database as canonical rows.
//!
//! The backup, the envelope check and the destination check all walk their
//! snapshot with this one walker, so the three digests agree whenever the rows
//! do. A canonical row drops everything a restore re-derives:
//!
//! * a document is keyed by its client identity, never by its surrogate;
//! * a document version keeps its system time and valid time;
//! * a hash-chained document drops its chain fields;
//! * an edge is keyed by its endpoints and label, without its system time or
//!   valid time;
//! * a vector is keyed by the primary key its surrogate binds, a version of
//!   it stamped with its node's start;
//! * a timeseries row drops its server-stamped system column;
//! * a columnar row version is keyed by its surrogate and keeps its
//!   `sys_from` and `sys_to`;
//! * a KV version keeps its start ordinal and drops its end ordinal and its
//!   TTL deadline from the hash;
//! * an array cell version drops its surrogate.

use std::collections::{BTreeMap, HashMap};

use nodedb_types::columnar::StrictSchema;
use nodedb_types::{CollectionType, DocumentMode};
use nodedb_wal::crypto::WalEncryptionKey;

use crate::Error;
use crate::control::backup::snapshot_keys::stored_collection_key;
use crate::control::security::catalog::StoredCollection;
use crate::types::{DatabaseId, TenantDataSnapshot};

use super::canonical::Row;

/// What canonicalising a collection's documents reads from its catalog entry.
#[derive(Debug, Clone, Default)]
pub(crate) struct DocShape {
    pub strict: Option<StrictSchema>,
    pub declared_primary_key: Option<String>,
    pub hash_chain: bool,
}

impl DocShape {
    /// The storage mode the Data Plane registers for `coll`, so a row decodes
    /// with the schema it was encoded with.
    pub fn of(coll: &StoredCollection) -> Self {
        let strict = match &coll.collection_type {
            CollectionType::Document(DocumentMode::Strict(schema)) => Some(schema.clone()),
            CollectionType::KeyValue(config) => Some(config.schema.clone()),
            CollectionType::Document(DocumentMode::Schemaless) | CollectionType::Columnar(_) => {
                None
            }
        };
        Self {
            strict,
            declared_primary_key: coll.declared_primary_key.clone(),
            hash_chain: coll.hash_chain,
        }
    }
}

/// Document shapes by bare collection name.
pub(crate) type Shapes = HashMap<String, DocShape>;

/// `surrogate → primary key` per bare collection name.
#[derive(Debug, Default)]
pub(crate) struct BindIndex(HashMap<String, HashMap<u32, Vec<u8>>>);

impl BindIndex {
    pub fn new<'a>(binds: impl IntoIterator<Item = (&'a str, u32, &'a [u8])>) -> Self {
        let mut index: HashMap<String, HashMap<u32, Vec<u8>>> = HashMap::new();
        for (collection, surrogate, pk) in binds {
            index
                .entry(collection.to_string())
                .or_default()
                .insert(surrogate, pk.to_vec());
        }
        Self(index)
    }

    pub fn pk(&self, collection: &str, surrogate: u32) -> Option<&[u8]> {
        self.0
            .get(collection)
            .and_then(|binds| binds.get(&surrogate))
            .map(Vec::as_slice)
    }
}

/// One database's snapshot, walked in the names of `database_id`.
pub(crate) struct Walk<'a> {
    pub tenant_id: u64,
    /// The database whose stored collection names the snapshot keys carry.
    pub database_id: DatabaseId,
    pub shapes: &'a Shapes,
    pub binds: &'a BindIndex,
    /// The segment encryption key: the WAL encryption key, `None` when at-rest
    /// encryption is off.
    pub kek: Option<&'a WalEncryptionKey>,
}

impl Walk<'_> {
    /// Emit every canonical row of `snap` into `sink`.
    pub fn snapshot(
        &self,
        snap: &TenantDataSnapshot,
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        self.documents(&snap.documents_versioned, sink)?;
        self.edges(&snap.edges, sink)?;
        self.kv_tables(&snap.kv_tables, sink)?;
        self.vectors(&snap.vectors, &snap.vector_versions, sink)?;
        self.timeseries(&snap.timeseries, &snap.flushed_ts_segments, sink)?;
        self.columnar(&snap.columnar_engines, sink)?;
        self.crdt(&snap.crdt_state, sink)?;
        self.arrays(&snap.arrays, sink)
    }

    /// The bare catalog name of a collection the snapshot names as `stored`.
    pub(super) fn bare(&self, stored: &str) -> String {
        stored_collection_key(self.database_id, stored)
            .name()
            .to_string()
    }

    /// The part after `"{db}:{tid}:"` of a scoped section key.
    pub(super) fn scoped_rest<'k>(&self, key: &'k str) -> Result<&'k str, Error> {
        let mut parts = key.splitn(3, ':');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(_), Some(tid), Some(rest))
                if tid.parse::<u64>().ok() == Some(self.tenant_id) && !rest.is_empty() =>
            {
                Ok(rest)
            }
            _ => Err(malformed(key)),
        }
    }
}

pub(super) fn malformed(key: &str) -> Error {
    let prefix: String = key.chars().take(64).collect();
    Error::Serialization {
        format: "backup".into(),
        detail: format!("backup verification: section key {prefix:?} is malformed"),
    }
}

/// The per-part tallies of rows, keyed by `(bare collection, part)`.
pub(crate) type PartKey = (String, nodedb_types::backup_envelope::VerifiedPart);

/// Tallies of canonical rows by collection part.
pub(crate) type Tallies = BTreeMap<PartKey, nodedb_types::backup_envelope::VerifiedTally>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::Value;
    use nodedb_types::surrogate::Surrogate;

    use super::*;
    use crate::engine::graph::edge_store::{EdgeValuePayload, versioned_edge_key};

    const TENANT: u64 = 3;

    fn tallies(snap: &TenantDataSnapshot, binds: &[(&str, u32, &[u8])]) -> Tallies {
        let shapes = Shapes::new();
        let binds = BindIndex::new(binds.iter().copied());
        let walk = Walk {
            tenant_id: TENANT,
            database_id: DatabaseId::DEFAULT,
            shapes: &shapes,
            binds: &binds,
            kek: None,
        };
        let mut tallies = Tallies::new();
        walk.snapshot(snap, &mut |row| {
            tallies
                .entry((row.collection, row.part))
                .or_default()
                .add(&row.hash);
        })
        .expect("walk");
        tallies
    }

    /// One live version of a `people` row at system time 100.
    fn doc(surrogate: u32, city: &str) -> (String, Vec<u8>) {
        let body = Value::Object(HashMap::from([(
            "city".to_string(),
            Value::String(city.into()),
        )]));
        let body = nodedb_types::value_to_msgpack(&body).expect("encode");
        (
            format!("0:{TENANT}:people:{surrogate:08x}\x00{:020}", 100),
            crate::engine::sparse::btree_versioned::encode_value(
                crate::engine::sparse::btree_versioned::TAG_LIVE,
                i64::MIN,
                i64::MAX,
                &body,
            ),
        )
    }

    fn edge(src: &str, dst: &str, system_from: i64) -> (String, Vec<u8>) {
        let key = versioned_edge_key("people", src, "knows", dst, system_from).expect("key");
        let value = EdgeValuePayload::new(system_from, i64::MAX, b"props".to_vec())
            .encode()
            .expect("payload");
        (key, value)
    }

    fn vectors(rows: Vec<(u32, Vec<f32>, Option<Surrogate>)>) -> (String, Vec<u8>) {
        (
            format!("0:{TENANT}:people:emb"),
            zerompk::to_msgpack_vec(&rows).expect("encode"),
        )
    }

    /// The source and the restored destination hold the same rows under other
    /// surrogates, other system times and in another order. Every part's tally
    /// agrees.
    #[test]
    fn the_digest_ignores_order_surrogates_and_system_time() {
        let source = TenantDataSnapshot {
            documents_versioned: vec![doc(0x2a, "paris"), doc(0x2b, "rome")],
            edges: vec![edge("alice", "bob", 100)],
            vectors: vec![vectors(vec![
                (0, vec![1.0, 0.0], Some(Surrogate::new(0x2a))),
                (1, vec![0.0, 1.0], Some(Surrogate::new(0x2b))),
            ])],
            ..Default::default()
        };
        let source_binds: [(&str, u32, &[u8]); 2] =
            [("people", 0x2a, b"alice"), ("people", 0x2b, b"bob")];
        let restored = TenantDataSnapshot {
            documents_versioned: vec![doc(0x91, "rome"), doc(0x90, "paris")],
            edges: vec![edge("alice", "bob", 9_999)],
            vectors: vec![vectors(vec![
                (7, vec![0.0, 1.0], Some(Surrogate::new(0x91))),
                (3, vec![1.0, 0.0], Some(Surrogate::new(0x90))),
            ])],
            ..Default::default()
        };
        let restored_binds: [(&str, u32, &[u8]); 2] =
            [("people", 0x90, b"alice"), ("people", 0x91, b"bob")];

        let expected = tallies(&source, &source_binds);
        assert_eq!(expected.len(), 3, "documents, edges and vectors");
        assert_eq!(expected, tallies(&restored, &restored_binds));
    }

    /// A row whose value or identity changes changes its part's digest.
    #[test]
    fn the_digest_sees_a_changed_row() {
        let binds: [(&str, u32, &[u8]); 2] = [("people", 0x2a, b"alice"), ("people", 0x2b, b"bob")];
        let base = TenantDataSnapshot {
            documents_versioned: vec![doc(0x2a, "paris"), doc(0x2b, "rome")],
            ..Default::default()
        };
        let moved = TenantDataSnapshot {
            documents_versioned: vec![doc(0x2a, "paris"), doc(0x2b, "oslo")],
            ..Default::default()
        };
        let swapped = TenantDataSnapshot {
            documents_versioned: vec![doc(0x2a, "rome"), doc(0x2b, "paris")],
            ..Default::default()
        };
        let base_tallies = tallies(&base, &binds);
        assert_ne!(base_tallies, tallies(&moved, &binds));
        assert_ne!(base_tallies, tallies(&swapped, &binds));
    }
}
