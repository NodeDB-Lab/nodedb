//! [`OriginSchemaRegistry`] — per-array `SchemaDoc` cache with redb persistence.
//!
//! Mirrors the API of Lite's `SchemaRegistry<S>` but stores snapshots in a
//! dedicated redb table (`array_schema_docs`) inside the same database opened
//! by [`OriginOpLog`]. Pass the same `Arc<redb::Database>` to share the file.
//!
//! # Storage format
//!
//! Each key is the UTF-8 array name. Each value is a MessagePack-encoded
//! `PersistedSchema { replica_id: u64, schema_hlc_bytes: Vec<u8>, loro_snapshot: Vec<u8> }`.
//! The `schema_hlc_bytes` invariant is `len == 18`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_array::sync::HlcGenerator;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::sync::schema_crdt::SchemaDoc;
use redb::{Database, ReadableTable, TableDefinition};
use tracing::warn;

use crate::Error;

/// redb table: array name (bytes) → msgpack-encoded `PersistedSchema`.
const SCHEMA_DOCS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_schema_docs");

/// Persisted representation of a schema entry.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct PersistedSchema {
    replica_id: u64,
    schema_hlc_bytes: Vec<u8>,
    loro_snapshot: Vec<u8>,
}

/// Per-array [`SchemaDoc`] registry backed by a redb database.
///
/// Thread-safe via an internal [`Mutex`] over the in-memory cache.
/// All persistence calls take synchronous redb transactions.
pub struct OriginSchemaRegistry {
    db: Arc<Database>,
    replica_id: ReplicaId,
    hlc_gen: Arc<HlcGenerator>,
    docs: Mutex<HashMap<String, SchemaDoc>>,
}

impl OriginSchemaRegistry {
    /// Open or create the schema registry table in `db`.
    ///
    /// Cold-loads all persisted schemas into the in-memory cache.
    pub fn open(
        db: Arc<Database>,
        replica_id: ReplicaId,
        hlc_gen: Arc<HlcGenerator>,
    ) -> crate::Result<Self> {
        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry begin_write init: {e}"),
            })?;
            txn.open_table(SCHEMA_DOCS).map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry open_table init: {e}"),
            })?;
            txn.commit().map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry commit init: {e}"),
            })?;
        }

        // Cold-load.
        let docs = Self::load_all(&db, replica_id, &hlc_gen)?;

        Ok(Self {
            db,
            replica_id,
            hlc_gen,
            docs: Mutex::new(docs),
        })
    }

    /// Return the current `schema_hlc` for `array`, or `None` if unknown.
    pub fn schema_hlc(&self, array: &str) -> Option<Hlc> {
        let docs = self.docs.lock().ok()?;
        docs.get(array).map(|d| d.schema_hlc())
    }

    /// Return the `tile_extents` for `array`, or `None` if the array is not
    /// registered or the schema cannot be decoded.
    ///
    /// Used by tile-aware shard routing to convert coordinates to tile IDs.
    pub fn tile_extents(&self, array: &str) -> Option<Vec<u64>> {
        let docs = self.docs.lock().ok()?;
        let doc = docs.get(array)?;
        doc.to_schema().ok().map(|s| s.tile_extents)
    }

    /// Apply a remote Loro snapshot for `array`.
    ///
    /// Creates the entry if absent. Persists the updated snapshot.
    pub fn import_snapshot(
        &self,
        array: &str,
        snapshot_bytes: &[u8],
        remote_hlc: Hlc,
    ) -> crate::Result<()> {
        let mut docs = self.docs.lock().map_err(|_| Error::Storage {
            engine: "array_sync".into(),
            detail: "schema_registry lock poisoned".into(),
        })?;

        let doc = docs
            .entry(array.to_owned())
            .or_insert_with(|| SchemaDoc::new(self.replica_id));

        doc.import_snapshot(snapshot_bytes, remote_hlc, &self.hlc_gen)
            .map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry import_snapshot '{array}': {e}"),
            })?;

        let schema_hlc = doc.schema_hlc();
        let snapshot = doc.export_snapshot().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry export after import '{array}': {e}"),
        })?;
        drop(docs);

        self.persist(array, schema_hlc, snapshot)
    }

    /// Apply a Raft-committed Loro snapshot for `array`, preserving the exact
    /// committed HLC on every replica.
    ///
    /// Unlike [`import_snapshot`] (CRDT sync path), this sets `schema_hlc`
    /// to exactly `committed_hlc` so all replicas converge to the same value
    /// after applying the same Raft log entry. Persists the updated snapshot.
    pub fn import_snapshot_replicated(
        &self,
        array: &str,
        snapshot_bytes: &[u8],
        committed_hlc: Hlc,
    ) -> crate::Result<()> {
        let mut docs = self.docs.lock().map_err(|_| Error::Storage {
            engine: "array_sync".into(),
            detail: "schema_registry lock poisoned".into(),
        })?;

        let doc = docs
            .entry(array.to_owned())
            .or_insert_with(|| SchemaDoc::new(self.replica_id));

        doc.import_snapshot_replicated(snapshot_bytes, committed_hlc)
            .map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry import_snapshot_replicated '{array}': {e}"),
            })?;

        let snapshot = doc.export_snapshot().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry export after replicated import '{array}': {e}"),
        })?;
        drop(docs);

        self.persist(array, committed_hlc, snapshot)
    }

    /// Decode and return the `ArraySchema` for `array`, or `None` if unknown or
    /// if the schema document cannot be decoded.
    ///
    /// Used by the distributed applier to build `ArrayCatalogEntry` on every node
    /// after applying a committed `ArraySchema` Raft entry.
    pub fn to_array_schema(
        &self,
        array: &str,
    ) -> Option<nodedb_array::schema::array_schema::ArraySchema> {
        let docs = self.docs.lock().ok()?;
        let doc = docs.get(array)?;
        doc.to_schema().ok()
    }

    /// Export the current Loro snapshot bytes for `array`, or `None` if unknown.
    pub fn export_snapshot(&self, array: &str) -> crate::Result<Option<Vec<u8>>> {
        let docs = self.docs.lock().map_err(|_| Error::Storage {
            engine: "array_sync".into(),
            detail: "schema_registry lock poisoned".into(),
        })?;
        if let Some(doc) = docs.get(array) {
            let bytes = doc.export_snapshot().map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry export '{array}': {e}"),
            })?;
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    fn persist(&self, array: &str, schema_hlc: Hlc, loro_snapshot: Vec<u8>) -> crate::Result<()> {
        let persisted = PersistedSchema {
            replica_id: self.replica_id.as_u64(),
            schema_hlc_bytes: schema_hlc.to_bytes().to_vec(),
            loro_snapshot,
        };
        let bytes = zerompk::to_msgpack_vec(&persisted).map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry persist encode '{array}': {e}"),
        })?;

        let txn = self.db.begin_write().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry persist begin_write '{array}': {e}"),
        })?;
        {
            let mut table = txn.open_table(SCHEMA_DOCS).map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry persist open_table '{array}': {e}"),
            })?;
            table
                .insert(array.as_bytes(), bytes.as_slice())
                .map_err(|e| Error::Storage {
                    engine: "array_sync".into(),
                    detail: format!("schema_registry persist insert '{array}': {e}"),
                })?;
        }
        txn.commit().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry persist commit '{array}': {e}"),
        })
    }

    fn load_all(
        db: &Database,
        _replica_id: ReplicaId,
        hlc_gen: &Arc<HlcGenerator>,
    ) -> crate::Result<HashMap<String, SchemaDoc>> {
        let txn = db.begin_read().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry load_all begin_read: {e}"),
        })?;
        let table = txn.open_table(SCHEMA_DOCS).map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry load_all open_table: {e}"),
        })?;

        let mut docs = HashMap::new();
        let iter = table.iter().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry load_all iter: {e}"),
        })?;

        for entry in iter {
            let (k, v) = entry.map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry load_all entry: {e}"),
            })?;
            let name = match std::str::from_utf8(k.value()) {
                Ok(s) => s.to_owned(),
                Err(e) => {
                    warn!(error = %e, "schema_registry: skipping non-UTF8 key");
                    continue;
                }
            };
            let persisted: PersistedSchema = match zerompk::from_msgpack(v.value()) {
                Ok(p) => p,
                Err(e) => {
                    warn!(name, error = %e, "schema_registry: skipping corrupt schema entry");
                    continue;
                }
            };
            let hlc_arr: [u8; 18] = match persisted.schema_hlc_bytes.try_into() {
                Ok(a) => a,
                Err(v) => {
                    warn!(
                        name,
                        len = v.len(),
                        "schema_registry: skipping entry with wrong hlc_bytes length"
                    );
                    continue;
                }
            };
            let schema_hlc = Hlc::from_bytes(&hlc_arr);
            let mut doc = SchemaDoc::new(ReplicaId::new(persisted.replica_id));
            if let Err(e) = doc.import_snapshot(&persisted.loro_snapshot, schema_hlc, hlc_gen) {
                warn!(name, error = %e, "schema_registry: skipping corrupt loro snapshot");
                continue;
            }
            docs.insert(name, doc);
        }
        Ok(docs)
    }
}
