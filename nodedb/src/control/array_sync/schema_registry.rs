// SPDX-License-Identifier: BUSL-1.1

//! [`OriginSchemaRegistry`] — per-array `SchemaDoc` cache with redb persistence.
//!
//! Entries live in `array_schema_docs`, structurally keyed by database ID,
//! tenant ID, and a length-delimited UTF-8 array name.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_array::sync::HlcGenerator;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::sync::schema_crdt::SchemaDoc;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::warn;

use crate::Error;
use crate::types::DatabaseId;

/// redb table: `[database_id: u64 BE][tenant_id: u64 BE][name_len: u16 BE][array name bytes]` → persisted schema.
const SCHEMA_DOCS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("array_schema_docs");

type SchemaKey = (DatabaseId, u64, String);

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
    docs: Mutex<HashMap<SchemaKey, SchemaDoc>>,
}

impl crate::storage::RedbBacked for OriginSchemaRegistry {
    fn redb_database(&self) -> &redb::Database {
        &self.db
    }
}

impl OriginSchemaRegistry {
    /// Open or create the schema registry table in `db` and cold-load its
    /// entries.
    pub fn open(
        db: Arc<Database>,
        replica_id: ReplicaId,
        hlc_gen: Arc<HlcGenerator>,
    ) -> crate::Result<Self> {
        {
            let txn = db.begin_write().map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry begin_write init: {e}"),
            })?;
            txn.open_table(SCHEMA_DOCS).map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry open v2 table init: {e}"),
            })?;
            txn.commit().map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry commit init: {e}"),
            })?;
        }

        let docs = Self::load_all(&db)?;

        Ok(Self {
            db,
            replica_id,
            hlc_gen,
            docs: Mutex::new(docs),
        })
    }

    /// Return the current default-database schema HLC for `array`.
    pub fn schema_hlc(&self, array: &str) -> Option<Hlc> {
        self.schema_hlc_in_database(DatabaseId::DEFAULT, 0, array)
    }

    /// Return the schema HLC in an explicit database scope.
    pub fn schema_hlc_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Option<Hlc> {
        let docs = self.docs.lock().ok()?;
        docs.get(&(database_id, tenant_id, array.to_owned()))
            .map(|doc| doc.schema_hlc())
    }

    /// Return the default-database tile extents for `array`.
    pub fn tile_extents(&self, array: &str) -> Option<Vec<u64>> {
        self.tile_extents_in_database(DatabaseId::DEFAULT, 0, array)
    }

    /// Return tile extents in an explicit database scope.
    pub fn tile_extents_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Option<Vec<u64>> {
        let docs = self.docs.lock().ok()?;
        let doc = docs.get(&(database_id, tenant_id, array.to_owned()))?;
        doc.to_schema().ok().map(|schema| schema.tile_extents)
    }

    /// Apply a remote Loro snapshot for a default-database array.
    pub fn import_snapshot(
        &self,
        array: &str,
        snapshot_bytes: &[u8],
        remote_hlc: Hlc,
    ) -> crate::Result<()> {
        self.import_snapshot_in_database(DatabaseId::DEFAULT, 0, array, snapshot_bytes, remote_hlc)
    }

    /// Apply a remote Loro snapshot in an explicit database scope.
    ///
    /// Creates the entry if absent and persists the updated snapshot.
    pub fn import_snapshot_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        snapshot_bytes: &[u8],
        remote_hlc: Hlc,
    ) -> crate::Result<()> {
        let mut docs = self.docs.lock().map_err(|_| Error::Storage {
            engine: "array_sync".into(),
            detail: "schema_registry lock poisoned".into(),
        })?;
        let doc = docs
            .entry((database_id, tenant_id, array.to_owned()))
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

        self.persist(database_id, tenant_id, array, schema_hlc, snapshot)
    }

    /// Apply a Raft-committed Loro snapshot for a default-database array.
    pub fn import_snapshot_replicated(
        &self,
        array: &str,
        snapshot_bytes: &[u8],
        committed_hlc: Hlc,
    ) -> crate::Result<()> {
        self.import_snapshot_replicated_in_database(
            DatabaseId::DEFAULT,
            0,
            array,
            snapshot_bytes,
            committed_hlc,
        )
    }

    /// Apply a Raft-committed Loro snapshot in an explicit database scope,
    /// preserving the exact committed HLC on every replica.
    pub fn import_snapshot_replicated_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        snapshot_bytes: &[u8],
        committed_hlc: Hlc,
    ) -> crate::Result<()> {
        let mut docs = self.docs.lock().map_err(|_| Error::Storage {
            engine: "array_sync".into(),
            detail: "schema_registry lock poisoned".into(),
        })?;
        let doc = docs
            .entry((database_id, tenant_id, array.to_owned()))
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

        self.persist(database_id, tenant_id, array, committed_hlc, snapshot)
    }

    /// Decode and return the schema for a default-database array.
    pub fn to_array_schema(
        &self,
        array: &str,
    ) -> Option<nodedb_array::schema::array_schema::ArraySchema> {
        self.to_array_schema_in_database(DatabaseId::DEFAULT, 0, array)
    }

    /// Decode a schema in an explicit database scope.
    pub fn to_array_schema_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> Option<nodedb_array::schema::array_schema::ArraySchema> {
        let docs = self.docs.lock().ok()?;
        let doc = docs.get(&(database_id, tenant_id, array.to_owned()))?;
        doc.to_schema().ok()
    }

    /// Export the current default-database Loro snapshot bytes for `array`.
    pub fn export_snapshot(&self, array: &str) -> crate::Result<Option<Vec<u8>>> {
        self.export_snapshot_in_database(DatabaseId::DEFAULT, 0, array)
    }

    /// Export the current Loro snapshot bytes in an explicit database scope.
    pub fn export_snapshot_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        let docs = self.docs.lock().map_err(|_| Error::Storage {
            engine: "array_sync".into(),
            detail: "schema_registry lock poisoned".into(),
        })?;
        let Some(doc) = docs.get(&(database_id, tenant_id, array.to_owned())) else {
            return Ok(None);
        };
        let bytes = doc.export_snapshot().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry export '{array}': {e}"),
        })?;
        Ok(Some(bytes))
    }

    // ─── Internal helpers ─────────────────────────────────────────────────

    fn v2_key(database_id: DatabaseId, tenant_id: u64, array: &str) -> Option<Vec<u8>> {
        let name_len = u16::try_from(array.len()).ok()?;
        let mut key = Vec::with_capacity(8 + 8 + 2 + array.len());
        key.extend_from_slice(&database_id.as_u64().to_be_bytes());
        key.extend_from_slice(&tenant_id.to_be_bytes());
        key.extend_from_slice(&name_len.to_be_bytes());
        key.extend_from_slice(array.as_bytes());
        Some(key)
    }

    fn persist(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        array: &str,
        schema_hlc: Hlc,
        loro_snapshot: Vec<u8>,
    ) -> crate::Result<()> {
        let persisted = PersistedSchema {
            replica_id: self.replica_id.as_u64(),
            schema_hlc_bytes: schema_hlc.to_bytes().to_vec(),
            loro_snapshot,
        };
        let bytes = zerompk::to_msgpack_vec(&persisted).map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry persist encode '{array}': {e}"),
        })?;
        let key = Self::v2_key(database_id, tenant_id, array).ok_or_else(|| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry: array name too long: '{array}'"),
        })?;

        let txn = self.db.begin_write().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry persist begin_write '{array}': {e}"),
        })?;
        {
            let mut table = txn.open_table(SCHEMA_DOCS).map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry persist open v2 table '{array}': {e}"),
            })?;
            table
                .insert(key.as_slice(), bytes.as_slice())
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

    fn load_document(key_description: &str, value: &[u8]) -> Option<SchemaDoc> {
        let persisted: PersistedSchema = match zerompk::from_msgpack(value) {
            Ok(persisted) => persisted,
            Err(error) => {
                warn!(key = key_description, error = %error, "schema_registry: skipping corrupt schema entry");
                return None;
            }
        };
        let hlc_bytes: [u8; 18] = match persisted.schema_hlc_bytes.try_into() {
            Ok(bytes) => bytes,
            Err(bytes) => {
                warn!(
                    key = key_description,
                    len = bytes.len(),
                    "schema_registry: skipping entry with wrong hlc_bytes length"
                );
                return None;
            }
        };
        let schema_hlc = Hlc::from_bytes(&hlc_bytes);
        let mut doc = SchemaDoc::new(ReplicaId::new(persisted.replica_id));
        if let Err(error) = doc.import_snapshot_replicated(&persisted.loro_snapshot, schema_hlc) {
            warn!(key = key_description, error = %error, "schema_registry: skipping corrupt loro snapshot");
            return None;
        }
        Some(doc)
    }

    fn load_all(db: &Database) -> crate::Result<HashMap<SchemaKey, SchemaDoc>> {
        let txn = db.begin_read().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry load_all begin_read: {e}"),
        })?;
        let table = txn.open_table(SCHEMA_DOCS).map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry load_all open table: {e}"),
        })?;

        let mut docs = HashMap::new();
        let entries = table.iter().map_err(|e| Error::Storage {
            engine: "array_sync".into(),
            detail: format!("schema_registry load_all iter: {e}"),
        })?;
        for entry in entries {
            let (key, value) = entry.map_err(|e| Error::Storage {
                engine: "array_sync".into(),
                detail: format!("schema_registry load_all entry: {e}"),
            })?;
            let key = key.value();
            if key.len() < 18 {
                warn!(
                    len = key.len(),
                    "schema_registry: skipping malformed v2 key"
                );
                continue;
            }
            let Ok(database_bytes) = <[u8; 8]>::try_from(&key[..8]) else {
                warn!(
                    len = key.len(),
                    "schema_registry: skipping malformed v2 key"
                );
                continue;
            };
            let Ok(tenant_bytes) = <[u8; 8]>::try_from(&key[8..16]) else {
                warn!(
                    len = key.len(),
                    "schema_registry: skipping malformed v2 key"
                );
                continue;
            };
            let Ok(name_len_bytes) = <[u8; 2]>::try_from(&key[16..18]) else {
                warn!(
                    len = key.len(),
                    "schema_registry: skipping malformed v2 key"
                );
                continue;
            };
            let database_id = DatabaseId::new(u64::from_be_bytes(database_bytes));
            let tenant_id = u64::from_be_bytes(tenant_bytes);
            let name_len = u16::from_be_bytes(name_len_bytes) as usize;
            let name_bytes = &key[18..];
            if name_bytes.len() != name_len {
                warn!(
                    len = key.len(),
                    "schema_registry: skipping malformed v2 key"
                );
                continue;
            }
            let name = match std::str::from_utf8(name_bytes) {
                Ok(name) => name.to_owned(),
                Err(error) => {
                    warn!(error = %error, "schema_registry: skipping non-UTF8 v2 key");
                    continue;
                }
            };
            let description = format!("{database_id}/{tenant_id}/{name}");
            if let Some(doc) = Self::load_document(&description, value.value()) {
                docs.insert((database_id, tenant_id, name), doc);
            }
        }
        Ok(docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry(path: &std::path::Path) -> OriginSchemaRegistry {
        let database = Arc::new(Database::create(path).expect("create redb database"));
        let replica_id = ReplicaId::new(7);
        OriginSchemaRegistry::open(
            database,
            replica_id,
            Arc::new(HlcGenerator::new(replica_id)),
        )
        .expect("open registry")
    }

    fn empty_snapshot() -> Vec<u8> {
        SchemaDoc::new(ReplicaId::new(1))
            .export_snapshot()
            .expect("export empty schema snapshot")
    }

    fn hlc(logical: u16) -> Hlc {
        Hlc::new(1, logical, ReplicaId::new(1)).expect("valid HLC")
    }

    #[test]
    fn same_array_name_is_isolated_by_database() {
        let directory = tempfile::tempdir().expect("tempdir");
        let registry = test_registry(&directory.path().join("schemas.redb"));
        let snapshot = empty_snapshot();
        let database_id = DatabaseId::new(10);

        registry
            .import_snapshot_replicated_in_database(database_id, 1, "events", &snapshot, hlc(1))
            .expect("persist first tenant schema");
        registry
            .import_snapshot_replicated_in_database(database_id, 2, "events", &snapshot, hlc(2))
            .expect("persist second tenant schema");

        assert_eq!(
            registry.schema_hlc_in_database(database_id, 1, "events"),
            Some(hlc(1))
        );
        assert_eq!(
            registry.schema_hlc_in_database(database_id, 2, "events"),
            Some(hlc(2))
        );
        assert_eq!(registry.schema_hlc("events"), None);
    }

    #[test]
    fn schemas_persist_across_reopen() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("schemas.redb");
        let snapshot = empty_snapshot();
        let database = Arc::new(Database::create(&path).expect("create redb database"));
        let replica_id = ReplicaId::new(7);
        let registry = OriginSchemaRegistry::open(
            Arc::clone(&database),
            replica_id,
            Arc::new(HlcGenerator::new(replica_id)),
        )
        .expect("open registry");
        registry
            .import_snapshot_replicated_in_database(
                DatabaseId::new(9),
                0,
                "events",
                &snapshot,
                hlc(4),
            )
            .expect("persist schema");
        drop(registry);

        let reopened = OriginSchemaRegistry::open(
            database,
            replica_id,
            Arc::new(HlcGenerator::new(replica_id)),
        )
        .expect("reopen registry");
        assert_eq!(
            reopened.schema_hlc_in_database(DatabaseId::new(9), 0, "events"),
            Some(hlc(4))
        );
    }
}
