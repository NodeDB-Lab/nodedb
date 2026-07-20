// SPDX-License-Identifier: BUSL-1.1

//! Boot-time seeding of the per-core `doc_configs` schema registry.
//!
//! WAL redo replay (`CoreLoop::replay_all_wal`) runs synchronously on the
//! Data Plane core thread, before the core ever processes an SPSC request
//! — including the `DocumentOp::Register` broadcasts that normally
//! populate `doc_configs`. Left unseeded, strict (Binary Tuple) document
//! collections replay through the schemaless fallback path in `apply_put`
//! and get re-persisted as raw MessagePack, corrupting the strict store's
//! O(1) field layout.
//!
//! [`CoreLoop::seed_doc_configs`] closes that gap: it is called with the
//! catalog-sourced `CollectionConfig` registry (built by
//! `crate::bootstrap::data_plane::load_doc_config_registry`) immediately
//! before `replay_all_wal`, so every strict collection redo-replays
//! through its real schema and reproduces the same stored bytes a live
//! write would.

use crate::types::{DatabaseId, TenantId};

use super::state::CoreLoop;

pub(crate) type DocConfigSeedEntry = (
    (DatabaseId, TenantId, String),
    crate::engine::document::store::CollectionConfig,
);

impl CoreLoop {
    /// Insert every `(database, tenant, collection) -> CollectionConfig` entry
    /// into `doc_configs`. Called once at core startup, before WAL replay.
    pub fn seed_doc_configs(&mut self, entries: &[DocConfigSeedEntry]) {
        for (key, config) in entries {
            self.doc_configs.insert(key.clone(), config.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::Surrogate;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_wal::{RecordType, TombstoneSet, WalRecord, WalRecordArgs};

    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::strict_format;
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
    use crate::types::{DatabaseId, TenantId};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "strict_boot_replay";

    fn strict_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .unwrap()
    }

    /// MessagePack input document (no `_rowid` — the strict encode path
    /// injects it from the surrogate).
    fn doc_bytes() -> Vec<u8> {
        use nodedb_types::Value;
        let mut obj = std::collections::HashMap::new();
        obj.insert("name".to_string(), Value::String("alice".into()));
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    fn put_record(surrogate: u32) -> WalRecord {
        let payload = zerompk::to_msgpack_vec(&(
            COLL.to_string(),
            surrogate_to_doc_id(Surrogate::new(surrogate)),
            doc_bytes(),
            Option::<nodedb_types::sync::wire::SyncProvenance>::None,
            surrogate,
        ))
        .unwrap();
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: TID,
            vshard_id: 0,
            database_id: DB,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap()
    }

    /// Replaying a strict collection's redo record BEFORE `doc_configs` is
    /// seeded reproduces the boot-ordering bug: with no schema known, the
    /// document falls through to the schemaless branch and is stored as
    /// raw MessagePack, not Binary Tuple.
    #[test]
    fn redo_replay_without_seed_stores_raw_msgpack() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());

        let surrogate = 42u32;
        core.replay_document_redo(&[put_record(surrogate)], 1, &TombstoneSet::new());

        let stored = core
            .sparse
            .get(
                DB,
                TID,
                COLL,
                &surrogate_to_doc_id(Surrogate::new(surrogate)),
            )
            .unwrap()
            .expect("document should have been replayed");

        // The bug: with no strict schema known during replay, the document
        // falls through to the schemaless branch and is stored as a plain
        // MessagePack map, so it does NOT decode as a Binary Tuple against the
        // strict schema. (The paired `..._with_seed_stores_binary_tuple` test
        // proves the same record DOES decode once `doc_configs` is seeded.)
        assert!(
            strict_format::binary_tuple_to_value(&stored, &strict_schema()).is_none(),
            "unseeded strict replay must store raw MessagePack, not a Binary Tuple"
        );
    }

    /// With `doc_configs` seeded (mirroring the boot-path fix) BEFORE
    /// replay, the same redo record re-encodes as Binary Tuple, matching
    /// what a live write to a strict collection would have stored.
    #[test]
    fn redo_replay_with_seed_stores_binary_tuple() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());

        let entries = vec![(
            (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
            CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict {
                schema: strict_schema(),
            }),
        )];
        core.seed_doc_configs(&entries);

        let surrogate = 42u32;
        core.replay_document_redo(&[put_record(surrogate)], 1, &TombstoneSet::new());

        let stored = core
            .sparse
            .get(
                DB,
                TID,
                COLL,
                &surrogate_to_doc_id(Surrogate::new(surrogate)),
            )
            .unwrap()
            .expect("document should have been replayed");

        let decoded = strict_format::binary_tuple_to_value(&stored, &strict_schema())
            .expect("stored bytes should decode as a Binary Tuple");
        let obj = decoded.as_object().expect("decoded value is an object");
        assert_eq!(
            obj.get("name").and_then(|v| v.as_str()),
            Some("alice"),
            "decoded Binary Tuple should carry the original field"
        );
        // Not the raw MessagePack input anymore — it was re-encoded.
        assert_ne!(stored, doc_bytes());
    }
}
