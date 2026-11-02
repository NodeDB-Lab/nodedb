// SPDX-License-Identifier: BUSL-1.1

//! Installing a decoded generation's index registrations into the engine.
//!
//! Runs AFTER the collection's rows are back. That order is load-bearing in both
//! directions:
//!
//! * Rows must not be replayed through `put` while registrations are live, or
//!   PUT-driven maintenance would derive index entries alongside the exported
//!   ones — and the derived entries are not the same set. A `backfill=false`
//!   index deliberately omits the rows that predate it, and the engine's two
//!   sort-key extraction paths disagree on some column types.
//! * Nothing is installed until the whole generation has decoded, so a
//!   collection can never come back holding rows whose registrations failed to
//!   rebuild.

use super::index_decode::DecodedKvIndexes;
use crate::engine::kv::{KvEngine, RestoreCompositeIndexParams, RestoreFieldIndexParams};

/// Reinstate every registration on one collection, content included.
pub(super) fn restore_collection_indexes(
    engine: &mut KvEngine,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    indexes: &DecodedKvIndexes,
) {
    for field_index in &indexes.fields {
        engine.restore_field_index(RestoreFieldIndexParams {
            database_id,
            tenant_id,
            collection,
            field: &field_index.field,
            field_position: field_index.field_position,
            coverage: field_index.coverage,
            entries: &field_index.entries,
        });
    }

    for composite in &indexes.composites {
        engine.restore_composite_index(RestoreCompositeIndexParams {
            database_id,
            tenant_id,
            collection,
            fields: &composite.fields,
            field_positions: &composite.field_positions,
            coverage: composite.coverage,
            entries: &composite.entries,
        });
    }

    for sorted in &indexes.sorted {
        engine.restore_sorted_index(database_id, tenant_id, sorted.def.clone(), &sorted.entries);
    }
}

/// Round-trip tests for the index registrations a KV checkpoint carries.
///
/// These drive the real engine registration paths, then the real export → encode
/// → write → read → decode → restore chain, and assert against QUERIES on the
/// restored engine rather than against the exported structs: a registration that
/// survives as a struct but does not answer lookups is exactly the failure this
/// whole mechanism exists to prevent.
#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::super::format::{KV_CKPT_FORMAT_VERSION, KvCheckpointEntry, KvCheckpointFile};
    use super::super::index_decode::decode_kv_indexes;
    use super::super::index_export::export_collection_indexes;
    use super::super::paths::kv_ckpt_filename;
    use super::*;
    use crate::data::executor::handlers::kv::sorted_index_compute::{
        BuildSortedIndexDefParams, build_sorted_index_def,
    };
    use crate::engine::kv::sorted_index::manager::SortedIndexDef;
    use crate::engine::kv::{KvPutParams, RegisterIndexParams};

    const DB: u64 = 0;
    const TID: u64 = 7;
    const COLL: &str = "players";
    const NOW: u64 = 1_000;

    fn new_engine() -> KvEngine {
        KvEngine::new(0, 16, 0.75, 4, 64, 100, 128)
    }

    /// A player row carrying every field the three index kinds key on.
    fn row(name: &str, score: i64, region: &str, status: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({
            "player_id": name,
            "name": name,
            "score": score,
            "region": region,
            "status": status,
        }))
        .expect("encode row")
    }

    fn put(engine: &mut KvEngine, key: &[u8], value: Vec<u8>, surrogate: u32) {
        engine
            .put(KvPutParams {
                database_id: DB,
                tenant_id: TID,
                collection: COLL,
                key,
                value: &value,
                ttl_ms: 0,
                now_ms: NOW,
                surrogate: Surrogate(surrogate),
            })
            .expect("a bound row writes");
    }

    fn leaderboard_def() -> SortedIndexDef {
        let sort_columns = vec![("score".to_string(), "DESC".to_string())];
        build_sorted_index_def(BuildSortedIndexDefParams {
            collection: COLL,
            index_name: "lb",
            sort_columns: &sort_columns,
            key_column: "player_id",
            window_type: "",
            window_timestamp_column: "",
            window_start_ms: 0,
            window_end_ms: 0,
        })
        .expect("build leaderboard def")
    }

    fn composite_fields() -> Vec<String> {
        vec!["region".to_string(), "status".to_string()]
    }

    /// The `(tenant_id, collection, table_key)` the checkpoint writer would file
    /// this collection under.
    fn table_key_of(engine: &KvEngine, collection: &str) -> u64 {
        engine
            .live_collections()
            .find(|c| c.collection == collection)
            .map(|c| c.table_key)
            .expect("collection must be named for the checkpoint to see it")
    }

    /// Reinstall a decoded collection the way `load.rs` does: rows first, while the
    /// collection still holds zero registrations, then the registrations with their
    /// exported content.
    fn restore_like_load(engine: &mut KvEngine, file: &KvCheckpointFile) {
        for entry in &file.entries {
            engine
                .put_with_absolute_expiry(
                    KvPutParams {
                        database_id: DB,
                        tenant_id: TID,
                        collection: COLL,
                        key: &entry.key,
                        value: &entry.value,
                        ttl_ms: 0,
                        now_ms: NOW,
                        surrogate: Surrogate(entry.surrogate),
                    },
                    entry.expire_at_ms,
                )
                .expect("a checkpointed row is bound");
        }
        let indexes = decode_kv_indexes(&file.indexes).expect("indexes must decode");
        restore_collection_indexes(engine, DB, TID, COLL, &indexes);
    }

    /// Export one collection into the on-disk file shape.
    fn export_file(engine: &KvEngine, collection: &str) -> KvCheckpointFile {
        let coll = engine
            .live_collections()
            .find(|c| c.collection == collection)
            .expect("collection must be live");
        let entries = coll
            .table
            .map(|table| {
                table
                    .export_entries_with_surrogates()
                    .into_iter()
                    .map(|e| KvCheckpointEntry {
                        key: e.key,
                        value: e.value,
                        expire_at_ms: e.expire_at_ms,
                        surrogate: e.surrogate.0,
                        sys_from: e.sys_from,
                    })
                    .collect()
            })
            .unwrap_or_default();
        KvCheckpointFile {
            format_version: KV_CKPT_FORMAT_VERSION,
            entries,
            history: coll
                .table
                .map(super::super::history_codec::export_history)
                .unwrap_or_default(),
            indexes: export_collection_indexes(engine, coll.table_key),
        }
    }

    /// Encode, fsync to disk, read back and decode — the same calls the write and
    /// load paths make, so an index kind that cannot survive msgpack fails here.
    fn through_disk(file: &KvCheckpointFile) -> KvCheckpointFile {
        let tmp = tempfile::tempdir().expect("tempdir");
        let name = kv_ckpt_filename(TID, COLL);
        let path = tmp.path().join(&name);
        let bytes = zerompk::to_msgpack_vec(file).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(tmp.path(), &name, &bytes).expect("write");
        let read_back = nodedb_wal::segment::read_checkpoint_framed(&path).expect("read");
        zerompk::from_msgpack(&read_back).expect("decode")
    }

    /// An engine holding all three index kinds over the same collection.
    ///
    /// The composite index is seeded through `KvIndexSet::add_composite_index`
    /// directly because that is the only registration path composite indexes have —
    /// there is no `KvOp`, WAL record or engine-level method for them, so no
    /// production caller can create one today. The checkpoint still has to carry
    /// them: it is the engine's index state, and a checkpoint that silently dropped
    /// a kind of registration would be the exact bug this format exists to close.
    fn engine_with_every_index_kind() -> KvEngine {
        let mut engine = new_engine();

        // One row first, so the collection exists and can be named.
        put(&mut engine, b"p1", row("alice", 100, "us", "active"), 11);

        let tkey = table_key_of(&engine, COLL);
        engine.indexes.entry(tkey).or_default().add_composite_index(
            composite_fields(),
            vec![0, 1],
            // Registered over p1 without filing it.
            crate::engine::kv::index::IndexCoverage::SinceRegistration,
        );

        engine.register_index(RegisterIndexParams {
            database_id: DB,
            tenant_id: TID,
            collection: COLL,
            field: "name",
            field_position: 0,
            backfill: true,
            now_ms: NOW,
        });
        engine.register_sorted_index(DB, TID, COLL, leaderboard_def());

        put(&mut engine, b"p2", row("bob", 300, "us", "active"), 22);
        put(&mut engine, b"p3", row("carol", 200, "eu", "active"), 33);

        engine
    }

    fn lookup_name(engine: &KvEngine, name: &str) -> Vec<Vec<u8>> {
        engine.index_lookup_eq(
            DB,
            TID,
            COLL,
            "name",
            &nodedb_types::Value::String(name.into()),
        )
    }

    /// The index key a text field value is filed under.
    fn text_key(text: &str) -> Vec<u8> {
        crate::engine::kv::index::kv_index_key(&nodedb_types::Value::String(text.into()))
    }

    /// Every index kind must come back from disk answering the queries it answered
    /// before, over a collection whose rows came back too.
    #[test]
    fn every_index_kind_roundtrips_through_disk_and_serves_queries() {
        let engine = engine_with_every_index_kind();
        let exported = export_file(&engine, COLL);
        assert_eq!(exported.indexes.fields.len(), 1, "the field index exports");
        assert_eq!(
            exported.indexes.composites.len(),
            1,
            "the composite index exports"
        );
        assert_eq!(exported.indexes.sorted.len(), 1, "the sorted index exports");

        let decoded_file = through_disk(&exported);
        assert_eq!(
            decoded_file, exported,
            "the file must decode to exactly what was written"
        );

        let mut restored = new_engine();
        restore_like_load(&mut restored, &decoded_file);

        // Rows.
        assert_eq!(
            restored.get(DB, TID, COLL, b"p2", NOW).as_deref(),
            Some(row("bob", 300, "us", "active").as_slice())
        );

        // Single-field secondary index: registered with backfill, so every row.
        assert_eq!(lookup_name(&restored, "alice"), vec![b"p1".to_vec()]);
        assert_eq!(lookup_name(&restored, "bob"), vec![b"p2".to_vec()]);
        assert_eq!(lookup_name(&restored, "carol"), vec![b"p3".to_vec()]);

        // Sorted index: DESC by score.
        assert_eq!(
            restored.sorted_index_rank(DB, TID, "lb", b"p2", NOW),
            Some(1)
        );
        assert_eq!(
            restored.sorted_index_rank(DB, TID, "lb", b"p3", NOW),
            Some(2)
        );
        assert_eq!(
            restored.sorted_index_rank(DB, TID, "lb", b"p1", NOW),
            Some(3)
        );
        let top = restored
            .sorted_index_top_k(DB, TID, "lb", 2, NOW)
            .expect("the restored leaderboard must answer top-k");
        assert_eq!(top[0], (1, b"p2".to_vec()));
        assert_eq!(top[1], (2, b"p3".to_vec()));

        // Composite index: exact and prefix lookups.
        let tkey = table_key_of(&restored, COLL);
        let set = restored
            .index_set(tkey)
            .expect("the restored collection must hold its index set");
        let composite = set
            .get_composite_index(&composite_fields())
            .expect("the composite registration must survive");
        assert_eq!(
            composite.coverage(),
            crate::engine::kv::index::IndexCoverage::SinceRegistration,
            "a partial index must come back partial, or it would answer equalities"
        );
        assert_eq!(
            set.get_index("name").map(|index| index.coverage()),
            Some(crate::engine::kv::index::IndexCoverage::Complete)
        );
        assert_eq!(
            composite.lookup_eq(&[&text_key("eu"), &text_key("active")]),
            vec![b"p3".as_slice()]
        );
        assert_eq!(
            composite.lookup_prefix(&[&text_key("us")]),
            vec![b"p2".as_slice()],
            "content is restored verbatim: p1 predates the composite registration \
             and was never in it"
        );
    }

    /// A restored registration must keep being MAINTAINED, not just be present:
    /// rows written after the restore have to land in every index.
    #[test]
    fn restored_indexes_still_maintain_new_writes() {
        let engine = engine_with_every_index_kind();
        let mut restored = new_engine();
        restore_like_load(&mut restored, &export_file(&engine, COLL));

        put(&mut restored, b"p4", row("dave", 400, "us", "active"), 44);

        assert_eq!(lookup_name(&restored, "dave"), vec![b"p4".to_vec()]);
        assert_eq!(
            restored.sorted_index_rank(DB, TID, "lb", b"p4", NOW),
            Some(1),
            "dave's 400 must outrank bob's 300 in the restored leaderboard"
        );
        let tkey = table_key_of(&restored, COLL);
        let composite = restored
            .index_set(tkey)
            .and_then(|s| s.get_composite_index(&composite_fields()))
            .expect("composite registration");
        assert_eq!(
            composite.lookup_eq(&[&text_key("us"), &text_key("active")]),
            vec![b"p2".as_slice(), b"p4".as_slice()]
        );
    }

    /// The reason the checkpoint stores index CONTENT instead of re-deriving it from
    /// the restored rows: a `backfill=false` index deliberately omits the rows that
    /// predate it. Re-deriving would silently promote it to a full index and start
    /// answering with rows it was never meant to contain.
    #[test]
    fn backfill_false_index_stays_partial_across_restore() {
        let mut engine = new_engine();
        put(&mut engine, b"p1", row("alice", 100, "us", "active"), 11);
        engine.register_index(RegisterIndexParams {
            database_id: DB,
            tenant_id: TID,
            collection: COLL,
            field: "name",
            field_position: 0,
            backfill: false,
            now_ms: NOW,
        });
        put(&mut engine, b"p2", row("bob", 300, "us", "active"), 22);

        assert!(lookup_name(&engine, "alice").is_empty(), "precondition");
        assert_eq!(lookup_name(&engine, "bob"), vec![b"p2".to_vec()]);

        let mut restored = new_engine();
        restore_like_load(&mut restored, &through_disk(&export_file(&engine, COLL)));

        assert!(
            lookup_name(&restored, "alice").is_empty(),
            "a row the index deliberately never held must not appear after a restore"
        );
        assert_eq!(
            lookup_name(&restored, "bob"),
            vec![b"p2".to_vec()],
            "the rows it did hold must still be there"
        );
        assert_eq!(
            restored.get(DB, TID, COLL, b"p1", NOW).as_deref(),
            Some(row("alice", 100, "us", "active").as_slice()),
            "the unindexed row itself is still restored — only the index omits it"
        );
    }

    /// `CREATE INDEX` before the first `INSERT` leaves a collection with a
    /// registration and no rows. It has to reach the checkpoint anyway: its
    /// registration's WAL record is truncated away just the same.
    #[test]
    fn index_only_collection_survives_with_no_rows() {
        let mut engine = new_engine();
        engine.register_index(RegisterIndexParams {
            database_id: DB,
            tenant_id: TID,
            collection: COLL,
            field: "name",
            field_position: 0,
            backfill: true,
            now_ms: NOW,
        });

        let coll = engine
            .live_collections()
            .find(|c| c.collection == COLL)
            .expect("a collection with only registrations is still checkpointable");
        assert!(coll.table.is_none(), "it holds no rows yet");

        let file = through_disk(&export_file(&engine, COLL));
        assert!(file.entries.is_empty());
        assert_eq!(file.indexes.fields.len(), 1);

        let mut restored = new_engine();
        restore_like_load(&mut restored, &file);
        assert!(
            restored.has_indexes(DB, TID, COLL),
            "the registration must come back even with no rows to hang it on"
        );

        // And it must be live, not merely present.
        put(&mut restored, b"p1", row("alice", 100, "us", "active"), 11);
        assert_eq!(lookup_name(&restored, "alice"), vec![b"p1".to_vec()]);
    }

    /// A windowed leaderboard's bounds are part of its registration; losing them
    /// would silently change which entries the restored index reports.
    #[test]
    fn custom_window_bounds_survive_the_roundtrip() {
        let mut engine = new_engine();
        put(&mut engine, b"p1", row("alice", 100, "us", "active"), 11);
        let sort_columns = vec![
            ("score".to_string(), "DESC".to_string()),
            ("updated_at".to_string(), "ASC".to_string()),
        ];
        let def = build_sorted_index_def(BuildSortedIndexDefParams {
            collection: COLL,
            index_name: "lb_window",
            sort_columns: &sort_columns,
            key_column: "player_id",
            window_type: "CUSTOM",
            window_timestamp_column: "updated_at",
            window_start_ms: 1_700_000_000_000,
            window_end_ms: 1_700_100_000_000,
        })
        .expect("build windowed def");
        engine.register_sorted_index(DB, TID, COLL, def);

        let file = through_disk(&export_file(&engine, COLL));
        let mut restored = new_engine();
        restore_like_load(&mut restored, &file);

        let def = restored
            .sorted_index_def(DB, TID, "lb_window")
            .expect("the windowed registration must survive");
        assert_eq!(def.key_column, "player_id");
        assert_eq!(def.window.timestamp_column, "updated_at");
        match &def.window.window_type {
            crate::engine::kv::sorted_index::WindowType::Custom { start_ms, end_ms } => {
                assert_eq!(*start_ms, 1_700_000_000_000);
                assert_eq!(*end_ms, 1_700_100_000_000);
            }
            other => panic!("expected a custom window, got {other:?}"),
        }
        let columns = def.encoder.columns();
        assert_eq!(columns.len(), 2, "both sort columns must survive");
        assert_eq!(columns[0].name, "score");
        assert_eq!(
            columns[0].direction,
            crate::engine::kv::sorted_index::SortDirection::Desc
        );
        assert_eq!(columns[1].name, "updated_at");
        assert_eq!(
            columns[1].direction,
            crate::engine::kv::sorted_index::SortDirection::Asc
        );
    }
}
