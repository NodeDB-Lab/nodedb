// SPDX-License-Identifier: BUSL-1.1

//! The KV checkpoint load path: decode the published generation whole, install
//! its rows and index registrations, and record the replay floor it authorises.

use tracing::info;

use super::decoded::{DecodedKvCollection, DecodedKvGeneration};
use super::format::{KV_CKPT_FORMAT_VERSION, KvCheckpointFile};
use super::index_decode::decode_kv_indexes;
use super::index_restore::restore_collection_indexes;
use super::paths::{kv_ckpt_dir, kv_ckpt_gen_dir, parse_kv_ckpt_stem};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::snapshot::restore::database_id_from_qualified;

impl CoreLoop {
    /// Load the KV checkpoint from disk on startup, BEFORE WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/kv-ckpt/core-{core_id}/`), so no core-ownership filter on the
    /// filename is needed — a core only ever sees its own collections.
    ///
    /// Rows are reinstalled by replaying them through `KvEngine::put`, and the
    /// index registrations are reinstalled with their exported content once the
    /// rows are back — see `index_restore.rs` for why that order is the only
    /// sound one. The manifest's replay stamp then becomes the replay floor:
    /// `replay_kv_wal` skips the records the stamp names and applies every
    /// other one.
    ///
    /// # Fail-stop on corruption
    ///
    /// KV has no redb store behind it, so a published checkpoint is the only
    /// non-WAL home of its rows once the WAL below its LSN has been truncated.
    /// A checkpoint that exists but cannot be read or decoded is therefore
    /// unrecoverable data loss: this returns `Err` in that case instead of
    /// skipping it, and the boot sequence refuses to bring the core up. An
    /// absent checkpoint directory is not an error — WAL replay reconstructs
    /// everything.
    pub fn load_kv_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = kv_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = self.read_kv_manifest(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = kv_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it. The manifest
        // promises a complete set at one LSN; installing a subset and claiming
        // that LSN would silently drop the collections that failed to decode,
        // and installing a subset WITHOUT the floor would double-apply every
        // delta record against the rows that did load. Either way the failure
        // must be all-or-nothing, and it must abort boot rather than restore
        // nothing silently — the WAL below this LSN may already be gone.
        let decoded = self.decode_kv_generation(&gen_dir)?;

        let collections = decoded.len();
        let mut rows = 0usize;
        let mut indexes = 0usize;
        for ((tenant_id, collection), state) in decoded {
            let restored = self.restore_kv_checkpoint_collection(tenant_id, &collection, &state);
            rows += restored.rows;
            indexes += restored.indexes;
        }

        // Claimed only once every row AND every registration is in: the floor
        // suppresses WAL records, so claiming it over a half-restored generation
        // would turn a recoverable read failure into permanent data loss.
        let replay_prefix = manifest.replay.prefix;
        self.floors.kv_durable_lsn = crate::types::Lsn::new(replay_prefix);
        let applied_ranges = manifest.replay.applied_above.len();
        self.floors.replay_floors.kv.set(manifest.replay);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            collections,
            rows,
            indexes,
            replay_prefix,
            applied_ranges,
            "KV checkpoint restored"
        );
        Ok(())
    }

    /// Read and decode every collection file in a generation.
    ///
    /// `Err` if any file in the directory is unreadable, unparseable, or carries
    /// an unexpected format version — the caller then restores nothing.
    fn decode_kv_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> Result<DecodedKvGeneration, CheckpointDecodeError> {
        let entries =
            std::fs::read_dir(gen_dir).map_err(|source| CheckpointDecodeError::ScanDir {
                dir: gen_dir.to_path_buf(),
                source,
            })?;

        let mut decoded = DecodedKvGeneration::new();
        for entry in entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let (tenant_id, collection) = parse_kv_ckpt_stem(stem).ok_or_else(|| {
                CheckpointDecodeError::UnparseableFilename {
                    stem: stem.to_string(),
                }
            })?;

            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
                CheckpointDecodeError::ReadFile {
                    path: path.clone(),
                    source,
                }
            })?;
            let file = zerompk::from_msgpack::<KvCheckpointFile>(&bytes).map_err(|source| {
                CheckpointDecodeError::MsgpackDecode {
                    path: path.clone(),
                    source,
                }
            })?;
            if file.format_version != KV_CKPT_FORMAT_VERSION {
                return Err(CheckpointDecodeError::FormatVersion {
                    path: path.clone(),
                    found: file.format_version,
                    expected: KV_CKPT_FORMAT_VERSION,
                });
            }
            // Rebuilding the registrations here, not at install time, is what
            // keeps the generation all-or-nothing: this is the last fallible
            // step, and every one of them must be behind the caller's decision
            // to install.
            let indexes = decode_kv_indexes(&file.indexes).map_err(|source| {
                CheckpointDecodeError::KvIndexes {
                    path: path.clone(),
                    source: Box::new(source),
                }
            })?;
            decoded.insert(
                (tenant_id, collection),
                DecodedKvCollection {
                    entries: file.entries,
                    indexes,
                },
            );
        }
        Ok(decoded)
    }

    /// Install one decoded collection: its rows, then its index registrations.
    fn restore_kv_checkpoint_collection(
        &mut self,
        tenant_id: u64,
        collection: &str,
        state: &DecodedKvCollection,
    ) -> RestoredCounts {
        let now_ms = crate::engine::kv::current_ms();
        // `hash_to_collection` stores the db-qualified name, so the database id
        // is recoverable from the name itself — the same recovery the snapshot
        // restore path performs, reused so the rebuilt table key matches the one
        // live reads compute.
        let database_id = database_id_from_qualified(collection);

        // Rows first, while the collection still holds zero registrations: the
        // write path's zero-index fast path then leaves the indexes untouched,
        // so the exported index content installed below is the only content the
        // restored indexes get. Registering first and letting `put` derive the
        // content instead would not reproduce what was live — see
        // `index_restore.rs`.
        let mut restored = 0usize;
        for entry in &state.entries {
            // Drop rows whose TTL elapsed while the process was down; they would
            // otherwise reappear alive until the expiry wheel next ticked.
            if entry.expire_at_ms != 0 && entry.expire_at_ms <= now_ms {
                continue;
            }
            // Absolute expiry installed verbatim. Deriving `ttl_ms = expire_at -
            // now_ms` and letting `put` recompute `now_ms + ttl_ms` would drift
            // the instant forward by the checkpoint-to-restart delay.
            //
            // `put` — not a raw table insert — because it carries the surrogate
            // through, so the restored row keeps the cross-engine identity a
            // `Surrogate::ZERO` restore would sever.
            self.kv_engine.put_with_absolute_expiry(
                crate::engine::kv::KvPutParams {
                    database_id,
                    tenant_id,
                    collection,
                    key: &entry.key,
                    value: &entry.value,
                    // Unused by the absolute-expiry path; `expire_at_ms` is authoritative.
                    ttl_ms: 0,
                    now_ms,
                    surrogate: nodedb_types::Surrogate(entry.surrogate),
                },
                entry.expire_at_ms,
            );
            restored += 1;
        }

        restore_collection_indexes(
            &mut self.kv_engine,
            database_id,
            tenant_id,
            collection,
            &state.indexes,
        );

        RestoredCounts {
            rows: restored,
            indexes: state.indexes.fields.len()
                + state.indexes.composites.len()
                + state.indexes.sorted.len(),
        }
    }
}

/// What one collection's restore put back, for the load path's summary log.
struct RestoredCounts {
    rows: usize,
    indexes: usize,
}

#[cfg(test)]
mod tests {
    use super::super::format::{KvCheckpointEntry, KvCheckpointManifest};
    use super::super::index_format::KvCheckpointIndexes;
    use super::super::paths::{KV_CKPT_MANIFEST, kv_ckpt_filename};
    use super::*;
    use nodedb_types::Surrogate;
    use std::collections::HashSet;

    /// A db-qualified collection name must recover the database id the restore
    /// rebuilds the table key from — otherwise restored rows land under a key
    /// live reads never compute, and the collection reads back empty.
    #[test]
    fn qualified_collection_recovers_its_database_id() {
        assert_eq!(database_id_from_qualified("users"), 0);
        assert_eq!(database_id_from_qualified("2/orders"), 2);
    }

    fn new_engine() -> crate::engine::kv::KvEngine {
        crate::engine::kv::KvEngine::new(0, 16, 0.75, 4, 64, 100, 128)
    }

    /// Build an engine holding the given rows through the real `KvEngine` write
    /// path, so surrogates and expiry land exactly as production writes them.
    fn engine_with(rows: &[(&[u8], &[u8], u64, u32)]) -> crate::engine::kv::KvEngine {
        let mut engine = new_engine();
        for (key, value, expire_at, surrogate) in rows {
            engine.put_with_absolute_expiry(
                crate::engine::kv::KvPutParams {
                    database_id: 0,
                    tenant_id: 7,
                    collection: "users",
                    key,
                    value,
                    ttl_ms: 0,
                    now_ms: 1_000,
                    surrogate: Surrogate(*surrogate),
                },
                *expire_at,
            );
        }
        engine
    }

    /// The full disk round-trip: export a table (surrogates + absolute expiry
    /// intact), encode, write, read back, decode, replay into a fresh engine.
    /// Every row must return byte-identical, keep its surrogate, and keep its
    /// exact expiry instant.
    #[test]
    fn checkpoint_file_roundtrips_rows_surrogates_and_expiry() {
        let expire = 9_999_999u64;
        let engine = engine_with(&[
            (b"alice", b"va", 0, 11),
            (b"bob", b"vb", expire, 22),
            // A row written by an internal RMW path: surrogate unbound.
            (b"carol", b"vc", 0, 0),
        ]);

        let coll = engine.live_collections().next().expect("one collection");
        let entries: Vec<KvCheckpointEntry> = coll
            .table
            .expect("a collection with rows has a table")
            .export_entries_with_surrogates()
            .into_iter()
            .map(|e| KvCheckpointEntry {
                key: e.key,
                value: e.value,
                expire_at_ms: e.expire_at_ms,
                surrogate: e.surrogate.0,
            })
            .collect();
        assert_eq!(entries.len(), 3, "every live row must export");

        let written = KvCheckpointFile {
            format_version: KV_CKPT_FORMAT_VERSION,
            entries,
            indexes: KvCheckpointIndexes::default(),
        };

        let tmp = tempfile::tempdir().expect("tempdir");
        let name = kv_ckpt_filename(7, "users");
        let path = tmp.path().join(&name);
        let bytes = zerompk::to_msgpack_vec(&written).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(tmp.path(), &name, &bytes).expect("write");

        let read_back = nodedb_wal::segment::read_checkpoint_framed(&path).expect("read");
        let decoded: KvCheckpointFile = zerompk::from_msgpack(&read_back).expect("decode");
        assert_eq!(decoded, written, "the file must decode to what was written");

        // Replay into a fresh engine the way `restore_kv_checkpoint_collection`
        // does.
        let mut restored = new_engine();
        for entry in &decoded.entries {
            restored.put_with_absolute_expiry(
                crate::engine::kv::KvPutParams {
                    database_id: 0,
                    tenant_id: 7,
                    collection: "users",
                    key: &entry.key,
                    value: &entry.value,
                    ttl_ms: 0,
                    now_ms: 1_000,
                    surrogate: Surrogate(entry.surrogate),
                },
                entry.expire_at_ms,
            );
        }

        for (key, want) in [
            (&b"alice"[..], &b"va"[..]),
            (&b"bob"[..], &b"vb"[..]),
            (&b"carol"[..], &b"vc"[..]),
        ] {
            assert_eq!(
                restored.get(0, 7, "users", key, 1_000).as_deref(),
                Some(want),
                "row must survive the round-trip byte-identical"
            );
        }

        // Surrogates survive: the restored row is still reachable by its stable
        // cross-engine identity, which a `Surrogate::ZERO` restore would sever.
        assert_eq!(
            restored
                .key_for_surrogate(0, 7, "users", Surrogate(11))
                .as_deref(),
            Some(&b"alice"[..])
        );
        assert_eq!(
            restored
                .key_for_surrogate(0, 7, "users", Surrogate(22))
                .as_deref(),
            Some(&b"bob"[..])
        );

        // The absolute expiry instant survives verbatim — not re-derived from
        // wall-clock elapsed time at restore.
        let meta = restored
            .get_ttl_meta(0, 7, "users", b"bob")
            .expect("bob has meta");
        assert!(meta.has_ttl);
        assert_eq!(
            meta.expire_at_ms, expire,
            "expiry must not drift on restore"
        );
        let alice_meta = restored
            .get_ttl_meta(0, 7, "users", b"alice")
            .expect("alice has meta");
        assert!(!alice_meta.has_ttl, "a persistent row must not gain a TTL");
    }

    /// The manifest is the only record of what a generation holds, and the
    /// entire replay floor rests on it: it must survive the round-trip exactly.
    #[test]
    fn manifest_roundtrips_generation_and_stamp() {
        let replay = crate::data::executor::applied_prefix::ReplayStamp {
            prefix: 4_200,
            applied_above: vec![crate::data::executor::applied_prefix::stamp::LsnRange {
                start: 4_240,
                end: 4_242,
            }],
        };
        let written = KvCheckpointManifest {
            format_version: KV_CKPT_FORMAT_VERSION,
            generation: 9,
            replay: replay.clone(),
        };
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join(KV_CKPT_MANIFEST);
        let bytes = zerompk::to_msgpack_vec(&written).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(tmp.path(), KV_CKPT_MANIFEST, &bytes)
            .expect("write");

        let read_back = nodedb_wal::segment::read_checkpoint_framed(&path).expect("read");
        let decoded: KvCheckpointManifest = zerompk::from_msgpack(&read_back).expect("decode");
        assert_eq!(decoded.generation, 9);
        assert_eq!(decoded.format_version, KV_CKPT_FORMAT_VERSION);
        assert_eq!(decoded.replay, replay, "the stamp must survive exactly");
    }

    /// Entries still sitting in the rehash source are live rows. An export that
    /// walked only the primary slots would silently drop them while the
    /// checkpoint reported an LSN claiming they were durable.
    #[test]
    fn export_includes_entries_mid_rehash() {
        use crate::engine::kv::hash_table::KvHashTable;

        // Drive the table until an incremental rehash is actually in flight, so
        // the test cannot silently degrade into asserting the easy case.
        let mut table = KvHashTable::new(4, 0.5, 1, 64);
        let mut inserted = 0u32;
        while !table.is_rehashing() {
            let key = format!("k{inserted}");
            table.put(key.as_bytes(), b"v", 0, Surrogate(inserted + 1));
            inserted += 1;
            assert!(
                inserted < 1_000,
                "table never entered an incremental rehash"
            );
        }

        let exported = table.export_entries_with_surrogates();
        assert_eq!(
            exported.len(),
            table.len(),
            "export must yield every live row, including those still sitting in \
             the rehash source and not yet migrated into the primary slots"
        );
        let surrogates: HashSet<u32> = exported.iter().map(|e| e.surrogate.0).collect();
        assert_eq!(
            surrogates.len(),
            inserted as usize,
            "every row must keep its own distinct surrogate across the export"
        );
    }

    /// A core rooted at `dir`, so a corrupt manifest can be planted on disk and
    /// then read back through the real boot-time load path.
    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        use std::sync::Arc;

        use nodedb_bridge::buffer::RingBuffer;
        use nodedb_types::OrdinalClock;

        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let hlc = Arc::new(OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in this test
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            hlc,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    /// A manifest that exists but is corrupt (truncated / bad frame) must fail
    /// the load, not be treated as absent: KV has no redb store behind it, so
    /// the checkpoint is the only non-WAL home of its rows once the WAL below
    /// its LSN has been truncated. Silently skipping it would be permanent,
    /// unannounced data loss.
    #[test]
    fn corrupt_manifest_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = kv_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let manifest_path = ckpt_dir.join(KV_CKPT_MANIFEST);
        std::fs::write(&manifest_path, b"not a valid checkpoint frame")
            .expect("write garbage manifest");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_kv_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
    }

    /// A manifest whose replay stamp has an applied range at or below its
    /// prefix is malformed. Gating replay on it could skip a record no
    /// generation holds, so the load fails instead.
    #[test]
    fn a_manifest_with_a_malformed_stamp_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = kv_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let manifest = KvCheckpointManifest {
            format_version: KV_CKPT_FORMAT_VERSION,
            generation: 0,
            replay: crate::data::executor::applied_prefix::ReplayStamp {
                prefix: 10,
                applied_above: vec![crate::data::executor::applied_prefix::stamp::LsnRange {
                    start: 5,
                    end: 12,
                }],
            },
        };
        let bytes = zerompk::to_msgpack_vec(&manifest).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, KV_CKPT_MANIFEST, &bytes)
            .expect("write manifest");
        drop(core);

        let mut restored = open_core_at(dir.path());
        let error = restored
            .load_kv_checkpoints()
            .expect_err("a malformed stamp must fail the load");
        assert!(
            error.to_string().contains("replay stamp"),
            "the error names the stamp: {error}"
        );
    }
}
