// SPDX-License-Identifier: BUSL-1.1

//! The columnar checkpoint load path: decode the published generation whole,
//! install every engine with its flushed segments and surrogate sidecar, and
//! record the replay floor it authorises.

use tracing::info;

use super::paths::{columnar_ckpt_dir, columnar_ckpt_gen_dir};
use super::restore::RestoredColumnar;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Load the columnar checkpoint from disk on startup, BEFORE WAL replay and
    /// BEFORE `seed_columnar_schemas`.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/columnar-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own collections.
    ///
    /// Ordering against the schema seed is what keeps the two from fighting:
    /// `seed_columnar_schemas` skips any collection that already has an engine,
    /// so running first means a restored engine keeps the exact schema it was
    /// exported with — including the bitemporal columns the seed would prepend —
    /// and the seed only creates engines for collections this restore did not
    /// cover. Running after the seed would instead leave the seed's empty engine
    /// in place and silently discard the restored rows.
    ///
    /// The manifest's LSN then becomes the replay floor: the columnar replay
    /// arms skip the records already folded in and apply everything above.
    ///
    /// # Fail-stop on corruption
    ///
    /// Columnar is memory-only on both halves — nothing writes the live
    /// memtables or flushed segment bytes to disk outside this checkpoint — so
    /// a published generation is the only non-WAL home of its rows once the
    /// WAL below its LSN has been truncated. A checkpoint that exists but
    /// cannot be read or decoded is therefore unrecoverable data loss: this
    /// returns `Err` in that case instead of skipping it, and the boot
    /// sequence refuses to bring the core up. An absent checkpoint directory
    /// is not an error — WAL replay reconstructs everything.
    pub fn load_columnar_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = columnar_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = self.read_columnar_manifest(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = columnar_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it. The manifest
        // promises a complete set at one LSN; installing a subset and claiming
        // that LSN would silently drop the collections that failed to decode,
        // and installing a subset WITHOUT the floor would re-apply every
        // non-idempotent Update record against the rows that did load. Either
        // way the failure must abort boot rather than restore nothing silently
        // — the WAL below this LSN may already be gone.
        let decoded = self.decode_columnar_generation(&gen_dir)?;

        let collections = decoded.len();
        let mut segments = 0usize;
        let mut geometry_rows = 0usize;
        for (key, restored) in decoded {
            let RestoredColumnar {
                engine,
                segments: blobs,
                surrogates,
            } = restored;

            // The R-tree entries for this collection's geometry columns are
            // rebuilt from the restored rows, not carried in the checkpoint —
            // see `geometry_restore.rs`. Done BEFORE the maps are populated so
            // it reads the restored engine and blobs directly and cannot see a
            // half-installed state. A restored row that does not read refuses
            // the load: the rebuilt R-tree would silently miss it.
            geometry_rows += self.restore_columnar_geometry_indexes(&key, &engine, &blobs)?;

            segments += blobs.len();
            // Both halves are installed from one destructured value, in the same
            // iteration, unconditionally. There is no branch on which one of the
            // two maps gets an entry, so the outer-index agreement that
            // `scan_flushed.rs` relies on cannot be broken here.
            self.columnar_flushed_segments.insert(key.clone(), blobs);
            self.columnar_flushed_surrogates
                .insert(key.clone(), surrogates);
            self.columnar_engines.insert(key, engine);
        }

        // Claimed only once every engine is in: the floor suppresses WAL
        // records, so claiming it over a half-restored generation would turn a
        // recoverable read failure into permanent data loss.
        self.floors.columnar_durable_lsn = Lsn::new(manifest.replay.prefix);
        let replay_prefix = manifest.replay.prefix;
        let applied_ranges = manifest.replay.applied_above.len();
        self.floors.replay_floors.columnar.set(manifest.replay);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            collections,
            segments,
            geometry_rows,
            replay_prefix,
            applied_ranges,
            "columnar checkpoint restored"
        );
        Ok(())
    }
}

/// Load-path coverage for the columnar checkpoint: what a restart restores,
/// which generation is live, and the replay floor it sets.
#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;
    use nodedb_types::value::Value;

    use super::super::test_fixture::{ids, open_core, scan, scan_at, schema, seed_collection};
    use crate::data::executor::columnar_checkpoint::paths;
    use crate::types::{Lsn, TenantId};

    /// The headline claim: rows that existed ONLY in memory — in a flushed segment
    /// whose bytes no code path writes to disk, and in a live memtable — come back
    /// after a restart and are returned by a scan.
    ///
    /// This is the data-loss bug's direct regression: before the checkpoint, the WAL
    /// was the only copy of both populations while columnar writes advanced the
    /// watermark that authorised deleting it.
    #[test]
    fn restored_rows_are_visible_to_a_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_roundtrip";

        let mut core = open_core(dir.path());
        seed_collection(
            &mut core,
            coll,
            &[(1, "a", Surrogate(101)), (2, "b", Surrogate(102))],
            &[(3, "c", Surrogate(103))],
        );
        assert_eq!(ids(&scan(&mut core, coll, None)), vec![1, 2, 3]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        assert!(
            restored.columnar_engines.is_empty(),
            "a freshly opened core must hold nothing before the load runs"
        );
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![1, 2, 3],
            "every row must survive the restart: the flushed-segment rows AND the \
             rows still resident in the memtable"
        );
    }

    /// The reported LSN is a deletion authority: it is the watermark on success
    /// and comes back as the restored durable LSN. Replay is gated by the
    /// stamp instead: the outcome floor and the records applied above it, never
    /// the highest applied LSN, since a record below that can still be on its
    /// way. Too wide a gate drops a write; too narrow re-applies a folded one.
    #[test]
    fn the_stamp_becomes_the_restored_floor_and_its_prefix_the_durable_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_lsn";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(601))], &[]);
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(890));
        core.floors.applied_prefix.note_applied(Lsn::new(900));
        core.watermark = Lsn::new(900);

        let reported = core
            .checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        assert_eq!(
            reported,
            Lsn::new(890),
            "a successful flush reports the outcome floor, never the watermark: the \
             record at 891 can still be on its way"
        );
        drop(core);

        let mut restored = open_core(dir.path());
        assert!(
            !restored.floors.replay_floors.columnar.covers(1),
            "before the load, no floor is set and nothing is gated"
        );
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(restored.floors.columnar_durable_lsn, Lsn::new(890));
        let floor = &restored.floors.replay_floors.columnar;
        assert!(floor.covers(890), "the prefix is folded in");
        assert!(
            !floor.covers(891),
            "a record in flight below the applied one is NOT in the restored state"
        );
        assert!(floor.covers(900), "the applied record is folded in");
        assert!(
            !floor.covers(901),
            "a record above the stamp is NOT in the restored state and must replay"
        );
    }

    /// A flush that cannot write must surface a typed error rather than reporting
    /// an LSN. The caller's clamp depends on it: an `Ok(watermark)` here would
    /// authorise deleting the WAL segments holding the only copy of the rows this
    /// flush failed to write. The clamp itself is covered in
    /// `handlers/control/checkpoint_durable_lsn.rs`, where the contributor lives.
    #[test]
    fn a_failed_flush_errors_and_leaves_the_durable_lsn_untouched() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_fail";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(701))], &[]);
        core.watermark = Lsn::new(900);

        // Occupy the checkpoint directory's path with a FILE, so `create_dir_all`
        // fails and the flush cannot publish anything.
        std::fs::write(dir.path().join("columnar-ckpt"), b"not a directory")
            .expect("write blocking file");

        assert!(
            core.checkpoint_columnar_engines().is_err(),
            "a flush that cannot write must surface a typed error, never Ok"
        );
        assert_eq!(
            core.floors.columnar_durable_lsn,
            Lsn::ZERO,
            "a fresh core is durable through NOTHING, and a failed flush must not \
             advance that"
        );
    }

    /// A generation is only live once its manifest names it. Files written by a
    /// cycle that died before publishing must be inert: restoring them would install
    /// rows at an LSN nothing vouched for, and with them a floor that suppresses the
    /// records which would have corrected them.
    #[test]
    fn an_unpublished_generation_is_not_restored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_unpublished";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(801))], &[]);
        core.watermark = Lsn::new(500);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        // Remove the manifest, leaving the generation directory fully intact.
        let manifest = dir
            .path()
            .join("columnar-ckpt")
            .join("core-0")
            .join(paths::COLUMNAR_CKPT_MANIFEST);
        assert!(manifest.exists(), "the checkpoint published a manifest");
        std::fs::remove_file(&manifest).expect("remove manifest");

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert!(
            restored.columnar_engines.is_empty(),
            "without a manifest the generation is invisible"
        );
        assert!(
            !restored.floors.replay_floors.columnar.covers(1),
            "restoring nothing must gate nothing — replay falls back to the full WAL"
        );
    }

    /// Publishing a second generation must supersede the first, not merge with it.
    /// A restore that saw both would resurrect rows a later checkpoint recorded as
    /// deleted.
    #[test]
    fn a_republished_generation_supersedes_the_previous_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_regen";

        let mut core = open_core(dir.path());
        let key = seed_collection(
            &mut core,
            coll,
            &[],
            &[(1, "a", Surrogate(901)), (2, "b", Surrogate(902))],
        );
        core.watermark = Lsn::new(100);
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(100));
        core.checkpoint_columnar_engines()
            .expect("first checkpoint must publish");

        // Delete a row, then publish again at a higher LSN.
        core.columnar_engines
            .get_mut(&key)
            .expect("engine")
            .delete(&Value::Integer(1), 100)
            .expect("delete");
        core.watermark = Lsn::new(200);
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(200));
        core.checkpoint_columnar_engines()
            .expect("second checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![2],
            "the live generation is the SECOND one; the deleted row must not return"
        );
        assert_eq!(restored.floors.columnar_durable_lsn, Lsn::new(200));
    }

    /// Row history survives a checkpoint and a restart: a read at an
    /// ordinal before an overwrite or a delete sees the rows it saw before,
    /// flushed and resident alike, and a current read sees the newest.
    #[test]
    fn row_history_survives_a_checkpoint_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_history";

        let mut core = open_core(dir.path());
        let key = seed_collection(
            &mut core,
            coll,
            &[(1, "a", Surrogate(1001)), (2, "b", Surrogate(1002))],
            &[(3, "c", Surrogate(1003))],
        );
        {
            let engine = core.columnar_engines.get_mut(&key).expect("engine");
            engine
                .insert_with_surrogate(
                    &[Value::Integer(1), Value::String("a2".into())],
                    Surrogate(1001),
                    50,
                )
                .expect("overwrite");
            engine.delete(&Value::Integer(2), 60).expect("delete");
        }
        core.watermark = Lsn::new(100);
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(100));
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");

        let pinned = |ordinal| nodedb_types::SystemTimeScope::Pinned {
            ordinal,
            cut_marker: 0,
        };
        let names = |rows: Vec<serde_json::Value>| -> Vec<String> {
            let mut v: Vec<String> = rows
                .iter()
                .filter_map(|r| r.get("name").and_then(|x| x.as_str()).map(str::to_string))
                .collect();
            v.sort_unstable();
            v
        };
        assert_eq!(
            names(scan_at(&mut restored, coll, None, pinned(10))),
            vec!["a", "b", "c"],
            "before the overwrite and the delete"
        );
        assert_eq!(
            names(scan_at(&mut restored, coll, None, pinned(55))),
            vec!["a2", "b", "c"],
            "after the overwrite, before the delete"
        );
        assert_eq!(names(scan(&mut restored, coll, None)), vec!["a2", "c"]);
        assert_eq!(
            names(scan_at(
                &mut restored,
                coll,
                None,
                nodedb_types::SystemTimeScope::AllVersions
            )),
            vec!["a", "a2", "b", "c"],
            "every version"
        );
    }

    /// A restore must not be undone by the schema seed that runs after it.
    /// `seed_columnar_schemas` creates an EMPTY engine for every catalog collection
    /// and skips those that already exist — so the restored engine must be the one
    /// that survives, or every restored row is silently discarded at boot.
    #[test]
    fn the_schema_seed_does_not_replace_a_restored_engine() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_seed_order";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(1001))], &[]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        let mut restored = open_core(dir.path());
        restored
            .load_columnar_checkpoints()
            .expect("checkpoint load must succeed");
        // Exactly the boot order in `data/runtime.rs`: load, then seed.
        restored.seed_columnar_schemas(&[(
            nodedb_types::DatabaseId::DEFAULT,
            TenantId::new(1),
            coll.to_string(),
            schema(),
        )]);

        assert_eq!(
            ids(&scan(&mut restored, coll, None)),
            vec![1],
            "the seed must skip the restored engine, not overwrite it with an empty one"
        );
    }

    /// A manifest that exists but is corrupt must abort boot, not be treated as an
    /// absent generation: the WAL below the LSN it names may already be truncated,
    /// so silently restoring nothing would be permanent, unannounced data loss.
    #[test]
    fn a_corrupt_manifest_fails_the_load_instead_of_restoring_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let coll = "ck_corrupt_manifest";

        let mut core = open_core(dir.path());
        seed_collection(&mut core, coll, &[(1, "a", Surrogate(1101))], &[]);
        core.checkpoint_columnar_engines()
            .expect("checkpoint must publish");
        drop(core);

        // Overwrite the published manifest's bytes with garbage — a truncated /
        // corrupted frame, not a missing file.
        let manifest = dir
            .path()
            .join("columnar-ckpt")
            .join("core-0")
            .join(paths::COLUMNAR_CKPT_MANIFEST);
        assert!(manifest.exists(), "the checkpoint published a manifest");
        std::fs::write(&manifest, b"not a valid checkpoint frame").expect("corrupt manifest");

        let mut restored = open_core(dir.path());
        let err = restored
            .load_columnar_checkpoints()
            .expect_err("a corrupt manifest must fail the load, not silently skip it");
        let _ = err;
    }
}
