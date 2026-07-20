// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `RegisterSortedIndex` / `DropSortedIndex` records.
//!
//! `SortedIndexManager` (`engine/kv/sorted_index/manager.rs`) lives only
//! in-process — there is no durable catalog counterpart. This record is one of a
//! registered sorted index's (leaderboard's) two durable records; the other is
//! the KV checkpoint, which carries each collection's registrations and their
//! tree content in the same atomically published generation as its rows. That is
//! what makes gating this arm on the replay floor safe: a floor is only ever
//! installed by a generation that already holds every registration at or below
//! it, so a gated-out record is one the checkpoint has already accounted for.
//! See `kv_checkpoint`.
//!
//! Replay is LSN-ordered, so every
//! `kv_put` that preceded a `kv_register_sorted_index` record has already
//! been applied to `self.tables` by the time it replays, exactly mirroring
//! what the live registration saw when it ran (same backfill semantics as
//! `wal_replay_kv_index.rs`).
//!
//! `kv_register_sorted_index` carries `collection`, so it is tombstone-gated
//! like every other arm. `kv_drop_sorted_index` carries only `index_name` —
//! there is nothing to gate on directly. This is safe: if the owning
//! collection was tombstoned, the earlier `kv_register_sorted_index` record
//! for it was itself gated out during replay, so `sorted_indexes` never
//! gained the entry in the first place, and replaying the drop against a
//! nonexistent entry is a harmless no-op.

use tracing::warn;

use super::core_loop::CoreLoop;
use super::handlers::kv::sorted_index_compute::{
    BuildSortedIndexDefParams, build_sorted_index_def,
};

impl CoreLoop {
    /// Decode + tombstone-gate + replay one `kv_register_sorted_index` WAL
    /// record.
    ///
    /// Returns `None` when `payload` does not match the discriminator shape
    /// (caller tries the next candidate arm in `wal_replay/kv.rs`), otherwise
    /// `Some(count)` where `count` is the number of entries backfilled by
    /// `KvEngine::register_sorted_index` (`0` when tombstoned or the record
    /// is malformed).
    pub(super) fn try_replay_kv_register_sorted_index(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (
            disc,
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        ) = zerompk::from_msgpack::<(
            &str,
            String,
            String,
            Vec<(String, String)>,
            String,
            String,
            String,
            u64,
            u64,
        )>(payload)
        .ok()?;
        if disc != "kv_register_sorted_index" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let def = match build_sorted_index_def(BuildSortedIndexDefParams {
            collection: &collection,
            index_name: &index_name,
            sort_columns: &sort_columns,
            key_column: &key_column,
            window_type: &window_type,
            window_timestamp_column: &window_timestamp_column,
            window_start_ms,
            window_end_ms,
        }) {
            Ok(def) => def,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    index_name = %index_name,
                    ?e,
                    "WAL kv_register_sorted_index replay: malformed record, skipping"
                );
                return Some(0);
            }
        };

        let backfilled =
            self.kv_engine
                .register_sorted_index(database_id, tenant_id, &collection, def);
        Some(backfilled as usize)
    }

    /// Decode + replay one `kv_drop_sorted_index` WAL record.
    ///
    /// Returns `None` when `payload` does not match the discriminator shape,
    /// otherwise `Some(count)` — `1` if the index existed and was removed by
    /// `KvEngine::drop_sorted_index`, `0` if it did not exist. No tombstone
    /// gate: see the module-level doc comment for why the register arm's
    /// gate already covers this case.
    pub(super) fn try_replay_kv_drop_sorted_index(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
    ) -> Option<usize> {
        let (disc, index_name) = zerompk::from_msgpack::<(&str, String)>(payload).ok()?;
        if disc != "kv_drop_sorted_index" {
            return None;
        }

        let dropped = self
            .kv_engine
            .drop_sorted_index(database_id, tenant_id, &index_name);
        Some(usize::from(dropped))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::Surrogate;
    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;

    const TID: u64 = 1;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The tests drive replay directly and never tick the event loop.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// Append each plan through the production autocommit WAL path
    /// (`wal_append_if_write`), asserting every write plan produced a
    /// durable record, before reading the records back.
    fn append_via_autocommit(plans: &[PhysicalPlan]) -> (Vec<nodedb_wal::WalRecord>, Vec<u64>) {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let mut lsns = Vec::with_capacity(plans.len());
        for plan in plans {
            let outcome = wal_append_if_write(
                &wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
            .expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "kv sorted index writes must produce a durable WAL record"
            );
            lsns.push(outcome.lsn.expect("checked above").as_u64());
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");
        (records, lsns)
    }

    fn seed_put(collection: &str, key: &[u8], doc: serde_json::Value) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.into(),
            key: key.to_vec(),
            value: nodedb_types::json_to_msgpack(&doc).expect("encode seed doc"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        })
    }

    struct RegisterPlanArgs<'a> {
        collection: &'a str,
        index_name: &'a str,
        sort_columns: &'a [(&'a str, &'a str)],
        key_column: &'a str,
        window_type: &'a str,
        window_timestamp_column: &'a str,
        window_start_ms: u64,
        window_end_ms: u64,
    }

    fn register_plan(args: RegisterPlanArgs<'_>) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection: args.collection.into(),
            index_name: args.index_name.into(),
            sort_columns: args
                .sort_columns
                .iter()
                .map(|(n, d)| (n.to_string(), d.to_string()))
                .collect(),
            key_column: args.key_column.into(),
            window_type: args.window_type.into(),
            window_timestamp_column: args.window_timestamp_column.into(),
            window_start_ms: args.window_start_ms,
            window_end_ms: args.window_end_ms,
        })
    }

    #[test]
    fn register_sorted_index_backfills_and_replay_ranks_match_live() {
        let plans = &[
            seed_put(
                "players",
                b"alice",
                serde_json::json!({ "player_id": "alice", "score": 100 }),
            ),
            seed_put(
                "players",
                b"bob",
                serde_json::json!({ "player_id": "bob", "score": 300 }),
            ),
            seed_put(
                "players",
                b"charlie",
                serde_json::json!({ "player_id": "charlie", "score": 200 }),
            ),
            register_plan(RegisterPlanArgs {
                collection: "players",
                index_name: "lb_score",
                sort_columns: &[("score", "DESC")],
                key_column: "player_id",
                window_type: "",
                window_timestamp_column: "",
                window_start_ms: 0,
                window_end_ms: 0,
            }),
        ];
        let (records, _lsns) = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        // Same rank/top_k results the live registration + queries would have
        // produced (DESC by score: bob=1, charlie=2, alice=3).
        assert_eq!(
            h.core.kv_engine.sorted_index_rank(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "lb_score",
                b"bob",
                0
            ),
            Some(1)
        );
        assert_eq!(
            h.core.kv_engine.sorted_index_rank(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "lb_score",
                b"charlie",
                0
            ),
            Some(2)
        );
        assert_eq!(
            h.core.kv_engine.sorted_index_rank(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "lb_score",
                b"alice",
                0
            ),
            Some(3)
        );

        let top_k = h
            .core
            .kv_engine
            .sorted_index_top_k(DatabaseId::DEFAULT.as_u64(), TID, "lb_score", 3, 0)
            .expect("top_k must return entries after replay");
        assert_eq!(
            top_k,
            vec![
                (1, b"bob".to_vec()),
                (2, b"charlie".to_vec()),
                (3, b"alice".to_vec()),
            ],
            "top_k after replay must match what live registration + backfill would produce"
        );
    }

    #[test]
    fn custom_window_bounds_survive_replay_as_original_absolute_instants() {
        let plans = &[
            seed_put(
                "events",
                b"e1",
                serde_json::json!({ "ts": 1_700_000_500_000i64, "score": 10 }),
            ),
            register_plan(RegisterPlanArgs {
                collection: "events",
                index_name: "lb_custom",
                sort_columns: &[("ts", "ASC")],
                key_column: "id",
                window_type: "CUSTOM",
                window_timestamp_column: "ts",
                window_start_ms: 1_700_000_000_000,
                window_end_ms: 1_700_100_000_000,
            }),
        ];
        let (records, _lsns) = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let def = h
            .core
            .kv_engine
            .sorted_index_def(DatabaseId::DEFAULT.as_u64(), TID, "lb_custom")
            .expect("custom-window index must exist after replay");
        match def.window.window_type {
            crate::engine::kv::sorted_index::window::WindowType::Custom { start_ms, end_ms } => {
                assert_eq!(
                    start_ms, 1_700_000_000_000,
                    "CUSTOM window start must replay as the original absolute instant, not derived from now_ms"
                );
                assert_eq!(
                    end_ms, 1_700_100_000_000,
                    "CUSTOM window end must replay as the original absolute instant, not derived from now_ms"
                );
            }
            ref other => panic!("expected Custom window after replay, got {other:?}"),
        }
    }

    #[test]
    fn register_then_drop_leaves_no_entry_after_replay() {
        let plans = &[
            seed_put(
                "players",
                b"alice",
                serde_json::json!({ "player_id": "alice", "score": 100 }),
            ),
            register_plan(RegisterPlanArgs {
                collection: "players",
                index_name: "lb_score",
                sort_columns: &[("score", "DESC")],
                key_column: "player_id",
                window_type: "",
                window_timestamp_column: "",
                window_start_ms: 0,
                window_end_ms: 0,
            }),
            PhysicalPlan::Kv(KvOp::DropSortedIndex {
                index_name: "lb_score".into(),
            }),
        ];
        let (records, _lsns) = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert!(
            h.core
                .kv_engine
                .sorted_index_def(DatabaseId::DEFAULT.as_u64(), TID, "lb_score")
                .is_none(),
            "a dropped sorted index must not exist after replay"
        );
        assert!(
            h.core
                .kv_engine
                .sorted_index_rank(DatabaseId::DEFAULT.as_u64(), TID, "lb_score", b"alice", 0)
                .is_none(),
            "queries against a dropped-then-replayed sorted index must find nothing"
        );
    }

    #[test]
    fn tombstoned_collection_prevents_sorted_index_registration_on_replay() {
        let plans = &[
            seed_put(
                "players",
                b"alice",
                serde_json::json!({ "player_id": "alice", "score": 100 }),
            ),
            register_plan(RegisterPlanArgs {
                collection: "players",
                index_name: "lb_score",
                sort_columns: &[("score", "DESC")],
                key_column: "player_id",
                window_type: "",
                window_timestamp_column: "",
                window_start_ms: 0,
                window_end_ms: 0,
            }),
        ];
        let (records, lsns) = append_via_autocommit(plans);
        let register_lsn = lsns[1];

        // Simulate the collection having been tombstoned (e.g. truncated)
        // after the register record but before replay reads it: the
        // register record's LSN must be strictly less than the purge LSN
        // for `is_tombstoned` to gate it out.
        let mut tombstones = TombstoneSet::new();
        tombstones.insert(0, TID, "players".to_string(), register_lsn + 1);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &tombstones);

        assert!(
            h.core
                .kv_engine
                .sorted_index_def(DatabaseId::DEFAULT.as_u64(), TID, "lb_score")
                .is_none(),
            "a sorted index whose collection is tombstoned must never be registered on replay, \
             which is exactly why the drop arm needs no tombstone gate of its own"
        );
    }
}
