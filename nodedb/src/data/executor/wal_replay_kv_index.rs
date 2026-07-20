// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `RegisterIndex` / `DropIndex` secondary-index
//! records.
//!
//! Secondary index state (`KvEngine::indexes`) lives only in-process — there is
//! no durable catalog counterpart. This record is one of its two durable
//! records; the other is the KV checkpoint, which carries each collection's
//! registrations and their content in the same atomically published generation
//! as its rows. That is what makes gating this arm on the replay floor safe: a
//! floor is only ever installed by a generation that already holds every
//! registration at or below it, so a gated-out record is one the checkpoint has
//! already accounted for. See `kv_checkpoint`.
//!
//! Replay is
//! LSN-ordered, so every `kv_put` that preceded a `RegisterIndex` record has
//! already been applied to `self.tables` by the time it replays, exactly
//! mirroring what the live registration saw when it ran.
//!
//! `backfill` is encoded in the record (see `encode_kv_register_index`)
//! precisely because it cannot be inferred at replay time: it toggles
//! whether the live call scanned and populated the index from existing
//! rows, and a replay arm that guessed either way would diverge from
//! whichever choice the user actually made.

use super::core_loop::CoreLoop;
use crate::engine::kv::RegisterIndexParams;

impl CoreLoop {
    /// Try the `kv_register_index` / `kv_drop_index` record shapes in turn
    /// against one WAL payload. Returns `None` only when neither
    /// discriminator matches (caller tries the next candidate arm in
    /// `wal_replay/kv.rs`), otherwise `Some(puts)` from whichever arm decoded.
    pub(super) fn try_replay_kv_index(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        if let Some(applied) = self.try_replay_kv_register_index(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        ) {
            return Some(applied);
        }
        self.try_replay_kv_drop_index(payload, tenant_id, database_id, record_lsn, tombstones)
    }

    /// Decode + tombstone-gate + replay one `kv_register_index` WAL record.
    ///
    /// Returns `None` when `payload` does not match the discriminator shape,
    /// otherwise `Some(count)` where `count` is the number of index entries
    /// created by `KvEngine::register_index` (`0` when tombstoned, when the
    /// field was already indexed, or when `backfill` is `false`).
    fn try_replay_kv_register_index(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, field, field_position, backfill) =
            zerompk::from_msgpack::<(&str, String, String, usize, bool)>(payload).ok()?;
        if disc != "kv_register_index" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let backfilled = self.kv_engine.register_index(RegisterIndexParams {
            database_id,
            tenant_id,
            collection: &collection,
            field: &field,
            field_position,
            backfill,
            now_ms,
        });
        Some(backfilled)
    }

    /// Decode + tombstone-gate + replay one `kv_drop_index` WAL record.
    ///
    /// Returns `None` when `payload` does not match the discriminator shape,
    /// otherwise `Some(count)` where `count` is the number of index entries
    /// removed by `KvEngine::drop_index` (`0` when tombstoned or the field
    /// was not indexed).
    fn try_replay_kv_drop_index(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, field) =
            zerompk::from_msgpack::<(&str, String, String)>(payload).ok()?;
        if disc != "kv_drop_index" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let dropped = self
            .kv_engine
            .drop_index(database_id, tenant_id, &collection, &field);
        Some(dropped)
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
    fn append_via_autocommit(plans: &[PhysicalPlan]) -> Vec<nodedb_wal::WalRecord> {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
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
                "kv index writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    fn seed_put(collection: &str, key: &[u8], name: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: collection.into(),
            key: key.to_vec(),
            value: nodedb_types::json_to_msgpack(&serde_json::json!({ "name": name }))
                .expect("encode seed doc"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        })
    }

    fn lookup_by_name(core: &CoreLoop, collection: &str, name: &str) -> Vec<Vec<u8>> {
        // The index stores string field values as raw UTF-8 bytes (see
        // `json_value_to_index_bytes` in `engine_helpers.rs`), not
        // msgpack-encoded values.
        core.kv_engine.index_lookup_eq(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            collection,
            "name",
            name.as_bytes(),
        )
    }

    #[test]
    fn register_index_with_backfill_true_indexes_pre_existing_rows_after_replay() {
        let plans = &[
            seed_put("players", b"p1", "alice"),
            seed_put("players", b"p2", "bob"),
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: "players".into(),
                field: "name".into(),
                field_position: 0,
                backfill: true,
            }),
        ];
        let records = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            lookup_by_name(&h.core, "players", "alice"),
            vec![b"p1".to_vec()],
            "backfill=true must index rows that existed before registration"
        );
        assert_eq!(
            lookup_by_name(&h.core, "players", "bob"),
            vec![b"p2".to_vec()],
            "backfill=true must index rows that existed before registration"
        );
    }

    #[test]
    fn register_index_with_backfill_false_does_not_index_pre_existing_rows_after_replay() {
        let plans = &[
            seed_put("players", b"p1", "alice"),
            seed_put("players", b"p2", "bob"),
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: "players".into(),
                field: "name".into(),
                field_position: 0,
                backfill: false,
            }),
        ];
        let records = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert!(
            lookup_by_name(&h.core, "players", "alice").is_empty(),
            "backfill=false must NOT index rows that existed before registration"
        );
        assert!(
            lookup_by_name(&h.core, "players", "bob").is_empty(),
            "backfill=false must NOT index rows that existed before registration"
        );
    }

    #[test]
    fn register_index_with_backfill_false_still_indexes_rows_written_after_registration() {
        let plans = &[
            seed_put("players", b"p1", "alice"),
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: "players".into(),
                field: "name".into(),
                field_position: 0,
                backfill: false,
            }),
            seed_put("players", b"p2", "bob"),
        ];
        let records = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert!(
            lookup_by_name(&h.core, "players", "alice").is_empty(),
            "pre-registration row must remain unindexed"
        );
        assert_eq!(
            lookup_by_name(&h.core, "players", "bob"),
            vec![b"p2".to_vec()],
            "a row written after backfill=false registration must be indexed live, \
             proving the index is not merely absent"
        );
    }

    #[test]
    fn drop_index_removes_index_after_replay() {
        let plans = &[
            seed_put("players", b"p1", "alice"),
            PhysicalPlan::Kv(KvOp::RegisterIndex {
                collection: "players".into(),
                field: "name".into(),
                field_position: 0,
                backfill: true,
            }),
            PhysicalPlan::Kv(KvOp::DropIndex {
                collection: "players".into(),
                field: "name".into(),
            }),
        ];
        let records = append_via_autocommit(plans);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert!(
            !h.core
                .kv_engine
                .has_indexes(DatabaseId::DEFAULT.as_u64(), TID, "players"),
            "drop_index must remove the index after replay"
        );
        assert!(
            lookup_by_name(&h.core, "players", "alice").is_empty(),
            "a dropped index must not return lookups after replay"
        );
    }
}
