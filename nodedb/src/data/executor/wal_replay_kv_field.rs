// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `FieldSet` (HSET-style field merge) record.
//!
//! `wal_append_kv_op` logs `kv_field_set` as a delta record — the field
//! updates, not the post-merge document — because the Control Plane cannot
//! know the merged document before dispatch. Replay re-reads whatever value
//! is present in this core's KV engine at this point in LSN-ordered replay
//! and re-runs the exact same pure merge (`merge_field_updates`) the live
//! autocommit handler in `handlers/kv/field.rs` uses, so a staged value and
//! its durable replay never diverge. `merge_field_updates` builds a
//! `serde_json::Map`, which this workspace keeps `BTreeMap`-backed (the
//! `preserve_order` feature is not enabled), so re-encoding is deterministic
//! and byte-identical to the live write given the same inputs.

use tracing::warn;

use super::core_loop::CoreLoop;
use super::handlers::kv::field_compute::merge_field_updates;
use crate::data::executor::core_loop::write_index::KeyRepr;

impl CoreLoop {
    /// Decode + tombstone-gate + replay one `kv_field_set` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_field_set`
    /// discriminator shape (caller tries the next candidate arm), otherwise
    /// `Some(puts)` — `1` if the merge and write applied, `0` if tombstoned
    /// or the merge failed (a re-encode error against the previously-durable
    /// record, logged and skipped rather than fabricating a partial value).
    pub(super) fn try_replay_kv_field_set(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, updates, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<(String, Vec<u8>)>, u32)>(payload)
                .ok()?;
        if disc != "kv_field_set" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let current = self
            .kv_engine
            .get(database_id, tenant_id, &collection, &key, now_ms);
        let computed = match merge_field_updates(current.as_deref(), &updates) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(&key),
                    ?e,
                    "WAL kv_field_set replay: field merge failed, skipping record"
                );
                return Some(0);
            }
        };

        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id,
            tenant_id,
            collection: &collection,
            key: &key,
            value: &computed.new_value,
            ttl_ms: 0,
            now_ms,
            surrogate: nodedb_types::Surrogate::new(surrogate),
        });
        self.note_replay_write_lsn(
            database_id,
            tenant_id,
            &collection,
            Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
            record_lsn,
        );
        Some(1)
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
    /// durable record (`Some(lsn)`) before reading the records back. This is
    /// the load-bearing assertion that fails on the pre-fix code path where
    /// `kv_field_set` was written but never decoded by replay.
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
                "kv field_set autocommit writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    fn get_value(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<Vec<u8>> {
        let now_ms = crate::engine::kv::current_ms();
        core.kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, collection, key, now_ms)
    }

    fn json_field_bytes(value: serde_json::Value) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&value).expect("encode field value")
    }

    #[test]
    fn kv_field_set_merges_onto_existing_document_and_survives_replay() {
        let put_p1 = PhysicalPlan::Kv(KvOp::Put {
            collection: "players".into(),
            key: b"p1".to_vec(),
            value: nodedb_types::json_to_msgpack(&serde_json::json!({ "hp": 10 }))
                .expect("encode seed doc"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let field_set = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: "players".into(),
            key: b"p1".to_vec(),
            updates: vec![("mana".to_string(), json_field_bytes(serde_json::json!(5)))],
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_p1, field_set]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        // Compute the expected live merge independently, then assert replay
        // produces byte-identical bytes (not "some fields present").
        let seed = nodedb_types::json_to_msgpack(&serde_json::json!({ "hp": 10 }))
            .expect("encode seed doc");
        let expected = super::merge_field_updates(
            Some(&seed),
            &[("mana".to_string(), json_field_bytes(serde_json::json!(5)))],
        )
        .expect("live merge")
        .new_value;

        assert_eq!(
            get_value(&h.core, "players", b"p1"),
            Some(expected),
            "field_set merge onto an existing document must replay to the same bytes live produces"
        );
    }

    #[test]
    fn kv_field_set_onto_absent_key_creates_from_empty_object() {
        let field_set = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: "players".into(),
            key: b"fresh".to_vec(),
            updates: vec![("hp".to_string(), json_field_bytes(serde_json::json!(100)))],
            surrogate: Surrogate::new(3),
        });

        let records = append_via_autocommit(&[field_set]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let expected = super::merge_field_updates(
            None,
            &[("hp".to_string(), json_field_bytes(serde_json::json!(100)))],
        )
        .expect("live merge")
        .new_value;

        assert_eq!(
            get_value(&h.core, "players", b"fresh"),
            Some(expected),
            "field_set against an absent key must replay as a create from an empty object"
        );
    }

    #[test]
    fn kv_field_set_onto_non_object_value_replays_as_silently_treated_empty() {
        // Seed a value that is not a JSON object; live behavior silently
        // treats this as an empty object rather than erroring, and replay
        // must pin that exact behavior, not "improve" on it.
        let put_scalar = PhysicalPlan::Kv(KvOp::Put {
            collection: "players".into(),
            key: b"p2".to_vec(),
            value: json_field_bytes(serde_json::json!(42)),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let field_set = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: "players".into(),
            key: b"p2".to_vec(),
            updates: vec![("hp".to_string(), json_field_bytes(serde_json::json!(1)))],
            surrogate: Surrogate::new(2),
        });

        let records = append_via_autocommit(&[put_scalar, field_set]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let scalar_seed = json_field_bytes(serde_json::json!(42));
        let expected = super::merge_field_updates(
            Some(&scalar_seed),
            &[("hp".to_string(), json_field_bytes(serde_json::json!(1)))],
        )
        .expect("live merge treats non-object current value as empty")
        .new_value;

        assert_eq!(
            get_value(&h.core, "players", b"p2"),
            Some(expected),
            "field_set over a non-object current value must replay to the same \
             silently-treated-as-empty result live produces"
        );
    }

    #[test]
    fn kv_field_set_surrogate_survives_replay() {
        let field_set = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: "players".into(),
            key: b"p3".to_vec(),
            updates: vec![("hp".to_string(), json_field_bytes(serde_json::json!(7)))],
            surrogate: Surrogate::new(99),
        });

        let records = append_via_autocommit(&[field_set]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let now_ms = crate::engine::kv::current_ms();
        let (_, surrogate) = h
            .core
            .kv_engine
            .get_with_surrogate(DatabaseId::DEFAULT.as_u64(), TID, "players", b"p3", now_ms)
            .expect("surrogate recorded for replayed key");
        assert_eq!(
            surrogate,
            Surrogate::new(99),
            "the surrogate carried in the WAL record must survive replay, not Surrogate::ZERO"
        );
    }
}
