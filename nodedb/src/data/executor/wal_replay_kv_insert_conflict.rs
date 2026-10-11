// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `InsertOnConflictUpdate` (`INSERT ... ON CONFLICT
//! DO UPDATE`) delta record.
//!
//! `wal_append_kv_op` logs this as a DELTA record — the pre-merge incoming
//! (`EXCLUDED`) `value` and the `updates` assignment list, not the
//! post-merge document — because the Control Plane cannot know the merged
//! row before dispatch. Replay re-reads whatever value is present in this
//! core's KV engine at this point in LSN-ordered replay and re-runs the
//! exact same RMW merge (`merge_kv_conflict_body`) the live handler in
//! `handlers/kv/crud/write_upsert.rs` uses, so a staged value and its
//! durable replay never diverge. A key absent at replay time installs
//! `value` verbatim, matching the live handler's insert branch.
//!
//! The record has one shape, `("kv_insert_on_conflict_update", collection,
//! key, value, ttl_ms, updates, expire_at_ms, surrogate)`. `expire_at_ms` is
//! the Control-Plane-resolved absolute instant, `None` when the write has no
//! TTL. `surrogate` is the identity the live handler binds the row to, and
//! replay binds the same one.

use tracing::warn;

use super::core_loop::CoreLoop;
use super::handlers::kv::conflict_merge::merge_kv_conflict_body;
use crate::data::executor::core_loop::write_index::KeyRepr;
use nodedb_physical::physical_plan::UpdateValue;

/// Fields of a decoded `kv_insert_on_conflict_update` record, bundled so
/// [`CoreLoop::apply_replayed_insert_on_conflict_update`] stays under the
/// `too_many_arguments` clippy threshold (same convention as
/// `KvTransferFields` / `KvRegisterSortedIndexFields` in
/// `wal_dispatch_kv/encode.rs`).
struct ReplayedInsertOnConflictUpdate<'a> {
    database_id: u64,
    tenant_id: u64,
    now_ms: u64,
    record_lsn: u64,
    collection: &'a str,
    key: &'a [u8],
    value: &'a [u8],
    ttl_ms: u64,
    updates: &'a [(String, UpdateValue)],
    expire_at_ms: Option<u64>,
    surrogate: nodedb_types::Surrogate,
}

impl CoreLoop {
    /// Replay a `kv_insert_on_conflict_update` record. `None` when the payload
    /// is not one (the caller tries the next candidate arm in
    /// `wal_replay/kv.rs`).
    pub(super) fn try_replay_kv_insert_on_conflict_update(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, value, ttl_ms, updates, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<u8>,
                Vec<u8>,
                u64,
                Vec<(String, UpdateValue)>,
                Option<u64>,
                u32,
            )>(payload)
            .ok()?;
        // A record without the row's surrogate matches no shape; the KV pass
        // refuses it as unapplied.
        if disc != "kv_insert_on_conflict_update" || surrogate == 0 {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        Some(
            self.apply_replayed_insert_on_conflict_update(ReplayedInsertOnConflictUpdate {
                database_id,
                tenant_id,
                now_ms,
                record_lsn,
                collection: &collection,
                key: &key,
                value: &value,
                ttl_ms,
                updates: &updates,
                expire_at_ms,
                surrogate: nodedb_types::Surrogate::new(surrogate),
            }),
        )
    }

    /// RMW + write-back through `merge_kv_conflict_body`, the exact post-image
    /// the live handler stores: the incoming `value` for an absent key, the
    /// merge for a present one. A
    /// merge failure is logged and the record is skipped rather than
    /// fabricating a value.
    fn apply_replayed_insert_on_conflict_update(
        &mut self,
        f: ReplayedInsertOnConflictUpdate<'_>,
    ) -> usize {
        let ReplayedInsertOnConflictUpdate {
            database_id,
            tenant_id,
            now_ms,
            record_lsn,
            collection,
            key,
            value,
            ttl_ms,
            updates,
            expire_at_ms,
            surrogate,
        } = f;
        let existing_bytes = self
            .kv_engine
            .get(database_id, tenant_id, collection, key, now_ms);

        let stored_bytes = match merge_kv_conflict_body(
            existing_bytes.as_deref(),
            value,
            updates,
            self.declared_columns_of(database_id, tenant_id, collection),
        ) {
            Ok(b) => b,
            Err(e) => {
                // A division/modulo-by-zero here can only come from a
                // record logged by a build that did not fail the
                // statement at execution time; a shape or decode error
                // means the durable bytes no longer hold what the record
                // expects. Either way the record is skipped, never
                // fabricated, and startup continues.
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(key),
                    ?e,
                    "WAL kv_insert_on_conflict_update replay: merge failed, skipping record"
                );
                return 0;
            }
        };

        let params = crate::engine::kv::KvPutParams {
            database_id,
            tenant_id,
            collection,
            key,
            value: &stored_bytes,
            ttl_ms,
            now_ms,
            surrogate,
        };
        let written = match expire_at_ms {
            Some(expire_at_ms) => self
                .kv_engine
                .put_with_absolute_expiry(params, expire_at_ms),
            None => self.kv_engine.put(params),
        };
        if let Err(e) = written {
            self.replay_record_unapplied(
                "kv",
                "insert_on_conflict_identity",
                record_lsn,
                &e.to_string(),
            );
            return 0;
        }
        self.note_replay_write(
            database_id,
            tenant_id,
            collection,
            Some(KeyRepr::KvKey(Box::from(key))),
            record_lsn,
        );
        1
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::{KvOp, UpdateValue};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, Value};
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
            crate::data::executor::core_loop::test_governor(),
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
    /// the load-bearing assertion that fails on the pre-fix code path, where
    /// `InsertOnConflictUpdate` was WAL-logged via the generic `kv_put`
    /// encoder and `updates` was silently discarded.
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
                "kv insert-on-conflict-update autocommit writes must produce a durable WAL record"
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

    fn obj_bytes(fields: &[(&str, i64)]) -> Vec<u8> {
        let map = fields
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Integer(*v)))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode value")
    }

    #[test]
    fn insert_on_conflict_update_merges_onto_existing_key_and_survives_replay() {
        let seed = obj_bytes(&[("hp", 10)]);
        let excluded = obj_bytes(&[("hp", 1)]);
        let put_p1 = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            key: b"p1".to_vec(),
            value: seed.clone(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        let updates = vec![(
            "mana".to_string(),
            UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&Value::Integer(5)).expect("encode literal"),
            ),
        )];
        let upsert = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            key: b"p1".to_vec(),
            value: excluded.clone(),
            ttl_ms: 0,
            updates: updates.clone(),
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });

        let records = append_via_autocommit(&[put_p1, upsert]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        // Compute the expected live merge independently. `Value::Object` is
        // HashMap-backed, so msgpack key order is per-instance nondeterministic
        // (the live DP path and this replay build separate maps) — compare the
        // decoded logical value, not raw bytes.
        let existing_val = nodedb_types::value_from_msgpack(&seed).expect("decode seed");
        let excluded_val = nodedb_types::value_from_msgpack(&excluded).expect("decode excluded");
        let expected = crate::data::executor::handlers::upsert::apply_on_conflict_updates(
            existing_val,
            &excluded_val,
            &updates,
        )
        .unwrap();

        let stored = get_value(&h.core, "players", b"p1").expect("value present after replay");
        let stored_val = nodedb_types::value_from_msgpack(&stored).expect("decode stored value");
        assert_eq!(
            stored_val, expected,
            "insert-on-conflict-update onto an existing key must replay to the same value \
             live apply_on_conflict_updates produces, not the pre-merge excluded value"
        );
    }

    /// The live write binds the row to its surrogate, and restart replay
    /// binds the same one, on both the insert and the merge branch.
    #[test]
    fn insert_on_conflict_update_identity_survives_replay() {
        let updates = vec![(
            "mana".to_string(),
            UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&Value::Integer(5)).expect("encode literal"),
            ),
        )];
        let upsert = |key: &[u8], surrogate: u32| {
            PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
                key: key.to_vec(),
                value: obj_bytes(&[("hp", 1)]),
                ttl_ms: 0,
                updates: updates.clone(),
                surrogate: Surrogate::new(surrogate),
                rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: Vec::new(),
            })
        };
        // `fresh` takes the insert branch. `p1` is written twice, so its
        // second record takes the merge branch.
        let records =
            append_via_autocommit(&[upsert(b"fresh", 41), upsert(b"p1", 42), upsert(b"p1", 42)]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let did = DatabaseId::DEFAULT.as_u64();
        for (key, surrogate) in [(b"fresh".as_slice(), 41), (b"p1".as_slice(), 42)] {
            assert_eq!(
                h.core
                    .kv_engine
                    .key_for_surrogate(did, TID, "players", Surrogate::new(surrogate)),
                Some(key.to_vec()),
                "the replayed row must resolve through the surrogate the live write bound"
            );
        }
    }

    #[test]
    fn insert_on_conflict_update_onto_absent_key_installs_value_verbatim() {
        let excluded = obj_bytes(&[("hp", 100)]);
        let updates = vec![(
            "mana".to_string(),
            UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&Value::Integer(5)).expect("encode literal"),
            ),
        )];
        let upsert = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "players"),
            key: b"fresh".to_vec(),
            value: excluded.clone(),
            ttl_ms: 0,
            updates,
            surrogate: Surrogate::new(3),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });

        let records = append_via_autocommit(&[upsert]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_value(&h.core, "players", b"fresh"),
            Some(excluded),
            "insert-on-conflict-update against an absent key must install the incoming value \
             verbatim, matching the live handler's insert branch"
        );
    }

    #[test]
    fn insert_on_conflict_update_with_ttl_survives_replay_with_recorded_expiry() {
        use crate::control::server::wal_dispatch_kv::encode::encode_kv_insert_on_conflict_update;

        let seed = obj_bytes(&[("hp", 10)]);
        let put_p1 = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            key: b"s1".to_vec(),
            value: seed.clone(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });

        let excluded = obj_bytes(&[("hp", 1)]);
        let updates = vec![(
            "mana".to_string(),
            UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&Value::Integer(5)).expect("encode literal"),
            ),
        )];
        // Encode a record with an explicit absolute instant directly, so the
        // test pins "replay installs the recorded instant verbatim" rather
        // than "replay recomputes now_ms + ttl_ms" (which would drift).
        let entry = encode_kv_insert_on_conflict_update(
            "sessions",
            b"s1",
            &excluded,
            5_000,
            &updates,
            Some(6_000),
            1,
        )
        .expect("encode kv_insert_on_conflict_update with absolute expiry");

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &put_p1,
        )
        .expect("wal append seed put");
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &entry,
            )
            .expect("append raw kv_insert_on_conflict_update record");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        // Value::Object is HashMap-backed → compare decoded logical value, not
        // raw bytes (per-instance key ordering differs between live and replay).
        let existing_val = nodedb_types::value_from_msgpack(&seed).expect("decode seed");
        let excluded_val = nodedb_types::value_from_msgpack(&excluded).expect("decode excluded");
        let expected = crate::data::executor::handlers::upsert::apply_on_conflict_updates(
            existing_val,
            &excluded_val,
            &updates,
        )
        .unwrap();
        // Read at now_ms=0: this record installs an absolute expiry of 6_000,
        // which is already in the past on the wall clock `get_value` uses, so
        // read before expiry to assert the merged value landed.
        let stored = h
            .core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, "sessions", b"s1", 0)
            .expect("value present after replay");
        let stored_val = nodedb_types::value_from_msgpack(&stored).expect("decode stored value");
        assert_eq!(stored_val, expected);
        let ttl =
            h.core
                .kv_engine
                .get_ttl_ms(DatabaseId::DEFAULT.as_u64(), TID, "sessions", b"s1", 0);
        assert_eq!(
            ttl,
            Some(6_000),
            "replay must install the recorded absolute expiry verbatim (expire_at_ms - \
             now_ms(0) == 6000), not recompute now_ms + ttl_ms at replay time"
        );
    }

    /// Replay a raw-body seed `Put` followed by an `ON CONFLICT DO UPDATE SET
    /// value = EXCLUDED.value` and return the stored bytes.
    fn replay_raw_overwrite(seed: &[u8], incoming: &[u8]) -> Vec<u8> {
        let put = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "raw"),
            key: b"k".to_vec(),
            value: seed.to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });
        let upsert = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "raw"),
            key: b"k".to_vec(),
            value: incoming.to_vec(),
            ttl_ms: 0,
            updates: vec![(
                "value".to_string(),
                UpdateValue::Expr(nodedb_query::SqlExpr::ExcludedColumn("value".to_string())),
            )],
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let records = append_via_autocommit(&[put, upsert]);
        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());
        get_value(&h.core, "raw", b"k").expect("value present after replay")
    }

    #[test]
    fn raw_body_on_conflict_update_replays_as_raw_bytes() {
        // The stored body is raw scalar bytes, not msgpack. Replay must
        // merge it as `{"value": "first"}` and write the raw result back,
        // never a msgpack map and never a decode-failure skip.
        assert_eq!(
            replay_raw_overwrite(b"first", b"second-longer-value"),
            b"second-longer-value".to_vec()
        );
    }

    #[test]
    fn raw_single_byte_body_on_conflict_update_keeps_its_shape() {
        // 0x31 is a valid msgpack fixint; the body must still be read as
        // the string "1" and written back as one raw byte.
        assert_eq!(replay_raw_overwrite(b"1", b"2"), b"2".to_vec());
    }

    #[test]
    fn insert_on_conflict_update_decode_failure_is_skipped_not_panicking() {
        // A payload with a mismatched discriminator (and thus a shape no
        // arm decodes as `kv_insert_on_conflict_update`) must be reported as
        // `None` up the try-arm chain, never panic, so `replay_kv_wal` moves
        // on to the next candidate arm / record.
        let bogus = zerompk::to_msgpack_vec(&("kv_put", "players", b"p1", b"v1", 0u64))
            .expect("encode bogus payload");

        let mut h = make_core();
        let result = h.core.try_replay_kv_insert_on_conflict_update(
            &bogus,
            TID,
            DatabaseId::DEFAULT.as_u64(),
            0,
            1,
            &TombstoneSet::new(),
        );
        assert_eq!(
            result, None,
            "a non-kv_insert_on_conflict_update-shaped payload must return None, not panic \
             or fabricate a value"
        );
    }
}
