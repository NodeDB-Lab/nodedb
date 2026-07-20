// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `InsertOnConflictUpdate` (`INSERT ... ON CONFLICT
//! DO UPDATE`) delta record.
//!
//! `wal_append_kv_op` logs this as a DELTA record — the pre-merge incoming
//! (`EXCLUDED`) `value` and the `updates` assignment list, not the
//! post-merge document — because the Control Plane cannot know the merged
//! row before dispatch. Replay re-reads whatever value is present in this
//! core's KV engine at this point in LSN-ordered replay and re-runs the
//! exact same RMW merge (`apply_on_conflict_updates`) the live handler in
//! `handlers/kv/crud/write_upsert.rs` uses, so a staged value and its
//! durable replay never diverge. A key absent at replay time installs
//! `value` verbatim, matching the live handler's insert branch.
//!
//! `kv_insert_on_conflict_update` optionally carries a Control-Plane-resolved
//! absolute `expire_at_ms` as a trailing seventh element, present only when
//! the live write's `ttl_ms > 0` (same additive-shape convention as
//! `encode_kv_put` / `encode_kv_incr`). Both shapes are genuinely produced
//! in production, so both must be decoded; the seven-element shape is tried
//! first — zerompk's strict array-length check means it can never match the
//! six-element tuple, so try-order does not matter for correctness, but
//! skipping it would silently drop the recorded absolute instant.
//!
//! Like the rest of the `Put` family, this local WAL record does not carry
//! the surrogate (it lives in the separately-durable surrogate catalog), so
//! replay passes `Surrogate::ZERO`, matching `kv_put` / `kv_batch_put` replay.

use tracing::warn;

use super::core_loop::CoreLoop;
use super::handlers::upsert::apply_on_conflict_updates;
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
}

impl CoreLoop {
    /// Try both `kv_insert_on_conflict_update` WAL payload shapes in turn,
    /// seven-element (absolute expiry) before six-element (no TTL). Returns
    /// `None` when neither decodes (caller tries the next candidate arm in
    /// `wal_replay/kv.rs`), otherwise `Some(puts)` from whichever shape
    /// decoded.
    pub(super) fn try_replay_kv_insert_on_conflict_update(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        if let Some(applied) = self.try_replay_kv_insert_on_conflict_update_with_expiry(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        ) {
            return Some(applied);
        }
        self.try_replay_kv_insert_on_conflict_update_no_expiry(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        )
    }

    /// Seven-element shape: `("kv_insert_on_conflict_update", collection,
    /// key, value, ttl_ms, updates, expire_at_ms)` — recorded only when the
    /// live write's `ttl_ms > 0`.
    fn try_replay_kv_insert_on_conflict_update_with_expiry(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, value, ttl_ms, updates, expire_at_ms) =
            zerompk::from_msgpack::<(
                &str,
                String,
                Vec<u8>,
                Vec<u8>,
                u64,
                Vec<(String, UpdateValue)>,
                u64,
            )>(payload)
            .ok()?;
        if disc != "kv_insert_on_conflict_update" {
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
                expire_at_ms: Some(expire_at_ms),
            }),
        )
    }

    /// Six-element shape: `("kv_insert_on_conflict_update", collection, key,
    /// value, ttl_ms, updates)` — recorded when the live write's `ttl_ms ==
    /// 0` (no TTL to carry an absolute instant for).
    fn try_replay_kv_insert_on_conflict_update_no_expiry(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, value, ttl_ms, updates) = zerompk::from_msgpack::<(
            &str,
            String,
            Vec<u8>,
            Vec<u8>,
            u64,
            Vec<(String, UpdateValue)>,
        )>(payload)
        .ok()?;
        if disc != "kv_insert_on_conflict_update" {
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
                expire_at_ms: None,
            }),
        )
    }

    /// Shared RMW + write-back for both shapes: absent key installs `value`
    /// verbatim (the live handler's insert branch); present key decodes the
    /// existing + incoming (`EXCLUDED`) rows and re-runs
    /// `apply_on_conflict_updates`, the exact merge the live handler uses.
    /// Any decode/encode failure is logged and the record is skipped rather
    /// than fabricating a value — a mismatch here means the previously
    /// durable bytes are no longer decodable, not a computation the live
    /// path would have failed identically.
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
        } = f;
        let existing_bytes = self
            .kv_engine
            .get(database_id, tenant_id, collection, key, now_ms);

        let stored_bytes: Vec<u8> = match &existing_bytes {
            None => value.to_vec(),
            Some(existing_raw) => {
                let existing_val = match nodedb_types::value_from_msgpack(existing_raw) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            core = self.core_id,
                            collection = %collection,
                            key = %String::from_utf8_lossy(key),
                            ?e,
                            "WAL kv_insert_on_conflict_update replay: failed to decode existing \
                             value, skipping record"
                        );
                        return 0;
                    }
                };
                let excluded_val = match nodedb_types::value_from_msgpack(value) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            core = self.core_id,
                            collection = %collection,
                            key = %String::from_utf8_lossy(key),
                            ?e,
                            "WAL kv_insert_on_conflict_update replay: failed to decode incoming \
                             value, skipping record"
                        );
                        return 0;
                    }
                };
                let merged = apply_on_conflict_updates(existing_val, &excluded_val, updates);
                match nodedb_types::value_to_msgpack(&merged) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(
                            core = self.core_id,
                            collection = %collection,
                            key = %String::from_utf8_lossy(key),
                            ?e,
                            "WAL kv_insert_on_conflict_update replay: failed to encode merged \
                             value, skipping record"
                        );
                        return 0;
                    }
                }
            }
        };

        match expire_at_ms {
            Some(expire_at_ms) => {
                self.kv_engine.put_with_absolute_expiry(
                    crate::engine::kv::KvPutParams {
                        database_id,
                        tenant_id,
                        collection,
                        key,
                        value: &stored_bytes,
                        ttl_ms,
                        now_ms,
                        surrogate: nodedb_types::Surrogate::ZERO,
                    },
                    expire_at_ms,
                );
            }
            None => {
                self.kv_engine.put(crate::engine::kv::KvPutParams {
                    database_id,
                    tenant_id,
                    collection,
                    key,
                    value: &stored_bytes,
                    ttl_ms,
                    now_ms,
                    surrogate: nodedb_types::Surrogate::ZERO,
                });
            }
        }
        self.note_replay_write_lsn(
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
    use nodedb_types::{Surrogate, Value};
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
            collection: "players".into(),
            key: b"p1".to_vec(),
            value: seed.clone(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let updates = vec![(
            "mana".to_string(),
            UpdateValue::Literal(
                nodedb_types::value_to_msgpack(&Value::Integer(5)).expect("encode literal"),
            ),
        )];
        let upsert = PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection: "players".into(),
            key: b"p1".to_vec(),
            value: excluded.clone(),
            ttl_ms: 0,
            updates: updates.clone(),
            surrogate: Surrogate::new(1),
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
        let expected = super::apply_on_conflict_updates(existing_val, &excluded_val, &updates);

        let stored = get_value(&h.core, "players", b"p1").expect("value present after replay");
        let stored_val = nodedb_types::value_from_msgpack(&stored).expect("decode stored value");
        assert_eq!(
            stored_val, expected,
            "insert-on-conflict-update onto an existing key must replay to the same value \
             live apply_on_conflict_updates produces, not the pre-merge excluded value"
        );
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
            collection: "players".into(),
            key: b"fresh".to_vec(),
            value: excluded.clone(),
            ttl_ms: 0,
            updates,
            surrogate: Surrogate::new(3),
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
            collection: "sessions".into(),
            key: b"s1".to_vec(),
            value: seed.clone(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
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
        wal.append_put(
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
        let expected = super::apply_on_conflict_updates(existing_val, &excluded_val, &updates);
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
