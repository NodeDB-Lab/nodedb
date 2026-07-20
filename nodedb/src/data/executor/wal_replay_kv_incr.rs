// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `Incr` (integer increment) delta record.
//!
//! `wal_append_kv_op` appends the `kv_incr` record BEFORE dispatch, so an
//! `Incr` that failed live (type mismatch / overflow) still has a durable
//! record. Replay re-runs the same computation against whatever value is
//! present in this core's KV engine at this point in LSN-ordered replay — a
//! live-failed increment replays to the same no-op, converging rather than
//! diverging, so no success-gate is applied here (same rationale as
//! `kv_incr_float` in `wal_replay_kv_atomic.rs`).
//!
//! `kv_incr` optionally carries a Control-Plane-resolved absolute
//! `expire_at_ms` as a trailing seventh element, present only when the live
//! write's `ttl_ms > 0` (see `encode_kv_incr`'s doc comment — `ttl_ms == 0`
//! means "preserve whatever TTL the key already had", which has no instant
//! to carry). Both shapes are genuinely produced in production, so both must
//! be decoded; the seven-element shape is tried first because zerompk's
//! strict array-length check means it can never match the six-element
//! tuple, but skipping it would silently drop the recorded absolute instant.
//! When present, replay installs it verbatim via
//! `KvEngine::incr_with_absolute_expiry` instead of recomputing
//! `now_ms + ttl_ms`, which would drift the expiry forward by the
//! crash-to-restart delay.
//!
//! Unlike the `Put` family, `kv_incr` carries its own surrogate in the
//! record rather than relying on the separately-durable surrogate catalog,
//! so replay reconstructs it from the payload's `u32` instead of using
//! `Surrogate::ZERO`.

use tracing::warn;

use super::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::engine::kv::{AtomicError, AtomicKeyCtx};

impl CoreLoop {
    /// Try both `kv_incr` WAL payload shapes in turn, seven-element
    /// (absolute expiry) before six-element (preserve). Returns `None` when
    /// neither decodes (caller tries the next candidate arm in
    /// `wal_replay/kv.rs`), otherwise `Some(puts)` from whichever shape
    /// decoded.
    pub(super) fn try_replay_kv_incr(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        if let Some(applied) = self.try_replay_kv_incr_with_expiry(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        ) {
            return Some(applied);
        }
        self.try_replay_kv_incr_preserve(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        )
    }

    /// Seven-element shape: `("kv_incr", collection, key, delta, ttl_ms,
    /// surrogate, expire_at_ms)` — recorded only when the live write's
    /// `ttl_ms > 0`.
    ///
    /// Returns `None` when `payload` does not match this shape, otherwise
    /// `Some(puts)` — `1` if the increment applied, `0` if tombstoned or the
    /// current value was not numeric (a type-mismatch or overflow replays to
    /// the same no-op the live dispatch produced).
    fn try_replay_kv_incr_with_expiry(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, delta, ttl_ms, surrogate, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, i64, u64, u32, u64)>(payload).ok()?;
        if disc != "kv_incr" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        let result = self.kv_engine.incr_with_absolute_expiry(
            AtomicKeyCtx {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                now_ms,
                surrogate: nodedb_types::Surrogate::new(surrogate),
            },
            delta,
            ttl_ms,
            expire_at_ms,
        );
        let applied = self.log_kv_incr_result(&collection, &key, delta, result);
        if applied > 0 {
            self.note_replay_write_lsn(
                database_id,
                tenant_id,
                &collection,
                Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                record_lsn,
            );
        }
        Some(applied)
    }

    /// Six-element shape: `("kv_incr", collection, key, delta, ttl_ms,
    /// surrogate)` — recorded when the live write's `ttl_ms == 0` (preserve
    /// whatever TTL the key already had; no absolute instant to carry).
    ///
    /// Returns `None` when `payload` does not match this shape, otherwise
    /// `Some(puts)` — `1` if the increment applied, `0` if tombstoned or the
    /// current value was not numeric.
    fn try_replay_kv_incr_preserve(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, delta, ttl_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, i64, u64, u32)>(payload).ok()?;
        if disc != "kv_incr" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        let result = self.kv_engine.incr(
            AtomicKeyCtx {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                now_ms,
                surrogate: nodedb_types::Surrogate::new(surrogate),
            },
            delta,
            ttl_ms,
        );
        let applied = self.log_kv_incr_result(&collection, &key, delta, result);
        if applied > 0 {
            self.note_replay_write_lsn(
                database_id,
                tenant_id,
                &collection,
                Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                record_lsn,
            );
        }
        Some(applied)
    }

    /// Shared result handling for both `kv_incr` shapes: `Ok` counts as one
    /// applied put; `TypeMismatch` / `Overflow` / `Encode` are
    /// correctly-converging no-ops (the live dispatch would have failed
    /// identically), logged and skipped rather than treated as errors.
    fn log_kv_incr_result(
        &self,
        collection: &str,
        key: &[u8],
        delta: i64,
        result: Result<i64, AtomicError>,
    ) -> usize {
        match result {
            Ok(_) => 1,
            Err(AtomicError::TypeMismatch { detail }) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(key),
                    delta,
                    %detail,
                    "WAL kv_incr replay: type mismatch, skipping record"
                );
                0
            }
            Err(AtomicError::Overflow) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(key),
                    delta,
                    "WAL kv_incr replay: overflow, skipping record"
                );
                0
            }
            Err(AtomicError::Encode { detail }) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(key),
                    delta,
                    %detail,
                    "WAL kv_incr replay: re-encode failed, skipping record"
                );
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::control::server::wal_dispatch_kv::encode::encode_kv_incr;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::Surrogate;
    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;

    const TID: u64 = 1;

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
                "kv incr autocommit writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    fn get_i64(core: &CoreLoop, collection: &str, key: &[u8]) -> i64 {
        let now_ms = crate::engine::kv::current_ms();
        let bytes = core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, collection, key, now_ms)
            .expect("value present");
        zerompk::from_msgpack::<i64>(&bytes).expect("decode i64")
    }

    fn ttl_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<i64> {
        core.kv_engine
            .get_ttl_ms(DatabaseId::DEFAULT.as_u64(), TID, collection, key, 0)
    }

    #[test]
    fn kv_incr_survives_wal_replay_from_empty() {
        let put_p = PhysicalPlan::Kv(KvOp::Put {
            collection: "counters".into(),
            key: b"hits".to_vec(),
            value: zerompk::to_msgpack_vec(&5i64).expect("encode"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"hits".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_p, incr]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_i64(&h.core, "counters", b"hits"),
            8,
            "incr must replay against the seeded put, not be dropped (pre-fix value: 5)"
        );
    }

    #[test]
    fn kv_incr_replayed_twice_does_not_double_count() {
        let put_p = PhysicalPlan::Kv(KvOp::Put {
            collection: "counters".into(),
            key: b"hits".to_vec(),
            value: zerompk::to_msgpack_vec(&5i64).expect("encode"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let incr1 = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"hits".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let incr2 = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"hits".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_p, incr1, incr2]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_i64(&h.core, "counters", b"hits"),
            11,
            "each incr record must apply exactly once (5 + 3 + 3 = 11, not 14 or 8)"
        );
    }

    #[test]
    fn kv_incr_with_zero_ttl_preserves_existing_expiry() {
        let put_p = PhysicalPlan::Kv(KvOp::Put {
            collection: "counters".into(),
            key: b"temp".to_vec(),
            value: zerompk::to_msgpack_vec(&5i64).expect("encode"),
            ttl_ms: 60_000,
            surrogate: Surrogate::new(1),
        });
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"temp".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_p, incr]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(get_i64(&h.core, "counters", b"temp"), 8);
        let ttl = ttl_ms(&h.core, "counters", b"temp");
        assert!(
            ttl.is_some() && ttl.unwrap() > 0,
            "incr with ttl_ms == 0 must preserve the expiry set by the seeding put, not clear it"
        );
    }

    #[test]
    fn kv_incr_replay_installs_recorded_absolute_expiry_not_replay_time_clock() {
        // Encode a record whose absolute instant is 1000 with ttl_ms = 5000:
        // a drifting implementation that recomputed `current_ms() + ttl_ms`
        // at replay time would install a value far larger than 6000, since
        // real wall-clock time is vastly greater than 1000.
        let put_seed = PhysicalPlan::Kv(KvOp::Put {
            collection: "counters".into(),
            key: b"daily".to_vec(),
            value: zerompk::to_msgpack_vec(&0i64).expect("encode"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let entry = encode_kv_incr("counters", b"daily", 1, 5_000, 1, Some(6_000))
            .expect("encode kv_incr with absolute expiry");

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &put_seed,
        )
        .expect("wal append seed put");
        wal.append_put(
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &entry,
        )
        .expect("append raw kv_incr record");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            ttl_ms(&h.core, "counters", b"daily"),
            Some(6_000),
            "replay must install the recorded absolute expiry verbatim (expire_at_ms - \
             now_ms(0) == 6000), not recompute now_ms + ttl_ms at replay time"
        );
    }

    #[test]
    fn kv_incr_over_non_numeric_value_replays_as_noop() {
        let put_str = PhysicalPlan::Kv(KvOp::Put {
            collection: "counters".into(),
            key: b"str".to_vec(),
            value: zerompk::to_msgpack_vec(&"hello").expect("encode"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"str".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_str, incr]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let bytes = crate::engine::kv::current_ms();
        let value = h
            .core
            .kv_engine
            .get(DatabaseId::DEFAULT.as_u64(), TID, "counters", b"str", bytes)
            .expect("str survives replay");
        let decoded: String = zerompk::from_msgpack(&value).expect("decode string");
        assert_eq!(
            decoded, "hello",
            "incr over a non-numeric value must replay to a no-op, value unchanged"
        );
    }

    #[test]
    fn production_wal_append_emits_seven_element_shape_for_ttl_bearing_incr() {
        let observed_now_ms = crate::engine::kv::current_ms();

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");

        let plan = PhysicalPlan::Kv(KvOp::Incr {
            collection: "counters".into(),
            key: b"daily".to_vec(),
            delta: 1,
            ttl_ms: 86_400_000,
            surrogate: Surrogate::new(7),
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append incr");
        assert!(outcome.lsn.is_some());
        let resolved = outcome
            .resolved_now_ms
            .expect("TTL-bearing Incr must always resolve a TTL instant");
        assert!(resolved >= observed_now_ms);

        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");
        let record = records
            .iter()
            .find(|r| r.header.tenant_id == TID)
            .expect("incr record present");

        let (disc, _collection, _key, _delta, ttl_ms_field, _surrogate, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, i64, u64, u32, u64)>(&record.payload)
                .expect("seven-element kv_incr shape");
        assert_eq!(disc, "kv_incr");
        assert_eq!(ttl_ms_field, 86_400_000);
        assert_eq!(
            expire_at_ms,
            resolved + 86_400_000,
            "the emitted record must carry the same instant wal_append_if_write resolved"
        );
    }
}
