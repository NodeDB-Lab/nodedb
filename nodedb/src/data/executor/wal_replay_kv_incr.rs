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
//! The record is `("kv_incr", collection, key, delta, ttl_ms, surrogate,
//! shape, expire_at_ms)`. `shape` is the row an absent key becomes, so replay
//! creates the same row the live write did. `expire_at_ms` is `Some` only
//! when the live write's `ttl_ms > 0`: replay installs that instant verbatim
//! via `KvEngine::incr_with_absolute_expiry` instead of recomputing
//! `now_ms + ttl_ms`, which would drift the expiry forward by the
//! crash-to-restart delay. `ttl_ms == 0` preserves the key's existing TTL.
//!
//! Unlike the `Put` family, `kv_incr` carries its own surrogate in the
//! record rather than relying on the separately-durable surrogate catalog,
//! so replay reconstructs it from the payload's `u32` instead of using
//! `Surrogate::ZERO`.

use nodedb_physical::physical_plan::KvCounterShape;
use tracing::warn;

use super::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::replay_abort::abort_replay;
use crate::engine::kv::{AtomicError, AtomicKeyCtx, IncrStep, Incremented};

/// The decoded `kv_incr` record.
type KvIncrRecord<'a> = (
    &'a str,
    String,
    Vec<u8>,
    i64,
    u64,
    u32,
    KvCounterShape,
    Option<u64>,
);

impl CoreLoop {
    /// Decode + tombstone-gate + replay one `kv_incr` WAL record.
    ///
    /// Returns `None` when `payload` is not a `kv_incr` record (caller tries
    /// the next candidate arm in `wal_replay/kv.rs`), otherwise `Some(puts)`:
    /// `1` if the increment applied, `0` if tombstoned or the live write
    /// computed no value (a type mismatch or overflow replays to the same
    /// no-op).
    pub(super) fn try_replay_kv_incr(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, delta, ttl_ms, surrogate, shape, expire_at_ms) =
            zerompk::from_msgpack::<KvIncrRecord<'_>>(payload).ok()?;
        if disc != "kv_incr" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        let ctx = AtomicKeyCtx {
            database_id,
            tenant_id,
            collection: &collection,
            key: &key,
            now_ms,
            surrogate: nodedb_types::Surrogate::new(surrogate),
        };
        // Replay re-applies a write the policy already admitted when it was
        // first accepted. Re-deciding it here would make recovery depend on
        // the policies of whoever happens to be connected.
        let admit = &crate::engine::kv::admit_any;
        let result = match expire_at_ms {
            Some(expire_at_ms) => self.kv_engine.incr_with_absolute_expiry(
                ctx,
                IncrStep {
                    delta,
                    ttl_ms,
                    shape: &shape,
                },
                expire_at_ms,
                admit,
            ),
            None => self.kv_engine.incr(ctx, delta, ttl_ms, &shape, admit),
        };
        let applied = self.log_kv_incr_result(&collection, &key, delta, record_lsn, result);
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

    /// Shared result handling for a `kv_incr` replay: `Ok` counts as one
    /// applied put; `TypeMismatch` / `Counter` / `Encode` are
    /// correctly-converging no-ops (the live dispatch would have failed
    /// identically), logged and skipped rather than treated as errors.
    ///
    /// `Rejected` is not one of those: it is a committed record this build
    /// declined to apply, so it aborts recovery rather than converging — see
    /// its arm.
    fn log_kv_incr_result(
        &self,
        collection: &str,
        key: &[u8],
        delta: i64,
        record_lsn: u64,
        result: Result<Incremented<i64>, AtomicError>,
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
            Err(AtomicError::Counter(fault)) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(key),
                    delta,
                    fault = fault.message(),
                    "WAL kv_incr replay: no value computed, skipping record"
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
            // Unreachable by construction: replay hands the engine
            // `admit_any`, so there is no predicate here that could refuse an
            // image. Reaching this arm means a redo path acquired a real
            // write policy, and recovery would then be re-deciding writes that
            // were already admitted when they were accepted — against
            // whichever identity happens to be connected at restart. Every
            // record it disagreed with would be dropped, leaving a hole in the
            // replayed suffix that no later read can tell apart from data
            // never written. So it takes the same exit every other unapplyable
            // committed record takes, which files a forensic report first.
            Err(AtomicError::Rejected(error)) => abort_replay(
                "kv",
                "incr_admission",
                self.core_id,
                record_lsn,
                &format!(
                    "the RLS write gate refused a committed increment on \
                     '{collection}': {error}"
                ),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::control::server::wal_dispatch_kv::encode::{KvIncrRecord, encode_kv_incr};
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::{KvCounterShape, KvOp};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};
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
        std::str::from_utf8(&bytes)
            .expect("a raw counter is UTF-8 text")
            .parse::<i64>()
            .expect("a raw counter is decimal text")
    }

    fn ttl_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<i64> {
        core.kv_engine
            .get_ttl_ms(DatabaseId::DEFAULT.as_u64(), TID, collection, key, 0)
    }

    #[test]
    fn kv_incr_survives_wal_replay_from_empty() {
        let put_p = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"hits".to_vec(),
            value: b"5".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"hits".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Raw,
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"hits".to_vec(),
            value: b"5".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let incr1 = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"hits".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Raw,
        });
        let incr2 = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"hits".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Raw,
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"temp".to_vec(),
            value: b"5".to_vec(),
            ttl_ms: 60_000,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"temp".to_vec(),
            delta: 3,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Raw,
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"daily".to_vec(),
            value: b"0".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let entry = encode_kv_incr(KvIncrRecord {
            collection: "counters",
            key: b"daily",
            delta: 1,
            ttl_ms: 5_000,
            surrogate: 1,
            shape: &KvCounterShape::Raw,
            expire_at_ms: Some(6_000),
        })
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
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
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
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"str".to_vec(),
            value: b"hello".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"str".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Raw,
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
        assert_eq!(
            value,
            b"hello".to_vec(),
            "incr over a non-numeric value must replay to a no-op, value unchanged"
        );
    }

    #[test]
    fn kv_incr_on_an_absent_key_replays_the_typed_row_it_created() {
        let mut template_row = std::collections::HashMap::new();
        template_row.insert(
            "status".to_string(),
            nodedb_types::Value::String("new".into()),
        );
        let template = nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(template_row))
            .expect("encode template");
        let incr = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"fresh".to_vec(),
            delta: 5,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Typed {
                column: Some("n".into()),
                template,
            },
        });

        let records = append_via_autocommit(&[incr]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let now_ms = crate::engine::kv::current_ms();
        let bytes = h
            .core
            .kv_engine
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "counters",
                b"fresh",
                now_ms,
            )
            .expect("the fresh row survives replay");
        let nodedb_types::Value::Object(row) =
            nodedb_types::value_from_msgpack(&bytes).expect("decode row")
        else {
            panic!("replay must recreate a typed row");
        };
        assert_eq!(row.get("n"), Some(&nodedb_types::Value::Integer(5)));
        assert_eq!(
            row.get("status"),
            Some(&nodedb_types::Value::String("new".into()))
        );
    }

    #[test]
    fn production_wal_append_records_the_resolved_expiry_for_ttl_bearing_incr() {
        let observed_now_ms = crate::engine::kv::current_ms();

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");

        let plan = PhysicalPlan::Kv(KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "counters"),
            key: b"daily".to_vec(),
            delta: 1,
            ttl_ms: 86_400_000,
            surrogate: Surrogate::new(7),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            shape: KvCounterShape::Raw,
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

        let (disc, _collection, _key, _delta, ttl_ms_field, _surrogate, _shape, expire_at_ms) =
            zerompk::from_msgpack::<super::KvIncrRecord<'_>>(&record.payload)
                .expect("kv_incr record");
        assert_eq!(disc, "kv_incr");
        assert_eq!(ttl_ms_field, 86_400_000);
        assert_eq!(
            expire_at_ms,
            Some(resolved + 86_400_000),
            "the emitted record must carry the same instant wal_append_if_write resolved"
        );
    }
}
