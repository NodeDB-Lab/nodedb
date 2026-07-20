// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the atomic KV `Cas` / `IncrFloat` / `GetSet` records.
//!
//! Unlike `kv_transfer` / `kv_transfer_item` (see `wal_replay_kv_transfer.rs`),
//! these three ops carry no precondition that can be unmet at replay time:
//! `cas` creates the key when `expected` is empty and the key is absent,
//! `incr_float` inits an absent key to `0.0`, and `getset` treats an absent
//! key as `old = None` and writes unconditionally. So replay is a direct call
//! into the live `KvEngine` method — the same one the autocommit dispatch
//! path calls — against whatever state this core's KV engine holds at this
//! point in LSN-ordered replay.
//!
//! `wal_append_if_write` appends the WAL record BEFORE dispatch, so a `Cas`
//! that failed its compare live (or an `IncrFloat` over a non-numeric value)
//! still has a durable record. Replay re-runs the same computation against
//! the same pre-state, fails identically, and mutates nothing — this
//! converges rather than diverges, so no success-gate is applied here.

use tracing::warn;

use super::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::engine::kv::{AtomicError, AtomicKeyCtx};

impl CoreLoop {
    /// Try each atomic KV delta-record shape (`kv_cas`, `kv_incr_float`,
    /// `kv_getset`) in turn against one WAL payload. Returns `None` only when
    /// none of the three discriminators match (caller tries the next
    /// candidate arm in `wal_replay/kv.rs`), otherwise `Some(puts)` from
    /// whichever arm decoded.
    pub(super) fn try_replay_kv_atomic(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        if let Some(applied) = self.try_replay_kv_cas(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        ) {
            return Some(applied);
        }
        if let Some(applied) = self.try_replay_kv_incr_float(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        ) {
            return Some(applied);
        }
        self.try_replay_kv_getset(
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
            tombstones,
        )
    }

    /// Decode + tombstone-gate + replay one `kv_cas` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_cas` discriminator
    /// shape (caller tries the next candidate arm), otherwise `Some(puts)` —
    /// `1` if the compare matched and the swap was applied, `0` if tombstoned
    /// or the compare failed (a correctly-converging no-op, not a skip).
    fn try_replay_kv_cas(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, expected, new_value, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, Vec<u8>, u32)>(payload)
                .ok()?;
        if disc != "kv_cas" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        let result = self.kv_engine.cas(
            AtomicKeyCtx {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                now_ms,
                surrogate: nodedb_types::Surrogate::new(surrogate),
            },
            &expected,
            &new_value,
        );
        if result.success {
            self.note_replay_write_lsn(
                database_id,
                tenant_id,
                &collection,
                Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                record_lsn,
            );
        }
        Some(usize::from(result.success))
    }

    /// Decode + tombstone-gate + replay one `kv_incr_float` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_incr_float`
    /// discriminator shape, otherwise `Some(puts)` — `1` if the increment
    /// applied, `0` if tombstoned or the current value was not numeric (a
    /// type-mismatch replays to the same no-op the live dispatch produced).
    fn try_replay_kv_incr_float(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, delta, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, f64, u32)>(payload).ok()?;
        if disc != "kv_incr_float" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        match self.kv_engine.incr_float(
            AtomicKeyCtx {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                now_ms,
                surrogate: nodedb_types::Surrogate::new(surrogate),
            },
            delta,
        ) {
            Ok(_) => {
                self.note_replay_write_lsn(
                    database_id,
                    tenant_id,
                    &collection,
                    Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                    record_lsn,
                );
                Some(1)
            }
            Err(AtomicError::TypeMismatch { detail }) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(&key),
                    %detail,
                    "WAL kv_incr_float replay: type mismatch, skipping record"
                );
                Some(0)
            }
            Err(AtomicError::Overflow) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(&key),
                    "WAL kv_incr_float replay: overflow (NaN/Inf), skipping record"
                );
                Some(0)
            }
            Err(AtomicError::Encode { detail }) => {
                warn!(
                    core = self.core_id,
                    collection = %collection,
                    key = %String::from_utf8_lossy(&key),
                    %detail,
                    "WAL kv_incr_float replay: re-encode failed, skipping record"
                );
                Some(0)
            }
        }
    }

    /// Decode + tombstone-gate + replay one `kv_getset` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_getset`
    /// discriminator shape, otherwise `Some(1)` (or `Some(0)` if tombstoned).
    /// `getset` writes unconditionally regardless of whether the key existed.
    fn try_replay_kv_getset(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, new_value, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u32)>(payload).ok()?;
        if disc != "kv_getset" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        self.kv_engine.getset(
            AtomicKeyCtx {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                now_ms,
                surrogate: nodedb_types::Surrogate::new(surrogate),
            },
            &new_value,
        );
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
    /// (`wal_append_if_write`), then read the records back. Unlike the
    /// `kv_transfer` unit, `Cas` / `IncrFloat` / `GetSet` already produce a
    /// durable record today — the bug here is that replay never decoded
    /// them, so the load-bearing assertion is the post-replay value check,
    /// not the `lsn.is_some()` check.
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
                "kv atomic writes must produce a durable WAL record"
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

    #[test]
    fn kv_cas_survives_wal_replay_from_empty() {
        let put_p1 = PhysicalPlan::Kv(KvOp::Put {
            collection: "state".into(),
            key: b"p1".to_vec(),
            value: b"idle".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let cas = PhysicalPlan::Kv(KvOp::Cas {
            collection: "state".into(),
            key: b"p1".to_vec(),
            expected: b"idle".to_vec(),
            new_value: b"in_match".to_vec(),
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_p1, cas]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_value(&h.core, "state", b"p1"),
            Some(b"in_match".to_vec()),
            "CAS must replay its swap against the pre-state, not just the put"
        );
    }

    #[test]
    fn kv_cas_mismatch_replays_as_noop() {
        let put_p1 = PhysicalPlan::Kv(KvOp::Put {
            collection: "state".into(),
            key: b"p1".to_vec(),
            value: b"fighting".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        // Live dispatch would have failed this compare (expected "idle" but
        // the seeded value is "fighting"); the WAL record still exists
        // because `wal_append_if_write` appends before dispatch.
        let cas = PhysicalPlan::Kv(KvOp::Cas {
            collection: "state".into(),
            key: b"p1".to_vec(),
            expected: b"idle".to_vec(),
            new_value: b"in_match".to_vec(),
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_p1, cas]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_value(&h.core, "state", b"p1"),
            Some(b"fighting".to_vec()),
            "a CAS whose expected value does not match must replay to a no-op, value unchanged"
        );
    }

    #[test]
    fn kv_cas_empty_expected_against_absent_key_replays_as_create() {
        // No prior put: the key is absent when this record replays.
        let cas = PhysicalPlan::Kv(KvOp::Cas {
            collection: "state".into(),
            key: b"player1".to_vec(),
            expected: Vec::new(),
            new_value: b"idle".to_vec(),
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[cas]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_value(&h.core, "state", b"player1"),
            Some(b"idle".to_vec()),
            "an empty-expected CAS against an absent key must replay as a create"
        );
    }

    #[test]
    fn kv_incr_float_survives_wal_replay_from_empty() {
        let incr1 = PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: "scores".into(),
            key: b"dmg".to_vec(),
            delta: 3.0,
            surrogate: Surrogate::new(1),
        });
        let incr2 = PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: "scores".into(),
            key: b"dmg".to_vec(),
            delta: 1.5,
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[incr1, incr2]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let bytes = get_value(&h.core, "scores", b"dmg").expect("dmg survives replay");
        let value: f64 = zerompk::from_msgpack(&bytes).expect("decode f64");
        assert!(
            (value - 4.5).abs() < f64::EPSILON,
            "incr_float must replay both increments against the empty-start state, got {value}"
        );
    }

    #[test]
    fn kv_incr_float_non_numeric_replays_as_noop() {
        let put_str = PhysicalPlan::Kv(KvOp::Put {
            collection: "scores".into(),
            key: b"str".to_vec(),
            value: zerompk::to_msgpack_vec(&"hello").expect("encode"),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let incr = PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: "scores".into(),
            key: b"str".to_vec(),
            delta: 1.0,
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_str, incr]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let bytes = get_value(&h.core, "scores", b"str").expect("str survives replay");
        let value: String = zerompk::from_msgpack(&bytes).expect("decode string");
        assert_eq!(
            value, "hello",
            "incr_float over a non-numeric value must replay to a no-op, value unchanged"
        );
    }

    #[test]
    fn kv_getset_survives_wal_replay_from_empty() {
        let put_tok = PhysicalPlan::Kv(KvOp::Put {
            collection: "session".into(),
            key: b"tok".to_vec(),
            value: b"old-token".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let getset = PhysicalPlan::Kv(KvOp::GetSet {
            collection: "session".into(),
            key: b"tok".to_vec(),
            new_value: b"new-token".to_vec(),
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[put_tok, getset]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_value(&h.core, "session", b"tok"),
            Some(b"new-token".to_vec()),
            "getset must replay its unconditional write, not just the seeding put"
        );
    }

    #[test]
    fn kv_getset_against_absent_key_replays_as_create() {
        let getset = PhysicalPlan::Kv(KvOp::GetSet {
            collection: "session".into(),
            key: b"fresh".to_vec(),
            new_value: b"new-token".to_vec(),
            surrogate: Surrogate::new(1),
        });

        let records = append_via_autocommit(&[getset]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            get_value(&h.core, "session", b"fresh"),
            Some(b"new-token".to_vec()),
            "getset against an absent key must replay as a create"
        );
    }
}
