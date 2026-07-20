// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `Expire` / `Persist` TTL-mutation records.
//!
//! Both records were durably WAL-appended (`wal_append_kv_op`'s `KvOp::Expire`
//! / `KvOp::Persist` arms) but had no decode arm in `replay_kv_wal` before this
//! module existed, so both were silently lost on every crash-restart. WAL replay
//! is the recovery path for these writes above the KV checkpoint's replay floor;
//! at or below that floor the checkpoint already carries each row's resolved
//! absolute `expire_at_ms`, so the gate in `skip_kv_replay_record` is what stops
//! a TTL mutation from being applied twice.
//!
//! `kv_expire` always carries the Control-Plane-resolved absolute
//! `expire_at_ms` (see `encode_kv_expire`'s doc comment for why `EXPIRE` has
//! no "no TTL" shape the way `PUT` does). Replay installs that instant
//! verbatim via `KvEngine::expire_with_absolute_expiry` rather than
//! recomputing `now_ms + ttl_ms` at replay time, which would drift the expiry
//! forward by the crash-to-restart delay.
//!
//! `kv_persist` clears a key's expiry outright and involves no clock at all,
//! so there is no drift hazard to guard against — only the loss.
//!
//! Replay is LSN-ordered and the Control Plane serializes WAL appends per
//! vshard, so a `Persist` record can never precede the `Put`/`Expire` that
//! set the TTL it clears: by the time this record replays, every earlier-LSN
//! write to the same key has already been applied to `self.kv_engine`,
//! exactly mirroring the order the live write saw.

use tracing::warn;

use super::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;

impl CoreLoop {
    /// Decode + tombstone-gate + replay one `kv_expire` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_expire`
    /// discriminator shape (caller tries the next candidate arm), otherwise
    /// `Some(applied)` — `1` if the expiry was installed, `0` if tombstoned
    /// or the key was absent at this point in LSN order.
    ///
    /// An absent key here is not an error: replay is LSN-ordered and writes
    /// to a given key replay in the same order the live system applied them,
    /// so if the key is absent at this point, the original live `EXPIRE`
    /// would have failed identically (`execute_kv_expire` responds
    /// `NotFound`). It is `warn!` + skip rather than a panic — the record
    /// is not malformed, the target key just does not exist.
    pub(super) fn try_replay_kv_expire(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, _ttl_ms, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64, u64)>(payload).ok()?;
        if disc != "kv_expire" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let applied = self.kv_engine.expire_with_absolute_expiry(
            database_id,
            tenant_id,
            &collection,
            &key,
            expire_at_ms,
        );
        if !applied {
            warn!(
                core = self.core_id,
                collection = %collection,
                key = %String::from_utf8_lossy(&key),
                "WAL kv_expire replay: key absent at this point in LSN order, skipping record"
            );
            return Some(0);
        }
        self.note_replay_write_lsn(
            database_id,
            tenant_id,
            &collection,
            Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
            record_lsn,
        );
        Some(1)
    }

    /// Decode + tombstone-gate + replay one `kv_persist` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_persist`
    /// discriminator shape (caller tries the next candidate arm), otherwise
    /// `Some(applied)` — `1` if the expiry was cleared, `0` if tombstoned or
    /// the key was absent at this point in LSN order (same no-op rationale
    /// as `try_replay_kv_expire`; `KvEngine::persist` touches no clock, so
    /// there is no drift hazard here at all, only the historical loss).
    pub(super) fn try_replay_kv_persist(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>)>(payload).ok()?;
        if disc != "kv_persist" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        let applied = self
            .kv_engine
            .persist(database_id, tenant_id, &collection, &key);
        if !applied {
            warn!(
                core = self.core_id,
                collection = %collection,
                key = %String::from_utf8_lossy(&key),
                "WAL kv_persist replay: key absent at this point in LSN order, skipping record"
            );
            return Some(0);
        }
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
    /// durable record before reading the records back. This is the
    /// load-bearing assertion that fails on the pre-fix code path where
    /// `kv_expire` / `kv_persist` were written but never decoded by replay.
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
                "kv expire/persist autocommit writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    fn ttl_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<i64> {
        core.kv_engine
            .get_ttl_ms(DatabaseId::DEFAULT.as_u64(), TID, collection, key, 0)
    }

    #[test]
    fn kv_expire_replay_installs_recorded_absolute_expiry_not_replay_time_clock() {
        // Encode a record whose absolute instant is 1000 with ttl_ms = 5000:
        // a drifting implementation that recomputed `current_ms() + ttl_ms`
        // at replay time would install a value far larger than 6000, since
        // real wall-clock time is vastly greater than 1000.
        let put_seed = PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"tok1".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let entry = crate::control::server::wal_dispatch_kv::encode::encode_kv_expire(
            "sessions", b"tok1", 5_000, 6_000,
        )
        .expect("encode kv_expire with absolute expiry");

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
        .expect("append raw kv_expire record");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            ttl_ms(&h.core, "sessions", b"tok1"),
            Some(6_000),
            "replay must install the recorded absolute expiry verbatim (expire_at_ms - \
             now_ms(0) == 6000), not recompute now_ms + ttl_ms at replay time"
        );
    }

    #[test]
    fn production_wal_append_emits_absolute_instant_for_expire() {
        let observed_now_ms = crate::engine::kv::current_ms();

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let put_seed = PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"tok2".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &put_seed,
        )
        .expect("wal append seed put");

        let plan = PhysicalPlan::Kv(KvOp::Expire {
            collection: "sessions".into(),
            key: b"tok2".to_vec(),
            ttl_ms: 5_000,
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append expire");
        assert!(outcome.lsn.is_some());
        let resolved = outcome
            .resolved_now_ms
            .expect("Expire must always resolve a TTL instant");
        assert!(resolved >= observed_now_ms);

        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");
        let expire_record = records
            .iter()
            .rev()
            .find(|r| r.header.tenant_id == TID)
            .expect("expire record present");
        let (disc, _collection, _key, ttl_ms_field, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64, u64)>(&expire_record.payload)
                .expect("five-element kv_expire shape");
        assert_eq!(disc, "kv_expire");
        assert_eq!(ttl_ms_field, 5_000);
        assert_eq!(
            expire_at_ms,
            resolved + 5_000,
            "the emitted record must carry the same instant wal_append_if_write resolved"
        );

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            ttl_ms(&h.core, "sessions", b"tok2"),
            Some(expire_at_ms as i64),
            "replay (now_ms = 0) must install exactly the recorded absolute expire_at_ms"
        );
    }

    #[test]
    fn kv_persist_replay_clears_ttl_set_by_a_prior_put() {
        // Pre-fix, the Persist record was decoded by nothing and dropped
        // silently: the key would still carry its TTL after replay. This
        // assertion is what bites on that code path.
        let put_p = PhysicalPlan::Kv(KvOp::Put {
            collection: "sessions".into(),
            key: b"tok3".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 60_000,
            surrogate: Surrogate::new(1),
        });
        let persist_p = PhysicalPlan::Kv(KvOp::Persist {
            collection: "sessions".into(),
            key: b"tok3".to_vec(),
        });

        let records = append_via_autocommit(&[put_p, persist_p]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            ttl_ms(&h.core, "sessions", b"tok3"),
            Some(-1),
            "kv_persist replay must clear the TTL a prior Put installed"
        );
    }

    #[test]
    fn kv_expire_against_absent_key_replays_as_a_no_op() {
        let entry = crate::control::server::wal_dispatch_kv::encode::encode_kv_expire(
            "sessions", b"ghost", 5_000, 6_000,
        )
        .expect("encode kv_expire with absolute expiry");

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        wal.append_put(
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &entry,
        )
        .expect("append raw kv_expire record");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        // Must not panic.
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            ttl_ms(&h.core, "sessions", b"ghost"),
            None,
            "expiring an absent key must not fabricate a phantom key"
        );
    }
}
