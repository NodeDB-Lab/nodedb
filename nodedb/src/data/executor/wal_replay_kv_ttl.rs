// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for the KV TTL / `expire_at_ms` drift fix.
//!
//! `KvOp::Put` / `Insert` / `InsertIfAbsent` / `InsertOnConflictUpdate` /
//! `BatchPut` resolve `now_ms` exactly once, in the Control Plane, at
//! WAL-append time (`wal_dispatch_kv::wal_append_kv_op`). That resolved
//! instant is encoded into the WAL record's trailing `expire_at_ms` element
//! AND threaded onto the dispatched `Request` (`resolved_now_ms`) so the live
//! Data-Plane apply installs the SAME instant. Replay installs the encoded
//! instant verbatim rather than recomputing `now_ms + ttl_ms` at replay time,
//! which would drift the expiry forward by the crash-to-restart delay. The KV
//! checkpoint stores the same resolved absolute instant for the rows below its
//! replay floor, and WAL replay recovers everything above it — an untested drift
//! on either path silently outlives its intended TTL after a crash-restart.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Response,
    };
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::kv::crud::KvWriteParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{QualifiedCollection, Surrogate};
    use nodedb_wal::TombstoneSet;

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

    fn installed_expire_at_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> i64 {
        // `now_ms = 0` makes `get_ttl_ms`'s `expire_at_ms.saturating_sub(now_ms)`
        // return `expire_at_ms` itself, verbatim.
        core.kv_engine
            .get_ttl_ms(DatabaseId::DEFAULT.as_u64(), TID, collection, key, 0)
            .expect("key must exist with a TTL")
    }

    // ── Replay installs the recorded absolute instant, not a recomputed one ──

    #[test]
    fn kv_put_replay_installs_recorded_absolute_expiry_not_replay_time_clock() {
        // `expire_at_ms = 6000` simulates a write from long ago: resolved_now_ms
        // (1000) + ttl_ms (5000). Real wall-clock at test-run time is always
        // vastly greater than 6000, so a drifting implementation (replay
        // recomputing `current_ms() + ttl_ms`) fails this assertion loudly.
        let entry = crate::control::server::wal_dispatch_kv::encode::encode_kv_put(
            "sessions",
            b"tok1",
            b"payload",
            5_000,
            Some(6_000),
            1,
        )
        .expect("encode kv_put with absolute expiry");

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &entry,
            )
            .expect("append raw kv_put record");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        assert_eq!(
            installed_expire_at_ms(&h.core, "sessions", b"tok1"),
            6_000,
            "replay must install the recorded absolute expiry verbatim, not \
             recompute now_ms + ttl_ms at replay time"
        );
    }

    #[test]
    fn kv_batch_put_replay_installs_recorded_absolute_expiry_not_replay_time_clock() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let entry = crate::control::server::wal_dispatch_kv::encode::encode_kv_batch_put(
            "carts",
            &entries,
            5_000,
            Some(6_000),
            &[1, 2],
        )
        .expect("encode kv_batch_put with absolute expiry");

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &entry,
            )
            .expect("append raw kv_batch_put record");
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        for key in [b"k1".as_slice(), b"k2".as_slice()] {
            assert_eq!(
                installed_expire_at_ms(&h.core, "carts", key),
                6_000,
                "batch_put replay must install the recorded absolute expiry \
                 verbatim for every entry, not recompute now_ms + ttl_ms"
            );
        }
    }

    // ── Production WAL-append path emits a plausible instant, and no instant
    //    at all for non-TTL writes ──

    #[test]
    fn production_wal_append_emits_a_resolved_instant_for_a_ttl_put() {
        let observed_now_ms = crate::engine::kv::current_ms();

        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            key: b"tok2".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 5_000,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append");
        assert!(outcome.lsn.is_some());
        let resolved = outcome
            .resolved_now_ms
            .expect("TTL-bearing Put must resolve a TTL instant");
        assert!(
            resolved >= observed_now_ms,
            "resolved_now_ms ({resolved}) must be at or after the instant \
             observed just before the call ({observed_now_ms})"
        );

        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");
        let record = records
            .iter()
            .find(|r| r.header.tenant_id == TID)
            .expect("one record");
        let (disc, _collection, _key, _value, ttl_ms, expire_at_ms, surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                &record.payload,
            )
            .expect("current kv_put shape");
        assert_eq!(disc, "kv_put");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, Some(resolved + 5_000));
        assert_eq!(
            surrogate, 1,
            "the append path must journal the plan's surrogate, not drop it"
        );
    }

    #[test]
    fn production_wal_append_carries_no_instant_for_a_no_ttl_put() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            key: b"tok3".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append");
        assert_eq!(
            outcome.resolved_now_ms, None,
            "a non-TTL Put must not resolve a TTL instant"
        );

        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");
        let record = &records[0];
        let (_disc, _collection, _key, _value, _ttl_ms, expire_at_ms, _surrogate) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(
                &record.payload,
            )
            .expect("current kv_put shape for a non-TTL write");
        assert_eq!(expire_at_ms, None);
    }

    // ── Live apply installs the Control-Plane-resolved instant verbatim ──

    fn task_with_resolved_now_ms(
        plan: PhysicalPlan,
        resolved_now_ms: Option<u64>,
    ) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: crate::types::RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    #[test]
    fn execute_kv_put_installs_task_resolved_now_ms_verbatim() {
        let mut h = make_core();
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            key: b"tok4".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 5_000,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        // 1_000 is vastly less than the real wall clock, so a live-apply path
        // that ignores `resolved_now_ms` and reads the wall clock instead
        // fails this assertion loudly.
        let task = task_with_resolved_now_ms(plan, Some(1_000));

        let resp: Response = h.core.execute_kv_put(
            &task,
            KvWriteParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: "sessions",
                key: b"tok4",
                value: b"payload",
                ttl_ms: 5_000,
                surrogate: Surrogate::new(1),
                returning: None,
                rls_filters: &[],
            },
        );
        assert_eq!(resp.status, crate::bridge::envelope::Status::Ok);

        assert_eq!(
            installed_expire_at_ms(&h.core, "sessions", b"tok4"),
            1_000 + 5_000,
            "live apply must install resolved_now_ms + ttl_ms, not the wall clock"
        );
    }

    #[test]
    fn execute_kv_insert_installs_task_resolved_now_ms_verbatim() {
        // Regression test: `execute_kv_insert` and `execute_kv_insert_if_absent`
        // once derived `now_ms` as `task.resolved_now_ms().unwrap_or_else(current_ms)`,
        // skipping the `epoch_system_ms` (Calvin) fallback that `execute_kv_put`
        // and `execute_kv_batch_put` both apply. Both now share
        // `CoreLoop::kv_ttl_now_ms`, which this test pins for `execute_kv_insert`.
        let mut h = make_core();
        let plan = PhysicalPlan::Kv(KvOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "sessions"),
            key: b"tok5".to_vec(),
            value: b"payload".to_vec(),
            ttl_ms: 5_000,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
        });
        let task = task_with_resolved_now_ms(plan, Some(1_000));

        let resp: Response = h.core.execute_kv_insert(
            &task,
            KvWriteParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: "sessions",
                key: b"tok5",
                value: b"payload",
                ttl_ms: 5_000,
                surrogate: Surrogate::new(1),
                returning: None,
                rls_filters: &[],
            },
        );
        assert_eq!(resp.status, crate::bridge::envelope::Status::Ok);

        assert_eq!(
            installed_expire_at_ms(&h.core, "sessions", b"tok5"),
            1_000 + 5_000,
            "execute_kv_insert must install resolved_now_ms + ttl_ms, not the wall clock"
        );
    }
}
