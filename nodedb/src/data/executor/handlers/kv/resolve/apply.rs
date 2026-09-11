// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `KvOp::ResolvedWrite`. Runs on every replica; the
//! plan carries the decided verdict and mutations, nothing recomputed.
//!
//! Drift check: every mutation's `precondition` is checked before the first
//! mutation runs, all-or-nothing; a failure returns `OllpRetryRequired` —
//! same contract the columnar resolved-row apply uses.

use nodedb_physical::physical_plan::KvResolvedMutation;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `KvOp::ResolvedWrite`: check every precondition, apply every
    /// mutation, and return the shipped payload verbatim.
    pub(in crate::data::executor) fn execute_kv_resolved_write(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        mutations: &[KvResolvedMutation],
        response_payload: &[u8],
        rls_write_check: &nodedb_types::RlsWriteCheck,
    ) -> Response {
        debug!(
            core = self.core_id,
            count = mutations.len(),
            "kv resolved write"
        );
        let now_ms = self.kv_ttl_now_ms(task);

        for mutation in mutations {
            let current = self.kv_engine.get(
                did,
                tid,
                mutation.collection().as_str(),
                mutation.key(),
                now_ms,
            );
            if current.as_deref() != mutation.precondition() {
                return self.response_error(task, ErrorCode::OllpRetryRequired);
            }
            // A still-absent key has nothing to expire/persist — same
            // `NotFound` the live handlers return, raised before any mutation.
            if current.is_none()
                && matches!(
                    mutation,
                    KvResolvedMutation::Expire { .. } | KvResolvedMutation::Persist { .. }
                )
            {
                return self.response_error(task, ErrorCode::NotFound);
            }
            // Gate stays on every write path even as a no-op here.
            if let KvResolvedMutation::Put {
                collection,
                key,
                value,
                ..
            } = mutation
                && let Err(error) =
                    admit_kv_row(rls_write_check, value, key, tid, collection.as_str())
            {
                return self.response_error(task, error);
            }
        }

        for mutation in mutations {
            self.apply_kv_resolved_mutation(task, did, tid, mutation, now_ms);
        }

        self.response_with_payload(task, response_payload.to_vec())
    }

    /// Apply one already-checked mutation and emit its change event. `old` on
    /// the event is the precondition the drift check just confirmed.
    fn apply_kv_resolved_mutation(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        mutation: &KvResolvedMutation,
        now_ms: u64,
    ) {
        match mutation {
            KvResolvedMutation::Put {
                collection,
                key,
                value,
                ttl_ms,
                expire_at_ms,
                surrogate,
                precondition,
            } => {
                self.kv_engine.put_with_absolute_expiry(
                    crate::engine::kv::KvPutParams {
                        database_id: did,
                        tenant_id: tid,
                        collection: collection.as_str(),
                        key,
                        value,
                        ttl_ms: *ttl_ms,
                        now_ms,
                        surrogate: *surrogate,
                    },
                    *expire_at_ms,
                );
                if let Some(ref m) = self.metrics {
                    m.record_kv_put();
                }
                let op = match precondition {
                    Some(_) => crate::event::WriteOp::Update,
                    None => crate::event::WriteOp::Insert,
                };
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection.as_str(),
                    op,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    Some(value),
                    precondition.as_deref(),
                );
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
            KvResolvedMutation::Delete {
                collection,
                key,
                precondition,
            } => {
                self.kv_engine
                    .delete(did, tid, collection.as_str(), &[key.to_vec()], now_ms);
                if let Some(ref m) = self.metrics {
                    m.record_kv_delete();
                }
                let key_str = String::from_utf8_lossy(key);
                self.emit_write_event(
                    task,
                    collection.as_str(),
                    crate::event::WriteOp::Delete,
                    crate::engine::document::store::RowIdentity::from_user_key(key_str.as_ref()),
                    None,
                    precondition.as_deref(),
                );
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
            // No body change, so no event — matches the live handlers.
            KvResolvedMutation::Expire {
                collection,
                key,
                ttl_ms,
                resolved_now_ms,
                precondition: _,
            } => {
                self.kv_engine.expire_with_absolute_expiry(
                    did,
                    tid,
                    collection.as_str(),
                    key,
                    resolved_now_ms.saturating_add(*ttl_ms),
                );
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
            KvResolvedMutation::Persist {
                collection,
                key,
                precondition: _,
            } => {
                self.kv_engine.persist(did, tid, collection.as_str(), key);
                self.note_kv_write_lsn(task, did, tid, collection.as_str(), key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::Response;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{KvOp, KvResolveOutcome, KvResolvedMutation};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
    use crate::types::{DatabaseId, TenantId, VShardId};

    const TID: u64 = 1;
    const COLLECTION: &str = "kv_resolved";

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

    fn did() -> u64 {
        DatabaseId::DEFAULT.as_u64()
    }

    fn task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TID),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Kv(KvOp::Get {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                key: b"seed".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            }),
            None,
        )
    }

    fn seed(core: &mut CoreLoop, collection: &str, key: &[u8], value: &[u8]) {
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: did(),
            tenant_id: TID,
            collection,
            key,
            value,
            ttl_ms: 0,
            now_ms: crate::engine::kv::current_ms(),
            surrogate: Surrogate::new(1),
        });
    }

    fn stored(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<Vec<u8>> {
        core.kv_engine
            .get(did(), TID, collection, key, crate::engine::kv::current_ms())
    }

    fn i64_bytes(v: i64) -> Vec<u8> {
        zerompk::to_msgpack_vec(&v).expect("encode i64")
    }

    /// Run the resolve handler and decode its outcome.
    fn resolve(h: &CoreHarness, op: &KvOp) -> KvResolveOutcome {
        let t = task();
        let resp = h.core.execute_kv_resolve_write(&t, did(), TID, op);
        assert_eq!(
            resp.status,
            Status::Ok,
            "resolve failed: {:?}",
            resp.error_code
        );
        zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode resolve outcome")
    }

    /// Apply an already-resolved outcome, as a replica does.
    fn apply(h: &mut CoreHarness, outcome: &KvResolveOutcome) -> Response {
        let t = task();
        h.core.execute_kv_resolved_write(
            &t,
            did(),
            TID,
            &outcome.mutations,
            &outcome.response_payload,
            &RlsWriteCheck::decided_earlier_in_request(),
        )
    }

    fn incr_op(key: &[u8], delta: i64) -> KvOp {
        KvOp::Incr {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: key.to_vec(),
            delta,
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        }
    }

    #[test]
    fn drifted_precondition_retries_and_mutates_nothing() {
        let mut h = make_core();
        seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(5));

        let outcome = resolve(&h, &incr_op(b"counter", 3));

        // A concurrent write lands between the resolve and the apply.
        seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(99));

        let resp = apply(&mut h, &outcome);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "a drifted precondition must yield OllpRetryRequired"
        );
        assert_eq!(
            stored(&h.core, COLLECTION, b"counter"),
            Some(i64_bytes(99)),
            "a refused apply must not mutate anything"
        );
    }

    #[test]
    fn drift_scan_refuses_before_the_first_mutation_applies() {
        let mut h = make_core();
        seed(&mut h.core, COLLECTION, b"a", &i64_bytes(1));
        seed(&mut h.core, COLLECTION, b"b", &i64_bytes(2));

        // Hand-built two-mutation write whose SECOND precondition is stale: the
        // first must not land either.
        let outcome = KvResolveOutcome {
            mutations: vec![
                KvResolvedMutation::Put {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                    key: b"a".to_vec(),
                    value: i64_bytes(10),
                    ttl_ms: 0,
                    expire_at_ms: 0,
                    surrogate: Surrogate::new(1),
                    precondition: Some(i64_bytes(1)),
                },
                KvResolvedMutation::Put {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                    key: b"b".to_vec(),
                    value: i64_bytes(20),
                    ttl_ms: 0,
                    expire_at_ms: 0,
                    surrogate: Surrogate::new(2),
                    precondition: Some(i64_bytes(777)),
                },
            ],
            response_payload: Vec::new(),
        };

        let resp = apply(&mut h, &outcome);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert_eq!(stored(&h.core, COLLECTION, b"a"), Some(i64_bytes(1)));
        assert_eq!(stored(&h.core, COLLECTION, b"b"), Some(i64_bytes(2)));
    }

    #[test]
    fn absent_key_precondition_requires_the_key_to_stay_absent() {
        let mut h = make_core();

        let outcome = KvResolveOutcome {
            mutations: vec![KvResolvedMutation::Put {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                key: b"fresh".to_vec(),
                value: i64_bytes(1),
                ttl_ms: 0,
                expire_at_ms: 0,
                surrogate: Surrogate::new(1),
                precondition: None,
            }],
            response_payload: Vec::new(),
        };

        // Someone created the key first: absent-means-absent, so this drifts.
        seed(&mut h.core, COLLECTION, b"fresh", &i64_bytes(42));
        let resp = apply(&mut h, &outcome);
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired)
        );
        assert_eq!(stored(&h.core, COLLECTION, b"fresh"), Some(i64_bytes(42)));
    }

    #[test]
    fn persist_on_an_absent_key_reports_not_found_at_apply() {
        let mut h = make_core();
        let outcome = KvResolveOutcome {
            mutations: vec![KvResolvedMutation::Persist {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                key: b"gone".to_vec(),
                precondition: None,
            }],
            response_payload: Vec::new(),
        };
        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
    }
}
