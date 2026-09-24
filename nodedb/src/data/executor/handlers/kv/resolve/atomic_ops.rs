// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the KV atomics: `Incr`, `IncrFloat`, `Cas`, `GetSet`. Each
//! post-image comes from `nodedb_physical::kv_atomic::compute`, the same pure functions
//! `KvEngine::{incr, incr_float, cas, getset}` call — recomputing here would
//! let resolve and apply disagree.

use nodedb_physical::kv_atomic::compute;
use nodedb_physical::physical_plan::{KvCounterShape, KvResolveOutcome};

use super::context::{ResolveResult, ResolvedPut, expiry_from_ttl, one, put_mutation};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::{
    KvAtomicCtx, atomic_error_code, incr_float_reply,
};
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::response_codec;

/// Render a stored body for the `current_value` / `old_value` slot of an
/// atomic's reply, exactly as the live handlers do.
fn base64_body(body: Option<&[u8]>) -> Option<String> {
    body.map(|v| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v))
}

impl CoreLoop {
    /// Resolve `INCR`. `ttl_ms > 0` installs a fresh expiry; `ttl_ms == 0`
    /// preserves the existing one, matching `atomic_put` (not `KvEngine::put`).
    pub(super) fn resolve_kv_incr(
        &self,
        ctx: KvAtomicCtx<'_>,
        delta: i64,
        ttl_ms: u64,
        shape: &KvCounterShape,
    ) -> ResolveResult {
        let KvAtomicCtx {
            task,
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        let now_ms = self.kv_ttl_now_ms(task);
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let (new_value, new_bytes) = compute::incr(current.as_deref(), delta, shape)
            .map_err(|e| atomic_error_code(e.into(), collection))?;
        admit_kv_row(rls_write_check, &new_bytes, key, tid, collection)?;

        let expire_at_ms = if ttl_ms > 0 {
            expiry_from_ttl(ttl_ms, now_ms)
        } else {
            self.kv_resolve_preserved_expiry(did, tid, collection, key)
        };
        let response_payload =
            response_codec::encode_json_as_msgpack(&serde_json::json!({ "value": new_value }))?;
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: new_bytes,
                ttl_ms,
                expire_at_ms,
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }

    /// Resolve `INCR_FLOAT`. Always preserves the key's existing TTL, the
    /// same `ttl_ms = 0` call `KvEngine::incr_float` makes.
    pub(super) fn resolve_kv_incr_float(
        &self,
        ctx: KvAtomicCtx<'_>,
        delta: &str,
        shape: &KvCounterShape,
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        let now_ms = self.kv_read_now_ms();
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let (new_value, new_bytes) = compute::incr_float(current.as_deref(), delta, shape)
            .map_err(|e| atomic_error_code(e.into(), collection))?;
        admit_kv_row(rls_write_check, &new_bytes, key, tid, collection)?;

        let response_payload =
            response_codec::encode_json_as_msgpack(&incr_float_reply(new_value, &new_bytes))?;
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: new_bytes,
                ttl_ms: 0,
                expire_at_ms: self.kv_resolve_preserved_expiry(did, tid, collection, key),
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }

    /// Resolve `CAS`. A mismatch resolves to zero mutations and a reply
    /// reporting failure; it still goes through the propose loop as a no-op
    /// entry so replicas agree on a statement that wrote nothing.
    pub(super) fn resolve_kv_cas(
        &self,
        ctx: KvAtomicCtx<'_>,
        expected: &[u8],
        new_value: &[u8],
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        let now_ms = self.kv_read_now_ms();
        let current = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let (matches, write_bytes) = compute::cas(current.as_deref(), expected, new_value)
            .map_err(|e| atomic_error_code(e.into(), collection))?;
        // Decided on the image the swap stores, same as `execute_kv_cas`: a
        // swap into a typed row stores the row, not `new_value` itself.
        if matches {
            admit_kv_row(rls_write_check, &write_bytes, key, tid, collection)?;
        }

        let response_payload = response_codec::encode_json_as_msgpack(&serde_json::json!({
            "success": matches,
            "current_value": base64_body(current.as_deref()),
        }))?;

        if !matches {
            return Ok(KvResolveOutcome {
                mutations: Vec::new(),
                response_payload,
            });
        }
        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: write_bytes,
                ttl_ms: 0,
                expire_at_ms: self.kv_resolve_preserved_expiry(did, tid, collection, key),
                surrogate,
                precondition: current,
            }),
            response_payload,
        ))
    }

    /// Resolve `GETSET`. The old value is gated by `rls_filters` here — a row
    /// the policy hides comes back absent, matching `execute_kv_getset`.
    pub(super) fn resolve_kv_getset(
        &self,
        ctx: KvAtomicCtx<'_>,
        new_value: &[u8],
        rls_filters: &[u8],
    ) -> ResolveResult {
        let KvAtomicCtx {
            did,
            tid,
            collection,
            key,
            surrogate,
            rls_write_check,
            ..
        } = ctx;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }
        let now_ms = self.kv_read_now_ms();
        let old = self.kv_resolve_read(did, tid, collection, key, now_ms);
        let write_bytes = compute::getset(old.as_deref(), new_value)
            .map_err(|e| atomic_error_code(e.into(), collection))?;
        // Decided on the image the write stores, same as `execute_kv_getset`.
        admit_kv_row(rls_write_check, &write_bytes, key, tid, collection)?;

        let disclosable_old = match &old {
            Some(bytes) => match self.row_passes_rls(bytes, rls_filters) {
                Ok(true) => old.as_deref(),
                Ok(false) => None,
                Err(e) => {
                    return Err(ErrorCode::Internal {
                        detail: e.to_string(),
                    });
                }
            },
            None => None,
        };
        let response_payload = response_codec::encode_json_as_msgpack(
            &serde_json::json!({ "old_value": base64_body(disclosable_old) }),
        )?;

        Ok(one(
            put_mutation(ResolvedPut {
                collection,
                key,
                value: write_bytes,
                ttl_ms: 0,
                expire_at_ms: self.kv_resolve_preserved_expiry(did, tid, collection, key),
                surrogate,
                precondition: old,
            }),
            response_payload,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::CoreLoop;
    use std::sync::Arc;

    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::task::ExecutionTask;
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

    /// A raw counter body: the decimal text of `v`.
    fn i64_bytes(v: i64) -> Vec<u8> {
        v.to_string().into_bytes()
    }

    /// Run the resolve handler and decode its outcome.
    fn resolve(h: &CoreHarness, op: &KvOp) -> nodedb_physical::physical_plan::KvResolveOutcome {
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
    fn apply(
        h: &mut CoreHarness,
        outcome: &nodedb_physical::physical_plan::KvResolveOutcome,
    ) -> crate::bridge::envelope::Response {
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
            shape: nodedb_physical::physical_plan::KvCounterShape::Raw,
        }
    }

    #[test]
    fn incr_resolves_to_the_computed_post_image_and_applies_it() {
        let mut h = make_core();
        seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(5));

        let outcome = resolve(&h, &incr_op(b"counter", 3));
        assert_eq!(outcome.mutations.len(), 1);
        match &outcome.mutations[0] {
            nodedb_physical::physical_plan::KvResolvedMutation::Put {
                collection,
                key,
                value,
                precondition,
                ..
            } => {
                assert_eq!(collection.as_str(), COLLECTION);
                assert_eq!(key.as_slice(), b"counter");
                assert_eq!(value, &i64_bytes(8), "post-image must be 5 + 3");
                assert_eq!(
                    precondition.as_deref(),
                    Some(i64_bytes(5).as_slice()),
                    "precondition must pin the exact image the resolve read"
                );
            }
            other => panic!("expected a Put mutation, got {other:?}"),
        }

        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(stored(&h.core, COLLECTION, b"counter"), Some(i64_bytes(8)));
        assert_eq!(
            resp.payload.as_bytes(),
            outcome.response_payload.as_slice(),
            "the apply must hand back the resolved payload verbatim"
        );
    }

    #[test]
    fn cas_mismatch_resolves_to_zero_mutations_and_still_replies() {
        let mut h = make_core();
        let stored_value = zerompk::to_msgpack_vec(&"actual").expect("encode");
        seed(&mut h.core, COLLECTION, b"slot", &stored_value);

        let op = KvOp::Cas {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: b"slot".to_vec(),
            expected: b"not-the-stored-value".to_vec(),
            new_value: b"next".to_vec(),
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        };
        let outcome = resolve(&h, &op);
        assert!(
            outcome.mutations.is_empty(),
            "a CAS that did not match writes nothing"
        );
        assert!(
            !outcome.response_payload.is_empty(),
            "it still owes the caller its failure reply"
        );

        let reported: serde_json::Value =
            nodedb_types::json_from_msgpack(&outcome.response_payload).expect("decode cas reply");
        assert_eq!(reported["success"], serde_json::json!(false));

        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(stored(&h.core, COLLECTION, b"slot"), Some(stored_value));
    }

    #[test]
    fn cas_match_resolves_to_one_put() {
        let mut h = make_core();
        seed(&mut h.core, COLLECTION, b"slot", b"actual");

        let op = KvOp::Cas {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: b"slot".to_vec(),
            expected: b"actual".to_vec(),
            new_value: b"next".to_vec(),
            surrogate: Surrogate::new(1),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        };
        let outcome = resolve(&h, &op);
        assert_eq!(outcome.mutations.len(), 1);

        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(stored(&h.core, COLLECTION, b"slot"), Some(b"next".to_vec()));
    }
}
