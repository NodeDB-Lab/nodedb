// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handler for `KvOp::ResolveWrite`. Reports what the wrapped
//! write would apply and reply, mutating nothing — run before proposing a
//! governed state-dependent KV write, since a follower has no writing
//! identity and re-deriving after commit risks a different answer per
//! replica. Read-only, so this takes `&self`.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::atomic::KvAtomicCtx;
use crate::data::executor::handlers::kv::crud::{KvDeleteParams, KvInsertOnConflictUpdateParams};
use crate::data::executor::handlers::kv::field::KvFieldSetArgs;
use crate::data::executor::handlers::kv::predicate::KvPredicateCtx;
use crate::data::executor::handlers::kv::transfer::{TransferItemParams, TransferParams};
use crate::data::executor::handlers::kv::ttl::KvTtlTarget;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    /// Handle `KvOp::ResolveWrite`: resolve `inner` against current state,
    /// decide every policy that governs it, and report the mutation list plus
    /// the reply. Mutates nothing.
    pub(in crate::data::executor) fn execute_kv_resolve_write(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        inner: &KvOp,
    ) -> Response {
        debug!(core = self.core_id, "kv resolve write");

        let outcome = match inner {
            KvOp::InsertOnConflictUpdate {
                collection,
                key,
                value,
                ttl_ms,
                updates,
                surrogate,
                rls_write_check,
                returning,
                rls_filters,
            } => self.resolve_kv_insert_on_conflict_update(
                KvInsertOnConflictUpdateParams {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    value,
                    ttl_ms: *ttl_ms,
                    updates,
                    surrogate: *surrogate,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                },
                task,
            ),
            KvOp::Delete {
                collection,
                keys,
                rls_write_check,
                returning,
                rls_filters,
            } => self.resolve_kv_delete(KvDeleteParams {
                did,
                tid,
                collection: collection.as_str(),
                keys,
                rls_write_check,
                returning: returning.as_ref(),
                rls_filters,
            }),
            KvOp::Expire {
                collection,
                key,
                ttl_ms,
                rls_write_check,
            } => self.resolve_kv_expire(
                KvTtlTarget {
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    rls_write_check,
                },
                *ttl_ms,
                task,
            ),
            KvOp::Persist {
                collection,
                key,
                rls_write_check,
            } => self.resolve_kv_persist(KvTtlTarget {
                did,
                tid,
                collection: collection.as_str(),
                key,
                rls_write_check,
            }),
            KvOp::FieldSet {
                collection,
                key,
                updates,
                surrogate,
                if_present,
                rls_write_check,
                returning,
                rls_filters,
            } => self.resolve_kv_field_set(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                KvFieldSetArgs {
                    updates,
                    if_present: *if_present,
                    returning: returning.as_ref(),
                    rls_filters,
                },
            ),
            KvOp::Incr {
                collection,
                key,
                delta,
                ttl_ms,
                surrogate,
                rls_write_check,
                shape,
            } => self.resolve_kv_incr(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                *delta,
                *ttl_ms,
                shape,
            ),
            KvOp::IncrFloat {
                collection,
                key,
                delta,
                surrogate,
                rls_write_check,
                shape,
            } => self.resolve_kv_incr_float(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                delta,
                shape,
            ),
            KvOp::Cas {
                collection,
                key,
                expected,
                new_value,
                surrogate,
                rls_write_check,
            } => self.resolve_kv_cas(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                expected,
                new_value,
            ),
            KvOp::GetSet {
                collection,
                key,
                new_value,
                surrogate,
                rls_filters,
                rls_write_check,
            } => self.resolve_kv_getset(
                KvAtomicCtx {
                    task,
                    did,
                    tid,
                    collection: collection.as_str(),
                    key,
                    surrogate: *surrogate,
                    rls_write_check,
                },
                new_value,
                rls_filters,
            ),
            KvOp::Transfer {
                collection,
                source_key,
                dest_key,
                field,
                amount,
                debit_surrogate,
                credit_surrogate,
                rls_write_check,
            } => self.resolve_kv_transfer(TransferParams {
                did,
                tid,
                collection: collection.as_str(),
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: *debit_surrogate,
                credit_surrogate: *credit_surrogate,
                rls_write_check,
            }),
            KvOp::TransferItem {
                source_collection,
                dest_collection,
                item_key,
                dest_key,
                surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            } => self.resolve_kv_transfer_item(TransferItemParams {
                did,
                tid,
                source_collection: source_collection.as_str(),
                dest_collection: dest_collection.as_str(),
                item_key,
                dest_key,
                surrogate: *surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            }),
            KvOp::PredicateUpdate {
                collection,
                filters,
                updates,
                rls_write_check,
                returning,
                rls_filters,
            } => self.resolve_kv_predicate_update(
                KvPredicateCtx {
                    did,
                    tid,
                    collection: collection.as_str(),
                    filters,
                    rls_write_check,
                    returning: returning.as_ref(),
                    rls_filters,
                },
                updates,
            ),
            KvOp::PredicateDelete {
                collection,
                filters,
                rls_write_check,
                returning,
                rls_filters,
            } => self.resolve_kv_predicate_delete(KvPredicateCtx {
                did,
                tid,
                collection: collection.as_str(),
                filters,
                rls_write_check,
                returning: returning.as_ref(),
                rls_filters,
            }),
            // Every other op is unwrapped by `resolver_for_plan` already.
            other => Err(ErrorCode::Internal {
                detail: format!(
                    "kv resolve-write wraps an op with no state-dependent image: {}",
                    kv_op_name(other)
                ),
            }),
        };

        match outcome {
            Ok(resolved) => match response_codec::encode(&resolved) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, e),
            },
            Err(code) => self.response_error(task, code),
        }
    }
}

/// Variant name for the refusal above, so the message names the op without
/// putting a whole plan's bytes into an error string.
fn kv_op_name(op: &KvOp) -> &'static str {
    match op {
        KvOp::Get { .. } => "Get",
        KvOp::Put { .. } => "Put",
        KvOp::Insert { .. } => "Insert",
        KvOp::InsertIfAbsent { .. } => "InsertIfAbsent",
        KvOp::InsertOnConflictUpdate { .. } => "InsertOnConflictUpdate",
        KvOp::Delete { .. } => "Delete",
        KvOp::Scan { .. } => "Scan",
        KvOp::Expire { .. } => "Expire",
        KvOp::Persist { .. } => "Persist",
        KvOp::GetTtl { .. } => "GetTtl",
        KvOp::BatchGet { .. } => "BatchGet",
        KvOp::BatchPut { .. } => "BatchPut",
        KvOp::RegisterIndex { .. } => "RegisterIndex",
        KvOp::DropIndex { .. } => "DropIndex",
        KvOp::FieldGet { .. } => "FieldGet",
        KvOp::FieldSet { .. } => "FieldSet",
        KvOp::Truncate { .. } => "Truncate",
        KvOp::Incr { .. } => "Incr",
        KvOp::IncrFloat { .. } => "IncrFloat",
        KvOp::Cas { .. } => "Cas",
        KvOp::GetSet { .. } => "GetSet",
        KvOp::Transfer { .. } => "Transfer",
        KvOp::TransferItem { .. } => "TransferItem",
        KvOp::RegisterSortedIndex { .. } => "RegisterSortedIndex",
        KvOp::DropSortedIndex { .. } => "DropSortedIndex",
        KvOp::SortedIndexRank { .. } => "SortedIndexRank",
        KvOp::SortedIndexTopK { .. } => "SortedIndexTopK",
        KvOp::SortedIndexRange { .. } => "SortedIndexRange",
        KvOp::SortedIndexCount { .. } => "SortedIndexCount",
        KvOp::SortedIndexScore { .. } => "SortedIndexScore",
        KvOp::SortedIndexTxnRead { .. } => "SortedIndexTxnRead",
        KvOp::MaterializeScan { .. } => "MaterializeScan",
        KvOp::ResolveWrite(_) => "ResolveWrite",
        KvOp::ResolvedWrite { .. } => "ResolvedWrite",
        KvOp::PredicateUpdate { .. } => "PredicateUpdate",
        KvOp::PredicateDelete { .. } => "PredicateDelete",
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::task::ExecutionTask;
    use nodedb_physical::physical_plan::KvOp;
    use std::sync::Arc;

    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    use crate::bridge::envelope::Status;
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
            crate::bridge::envelope::PhysicalPlan::Kv(KvOp::Get {
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
    fn resolve_refuses_an_op_with_no_state_dependent_image() {
        let h = make_core();
        let op = KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            key: b"k".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        };
        let t = task();
        let resp = h.core.execute_kv_resolve_write(&t, did(), TID, &op);
        assert_eq!(resp.status, Status::Error);
    }

    #[test]
    fn resolve_mutates_nothing() {
        let mut h = make_core();
        seed(&mut h.core, COLLECTION, b"counter", &i64_bytes(5));
        let _ = resolve(&h, &incr_op(b"counter", 3));
        assert_eq!(
            stored(&h.core, COLLECTION, b"counter"),
            Some(i64_bytes(5)),
            "resolve is read-only"
        );
    }
}
