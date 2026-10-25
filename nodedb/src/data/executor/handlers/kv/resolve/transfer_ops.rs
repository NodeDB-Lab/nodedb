// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the two atomic transfers: `Transfer` (fungible balance move)
//! and `TransferItem` (non-fungible row move between two collections). Both
//! decide every governing policy before producing a single mutation — a
//! transfer is one write, so a rejection on either side resolves neither half.

use nodedb_physical::physical_plan::KvResolveOutcome;

use super::context::{ResolveResult, ResolvedPut, delete_mutation, put_mutation};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::kv::declared_body::fit_kv_image;
use crate::data::executor::handlers::kv::rls::admit_kv_row;
use crate::data::executor::handlers::kv::transfer::{TransferItemParams, TransferParams};
use crate::data::executor::handlers::kv::transfer_compute::{TransferError, compute_transfer};
use crate::data::executor::response_codec;
use crate::engine::kv::current_ms;

impl CoreLoop {
    /// Resolve an atomic fungible `Transfer`.
    ///
    /// The two puts are emitted in ascending key order, the lock ordering
    /// `execute_kv_transfer` documents and follows.
    pub(super) fn resolve_kv_transfer(&self, params: TransferParams<'_>) -> ResolveResult {
        let TransferParams {
            did,
            tid,
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            rls_write_check,
        } = params;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        let Some(source_bytes) = self.kv_resolve_read(did, tid, collection, source_key, now_ms)
        else {
            return Err(ErrorCode::NotFound);
        };
        let dest_bytes = self.kv_resolve_read(did, tid, collection, dest_key, now_ms);

        let computed = compute_transfer(
            &source_bytes,
            dest_bytes.as_deref().filter(|b| !b.is_empty()),
            field,
            amount,
            self.declared_columns_of(did, tid, collection),
        )
        .map_err(|e| match e {
            TransferError::Declared(error) => ErrorCode::from(error),
            TransferError::TypeMismatch(detail) => ErrorCode::TypeMismatch {
                collection: collection.to_string(),
                detail,
            },
            TransferError::InsufficientBalance { have, need } => ErrorCode::InsufficientBalance {
                collection: collection.to_string(),
                detail: format!("source has {have}, need {need}"),
            },
        })?;

        admit_kv_row(
            rls_write_check,
            &computed.new_source,
            source_key,
            tid,
            collection,
            &self.crs_context(),
        )?;
        admit_kv_row(
            rls_write_check,
            &computed.new_dest,
            dest_key,
            tid,
            collection,
            &self.crs_context(),
        )?;

        // `execute_kv_transfer` puts both rows with `ttl_ms: 0`, clearing any
        // TTL either held. Preserved verbatim.
        let debit = put_mutation(ResolvedPut {
            collection,
            key: source_key,
            value: computed.new_source,
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: debit_surrogate,
            precondition: Some(source_bytes),
        });
        let credit = put_mutation(ResolvedPut {
            collection,
            key: dest_key,
            value: computed.new_dest,
            ttl_ms: 0,
            expire_at_ms: 0,
            surrogate: credit_surrogate,
            precondition: dest_bytes,
        });
        let mutations = if source_key <= dest_key {
            vec![debit, credit]
        } else {
            vec![credit, debit]
        };

        let response_payload = response_codec::encode_json_as_msgpack(&serde_json::json!({
            "source_key": String::from_utf8_lossy(source_key),
            "dest_key": String::from_utf8_lossy(dest_key),
            "field": field,
            "amount": computed.amount.to_json(),
            "source_balance": computed.source_balance_after.to_json(),
            "dest_balance": computed.dest_balance_after.to_json(),
        }))?;
        Ok(KvResolveOutcome {
            mutations,
            response_payload,
        })
    }

    /// Resolve an atomic non-fungible `TransferItem`. The row leaving the
    /// source and the row arriving at the destination are two different
    /// images governed by two independent collections — both decided here.
    pub(super) fn resolve_kv_transfer_item(&self, params: TransferItemParams<'_>) -> ResolveResult {
        let TransferItemParams {
            did,
            tid,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check,
            dest_rls_write_check,
        } = params;
        if self.kv_engine.is_over_budget() {
            return Err(ErrorCode::ResourcesExhausted);
        }

        let now_ms = current_ms();
        let Some(item_data) = self.kv_resolve_read(did, tid, source_collection, item_key, now_ms)
        else {
            return Err(ErrorCode::NotFound);
        };
        // The row arriving at the destination meets the destination's
        // declared numeric columns.
        let dest_data = fit_kv_image(
            &item_data,
            self.declared_columns_of(did, tid, dest_collection),
        )?
        .unwrap_or_else(|| item_data.clone());
        admit_kv_row(
            source_rls_write_check,
            &item_data,
            item_key,
            tid,
            source_collection,
            &self.crs_context(),
        )?;
        admit_kv_row(
            dest_rls_write_check,
            &dest_data,
            dest_key,
            tid,
            dest_collection,
            &self.crs_context(),
        )?;

        // The destination is read only to pin its drift precondition: the live
        // handler overwrites whatever is there, and so does the apply.
        let dest_existing = self.kv_resolve_read(did, tid, dest_collection, dest_key, now_ms);

        let response_payload = response_codec::encode_json_as_msgpack(&serde_json::json!({
            "item_key": String::from_utf8_lossy(item_key),
            "dest_key": String::from_utf8_lossy(dest_key),
            "source_collection": source_collection,
            "dest_collection": dest_collection,
        }))?;

        Ok(KvResolveOutcome {
            mutations: vec![
                delete_mutation(source_collection, item_key, Some(item_data)),
                put_mutation(ResolvedPut {
                    collection: dest_collection,
                    key: dest_key,
                    value: dest_data,
                    ttl_ms: 0,
                    expire_at_ms: 0,
                    surrogate,
                    precondition: dest_existing,
                }),
            ],
            response_payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::core_loop::CoreLoop;
    use std::sync::Arc;

    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Status};
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{DatabaseId, TenantId, VShardId};

    const TID: u64 = 1;
    const COLLECTION: &str = "kv_resolved";
    const DEST_COLLECTION: &str = "kv_resolved_dest";

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
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: did(),
                tenant_id: TID,
                collection,
                key,
                value,
                ttl_ms: 0,
                now_ms: crate::engine::kv::current_ms(),
                surrogate: crate::engine::kv::test_support::row_surrogate(key),
            })
            .expect("a bound row writes");
    }

    fn stored(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<Vec<u8>> {
        core.kv_engine
            .get(did(), TID, collection, key, crate::engine::kv::current_ms())
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

    #[test]
    fn transfer_item_resolves_a_delete_and_a_put_across_two_collections() {
        let mut h = make_core();
        let body =
            nodedb_types::json_to_msgpack(&serde_json::json!({ "sword": 1 })).expect("encode");
        seed(&mut h.core, COLLECTION, b"item", &body);

        let op = KvOp::TransferItem {
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, DEST_COLLECTION),
            item_key: b"item".to_vec(),
            dest_key: b"owned".to_vec(),
            surrogate: Surrogate::new(7),
            source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        };
        let outcome = resolve(&h, &op);
        assert_eq!(outcome.mutations.len(), 2);
        match &outcome.mutations[0] {
            nodedb_physical::physical_plan::KvResolvedMutation::Delete {
                collection,
                key,
                precondition,
            } => {
                assert_eq!(collection.as_str(), COLLECTION);
                assert_eq!(key.as_slice(), b"item");
                assert_eq!(precondition.as_deref(), Some(body.as_slice()));
            }
            other => panic!("expected the source Delete first, got {other:?}"),
        }
        match &outcome.mutations[1] {
            nodedb_physical::physical_plan::KvResolvedMutation::Put {
                collection,
                key,
                value,
                precondition,
                ..
            } => {
                assert_eq!(collection.as_str(), DEST_COLLECTION);
                assert_eq!(key.as_slice(), b"owned");
                assert_eq!(value, &body);
                assert_eq!(
                    precondition.as_deref(),
                    None,
                    "the destination key was absent, so the apply requires it to stay absent"
                );
            }
            other => panic!("expected the destination Put second, got {other:?}"),
        }

        let resp = apply(&mut h, &outcome);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(stored(&h.core, COLLECTION, b"item"), None);
        assert_eq!(stored(&h.core, DEST_COLLECTION, b"owned"), Some(body));
    }

    #[test]
    fn transfer_item_on_a_missing_row_is_not_found() {
        let h = make_core();
        let op = KvOp::TransferItem {
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, DEST_COLLECTION),
            item_key: b"nope".to_vec(),
            dest_key: b"owned".to_vec(),
            surrogate: Surrogate::new(7),
            source_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            dest_rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        };
        let t = task();
        let resp = h.core.execute_kv_resolve_write(&t, did(), TID, &op);
        assert_eq!(resp.error_code.as_deref(), Some(&ErrorCode::NotFound));
    }
}
