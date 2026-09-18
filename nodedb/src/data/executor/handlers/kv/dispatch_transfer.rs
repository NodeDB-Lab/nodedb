// SPDX-License-Identifier: BUSL-1.1

//! KV dispatch for the two-key transfer ops, `Transfer` and `TransferItem`.
//! Split from `dispatch.rs` by op family; each fills the handler's params
//! from the op's fields exactly as the single match arm did.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    /// Run a `Transfer`: atomic fungible move of `field` between two keys.
    pub(super) fn dispatch_kv_transfer(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
            rls_write_check,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_kv_transfer: plan is not Transfer".into(),
                },
            );
        };
        self.execute_kv_transfer(
            task,
            super::transfer::TransferParams {
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
            },
        )
    }

    /// Run a `TransferItem`: atomic non-fungible move between collections.
    pub(super) fn dispatch_kv_transfer_item(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
            source_rls_write_check,
            dest_rls_write_check,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_kv_transfer_item: plan is not TransferItem".into(),
                },
            );
        };
        self.execute_kv_transfer_item(
            task,
            super::transfer::TransferItemParams {
                did,
                tid,
                source_collection: source_collection.as_str(),
                dest_collection: dest_collection.as_str(),
                item_key,
                dest_key,
                surrogate: *surrogate,
                source_rls_write_check,
                dest_rls_write_check,
            },
        )
    }
}
