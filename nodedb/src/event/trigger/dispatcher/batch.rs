// SPDX-License-Identifier: BUSL-1.1

//! Batch trigger dispatch: a `TriggerBatch` (multiple rows) → matching
//! AFTER triggers, with WHEN-clause pre-filtering across the whole batch.
//!
//! Not currently wired into the Normal-mode consumer loop — per-event
//! dispatch (`dispatch_triggers` in `single.rs`) is the sole production path
//! for AFTER-ROW trigger firing (see `event::consumer::pipeline::deliver_events`).
//! This batch path (and its `TriggerBatchCollector`) remains available for a
//! future WHEN-clause-batched throughput optimization; for `BatchSafe`
//! triggers it could dispatch a single bulk DML, but for now it still fires
//! per-row with WHEN evaluated once per row and short-circuited at the
//! parse-and-eval boundary.

use std::sync::Arc;

use tracing::warn;

use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::identity::trigger_identity;
use crate::event::action::{
    ActionContext, ActionId, ActionKey, ActionPayload, ActionRetryQueue, FailedAction,
};

pub async fn dispatch_trigger_batch(
    batch: &crate::control::trigger::batch::collector::TriggerBatch,
    state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
) {
    use crate::control::security::catalog::trigger_types::{TriggerGranularity, TriggerTiming};
    use crate::control::trigger::batch::when_filter;
    use crate::control::trigger::fire_common;
    use crate::control::trigger::registry::DmlEvent;

    let tenant_id = TenantId::new(batch.tenant_id);
    let identity = trigger_identity(tenant_id);
    let mode_filter = Some(TriggerExecutionMode::Async);

    let dml_event = match batch.operation.as_str() {
        "INSERT" => DmlEvent::Insert,
        "UPDATE" => DmlEvent::Update,
        "DELETE" => DmlEvent::Delete,
        _ => return,
    };

    let triggers = state.trigger_registry.get_matching(
        batch.database_id,
        batch.tenant_id,
        &batch.collection,
        dml_event,
    );

    let after_row_triggers: Vec<_> = triggers
        .iter()
        .filter(|t| t.timing == TriggerTiming::After)
        .filter(|t| t.granularity == TriggerGranularity::Row)
        .filter(|t| mode_filter.is_none() || Some(t.execution_mode) == mode_filter)
        .collect();

    if after_row_triggers.is_empty() {
        return;
    }

    for trigger in &after_row_triggers {
        // An AFTER trigger fires post-commit — there is no statement left to
        // fail. A division/modulo-by-zero in its WHEN predicate is surfaced
        // observably (warn) and skips this trigger, rather than being
        // silently folded to "does not fire".
        let mask = match when_filter::filter_batch_by_when(
            &batch.rows,
            &batch.collection,
            &batch.operation,
            trigger.when_condition.as_deref(),
        ) {
            Ok(mask) => mask,
            Err(e) => {
                tracing::warn!(
                    trigger = %trigger.name,
                    collection = %batch.collection,
                    error = %e,
                    "AFTER trigger WHEN predicate raised an evaluation error; skipping trigger for this batch"
                );
                continue;
            }
        };

        let passing = when_filter::count_passing(&mask);
        if passing == 0 {
            continue;
        }

        for (row, &passes) in batch.rows.iter().zip(mask.iter()) {
            if !passes {
                continue;
            }

            let bindings =
                when_filter::build_row_bindings(row, &batch.collection, &batch.operation);

            let report = fire_common::fire_triggers(fire_common::FireTriggersParams {
                state,
                identity: &identity,
                tenant_id,
                collection: &batch.collection,
                triggers: std::slice::from_ref(trigger),
                bindings: &bindings,
                cascade_depth: 0,
                // The batch dispatch path does not carry per-row source
                // LSN/sequence/vShard (TriggerBatch aggregates rows and drops
                // that context), and it is not wired into the production
                // consumer loop. Cross-shard origination is therefore not
                // available here; see the tracked follow-up.
                cross_shard_origin: None,
                on_error: fire_common::FireErrorPolicy::Abort,
            })
            .await;

            if let Err(e) = report.into_result() {
                warn!(
                    trigger = %trigger.name,
                    collection = %batch.collection,
                    row_id = %row.row_id,
                    error = %e,
                    "batch trigger fire failed, enqueuing row for retry"
                );
                retry_queue.enqueue(FailedAction {
                    key: ActionKey {
                        // The batch path does not carry per-row source
                        // LSN/sequence/vShard (see cross_shard_origin note
                        // above); it is not wired into the production consumer
                        // loop.
                        source_lsn: 0,
                        source_sequence: 0,
                        source_vshard: 0,
                        action: ActionId::TriggerRow {
                            trigger_name: trigger.name.clone(),
                        },
                    },
                    payload: ActionPayload::TriggerRow {
                        operation: batch.operation.clone(),
                        new_fields: row.new_fields().cloned(),
                        old_fields: row.old_fields().cloned(),
                    },
                    context: ActionContext {
                        database_id: batch.database_id,
                        tenant_id: batch.tenant_id,
                        collection: batch.collection.clone(),
                        row_id: row.row_id.clone(),
                        cascade_depth: 0,
                    },
                    attempts: 0,
                    last_error: e.to_string(),
                });
            }
        }
    }
}
