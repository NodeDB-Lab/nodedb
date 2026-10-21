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

use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::enqueue::{ActionSource, record_row_failures};
use super::identity::trigger_identity;
use crate::control::planner::procedural::executor::core::CrossShardOrigin;
use crate::control::trigger::batch::collector::RowSource;
use crate::event::action::ActionRetryQueue;

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
            &crate::control::crs_context::crs_context_for(state),
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

            let position = row.source.unwrap_or(RowSource {
                lsn: 0,
                sequence: 0,
                vshard: 0,
            });
            let report = fire_common::fire_triggers(fire_common::FireTriggersParams {
                state,
                identity: &identity,
                tenant_id,
                collection: &batch.collection,
                triggers: std::slice::from_ref(trigger),
                bindings: &bindings,
                cascade_depth: 0,
                cross_shard_origin: row.source.map(|source| CrossShardOrigin {
                    source_lsn: source.lsn,
                    source_sequence: source.sequence,
                    source_vshard: source.vshard,
                    source_collection: batch.collection.clone(),
                }),
                on_error: fire_common::FireErrorPolicy::Abort,
                joined: None,
            })
            .await;

            record_row_failures(
                &ActionSource {
                    database_id: batch.database_id,
                    tenant_id: batch.tenant_id,
                    collection: &batch.collection,
                    row_id: &row.row_id,
                    operation: &batch.operation,
                    source_lsn: position.lsn,
                    source_sequence: position.sequence,
                    source_vshard: position.vshard,
                    cascade_depth: 0,
                },
                report,
                row.new_fields(),
                row.old_fields(),
                retry_queue,
            );
        }
    }
}
