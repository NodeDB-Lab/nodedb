//! Batch trigger dispatch: a `TriggerBatch` (multiple rows) → matching
//! AFTER triggers, with WHEN-clause pre-filtering across the whole batch.
//!
//! Called by the consumer loop after the batch collector yields a full
//! batch. For `BatchSafe` triggers we can in the future dispatch a single
//! bulk DML; for now they still fire per-row but with WHEN evaluated once
//! per row and short-circuited at the parse-and-eval boundary.

use std::sync::Arc;

use tracing::warn;

use crate::control::security::catalog::trigger_types::TriggerExecutionMode;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::retry::{RetryEntry, TriggerRetryQueue};
use super::identity::trigger_identity;

pub async fn dispatch_trigger_batch(
    batch: &crate::control::trigger::batch::collector::TriggerBatch,
    state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
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

    let triggers =
        state
            .trigger_registry
            .get_matching(batch.tenant_id, &batch.collection, dml_event);

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
        let mask = when_filter::filter_batch_by_when(
            &batch.rows,
            &batch.collection,
            &batch.operation,
            trigger.when_condition.as_deref(),
        );

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

            let result = fire_common::fire_triggers(
                state,
                &identity,
                tenant_id,
                &batch.collection,
                std::slice::from_ref(trigger),
                &bindings,
                0,
            )
            .await;

            if let Err(e) = result {
                warn!(
                    trigger = %trigger.name,
                    collection = %batch.collection,
                    row_id = %row.row_id,
                    error = %e,
                    "batch trigger fire failed, enqueuing row for retry"
                );
                retry_queue.enqueue(RetryEntry {
                    tenant_id: batch.tenant_id,
                    collection: batch.collection.clone(),
                    row_id: row.row_id.clone(),
                    operation: batch.operation.clone(),
                    trigger_name: trigger.name.clone(),
                    new_fields: row.new_fields().cloned(),
                    old_fields: row.old_fields().cloned(),
                    attempts: 0,
                    last_error: e.to_string(),
                    next_retry_at: std::time::Instant::now(),
                    source_lsn: 0,
                    source_sequence: 0,
                    cascade_depth: 0,
                });
            }
        }
    }
}
