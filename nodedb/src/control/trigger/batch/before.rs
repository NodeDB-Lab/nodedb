//! Batched BEFORE trigger execution.
//!
//! Processes a batch of rows through BEFORE triggers with:
//! - WHEN clause pre-filtering (skip non-matching rows)
//! - Per-row error accumulation (soft failures don't abort the batch)
//! - NEW row mutation (trigger body can modify fields via ASSIGN)
//!
//! Returns the (possibly mutated) batch with an error accumulator.
//! Rows that failed trigger validation are marked in the accumulator;
//! the caller decides whether to abort or proceed with passing rows.

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::StatementExecutor;
use crate::control::security::catalog::trigger_types::TriggerTiming;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::collector::TriggerBatchRow;
use super::error_blame::ErrorAccumulator;
use super::when_filter;
use crate::control::trigger::fire_common::resolve_trigger_identity;
use crate::control::trigger::registry::DmlEvent;

/// Result of batched BEFORE trigger execution.
pub struct BeforeBatchResult {
    /// Rows after BEFORE trigger processing (possibly mutated).
    pub rows: Vec<TriggerBatchRow>,
    /// Per-row errors from triggers that raised exceptions.
    pub errors: ErrorAccumulator,
}

/// Execute BEFORE triggers over a batch of rows.
///
/// For each BEFORE trigger on the collection:
/// 1. Pre-filter by WHEN clause (vectorized)
/// 2. For each passing row, execute the trigger body
/// 3. Capture NEW mutations from ASSIGN statements
/// 4. Capture per-row errors from RAISE EXCEPTION
///
/// Returns the mutated batch + error accumulator.
#[allow(clippy::too_many_arguments)]
pub async fn execute_before_batch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    event: DmlEvent,
    mut rows: Vec<TriggerBatchRow>,
    cascade_depth: u32,
) -> crate::Result<BeforeBatchResult> {
    let triggers = state
        .trigger_registry
        .get_matching(tenant_id.as_u32(), collection, event);

    let before_triggers: Vec<_> = triggers
        .into_iter()
        .filter(|t| t.timing == TriggerTiming::Before)
        .collect();

    if before_triggers.is_empty() {
        return Ok(BeforeBatchResult {
            rows,
            errors: ErrorAccumulator::new(),
        });
    }

    crate::control::trigger::fire_common::check_cascade_depth(cascade_depth, collection)?;

    let mut errors = ErrorAccumulator::new();

    for trigger in &before_triggers {
        let op_str = event.as_str();

        // Pre-filter by WHEN clause.
        let mask = when_filter::filter_batch_by_when(
            &rows,
            collection,
            op_str,
            trigger.when_condition.as_deref(),
        );

        let effective_identity = resolve_trigger_identity(trigger, identity, tenant_id);

        let block = match state.block_cache.get_or_parse(&trigger.body_sql) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(trigger = %trigger.name, error = %e, "failed to parse BEFORE trigger body");
                continue;
            }
        };

        // Process each passing row.
        for (idx, (row, &passes)) in rows.iter_mut().zip(mask.iter()).enumerate() {
            if !passes {
                continue;
            }

            let bindings = build_before_bindings(row, collection, op_str);

            let executor = StatementExecutor::with_source(
                state,
                effective_identity.clone(),
                tenant_id,
                cascade_depth + 1,
                crate::event::EventSource::Trigger,
            );

            match executor.execute_block(&block, &bindings).await {
                Ok(()) => {
                    // Apply NEW mutations from ASSIGN statements.
                    let mutations = executor.take_new_mutations();
                    if !mutations.is_empty() {
                        if let Some(fields) = row.new_fields_mut() {
                            for (field, value) in mutations {
                                fields.insert(field, value);
                            }
                        }
                    }
                }
                Err(e) => {
                    // Soft error: record per-row, don't abort batch.
                    errors.record(&row.row_id, idx, &e.to_string());
                }
            }
        }
    }

    Ok(BeforeBatchResult { rows, errors })
}

/// Build RowBindings for a BEFORE trigger row.
fn build_before_bindings(row: &TriggerBatchRow, collection: &str, operation: &str) -> RowBindings {
    let new_row = row
        .new_fields()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let old_row = row
        .old_fields()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());

    match operation {
        "INSERT" => RowBindings::before_insert(collection, new_row),
        "UPDATE" => RowBindings::before_update(collection, old_row.unwrap_or_default(), new_row),
        "DELETE" => RowBindings::before_delete(collection, old_row.unwrap_or_default()),
        _ => RowBindings::before_insert(collection, new_row),
    }
}
