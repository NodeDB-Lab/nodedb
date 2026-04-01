//! `SyncDelegate` implementation — bridges the sync transport to NodeDbLite's engines.

use crate::storage::engine::StorageEngine;

use super::core::NodeDbLite;

/// Run an async future synchronously from within a sync SyncDelegate method.
/// Uses tokio's `block_in_place` to avoid blocking the async runtime.
#[cfg(not(target_arch = "wasm32"))]
fn block_on_async<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

#[cfg(not(target_arch = "wasm32"))]
impl<S: StorageEngine> crate::sync::SyncDelegate for NodeDbLite<S> {
    fn pending_deltas(&self) -> Vec<crate::engine::crdt::engine::PendingDelta> {
        self.pending_crdt_deltas().unwrap_or_default()
    }

    fn acknowledge(&self, mutation_id: u64) {
        if let Err(e) = self.acknowledge_deltas(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: acknowledge failed");
        }
    }

    fn reject(&self, mutation_id: u64) {
        if let Err(e) = self.reject_delta(mutation_id) {
            tracing::warn!(mutation_id, error = %e, "SyncDelegate: reject failed");
        }
    }

    fn reject_with_policy(
        &self,
        mutation_id: u64,
        hint: &nodedb_types::sync::compensation::CompensationHint,
    ) {
        use super::lock_ext::LockExt;

        let mut crdt = self.crdt.lock_or_recover();
        match crdt.reject_delta_with_policy(mutation_id, hint) {
            Some(nodedb_crdt::PolicyResolution::AutoResolved(action)) => {
                tracing::info!(
                    mutation_id,
                    action = ?action,
                    "SyncDelegate: delta auto-resolved by policy"
                );
            }
            Some(nodedb_crdt::PolicyResolution::Deferred {
                retry_after_ms,
                attempt,
            }) => {
                tracing::info!(
                    mutation_id,
                    retry_after_ms,
                    attempt,
                    "SyncDelegate: delta deferred for retry"
                );
            }
            Some(nodedb_crdt::PolicyResolution::Escalate) => {
                tracing::warn!(mutation_id, "SyncDelegate: delta escalated to DLQ (policy)");
            }
            Some(nodedb_crdt::PolicyResolution::WebhookRequired { webhook_url, .. }) => {
                tracing::warn!(
                    mutation_id,
                    webhook_url,
                    "SyncDelegate: delta requires webhook (not supported on Lite)"
                );
                // Fallback: treat as escalate.
                let _ = crdt.reject_delta(mutation_id);
            }
            None => {
                tracing::debug!(
                    mutation_id,
                    "SyncDelegate: reject_with_policy — delta not found"
                );
            }
        }
    }

    fn import_remote(&self, data: &[u8]) {
        if let Err(e) = self.import_remote_deltas(data) {
            tracing::warn!(error = %e, "SyncDelegate: import_remote failed");
        }
    }

    fn import_definition(&self, msg: &nodedb_types::sync::wire::DefinitionSyncMsg) {
        use super::definitions::*;

        let result = match (msg.definition_type.as_str(), msg.action.as_str()) {
            ("function", "put") => match sonic_rs::from_slice::<LiteStoredFunction>(&msg.payload) {
                Ok(func) => block_on_async(self.put_function(&func)),
                Err(e) => {
                    tracing::warn!(name = %msg.name, error = %e, "failed to deserialize function");
                    return;
                }
            },
            ("function", "delete") => block_on_async(self.delete_function(&msg.name)),
            ("trigger", "put") => match sonic_rs::from_slice::<LiteStoredTrigger>(&msg.payload) {
                Ok(trigger) => block_on_async(self.put_trigger(&trigger)),
                Err(e) => {
                    tracing::warn!(name = %msg.name, error = %e, "failed to deserialize trigger");
                    return;
                }
            },
            ("trigger", "delete") => block_on_async(self.delete_trigger(&msg.name)),
            ("procedure", "put") => {
                match sonic_rs::from_slice::<LiteStoredProcedure>(&msg.payload) {
                    Ok(p) => block_on_async(self.put_procedure(&p)),
                    Err(e) => {
                        tracing::warn!(name = %msg.name, error = %e, "failed to deserialize procedure");
                        return;
                    }
                }
            }
            ("procedure", "delete") => block_on_async(self.delete_procedure(&msg.name)),
            _ => {
                tracing::warn!(
                    definition_type = %msg.definition_type,
                    action = %msg.action,
                    "unknown definition type/action"
                );
                return;
            }
        };

        if let Err(e) = result {
            tracing::warn!(
                definition_type = %msg.definition_type,
                name = %msg.name,
                error = %e,
                "definition sync failed"
            );
        }
    }
}
