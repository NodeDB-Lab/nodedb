// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW read-path interception for the pgwire handler.
//!
//! Called from `execute_planned_sql_inner` after planning and before dispatch.
//! For `Shadowed` / `Materializing` clones this produces an augmented task
//! list (target + source) and then merges the source response with tombstone
//! filtering applied.
//!
//! Non-cloned databases and fully `Materialized` clones return `None` —
//! zero overhead for the common path.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::clone::resolver::{
    CloneReadParams, ResolveOutcome, filter_tombstoned_rows, resolve_read,
};
use crate::control::server::pgwire::handler::plan::{PlanKind, multirow_payload_to_response};
use crate::control::server::pgwire::handler::shape_encode;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::kv::apply_kv_wrap;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::super::types::error_to_sqlstate;
use super::super::super::core::NodeDbPgHandler;
use super::merge::{filter_kv_tombstoned_rows, merge_msgpack_arrays, wrap_single_map_as_array};
use super::temporal::extract_system_as_of_ms;

impl NodeDbPgHandler {
    /// Intercept read tasks for cloned collections.
    ///
    /// Returns `Some(responses)` when clone resolution handled the dispatch
    /// completely — the caller should return that directly.
    ///
    /// Returns `None` when the tasks do not target a cloned collection —
    /// the caller should continue with normal dispatch.
    pub(in crate::control::server::pgwire::handler::routing) async fn maybe_dispatch_clone_reads(
        &self,
        tasks: Vec<PhysicalTask>,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        projection: Option<&OutputSchema>,
        result_formats: &[FieldFormat],
    ) -> PgWireResult<Option<Vec<Response>>> {
        // Compute query LSN and wall-ms for the resolver.
        //
        // If the first task carries a `system_as_of_ms` (i.e. the query was
        // written with `FOR SYSTEM_TIME AS OF <ms>`), derive query_lsn from
        // that wall-clock time so the clone predation check works correctly.
        // Otherwise fall back to the current WAL LSN (normal reads).
        let (query_lsn, query_ms) =
            if let Some(as_of_ms) = extract_system_as_of_ms(tasks.first().map(|t| &t.plan)) {
                let lsn = self.state.ms_to_lsn(as_of_ms);
                (lsn, Some(as_of_ms))
            } else {
                let lsn = self.state.wal.next_lsn();
                let ms = self.state.ms_to_lsn_inverse(lsn);
                (lsn, ms)
            };

        let params = CloneReadParams {
            query_lsn,
            query_ms,
        };

        let outcome = resolve_read(&self.state, tasks, tenant_id, &params).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        match outcome {
            None => Ok(None),

            Some(ResolveOutcome::Passthrough(_tasks)) => {
                // Fully materialized — let the normal dispatch path handle it.
                Ok(None)
            }

            Some(ResolveOutcome::PreDatesClone(note)) => {
                // Query time predates the clone's creation — return empty.
                tracing::debug!(
                    message = note.message,
                    query_lsn = %note.query_lsn,
                    clone_created_at = %note.clone_created_at,
                    "clone read predates clone creation — returning empty result"
                );
                let empty: Vec<u8> =
                    nodedb_types::json_to_msgpack(&serde_json::json!([])).unwrap_or_default();
                match compose::shape_payload_no_plan(&empty, PlanKind::MultiRow, projection) {
                    ShapeOutcome::Rows(shaped) => {
                        let (response, notice) =
                            shape_encode::shaped_query_response(shaped, result_formats);
                        if let Some(n) = notice {
                            self.sessions.push_notice(addr, n);
                        }
                        Ok(Some(vec![response]))
                    }
                    ShapeOutcome::Passthrough => {
                        let shaped = multirow_payload_to_response(&empty);
                        if let Some(notice) = shaped.notice {
                            self.sessions.push_notice(addr, notice);
                        }
                        Ok(Some(vec![shaped.response]))
                    }
                }
            }

            Some(ResolveOutcome::Augmented {
                tasks,
                source_start_idx,
                origin: _,
                target_collection_key,
                note,
            }) => {
                if let Some(note) = note {
                    tracing::debug!(
                        message = note.message,
                        "clone read: T_lsn < clone_created_at (note attached)"
                    );
                }

                // Clone resolution adds source-database tasks after the initial
                // authorization pass. Re-authorize the complete augmented set
                // before either half can be dispatched.
                self.authorize_tasks(identity, &tasks)?;

                // Split tasks into target and source halves.
                let (target_tasks, source_tasks) = tasks.split_at(source_start_idx);

                // Dispatch target tasks (these are the primary tasks).
                let mut responses = Vec::with_capacity(target_tasks.len());
                for task in target_tasks {
                    let resp = self
                        .dispatch_task(task.clone(), None, None)
                        .await
                        .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;
                    responses.push(resp);
                }

                // Load tombstones for source row filtering.
                let tombstoned = self
                    .state
                    .credentials
                    .catalog()
                    .list_clone_tombstones(&target_collection_key)
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?;

                // Load KV tombstones for KV-engine key-based filtering.
                let kv_tombstoned = self
                    .state
                    .credentials
                    .catalog()
                    .list_kv_clone_tombstones(&target_collection_key)
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?;

                // Dispatch source tasks, filter tombstoned rows, merge into target responses.
                //
                // For a single-level clone there is one source task per target task.
                // For a multi-level clone chain the resolver emits one source task per
                // ancestor level per original target task, so source_tasks.len() may be
                // a multiple of target_tasks.len().  All source tasks that correspond to
                // the same original query task (index = source_idx % target_tasks.len())
                // must be merged into the same target response slot.
                let target_count = target_tasks.len().max(1);
                for (source_idx, source_task) in source_tasks.iter().enumerate() {
                    let response_idx = source_idx % target_count;
                    let source_resp = self
                        .dispatch_task(source_task.clone(), None, None)
                        .await
                        .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;

                    // For KvOp::Get: inject the primary key field into the raw map response
                    // so that projection and column-name assertions work correctly.
                    let normalized_payload =
                        apply_kv_wrap(&source_task.plan, source_resp.payload.as_ref());

                    // KvOp::Get responses arrive as a single msgpack map (not an array).
                    // Normalize to a 1-element array so tombstone filters and merge work
                    // uniformly across scan and point-get shapes.
                    let normalized_payload = wrap_single_map_as_array(normalized_payload);

                    // Apply surrogate tombstone filter (document engine rows).
                    // `filter_tombstoned_rows` returns `None` only when its input
                    // is not a well-formed msgpack array. Post-normalization
                    // (`wrap_single_map_as_array`) the input is guaranteed to be
                    // an empty slice or a valid array, so `None` here signals
                    // upstream corruption — log loudly and pass through unchanged
                    // rather than masking with `unwrap_or`.
                    let source_payload = match filter_tombstoned_rows(
                        &normalized_payload,
                        &tombstoned,
                    ) {
                        Some(p) => p,
                        None => {
                            tracing::warn!(
                                payload_len = normalized_payload.len(),
                                "clone read: filter_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                            );
                            normalized_payload
                        }
                    };

                    // Apply KV key tombstone filter (KV engine rows).
                    let source_payload = if !kv_tombstoned.is_empty() {
                        match filter_kv_tombstoned_rows(&source_payload, &kv_tombstoned) {
                            Some(p) => p,
                            None => {
                                tracing::warn!(
                                    payload_len = source_payload.len(),
                                    "clone read: filter_kv_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                                );
                                source_payload
                            }
                        }
                    } else {
                        source_payload
                    };

                    // Merge source rows into the corresponding target response.
                    // `response_idx` maps multi-level ancestor tasks back to the
                    // original query-task slot they serve.
                    if response_idx < responses.len() {
                        // Normalize target payload to array shape for uniform merge.
                        let target_payload = wrap_single_map_as_array(
                            responses[response_idx].payload.as_ref().to_vec(),
                        );
                        let merged = merge_msgpack_arrays(&target_payload, &source_payload)
                            .map_err(|e| {
                                let (severity, code, message) = error_to_sqlstate(&e);
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    severity.to_owned(),
                                    code.to_owned(),
                                    message,
                                )))
                            })?;
                        responses[response_idx] = crate::bridge::envelope::Response {
                            payload: merged.into(),
                            ..responses[response_idx].clone()
                        };
                    } else {
                        // More source tasks than target tasks — append standalone.
                        responses.push(crate::bridge::envelope::Response {
                            payload: source_payload.into(),
                            ..source_resp
                        });
                    }
                }

                // Convert raw Response objects to pgwire Responses.
                let mut pg_responses = Vec::with_capacity(responses.len());
                for resp in responses {
                    match compose::shape_payload_no_plan(
                        resp.payload.as_ref(),
                        PlanKind::MultiRow,
                        projection,
                    ) {
                        ShapeOutcome::Rows(shaped) => {
                            let (response, notice) =
                                shape_encode::shaped_query_response(shaped, result_formats);
                            if let Some(n) = notice {
                                self.sessions.push_notice(addr, n);
                            }
                            pg_responses.push(response);
                        }
                        ShapeOutcome::Passthrough => {
                            let shaped = multirow_payload_to_response(resp.payload.as_ref());
                            if let Some(notice) = shaped.notice {
                                self.sessions.push_notice(addr, notice);
                            }
                            pg_responses.push(shaped.response);
                        }
                    }
                }

                Ok(Some(pg_responses))
            }
        }
    }
}
