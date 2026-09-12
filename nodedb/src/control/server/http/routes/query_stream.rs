// SPDX-License-Identifier: BUSL-1.1

//! Lazy NDJSON streaming body for `POST /v1/query/stream`.
//!
//! When a query is an eligible single-task, autocommit-free (HTTP is
//! stateless), unordered multi-row SELECT — i.e. exactly
//! `Query(Exchange(Gather{as_aggregate:false}))` over a streamable scan — its
//! rows flow to the client one NDJSON line at a time straight off a
//! [`ResultStream`], instead of being materialized into a `String` first.
//!
//! A mid-stream error is surfaced **in band** as a final
//! `{"error": "..."}\n` line and the body then ends cleanly: NDJSON clients
//! parse line-by-line, so an in-band error line is the correct UX and matches
//! the shape the materialized path emits for a dispatch failure. The HTTP body
//! itself never errors, so the response stays `Content-Type:
//! application/x-ndjson` and 200.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;

use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::exchange::gather::gather_all_cores_stream_authorized;
use crate::control::server::exchange::streamable::streamable_gather_child;
use crate::control::server::response_shape::compose::shape_decoded_rows;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::result_stream::ResultStream;
use crate::control::server::shared::metering::DetachedMeterGuard;
use crate::data::executor::response_codec::decode_payload_to_json;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::auth::AppState;

/// Decide whether the single-task list is an eligible streamable SELECT and,
/// if so, open the row stream.
///
/// Mirrors the pgwire `maybe_stream_select` plan-shape predicate via
/// [`streamable_gather_child`] plus the single-task and no-set-op gates. HTTP
/// is stateless, so there is no autocommit / transaction-block check.
///
/// Returns `Ok(Some((stream, limit)))` when eligible, `Ok(None)` when the
/// caller should fall back to the materialized path, or `Err` when the
/// stream could not be opened.
pub(super) async fn try_open_stream(
    state: &AppState,
    tasks: &[PhysicalTask],
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    trace_id: crate::types::TraceId,
) -> crate::Result<Option<(ResultStream, usize)>> {
    let [task] = tasks else {
        return Ok(None);
    };
    if task.post_set_op != PostSetOp::None {
        return Ok(None);
    }
    let Some((child_plan, limit)) = streamable_gather_child(&task.plan) else {
        return Ok(None);
    };
    let mut child_task = task.clone();
    child_task.plan = child_plan.clone();
    let tenant_id = child_task.tenant_id;
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.shared.audit));
    let checked_child = match crate::control::server::shared::clone_write::intercept_and_authorize(
        crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
            state: &state.shared,
            task: child_task,
            identity,
            tenant_id,
            permissions: &state.shared.permissions,
            roles: &state.shared.roles,
            emitter: &emitter,
        },
    )
    .await?
    {
        // A streamable child is always a plain unordered scan (see
        // `streamable_gather_child`), never a clone-write plan shape, so this
        // is unreachable in practice — surfaced as a typed error rather than
        // silently accommodated as either a stream or a materialized response.
        crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(_) => {
            return Err(crate::Error::Internal {
                detail: "clone-write hook unexpectedly intercepted a streamable scan".into(),
            });
        }
        crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked) => {
            checked
        }
    };

    let stream = if let Some(gw) = state.shared.gateway.get() {
        let ctx = QueryContext {
            tenant_id: task.tenant_id,
            trace_id,
            database_id,
            txn_id: None,
        };
        gw.execute_stream(&ctx, checked_child).await
    } else {
        gather_all_cores_stream_authorized(&state.shared, checked_child.into_authorized(), trace_id)
    }?;

    Ok(Some((stream, limit)))
}

/// Everything [`ndjson_body_stream`] needs to build one response body.
pub(super) struct NdjsonBody {
    pub stream: ResultStream,
    /// Global take-N across the whole union.
    pub limit: usize,
    pub projection: Option<OutputSchema>,
    /// The statement's redaction inputs, resolved ONCE before the first batch
    /// is pulled. Re-resolving per batch would risk the first NDJSON lines
    /// going out unredacted.
    pub redaction: Option<QueryRedaction>,
    /// Owned so the body, which outlives the handler frame, can reach the
    /// redaction policy store for every batch.
    pub state: Arc<crate::control::state::SharedState>,
    pub lease_scope: crate::control::lease::QueryLeaseScope,
    pub meter_guard: Option<DetachedMeterGuard>,
}

/// Build a lazy NDJSON byte stream from a [`ResultStream`].
///
/// Each [`RowBatch`] payload is a standalone msgpack array; it is decoded to a
/// JSON array and each element is emitted as its own `<json>\n` line, with a
/// global take-N enforced at `limit`. A mid-stream `Err` is emitted as a final
/// `{"error": "..."}\n` line and the stream then ends — see the module docs for
/// why the HTTP body never errors.
///
/// [`RowBatch`]: crate::control::server::result_stream::RowBatch
pub(super) fn ndjson_body_stream(
    body: NdjsonBody,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    let NdjsonBody {
        stream,
        limit,
        projection,
        redaction,
        state,
        lease_scope,
        meter_guard,
    } = body;
    async_stream::stream! {
        // The body owns this scope for its complete polling lifetime. Dropping
        // the body on completion or client disconnect releases descriptors only
        // after the ResultStream is no longer reachable.
        let _lease_scope = lease_scope;
        // Owned by this generator for its whole polling lifetime, exactly
        // like `_lease_scope` above — whether the stream runs to completion,
        // ends on a mid-stream error, or is dropped early by a disconnected
        // client, this guard's `Drop` fires and bills exactly the rows
        // accumulated into it via `add_rows` below, never more.
        let mut meter_guard = meter_guard;
        let mut emitted: usize = 0;
        let mut batches = stream;
        while emitted < limit {
            let batch = match batches.next().await {
                None => break,
                Some(Ok(b)) => b,
                Some(Err(e)) => {
                    let (_status, msg) = GatewayErrorMap::to_http(&e);
                    let line = format!("{}\n", serde_json::json!({ "error": msg }));
                    yield Ok(Bytes::from(line));
                    return;
                }
            };

            let json_str = decode_payload_to_json(&batch.payload);
            let value = match sonic_rs::from_str::<serde_json::Value>(&json_str) {
                Ok(v) => v,
                Err(e) => {
                    // A malformed batch payload is surfaced as an in-band error
                    // line (matching the mid-stream dispatch-error path above)
                    // rather than silently dropping the batch.
                    let line = format!(
                        "{}\n",
                        serde_json::json!({ "error": format!("malformed response batch: {e}") })
                    );
                    yield Ok(Bytes::from(line));
                    return;
                }
            };
            // Row maps are keyed by `ShapedRows::cell_keys`, so each NDJSON
            // line serializes as-is; two output columns sharing a name emit
            // `{"id": …, "id_1": …}` rather than collapsing to one cell.
            // Only re-borrows the once-resolved inputs, so the very first
            // batch is redacted under the same policy as the last.
            let shaped = match shape_decoded_rows(
                &value,
                projection.as_ref(),
                redaction.as_ref().map(|r| r.ctx(&state.redaction)),
            ) {
                Ok(s) => s,
                Err(e) => {
                    // In-band error line, matching the malformed-batch path
                    // above: the HTTP body itself never errors.
                    let line = format!("{}\n", serde_json::json!({ "error": format!("{e}") }));
                    yield Ok(Bytes::from(line));
                    return;
                }
            };
            for row in shaped.rows {
                if emitted >= limit {
                    break;
                }
                let line = format!("{}\n", serde_json::Value::Object(row));
                emitted += 1;
                // Incremented for the row this line actually carries, right
                // before it is handed to the body sink below — a client that
                // disconnects after this `yield` still received the line (it
                // is queued to write), so counting here rather than after
                // the `yield` cannot undercount a delivered row.
                if let Some(guard) = meter_guard.as_mut() {
                    guard.add_rows(1);
                }
                yield Ok(Bytes::from(line));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::result_stream::RowBatch;
    use crate::control::state::SharedState;
    use crate::types::Lsn;

    /// A JSON-text array of `n` `{"id": i}` objects. `decode_payload_to_json`
    /// returns a JSON-leading payload as-is, so this exercises the same
    /// array-of-objects → per-line decode path a Data-Plane scan chunk drives.
    fn json_object_batch(start: usize, n: usize) -> Vec<u8> {
        let items: Vec<serde_json::Value> = (start..start + n)
            .map(|i| serde_json::json!({ "id": i }))
            .collect();
        serde_json::Value::Array(items).to_string().into_bytes()
    }

    /// Minimum real shared state: the body reads only `state.redaction`.
    fn test_state() -> Arc<SharedState> {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("query-stream.wal"))
                .expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 1);
        SharedState::new(dispatcher, wal).expect("construct shared state")
    }

    fn batch(start: usize, n: usize) -> crate::Result<RowBatch> {
        Ok(RowBatch {
            payload: json_object_batch(start, n),
            watermark_lsn: Lsn::ZERO,
            read_version_lsn: Lsn::ZERO,
        })
    }

    async fn collect_lines(
        stream: impl futures::Stream<Item = Result<Bytes, std::io::Error>>,
    ) -> Vec<String> {
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("body chunk");
            for line in String::from_utf8_lossy(&chunk).lines() {
                out.push(line.to_string());
            }
        }
        out
    }

    #[tokio::test]
    async fn streams_all_rows_across_batches() {
        // 2500 rows across three batches > the native/pgwire chunk size; assert
        // every row arrives as its own NDJSON line.
        let batches: Vec<crate::Result<RowBatch>> =
            vec![batch(0, 1000), batch(1000, 1000), batch(2000, 500)];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        let lines = collect_lines(ndjson_body_stream(NdjsonBody {
            stream,
            limit: usize::MAX,
            projection: None,
            redaction: None,
            state: test_state(),
            lease_scope: crate::control::lease::QueryLeaseScope::empty(),
            meter_guard: None,
        }))
        .await;
        assert_eq!(lines.len(), 2500, "all rows must stream as NDJSON lines");
    }

    #[tokio::test]
    async fn global_limit_caps_emitted_rows() {
        let batches: Vec<crate::Result<RowBatch>> = vec![batch(0, 1000), batch(1000, 1000)];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        let lines = collect_lines(ndjson_body_stream(NdjsonBody {
            stream,
            limit: 1500,
            projection: None,
            redaction: None,
            state: test_state(),
            lease_scope: crate::control::lease::QueryLeaseScope::empty(),
            meter_guard: None,
        }))
        .await;
        assert_eq!(lines.len(), 1500, "global take-N must cap the line count");
    }

    #[tokio::test]
    async fn mid_stream_error_becomes_in_band_error_line() {
        let batches: Vec<crate::Result<RowBatch>> = vec![
            batch(0, 10),
            Err(crate::Error::Dispatch {
                detail: "boom".into(),
            }),
        ];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        let lines = collect_lines(ndjson_body_stream(NdjsonBody {
            stream,
            limit: usize::MAX,
            projection: None,
            redaction: None,
            state: test_state(),
            lease_scope: crate::control::lease::QueryLeaseScope::empty(),
            meter_guard: None,
        }))
        .await;
        assert_eq!(lines.len(), 11, "10 rows + 1 in-band error line");
        let last: serde_json::Value =
            sonic_rs::from_str(lines.last().expect("error line")).expect("json error line");
        assert!(last.get("error").is_some(), "final line is an error object");
    }

    /// The streaming metering contract: a client that disconnects mid-stream
    /// must be billed for exactly the rows it received, never for the rows a
    /// full scan would have produced. Polls only 3 of 2000 available rows,
    /// then drops the stream without reaching the end of the generator —
    /// exactly what happens when axum drops a response body because the
    /// connected client went away.
    #[tokio::test]
    async fn early_client_disconnect_meters_only_rows_actually_sent() {
        use crate::bridge::dispatch::Dispatcher;
        use crate::control::security::identity::{
            AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
        };
        use crate::control::security::request_scope::RequestAuthScope;
        use crate::control::server::shared::metering::PlanMeteringInfo;
        use crate::wal::WalManager;
        use nodedb_physical::physical_plan::KvOp;

        let dir = tempfile::tempdir().expect("create test directory");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct shared state");
        std::sync::Arc::get_mut(&mut state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;

        let identity = AuthenticatedIdentity::new_regular(
            1,
            "stream-user",
            crate::types::TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        let scope = RequestAuthScope::for_database(
            &identity,
            state.auth_stores(),
            crate::types::DatabaseId::DEFAULT,
        );
        // Collection/engine only matter for attribution, not for this test's
        // assertion — any plan with an extractable collection works.
        let plan = crate::bridge::envelope::PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                crate::types::DatabaseId::DEFAULT,
                "widgets",
            ),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        let info = PlanMeteringInfo::extract(&plan);
        let guard = DetachedMeterGuard::new(&state, &scope, &info)
            .expect("metering enabled and collection present");

        let batches: Vec<crate::Result<RowBatch>> = vec![batch(0, 1000), batch(1000, 1000)];
        let stream: ResultStream = Box::pin(futures::stream::iter(batches));
        // The stream (and the guard it owns) must be dropped before draining,
        // which is what a client disconnecting mid-response does. `pin_mut!`
        // shadows the binding with a `Pin<&mut _>`, so `drop`ping that name
        // would only release the borrow and leave the stream — and its
        // pending row count — alive until end of scope. An inner block drops
        // the real value.
        {
            let body = ndjson_body_stream(NdjsonBody {
                stream,
                limit: usize::MAX,
                projection: None,
                redaction: None,
                state: Arc::clone(&state),
                lease_scope: crate::control::lease::QueryLeaseScope::empty(),
                meter_guard: Some(guard),
            });
            futures::pin_mut!(body);
            for _ in 0..3 {
                body.next()
                    .await
                    .expect("chunk available")
                    .expect("chunk ok");
            }
        }

        let events = state.usage_counter.drain();
        assert_eq!(
            events.len(),
            1,
            "dropping the stream mid-poll still records exactly one event"
        );
        assert_eq!(
            events[0].tokens, 3,
            "billed exactly the 3 rows actually sent to the client, not the 2000 planned"
        );
    }
}
