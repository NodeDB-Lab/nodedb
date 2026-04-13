//! Local execution of forwarded SQL queries.
//!
//! When a remote node forwards a query to this node (because this node is the
//! leader for the target vShard), the [`LocalForwarder`] executes it through
//! the same plan → dispatch → response pipeline as a direct client query.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nodedb_cluster::forward::RequestForwarder;
use nodedb_cluster::rpc_codec::{ForwardRequest, ForwardResponse};

use crate::bridge::envelope::{Priority, Request};
use crate::control::planner::context::QueryContext;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId};

/// Executes forwarded SQL queries on the local Data Plane.
pub struct LocalForwarder {
    state: Arc<SharedState>,
    next_request_id: AtomicU64,
}

impl LocalForwarder {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self {
            state,
            // Start forwarded request IDs at a high offset to avoid collision
            // with direct client request IDs.
            next_request_id: AtomicU64::new(1_000_000_000),
        }
    }

    fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }
}

impl RequestForwarder for LocalForwarder {
    async fn execute_forwarded(&self, req: ForwardRequest) -> ForwardResponse {
        let tenant_id = TenantId::new(req.tenant_id);

        // Use the remaining deadline from the request, capped at our local max.
        let deadline = Duration::from_millis(req.deadline_remaining_ms).min(Duration::from_secs(
            self.state.tuning.network.default_deadline_secs,
        ));

        // Plan the SQL locally. Build a fresh QueryContext per request so
        // the OriginCatalog is scoped to the *forwarded* request's tenant
        // (one LocalForwarder serves queries from every tenant on the
        // cluster — a single long-lived QueryContext would pin one tenant
        // or, with QueryContext::new(), have no catalog at all).
        let query_ctx = QueryContext::for_state(&self.state, req.tenant_id);
        let tasks = match query_ctx.plan_sql(&req.sql, tenant_id).await {
            Ok(t) => t,
            Err(e) => {
                return ForwardResponse {
                    success: false,
                    payloads: vec![],
                    error_message: format!("plan failed: {e}"),
                };
            }
        };

        if tasks.is_empty() {
            return ForwardResponse {
                success: true,
                payloads: vec![],
                error_message: String::new(),
            };
        }

        // Dispatch each task to the local Data Plane.
        let mut payloads = Vec::with_capacity(tasks.len());
        for task in tasks {
            let request_id = self.next_request_id();
            let request = Request {
                request_id,
                tenant_id: task.tenant_id,
                vshard_id: task.vshard_id,
                plan: task.plan,
                deadline: Instant::now() + deadline,
                priority: Priority::Normal,
                trace_id: req.trace_id,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
            };

            let rx = self.state.tracker.register_oneshot(request_id);

            let dispatch_result = match self.state.dispatcher.lock() {
                Ok(mut d) => d.dispatch(request),
                Err(poisoned) => poisoned.into_inner().dispatch(request),
            };

            if let Err(e) = dispatch_result {
                return ForwardResponse {
                    success: false,
                    payloads,
                    error_message: format!("dispatch failed: {e}"),
                };
            }

            match tokio::time::timeout(deadline, rx).await {
                Ok(Ok(resp)) => {
                    if resp.status == crate::bridge::envelope::Status::Error {
                        let err_msg = resp
                            .error_code
                            .as_ref()
                            .map(|c| format!("{c:?}"))
                            .unwrap_or_else(|| "unknown error".into());
                        return ForwardResponse {
                            success: false,
                            payloads,
                            error_message: err_msg,
                        };
                    }
                    payloads.push(resp.payload.to_vec());
                }
                Ok(Err(_)) => {
                    return ForwardResponse {
                        success: false,
                        payloads,
                        error_message: "response channel closed".into(),
                    };
                }
                Err(_) => {
                    return ForwardResponse {
                        success: false,
                        payloads,
                        error_message: format!("deadline exceeded ({}ms)", deadline.as_millis()),
                    };
                }
            }
        }

        ForwardResponse {
            success: true,
            payloads,
            error_message: String::new(),
        }
    }
}
