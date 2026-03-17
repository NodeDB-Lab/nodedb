//! Query forwarding trait for leader-based request routing.
//!
//! When a client connects to a non-leader node, the query is forwarded
//! to the leader for the target vShard. The [`RequestForwarder`] trait
//! abstracts local execution so the cluster crate doesn't depend on the
//! main binary's SharedState or pgwire infrastructure.

use crate::rpc_codec::{ForwardRequest, ForwardResponse};

/// Trait for executing forwarded SQL queries on the local Data Plane.
///
/// Implemented by the main binary crate using SharedState + QueryContext.
/// The cluster RPC handler calls this when it receives a `ForwardRequest`.
pub trait RequestForwarder: Send + Sync + 'static {
    /// Execute a forwarded SQL query locally and return the result.
    ///
    /// The implementation should:
    /// 1. Create a synthetic identity from the tenant_id (trusted node-to-node)
    /// 2. Plan the SQL through DataFusion
    /// 3. Dispatch to the local Data Plane
    /// 4. Collect response payloads
    /// 5. Return them in a ForwardResponse
    fn execute_forwarded(
        &self,
        req: ForwardRequest,
    ) -> impl std::future::Future<Output = ForwardResponse> + Send;
}

/// No-op forwarder for single-node mode or testing.
pub struct NoopForwarder;

impl RequestForwarder for NoopForwarder {
    async fn execute_forwarded(&self, _req: ForwardRequest) -> ForwardResponse {
        ForwardResponse {
            success: false,
            payloads: vec![],
            error_message: "query forwarding not available (single-node mode)".into(),
        }
    }
}
