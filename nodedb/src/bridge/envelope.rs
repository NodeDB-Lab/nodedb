use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

/// Response payload: heap-allocated bytes behind an `Arc<[u8]>`.
///
/// The `Deref<Target=[u8]>` impl provides transparent byte access.
/// Slab-backed zero-copy transport is defined in `super::slab` and will be
/// wired in once the Data Plane slab pool is integrated.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Heap-allocated payload.
    Heap(Arc<[u8]>),
}

impl Payload {
    /// Create a heap-backed payload from a Vec.
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self::Heap(Arc::from(v.into_boxed_slice()))
    }

    /// Create an empty payload.
    pub fn empty() -> Self {
        Self::Heap(Arc::from([].as_slice()))
    }

    /// Create from Arc<[u8]> (backward compat).
    pub fn from_arc(a: Arc<[u8]>) -> Self {
        Self::Heap(a)
    }

    /// Get the payload bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Heap(a) => a,
        }
    }

    /// Whether this payload is empty.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Convert to Vec<u8>.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Deref for Payload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<Arc<[u8]>> for Payload {
    fn from(a: Arc<[u8]>) -> Self {
        Self::Heap(a)
    }
}
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};

/// Request envelope: Control Plane -> Data Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Request {
    /// Globally unique request identifier (monotonic per connection).
    pub request_id: RequestId,

    /// Tenant scope — all data access is tenant-scoped by construction.
    pub tenant_id: TenantId,

    /// Target virtual shard.
    pub vshard_id: VShardId,

    /// Opaque plan digest identifying the physical operation to execute.
    pub plan: PhysicalPlan,

    /// Absolute deadline. Data Plane MUST stop at next safe point after expiry.
    pub deadline: Instant,

    /// Request priority for scheduling on the Data Plane.
    pub priority: Priority,

    /// Distributed trace identifier for cross-plane observability.
    pub trace_id: u64,

    /// Read consistency level for this request.
    pub consistency: ReadConsistency,

    /// Optional idempotency key for non-idempotent writes.
    /// If present, the Data Plane deduplicates by skipping execution
    /// when the same key has already been processed (returns the
    /// cached response status).
    pub idempotency_key: Option<u64>,
}

/// Response envelope: Data Plane -> Control Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Response {
    /// Echoed request identifier for correlation.
    pub request_id: RequestId,

    /// Outcome status.
    pub status: Status,

    /// Attempt number (for retry tracking).
    pub attempt: u32,

    /// Whether this is a partial result (more coming).
    pub partial: bool,

    /// Payload bytes produced by this response chunk.
    pub payload: Payload,

    /// Watermark LSN at the time of read (for snapshot consistency tracking).
    pub watermark_lsn: Lsn,

    /// Error code if status is not Ok.
    pub error_code: Option<ErrorCode>,
}

pub use super::physical_plan::PhysicalPlan;

/// Request priority. Higher priority requests are scheduled first on the Data Plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Background tasks (compaction, GC).
    Background = 0,
    /// Normal query traffic.
    Normal = 1,
    /// Elevated (e.g., interactive queries with tight deadlines).
    High = 2,
    /// System-critical (WAL replay, leader election responses).
    Critical = 3,
}

/// Response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Success.
    Ok,
    /// Partial success — more response chunks follow.
    Partial,
    /// Request failed with error.
    Error,
}

/// Deterministic error codes returned by the Data Plane.
///
/// Final outcomes are explicit, never opaque strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// Request exceeded its deadline.
    DeadlineExceeded,
    /// Constraint violation at commit time.
    RejectedConstraint { constraint: String },
    /// Pre-validation fast-reject.
    RejectedPrevalidation { reason: String },
    /// Document/collection not found.
    NotFound,
    /// Authorization failure.
    RejectedAuthz,
    /// Write conflict — client should retry.
    ConflictRetry,
    /// Fan-out limit exceeded for graph/scatter queries.
    FanOutExceeded,
    /// Memory budget exhausted — DataFusion should spill.
    ResourcesExhausted,
    /// Edge creation rejected: source or destination node does not exist.
    RejectedDanglingEdge { missing_node: String },
    /// Duplicate write detected via idempotency key.
    DuplicateWrite,
    /// Internal error (io_uring failure, corruption, etc.)
    Internal { detail: String },
}

impl From<crate::Error> for ErrorCode {
    fn from(e: crate::Error) -> Self {
        match e {
            crate::Error::DeadlineExceeded { .. } => Self::DeadlineExceeded,
            crate::Error::RejectedConstraint { constraint, .. } => {
                Self::RejectedConstraint { constraint }
            }
            crate::Error::RejectedPrevalidation { reason, .. } => {
                Self::RejectedPrevalidation { reason }
            }
            crate::Error::CollectionNotFound { .. } | crate::Error::DocumentNotFound { .. } => {
                Self::NotFound
            }
            crate::Error::RejectedAuthz { .. } => Self::RejectedAuthz,
            crate::Error::ConflictRetry { .. } => Self::ConflictRetry,
            crate::Error::FanOutExceeded { .. } => Self::FanOutExceeded,
            crate::Error::MemoryExhausted { .. } => Self::ResourcesExhausted,
            other => Self::Internal {
                detail: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::physical_plan::{DocumentOp, MetaOp};
    use std::time::Duration;

    fn sample_request() -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "users".into(),
                document_id: "doc-1".into(),
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0xABCD,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
        }
    }

    #[test]
    fn request_fields_accessible() {
        let req = sample_request();
        assert_eq!(req.request_id, RequestId::new(1));
        assert_eq!(req.tenant_id, TenantId::new(1));
        assert_eq!(req.trace_id, 0xABCD);
    }

    #[test]
    fn response_ok() {
        let resp = Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_arc(Arc::from(b"result".as_slice())),
            watermark_lsn: Lsn::new(42),
            error_code: None,
        };
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.watermark_lsn, Lsn::new(42));
        assert_eq!(&*resp.payload, b"result");
    }

    #[test]
    fn response_error() {
        let resp = Response {
            request_id: RequestId::new(2),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(ErrorCode::DeadlineExceeded),
        };
        assert_eq!(resp.error_code, Some(ErrorCode::DeadlineExceeded));
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::Background < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }

    #[test]
    fn cancel_plan() {
        let req = Request {
            request_id: RequestId::new(99),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: RequestId::new(42),
            }),
            deadline: Instant::now() + Duration::from_secs(1),
            priority: Priority::Critical,
            trace_id: 0,
            consistency: ReadConsistency::Eventual,
            idempotency_key: None,
        };
        match req.plan {
            PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
                assert_eq!(target_request_id, RequestId::new(42));
            }
            _ => panic!("expected Cancel plan"),
        }
    }
}
