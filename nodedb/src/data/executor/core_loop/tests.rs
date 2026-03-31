use super::*;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request, Status};
use crate::bridge::physical_plan::{DocumentOp, MetaOp};
use crate::types::*;
use nodedb_bridge::buffer::RingBuffer;
use std::time::{Duration, Instant};

fn make_core() -> (
    CoreLoop,
    Producer<BridgeRequest>,
    Consumer<BridgeResponse>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(0, req_rx, resp_tx, dir.path()).unwrap();
    (core, req_tx, resp_rx, dir)
}

pub fn make_core_with_dir(
    dir: &std::path::Path,
) -> (CoreLoop, Producer<BridgeRequest>, Consumer<BridgeResponse>) {
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(0, req_rx, resp_tx, dir).unwrap();
    (core, req_tx, resp_rx)
}

fn make_request(plan: PhysicalPlan) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: 0,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
    }
}

#[test]
fn empty_tick_processes_nothing() {
    let (mut core, _, _, _dir) = make_core();
    assert_eq!(core.tick(), 0);
}

#[test]
fn expired_task_returns_deadline_exceeded() {
    let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();
    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                deadline: Instant::now() - Duration::from_secs(1),
                ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                    rls_filters: Vec::new(),
                }))
            },
        })
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(resp.inner.status, Status::Error);
    assert_eq!(resp.inner.error_code, Some(ErrorCode::DeadlineExceeded));
}

#[test]
fn watermark_in_response() {
    let (mut core, mut req_tx, mut resp_rx, _dir) = make_core();
    core.advance_watermark(Lsn::new(99));
    core.sparse.put(1, "x", "y", b"data").unwrap();
    req_tx
        .try_push(BridgeRequest {
            inner: make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "x".into(),
                document_id: "y".into(),
                rls_filters: Vec::new(),
            })),
        })
        .unwrap();
    core.tick();
    let resp = resp_rx.try_pop().unwrap();
    assert_eq!(resp.inner.watermark_lsn, Lsn::new(99));
}

#[test]
fn cancel_removes_pending_task() {
    let (mut core, mut req_tx, _resp_rx, _dir) = make_core();
    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                request_id: RequestId::new(10),
                deadline: Instant::now() + Duration::from_secs(60),
                ..make_request(PhysicalPlan::Document(DocumentOp::PointGet {
                    collection: "x".into(),
                    document_id: "y".into(),
                    rls_filters: Vec::new(),
                }))
            },
        })
        .unwrap();
    core.drain_requests();
    assert_eq!(core.pending_count(), 1);

    req_tx
        .try_push(BridgeRequest {
            inner: Request {
                request_id: RequestId::new(99),
                priority: Priority::Critical,
                consistency: ReadConsistency::Eventual,
                ..make_request(PhysicalPlan::Meta(MetaOp::Cancel {
                    target_request_id: RequestId::new(10),
                }))
            },
        })
        .unwrap();
    assert_eq!(core.tick(), 2);
}
