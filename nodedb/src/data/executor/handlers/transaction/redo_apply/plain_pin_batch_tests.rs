// SPDX-License-Identifier: BUSL-1.1

//! Coalesced replicated puts derive their rows under their proposer's pins.
//!
//! `poll_write_batch` absorbs consecutive puts into one transaction. A batch
//! holds puts of one pin set only, and it applies under that set's view. Two
//! cores with different current configs coalesce the same pinned puts and
//! store the same bytes.

use std::sync::Arc;

use nodedb_physical::physical_plan::{DocConfigWire, DocumentOp, PhysicalPlan, RedoPins};
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, Value};

use super::install_pin_tests::{
    COLL, generated_wire, guarded_wire, pinned, register, schemaless_wire, stored, strict_v1,
    strict_wire,
};
use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_request};
use crate::data::executor::task::ExecutionTask;
use crate::types::RequestId;

/// A core whose current config of `tickets` is `wire`, and the ring its
/// responses land on.
struct BatchCore {
    core: CoreLoop,
    responses: nodedb_bridge::buffer::Consumer<BridgeResponse>,
    _requests: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
    _dir: tempfile::TempDir,
}

fn batch_core(wire: &DocConfigWire) -> BatchCore {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, requests, responses) = make_core_with_dir(dir.path());
    register(&mut core, wire, wire.digest().expect("digest"));
    BatchCore {
        core,
        responses,
        _requests: requests,
        _dir: dir,
    }
}

/// A put of row `t<surrogate>` of `collection` with `status` OPEN.
fn put_into(collection: &str, surrogate: u32) -> DocumentOp {
    let document_id = format!("t{surrogate}");
    let object = [("status".to_string(), Value::String("OPEN".into()))]
        .into_iter()
        .collect();
    DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        pk_bytes: document_id.as_bytes().to_vec(),
        document_id,
        value: nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body"),
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    }
}

/// Queue `op` on `held` as request `id`, carrying `pins`.
fn enqueue(held: &mut BatchCore, id: u64, op: DocumentOp, pins: Option<&RedoPins>) {
    let mut request = make_request(PhysicalPlan::Document(op));
    request.request_id = RequestId::new(id);
    request.config_pins = pins.cloned().map(Arc::new);
    held.core.task_queue.push(ExecutionTask::new(request));
}

/// The next `count` responses `held` sent.
fn responses(held: &mut BatchCore, count: usize) -> Vec<Response> {
    (0..count)
        .map(|_| held.responses.try_pop().expect("a response").inner)
        .collect()
}

fn assert_all_ok(responses: &[Response]) {
    for response in responses {
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }
}

/// Each proposer config, beside the config a replica holds after a later
/// DDL.
fn config_pairs() -> [(DocConfigWire, DocConfigWire); 2] {
    [
        (schemaless_wire(), guarded_wire()),
        (strict_wire(strict_v1()), generated_wire()),
    ]
}

/// Two cores coalesce the same pinned puts and store the same bytes.
#[test]
fn coalesced_pinned_puts_store_the_proposer_rows_on_every_replica() {
    for (proposer, other) in config_pairs() {
        let pins = pinned(&proposer);
        let mut cores = [batch_core(&proposer), batch_core(&other)];
        for held in &mut cores {
            for surrogate in 1..=3 {
                enqueue(
                    held,
                    u64::from(surrogate),
                    put_into(COLL, surrogate),
                    Some(&pins),
                );
            }
            assert_eq!(held.core.poll_write_batch(), 3, "{other:?}");
            assert_all_ok(&responses(held, 3));
        }
        let [on_proposer, on_other] = &cores;
        for surrogate in 1..=3 {
            assert_eq!(
                stored(&on_other.core, surrogate),
                stored(&on_proposer.core, surrogate),
                "{other:?}"
            );
        }
    }
}

/// A batch holds one pin set. A put with other pins, or none, waits for its
/// own batch.
#[test]
fn a_batch_never_merges_two_pin_sets() {
    let first = pinned(&schemaless_wire());
    let second = pinned(&guarded_wire());
    let mut held = batch_core(&schemaless_wire());
    enqueue(&mut held, 1, put_into(COLL, 1), Some(&first));
    enqueue(&mut held, 2, put_into(COLL, 2), Some(&first));
    enqueue(&mut held, 3, put_into(COLL, 3), Some(&second));
    enqueue(&mut held, 4, put_into(COLL, 4), None);
    assert_eq!(held.core.poll_write_batch(), 2);
    assert_all_ok(&responses(&mut held, 2));
    assert_eq!(held.core.task_queue.len(), 2);
    // The front put's pins match no put behind it, so it batches alone and
    // falls back to the single-put path.
    assert_eq!(held.core.poll_write_batch(), 0);
    assert_eq!(held.core.task_queue.len(), 2);
}

/// A coalesced put that writes a collection its pins lack is refused alone
/// and stores nothing. Its siblings store their rows.
#[test]
fn an_unpinned_collection_refuses_one_coalesced_put() {
    let pins = pinned(&schemaless_wire());
    let mut held = batch_core(&schemaless_wire());
    enqueue(&mut held, 1, put_into(COLL, 1), Some(&pins));
    enqueue(&mut held, 2, put_into("ghost", 2), Some(&pins));
    enqueue(&mut held, 3, put_into(COLL, 3), Some(&pins));
    assert_eq!(held.core.poll_write_batch(), 3);
    let answered = responses(&mut held, 3);
    assert_eq!(
        answered[0].status,
        Status::Ok,
        "{:?}",
        answered[0].error_code
    );
    assert!(
        matches!(
            answered[1].error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ),
        "{:?}",
        answered[1].error_code
    );
    assert_eq!(
        answered[2].status,
        Status::Ok,
        "{:?}",
        answered[2].error_code
    );
    assert!(!stored(&held.core, 1).is_empty());
    assert!(!stored(&held.core, 3).is_empty());
    assert!(held.core.apply_scope.judged.is_none());
}
