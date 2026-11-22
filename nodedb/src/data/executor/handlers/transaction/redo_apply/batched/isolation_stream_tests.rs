// SPDX-License-Identifier: BUSL-1.1

//! A parked install holds a client's unkeyed read back by its stream alone.
//!
//! The install parks on [`super::equivalence_tests::FILL`]. A list of a
//! tenant's collections reads core state no key bounds, so it waits for
//! the install. A client's list holds only later tasks of its own
//! connection and vShard. An internal list holds every later task.

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::bridge::envelope::{Request, RequestStream};
use crate::control::server::shared::session::ConnectionId;

use super::isolation_tests::{OTHER, TASK, parked_install_with, point_put};
use super::yield_tests::{INSTALL, OTHER_VSHARD, Ring, request, tick_until_answered_while_waiting};

/// The request id of the write queued behind the list.
const WRITE: u64 = TASK + 1;

/// Request `id` of `plan` on `stream`, with its events off the install's
/// vShard.
fn on_stream(stream: RequestStream, id: u64, plan: PhysicalPlan) -> Request {
    let mut request = request(id, plan);
    request.vshard_id = OTHER_VSHARD;
    request.stream = stream;
    request
}

fn connection(id: u64) -> RequestStream {
    RequestStream::client(1, ConnectionId::new(id).expect("non-zero id"))
}

fn list() -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::ListTenantCollections { tenant_id: 1 })
}

/// A core whose install parked with the list of `stream` queued behind
/// it, then a keyed write of [`OTHER`] on connection 2.
fn parked_list_then_write(stream: RequestStream) -> Ring {
    let mut ring = parked_install_with(
        |ring| ring.push_fill_install(),
        on_stream(stream, TASK, list()),
    );
    ring.push(
        on_stream(connection(2), WRITE, point_put(OTHER, Vec::new())),
        None,
    );
    ring.tick();
    ring
}

/// A client's list waits for the install. Another connection's keyed write
/// of an untouched collection runs meanwhile.
#[test]
fn a_client_list_waits_and_another_connections_write_runs() {
    let mut ring = parked_list_then_write(connection(1));
    assert!(
        ring.answer(WRITE).is_some(),
        "the other connection's write ran between the install's batches"
    );
    assert!(ring.answer(TASK).is_none(), "the list waits");
    assert!(
        ring.answer(INSTALL).is_none(),
        "the install is still parked"
    );
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[TASK]);
    ring.tick_until_answered(TASK);
}

/// An internal list holds every later task until the install ends.
#[test]
fn an_internal_list_holds_every_later_task() {
    let mut ring = parked_list_then_write(RequestStream::Internal);
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[TASK, WRITE]);
    ring.tick_until_answered(TASK);
    ring.tick_until_answered(WRITE);
}
