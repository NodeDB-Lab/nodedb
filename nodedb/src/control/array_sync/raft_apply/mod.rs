// SPDX-License-Identifier: BUSL-1.1

//! Array CRDT apply helpers invoked by the distributed Raft apply loop.
//!
//! These run on the Control Plane after Raft commit. They decode the replicated
//! entry, submit the resulting write through the shared Control-Plane write
//! funnel (which mints this replica's redo record), and update the authoritative
//! op-log / schema registry. See [`crate::control::distributed_applier`] for the
//! loop that calls these.
//!
//! Split by concern:
//! - [`op`]: the committed Lite-sync `ArrayOp` CRDT apply path.
//! - [`cell`]: the committed Raft-native array cell write (`ArrayCellPut` /
//!   `ArrayCellDelete`) apply path — the cluster SQL DML array path.
//! - [`schema`]: the committed `ArraySchema` apply path.
//! - [`common`]: shared scaffolding (position id, funnel submit, request
//!   builder, response await, array-open bootstrap) reused
//!   across all three.

mod cell;
mod common;
mod op;
mod schema;

pub(crate) use cell::{ArrayCellTarget, apply_array_cell_write};
pub(crate) use common::AppliedPosition;
pub(crate) use op::apply_array_op;
pub(crate) use schema::{ArraySchemaPayload, apply_array_schema};
