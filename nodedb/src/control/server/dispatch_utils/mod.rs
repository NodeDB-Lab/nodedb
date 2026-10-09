// SPDX-License-Identifier: BUSL-1.1

//! Shared dispatch utilities used by both the pgwire and native endpoints.

mod change_events;
mod dispatch;
mod durability_barrier;
mod durable_write;
mod minted;
mod owner_read;
mod submit_write;
mod types;
mod unlogged_dispatch;
mod write_abort;

pub(crate) use change_events::publish_settled_changes;
pub use dispatch::{dispatch_authorized_autocommit_write, dispatch_authorized_to_data_plane};
pub(crate) use dispatch::{
    dispatch_autocommit_write, dispatch_replayed_write_to_data_plane, dispatch_to_data_plane,
    dispatch_trusted_internal_write_to_data_plane,
};
pub use durability_barrier::writes_acked_without_durability;
pub(crate) use durable_write::{
    dispatch_authorized_durable_write, dispatch_authorized_durable_write_with_source,
    dispatch_authorized_task_by_class, dispatch_durable_autocommit_write,
};
pub(crate) use minted::{MintedRecords, RecordOwner, SentRecords};
pub(crate) use owner_read::{
    OwnedRead, OwnedReadScope, ReadPlacement, not_found_response, ok_payload_response,
    owner_response, prepare_local_pass, read_placement, route_owned_read,
};
pub(crate) use submit_write::{
    ChangeFeedOwner, PendingWrite, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering,
    dispatch_when_capacity_frees, enqueue_write, submit_write,
};
pub(crate) use types::{AutocommitWrite, WriteDispatch};
pub(crate) use unlogged_dispatch::{
    dispatch_routed_read_to_data_plane, dispatch_to_data_plane_with_txn,
};
pub(crate) use write_abort::{
    error_is_final_refusal, refusal_is_final, write_definitely_not_applied,
};
