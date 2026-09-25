// SPDX-License-Identifier: BUSL-1.1

//! Shared dispatch utilities used by both the pgwire and native endpoints.

mod change_events;
mod collect;
mod dispatch;
mod durability_barrier;
mod durable_write;
mod error_status;
mod minted;
mod submit_write;
mod types;
mod write_abort;

pub(crate) use change_events::{
    WriteChangeSet, extract_write_change_set, publish_change_set_with_lsn,
    publish_cluster_array_change_events, publish_origin_change_events,
};
pub(crate) use collect::{
    DeadlineCollect, DispatchCollectError, collect_bounded_response, collect_under_deadline,
};
pub use dispatch::{dispatch_authorized_autocommit_write, dispatch_authorized_to_data_plane};
pub(crate) use dispatch::{
    dispatch_authorized_autocommit_write_with_source, dispatch_authorized_minted_to_data_plane,
    dispatch_autocommit_write, dispatch_to_data_plane, dispatch_to_data_plane_with_txn,
    dispatch_trusted_internal_write_to_data_plane,
};
pub use durability_barrier::writes_acked_without_durability;
pub(crate) use durable_write::{
    dispatch_authorized_durable_write, dispatch_authorized_task_by_class,
    dispatch_durable_autocommit_write,
};
pub(crate) use error_status::reject_data_plane_error;
pub(crate) use minted::{
    Collect, MintedRecords, OwnedResponse, OwnedWait, RecordOwner, await_response_owned,
};
pub(crate) use submit_write::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
pub(crate) use types::{AutocommitWrite, WriteDispatch};
pub(crate) use write_abort::{
    error_is_final_refusal, refusal_is_final, write_definitely_not_applied,
};
