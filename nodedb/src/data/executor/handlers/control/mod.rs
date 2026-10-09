// SPDX-License-Identifier: BUSL-1.1

//! Control operation handlers — module root.

pub mod calvin;
mod calvin_active_verify;
mod calvin_overlay_stage;
mod calvin_overlay_stage_bulk;
mod calvin_passive_read;
pub mod calvin_reply;
mod calvin_resolve;
mod calvin_txn_id;
mod checkpoint_crdt;
mod checkpoint_durable_lsn;
mod checkpoint_install;
pub mod convert;
pub mod crdt;
pub mod crdt_apply;
pub mod crdt_constraints;
pub mod crdt_doc;
pub mod crdt_list;
pub mod crdt_materialize;
pub mod crdt_preview;
mod range_scan_versioned;
pub mod reindex;
pub mod snapshot;
pub mod synonym_group;

#[cfg(test)]
pub(in crate::data::executor) use calvin_txn_id::calvin_synthetic_txn_id;
