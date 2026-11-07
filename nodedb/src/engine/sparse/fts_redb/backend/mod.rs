// SPDX-License-Identifier: BUSL-1.1

mod core;
mod doc_lengths;
mod history_rows;
mod history_store;
mod meta;
pub(crate) mod posting_rows;
mod postings;
mod purge;
mod segments;
mod shared;
mod stats;

pub use core::RedbFtsBackend;
pub(crate) use history_store::{RedbHistoryRead, RedbHistoryTxn};
pub use segments::CompactCommit;
