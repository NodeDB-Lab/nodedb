// SPDX-License-Identifier: BUSL-1.1

//! KV engine operation handlers for the Data Plane executor.

pub(in crate::data::executor) mod atomic;
pub(in crate::data::executor) mod batch;
pub(in crate::data::executor) mod crud;
mod dispatch;
mod dispatch_scan;
mod dispatch_transfer;
pub(in crate::data::executor) mod field;
mod index;
mod materialize_scan;
pub(in crate::data::executor) mod predicate;
pub(in crate::data::executor) mod resolve;
pub(in crate::data::executor) mod rls;
mod scan;
pub(in crate::data::executor) mod sorted;
pub(in crate::data::executor) mod sorted_index_compute;
pub(in crate::data::executor) mod transfer;
pub(in crate::data::executor) mod ttl;

pub(in crate::data::executor) mod field_compute;
pub(in crate::data::executor) mod transfer_compute;
