// SPDX-License-Identifier: BUSL-1.1

//! Edge write handlers: EdgePut, EdgePutBatch, EdgeDelete, EdgeDeleteBatch.
//!
//! Split out of `graph.rs` to keep that file under the file-size limit; see
//! its module doc for the scoping rules that also apply here. Split further,
//! by op, to stay under the per-file line limit.

mod delete;
mod delete_batch;
mod put;
mod put_batch;
mod shared;

pub(in crate::data::executor) use shared::{EdgeDeleteParams, EdgePutParams};
