// SPDX-License-Identifier: BUSL-1.1

//! Helper for projecting post-write documents into a `RowsPayload` msgpack
//! blob the Control Plane decodes into multi-column pgwire rows. The single
//! choke point where the RLS read policy applies to DML output — every
//! caller passes full pre-projection documents so the policy sees columns
//! `RETURNING` never mentions.

mod columnar;
mod document;
mod kv;
mod project;
mod timeseries;
mod vector;

pub(in crate::data::executor) use document::StoredRow;
pub(in crate::data::executor) use document::build_stored_rows_payload;
pub(in crate::data::executor) use kv::{KvStoredRow, kv_stored_rows_payload};
pub(in crate::data::executor) use project::encode_stored_rows_payload;
pub(in crate::data::executor::handlers) use project::{
    build_rejecting_rows_payload, build_rows_payload,
};
pub(in crate::data::executor) use vector::{
    VectorRowColumns, VectorStoredRow, vector_stored_rows_payload,
};
