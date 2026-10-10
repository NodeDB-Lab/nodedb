// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral handlers for atomic KV SQL functions: KV_INCR, KV_DECR,
//! KV_INCR_FLOAT, KV_CAS, KV_GETSET.
//!
//! These are side-effecting operations that dispatch to the Data Plane via the
//! SPSC bridge, so they cannot be pure DataFusion UDFs. Instead they're
//! intercepted in the DDL router before DataFusion parsing. Each builds a
//! single-text-column `DdlResult` carrying the Data Plane's JSON payload.
//!
//! [`handlers`] holds the four public `SELECT KV_*(...)` entry points.
//! [`dispatch`] holds the shared in-transaction dispatch path
//! (`dispatch_and_respond`, reused by the sibling `transfer` module) plus the
//! argument-parsing and response-shaping helpers every KV DDL family in this
//! directory reuses (`kv_sorted_index`, `weighted_pick`, `rate_gate`,
//! `transfer`).

pub mod dispatch;
pub mod handlers;

pub(crate) use dispatch::{
    dispatch_and_respond, error_to_ddl, parse_function_args, single_text_col, split_args, unquote,
};
pub use handlers::{kv_cas, kv_getset, kv_incr, kv_incr_float};
