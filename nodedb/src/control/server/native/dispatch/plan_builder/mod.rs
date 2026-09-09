// SPDX-License-Identifier: BUSL-1.1

//! Physical plan construction from NDB opcodes.
//!
//! Each engine has its own sub-module with builder functions.
//! `build_plan()` dispatches to the appropriate engine module.

pub(crate) mod columnar;
pub(crate) mod crdt;
mod dispatch;
pub(crate) mod document;
pub(crate) mod graph;
mod helpers;
pub(crate) mod kv;
pub(crate) mod query;
pub(crate) mod spatial;
pub(crate) mod text;
pub(crate) mod timeseries;
pub(crate) mod vector;

pub(crate) use dispatch::build_plan;
pub(super) use helpers::{collection_type, declared_primary_key, parse_direction, require_doc_id};
