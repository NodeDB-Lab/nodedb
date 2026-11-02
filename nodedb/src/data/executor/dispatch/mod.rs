// SPDX-License-Identifier: BUSL-1.1

//! Main execute() dispatch: matches on PhysicalPlan variant and delegates
//! to the appropriate per-engine sub-dispatcher.

pub mod array;
pub mod bitmap;
pub mod columnar;
pub mod crdt;
pub mod document;
mod document_admit;
mod document_dml;
mod document_register;
mod execute;
pub mod graph;
pub mod kv;
pub mod meta;
mod meta_newest;
pub mod meta_retention;
pub mod query;
pub mod spatial;
pub mod text;
pub mod timeseries;
pub mod vector;
pub mod visitor;
