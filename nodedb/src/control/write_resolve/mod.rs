// SPDX-License-Identifier: BUSL-1.1

//! Resolve-before-propose for a governed predicate write under Raft.
//!
//! See [`resolver::EngineWriteResolver`] for the protocol and
//! [`run::run_write_resolve`] for the one loop that drives it.

mod columnar;
mod document;
mod graph;
mod kv;
mod propose;
mod resolved_rows;
mod resolver;
mod run;
mod select;
mod timeseries;
mod vector;

pub use columnar::ColumnarWriteResolver;
pub use document::DocumentWriteResolver;
pub use graph::GraphWriteResolver;
pub use kv::KvWriteResolver;
pub use resolved_rows::ResolvedRows;
pub use resolver::{EngineWriteResolver, WriteResolveContext};
pub use run::{MAX_WRITE_RESOLVE_RETRIES, run_authorized_write_resolve, run_write_resolve};
pub use select::resolver_for_plan;
pub use timeseries::TimeseriesWriteResolver;
pub use vector::VectorWriteResolver;
