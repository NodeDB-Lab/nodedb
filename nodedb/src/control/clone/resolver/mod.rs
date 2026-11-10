// SPDX-License-Identifier: BUSL-1.1

//! Copy-on-write read resolution algorithm.
//!
//! For a `Shadowed` or `Materializing` clone, resolves one physical task into
//! the target task plus its source-side chain-walk twins, which
//! `shared::clone_read` dispatches and merges. A collection that is not a
//! clone, or is fully `Materialized`, resolves to `None`.

pub mod filter;
pub mod refusal;
pub mod resolve;
pub mod rewrite;
pub mod rewrite_engine;

pub use filter::filter_tombstoned_rows;
pub use refusal::SourceRewrite;
pub use resolve::{CloneReadParams, ResolveOutcome, reads_live_clone, resolve_read};
