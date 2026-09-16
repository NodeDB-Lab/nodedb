// SPDX-License-Identifier: BUSL-1.1

mod auto_source;
mod columnar;
mod dispatch;
mod document;
mod kv;
mod reaper;
mod rls_gate;

pub mod progress;
pub mod walker;

pub use progress::CloneMaterializerHandle;
pub use walker::{
    MaterializeParams, force_materialize_blocking, materialize_database, run_scheduled_sweep,
};

// Shared with the `INSERT ... SELECT` orchestrator, which reuses the same
// local-dispatch primitive and source-scan cursor decode.
pub(crate) use auto_source::scan_source_page as scan_source_page_auto;
pub(crate) use dispatch::{dispatch_local, dispatch_local_on_vshard};
pub(crate) use document::read_all_source_rows;
