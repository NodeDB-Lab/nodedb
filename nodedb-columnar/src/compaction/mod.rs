// SPDX-License-Identifier: Apache-2.0

//! Single-segment compaction: rewrite a segment without its purged rows.
//!
//! Origin never rewrites a flushed segment: its segments are write-once and
//! ended rows are resolved at read time. Embedded (Lite) deployments rewrite
//! a segment in place once its delete ratio crosses a threshold. The rewrite
//! drops every purged row (ended, with no `sys_to`), keeps every open row and
//! every history row with its stamps, and encodes a new segment from the
//! survivors. `MutationEngine::install_compacted_segment` remaps the engine's
//! indexes, chains, stamps, and delete bitmap to the new row positions.

pub mod segment;

pub use segment::{CompactionResult, DEFAULT_DELETE_RATIO_THRESHOLD, compact_segment};
