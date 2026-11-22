// SPDX-License-Identifier: BUSL-1.1

//! Convert write-side PhysicalPlan variants to ReplicatedWrite for Raft proposal.
//!
//! Split by `PhysicalPlan` family. Each `entry_*` module holds the exhaustive
//! per-op write/not-write classification for one engine; its sibling module
//! holds the wire encoders it calls into. [`entry`] is the top-level dispatcher.
//! [`transaction_redo`] encodes a committed transaction's redo, which no plan
//! produces.

mod columnar;
mod crdt;
mod document;
mod document_join;
mod entry;
mod entry_array;
mod entry_columnar_family;
mod entry_document;
mod entry_graph;
mod entry_kv;
mod graph;
mod kv;
mod transaction_redo;
mod vector;

pub use entry::to_replicated_entry;
pub use transaction_redo::{
    CHUNK_ENTRY_ENVELOPE_BYTES, CalvinEntryStamps, RedoChunkSlot, RedoEntryTarget, RedoProposal,
    calvin_redo_proposal, chunk_count, chunk_entry, chunk_room, chunked_final_entry,
    chunks_complete, chunks_overrun, declare_final_keys, final_entry_stream, fits_inline,
    redo_abandon_entry, redo_header_frames, session_redo_proposal, stamp_calvin_final,
    transaction_redo_entry,
};
