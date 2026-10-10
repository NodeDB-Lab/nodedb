// SPDX-License-Identifier: BUSL-1.1

//! Background apply loop — reads committed Raft entries from the mpsc channel,
//! enqueues each through the shared Control-Plane write funnel (which appends
//! each entry's redo record on THIS replica before the enqueue), and resolves
//! propose waiters with the result.
//!
//! Entries of a group start in log order, so every core receives them in the
//! order the log fixed. Their outcomes are collected independently: a write
//! the core parks holds only its own position. Each group's applied index and
//! durable floor (see [`super::applied_index`]) advance in log order, to the
//! highest entry with every earlier entry finished and durable, so the next
//! boot replays only above the floor and no entry is applied by both WAL
//! replay and Raft log replay.
//!
//! Split by concern:
//! - [`driver`]: takes batches off the apply channel and collects finished
//!   applies.
//! - [`pipeline`]: every group's lane and the applies that run.
//! - [`lane`]: one group's queued and started entries, settled in log order.
//! - [`start`]: prepares one entry and routes it to its apply path.
//! - [`cut_barrier`]: a backup cut's barrier: the group's cut floor, the
//!   restore point's place, and a database backup's capture.
//! - [`context`]: the handles an apply borrows, and the futures the loop
//!   collects.
//! - [`calvin_read_result`]: forwards a committed `CalvinReadResult` entry to
//!   the local Calvin scheduler.
//! - [`calvin_redo`]: claims a stamped Calvin redo's position in the applied
//!   ledger and reports its install to the vShard's scheduler.
//! - [`write_dispatch`]: the generic decode + write-funnel enqueue path.
//! - [`transaction_redo`]: a committed transaction's redo, stamped with its
//!   Raft entry and applied through the WAL replay arms.
//! - [`redo_chunk`]: the chunks, the abandon and the final entry of a
//!   chunked redo stream.
//! - [`topic_publish`]: a committed durable-topic publication, appended to
//!   this replica's topic log at the entry's position.
//! - [`proposal_gate`]: skips a second committed copy of an applied proposal
//!   and records each applied proposal in the ledger.
//! - [`snapshot_gate`]: skips an entry an installed snapshot covers, and
//!   orders each write before a later snapshot restore.
//! - [`group_watch`]: per-group second-apply detection and backup cut floors.
//! - [`bookkeeping`]: applied-floor persistence + Raft log compaction trigger.
//! - [`helpers`]: shared response/result classification helpers.
//! - [`metadata_floor`]: holds a write until this node's catalog reached the
//!   one its proposer planned it against.
//! - [`array_cell_route`]: routes an array cell write to the incarnation its
//!   proposer wrote against.
//! - [`collection_route`]: applies a collection write only while each
//!   collection it names holds the incarnation its proposer planned against.

mod array_cell_route;
mod bookkeeping;
mod calvin_read_result;
mod calvin_redo;
mod collection_route;
mod context;
mod cut_barrier;
mod driver;
mod group_watch;
mod helpers;
mod lane;
mod metadata_floor;
mod pipeline;
mod proposal_gate;
mod redo_chunk;
mod snapshot_gate;
mod start;
mod surrogate_bind;
mod topic_publish;
mod transaction_redo;
mod write_dispatch;

pub use driver::run_apply_loop;
