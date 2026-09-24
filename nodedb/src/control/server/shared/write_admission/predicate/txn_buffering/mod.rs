// SPDX-License-Identifier: BUSL-1.1

//! `plan_requires_txn_buffering`: is this in-transaction statement a write
//! that must be buffered until COMMIT, or a read that executes immediately?
//!
//! `route_in_tx_write` (`control/server/shared/session/staging_gate.rs`) must
//! not answer this by calling `to_replicated_entry(..).is_some()` directly —
//! that function is a WAL/Raft ENCODER, not a classifier: it ends in a
//! catch-all `_ => None`, so any `PhysicalPlan` variant it has no encoder arm
//! for would be silently treated as a read (executed immediately, visible
//! before COMMIT, and NOT rolled back by ROLLBACK).
//!
//! `plan_requires_txn_buffering` reproduces `to_replicated_entry(..).is_some()`
//! variant-for-variant as a compile-time-exhaustive match, so a new
//! `PhysicalPlan` variant forces an explicit staging decision instead of
//! silently inheriting a wrong default. For most arms this is a
//! behavior-preserving reclassification, not a bug fix: every remaining
//! `false` arm below that is semantically a write (`required_permission` says
//! `Write`) carries a comment stating that fact about TODAY's behavior, and
//! fixing those is a separate, later change.
//!
//! One documented set of arms is the exception:
//! `DocumentOp::{Merge, UpdateFromJoin}` and `CrdtOp::RestoreToVersion`
//! classify `true` here even though `to_replicated_entry` has no encoder arm
//! for any of them — a deliberate divergence from the oracle (see the
//! equivalence test below, which pins the divergence explicitly rather than
//! papering over it).
//! `ArrayOp::{Put, Delete}` are NOT in this exception list:
//! `to_replicated_entry` has encoder arms for both (see
//! `control/wal_replication/encode/entry_array.rs::array_write`, which emits
//! the Raft-native `ArrayCellPut` / `ArrayCellDelete` cluster-write variants),
//! so they do not diverge from the oracle and are covered by
//! `array_and_cluster_array_variants_match_oracle` below.
//! `VectorOp::{DeleteBySurrogate, SparseInsert, SparseDelete,
//! MultiVectorInsert, MultiVectorDelete, DirectUpsert, DirectInsert,
//! DirectInsertIfAbsent, DirectDelete, DirectTruncate, DirectUpdate}` are
//! likewise NOT in this exception list: `to_replicated_entry` has encoder
//! arms for all eleven (see `control/wal_replication/encode/vector.rs::encode`), so they
//! do not diverge from the oracle and are covered by
//! `vector_variants_match_oracle` below.
//! `CrdtOp::{ListInsert, ListDelete, ListMove}` are likewise NOT in this
//! exception list: `to_replicated_entry` has encoder arms for all
//! three (see `control/wal_replication/encode/crdt.rs::encode`), so they do
//! not diverge from the oracle and are covered by
//! `crdt_variants_match_oracle` below.
//! `DocumentOp::BatchInsert` and `CrdtOp::{SetConstraints, DropConstraints}`
//! were also in this exception list, but `to_replicated_entry` now has
//! encoder arms for all three (see
//! `control/wal_replication/encode/document.rs::encode` and
//! `control/wal_replication/encode/crdt.rs::encode`), so they too no longer
//! diverge from the oracle and were moved into `document_variants_match_oracle`
//! / `crdt_variants_match_oracle` below. Without buffering,
//! each of these executed immediately against base state inside an explicit
//! transaction, was visible before COMMIT, and survived ROLLBACK: a
//! correctness bug, not a classification nuance. Closing it has two
//! documented consequences:
//!
//! 1. RYOW LOSS: a `Buffered` plan does not stage into the per-transaction
//!    overlay, so a read later in the SAME transaction does not observe the
//!    write until COMMIT. This matches how bulk Document DML already behaved
//!    in a transaction before it was staged. The single-node
//!    `ArrayOp::{Put, Delete}` is exempt: `is_stageable_write` routes it
//!    through `MetaOp::StageWrite` into `ArrayTxnOverlay`, so same-transaction
//!    array reads see it. The `ClusterArrayOp` wrapper is still `Buffered`.
//! 2. ONE INSTALL: COMMIT resolves every buffered plan into the
//!    transaction's redo record, and the record installs with undo. A
//!    sub-record that fails while it installs rolls back every write of the
//!    record before it.
//!
//! `ClusterArrayOp::{Put, Delete}` also classifies `true` while
//! `to_replicated_entry` has no encoder arm for either: they are Control-Plane
//! routing wrappers, and `session::txn_expand` reshapes each into per-shard
//! `ArrayOp::{Put, Delete}` plans before they enter the buffer, so COMMIT
//! replays those (already encoded) and never the wrapper. Without buffering,
//! an `INSERT INTO ARRAY` inside a transaction applied immediately and
//! survived ROLLBACK.
//!
//! A second, inverse divergence exists in the opposite direction:
//! `KvOp::{RegisterIndex, DropIndex}` classify `false` here (not buffered)
//! even though `to_replicated_entry` has an encoder arm for each and returns
//! `Some`. This is not a bug in either function: both are autocommit-only —
//! `resolve/entry.rs` (`data/executor/handlers/transaction/resolve/entry.rs`,
//! Kv index arm) rejects them with `PlanError` when they appear inside an
//! explicit transaction, so they are never routed through
//! `plan_requires_txn_buffering` for staging in practice. They only ever
//! reach `to_replicated_entry` via the autocommit path, where they replicate
//! normally. Pinned by `truncate_is_buffered_and_index_variants_are_not`
//! below via `assert_encoded_but_not_buffered` — the inverse of
//! `assert_buffered_but_unencoded` — and correspondingly excluded from
//! `kv_variants_match_oracle`. `VectorOp::{SetParams, DropIndex}` take the
//! same inverse divergence for the same reason: each rides its own
//! autocommit `VectorParams` / `VectorIndexDrop` record, and a transaction's
//! redo record carries no index DDL.
//!
//! `DocumentOp::Truncate`, `KvOp::Truncate`, `VectorOp::DirectTruncate`,
//! `ColumnarOp::Truncate`, and `TimeseriesOp::Truncate` classify `true`: in a
//! transaction they stage as a `TxnOverlay` truncate marker that hides every
//! base row without a newer overlay entry, and COMMIT replays the live
//! truncate in statement order. ROLLBACK and ROLLBACK TO SAVEPOINT drop the
//! marker.

#![deny(clippy::wildcard_enum_match_arm)]

pub mod classify;
pub mod task_set;

pub use classify::plan_requires_txn_buffering;
pub use task_set::all_writes_bufferable;
