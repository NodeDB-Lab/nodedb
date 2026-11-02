// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for CoreLoop startup recovery: KV, CRDT, and Array engines.
//!
//! Vector replay lives in `wal_replay_vector.rs`. The `kv_transfer` /
//! `kv_transfer_item` delta-record replay (decode, tombstone gate, and
//! mutation) lives in `wal_replay_kv_transfer.rs`. The `kv_cas` /
//! `kv_incr_float` / `kv_getset` delta-record replay lives in
//! `wal_replay_kv_atomic.rs`. The `kv_field_set` delta-record replay lives in
//! `wal_replay_kv_field.rs`. The `kv_register_index` / `kv_drop_index`
//! secondary-index replay lives in `wal_replay_kv_index.rs`.
//!
//! The absolute-overwrite `kv_put` / `kv_batch_put` arms live in `kv_put.rs`,
//! which also documents why those records carry the row surrogate. A
//! RESTORE's `kv_restore` keys replay in `kv_restore.rs`.
//!
//! Split by engine concern: `kv` (`replay_kv_wal`), CRDT record decoders
//! (`crdt`, `crdt_list`, and `crdt_doc`) coordinated by `crdt_ordered` into
//! one global-LSN replay stream, and `array` (`ensure_array_open_for_replay` +
//! `replay_array_wal`).

mod array;
mod array_restore;
mod crdt;
mod crdt_admission;
mod crdt_doc;
mod crdt_list;
mod crdt_ordered;
mod kv;
pub(in crate::data::executor) mod kv_put;
mod kv_restore;

pub(in crate::data::executor) use array::ArrayOpen;
