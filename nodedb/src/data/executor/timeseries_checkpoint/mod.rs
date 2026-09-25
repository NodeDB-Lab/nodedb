// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memtable checkpoint flush + boot-side partition-registry load.
//!
//! ## What is at stake
//!
//! `columnar_memtables` holds one `ColumnarMemtable` per timeseries collection,
//! and every ILP / JSON / msgpack ingest lands there. Those rows advance the
//! core watermark (`execute_timeseries_ingest` calls `note_collection_write_lsn`
//! with the Control Plane's `wal_lsn`). The periodic checkpoint reports them as
//! durable, and the manager then removes the `TimeseriesBatch` records below
//! that LSN. The checkpoint must therefore flush every memtable first. The
//! ingest path's 64 MiB threshold and the idle timer in
//! `handlers/compact/maintenance.rs` are not ordered against that removal, so
//! they cannot stand in for it.
//!
//! ## Why this reuses `flush_ts_collection` rather than writing a checkpoint blob
//!
//! Like the array engine, and unlike KV / columnar / the sync gate, timeseries
//! already has a real durable form and a boot path that reads it:
//!
//! * `flush_ts_collection` encodes the memtable into a partition directory via
//!   `ColumnarSegmentWriter` — per-column codecs, symbol dictionaries, a sparse
//!   block index, and a `partition.meta` carrying the block statistics the scan
//!   prunes on — and registers it in `ts_registries`.
//! * [`CoreLoop::load_ts_registries`] rebuilds `ts_registries` from those
//!   directories at boot, and `handlers/timeseries/raw_scan` reads registered
//!   partitions and the live memtable together, so a restored partition answers
//!   a scan exactly as the memtable did before the flush.
//!
//! A checkpoint blob would persist a second, redundant copy of state the engine
//! already knows how to write and read back — and a worse one: it would bypass
//! the column codecs, the sparse index, and the merge path that later compacts
//! those partitions.
//!
//! ## What LSN is durable after a flush
//!
//! The core watermark. A timeseries row reaches the memtable before its ingest
//! returns, and only then does `note_collection_write_lsn` raise the watermark,
//! so on this core's own thread — where the checkpoint runs, between tasks —
//! every row with `lsn <= watermark` is in a memtable. Flushing every non-empty
//! memtable therefore puts all of them in a partition.
//!
//! ## What restart replay skips
//!
//! Timeseries replay is NOT idempotent — an ingest is an APPEND, and
//! `raw_scan` reads partitions and memtable together, so re-applying a record
//! already folded into a partition shows every one of its rows twice. Each
//! collection therefore carries a replay stamp (`stamp`): the records whose
//! rows a partition holds or a truncate removed. Replay skips exactly those.
//! A record in flight when a flush ran is not named, even when a higher LSN
//! is, so it replays once.
//!
//! The stamp is per collection, which is what timeseries needs and what a
//! `ReplayFloors` field could not give it — those are engine-wide, because a
//! KV or columnar record can span two collections. A `TimeseriesBatch` names
//! exactly one.
//!
//! [`CoreLoop::load_ts_registries`] loads the stamps before replay, which is
//! the role `load_kv_checkpoints` plays for `ReplayFloors::kv`.

mod flush;
mod load;
pub mod stamp;
