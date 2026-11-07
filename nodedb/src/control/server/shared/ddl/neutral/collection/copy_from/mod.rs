// SPDX-License-Identifier: BUSL-1.1

//! Handlers for `COPY ... FROM` bulk import.
//!
//! `COPY <collection> FROM '<path>'` reads NDJSON, JSON array, or CSV. The
//! format is detected from the file extension when the WITH clause names
//! none. A server path needs superuser, and the path must open as a regular
//! file. `COPY <array> FROM STDIN`, `COPY <array> FROM '<path>.csv'` and
//! `COPY <array> FROM '<path>.tif'` load an array through `array_load`.
//!
//! Each engine's `EngineRules::plan_bulk_load` decides how it loads:
//! - Document (schemaless/strict), Columnar, KV, vector primary: one INSERT
//!   per row.
//! - Timeseries: refused (use ILP or INSERT with time_key column directly).
//! - Spatial: refused (use INSERT with geometry column directly).
//! - Array: typed cell chunks of about 4 MiB.
//!
//! A row format reads the whole file under a governor reservation. It then
//! passes over the file twice, holding one parsed row at a time. The first
//! pass checks every row, so an unparsable row writes nothing. The second
//! pass inserts.
//!
//! Transactions: outside a transaction block each document row and each
//! array chunk commits on its own, so a refused insert stops the load and
//! leaves the rows before it committed. An array load streams, so its
//! memory stays at a few chunks whatever the input size. Inside a block
//! every row and chunk stages into the transaction, and a refusal aborts
//! the block, so the load commits or rolls back whole. Inside a block an
//! array chunk that takes the staged redo past the 64 MiB WAL record cap is
//! refused with `54000` before it stages.

pub mod array_load;
mod csv_import;
mod entry;
mod import_ctx;
mod json_import;
mod row_memory;
mod source_file;

pub(crate) use array_load::{ArrayLoadCtx, ArrayLoader, LoadInput, TextFormat};
pub use entry::{CopyFromOptions, copy_from_file};
