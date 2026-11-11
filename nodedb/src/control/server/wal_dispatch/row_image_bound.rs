// SPDX-License-Identifier: BUSL-1.1

//! One stored row fits one write-set part at the WAL record limit.
//!
//! The Data Plane refuses a row whose id or any image is over
//! [`MAX_ROW_IMAGE_BYTES`], or whose prior event image is over
//! [`MAX_OLD_EVENT_IMAGE_BYTES`]. The catalog refuses a collection name over
//! [`MAX_COLLECTION_NAME_BYTES`] and a schema witness over
//! [`MAX_SCHEMA_WITNESS_BYTES`]. The bound below sums the largest sub-record
//! such a row journals, in the encoding the part writes, and the assertion
//! holds it within [`part_budget`] at [`MAX_RECORD_PAYLOAD`], the limit every
//! node uses. A row the Data Plane accepts therefore never fails to journal.

use nodedb_types::{
    MAX_COLLECTION_NAME_BYTES, MAX_OLD_EVENT_IMAGE_BYTES, MAX_ROW_IMAGE_BYTES,
    MAX_SCHEMA_WITNESS_BYTES,
};
use nodedb_wal::writer::MAX_RECORD_PAYLOAD;

use crate::wal::redo::continuation::SUB_RECORD_OVERHEAD;

use super::write_set_redo::part_budget;

/// The header bytes of a MessagePack `bin 32` or `str 32`.
const HEADER_32: usize = 5;

/// The bytes a database qualifier adds to a collection name: a `u64` in
/// decimal and the separator.
const QUALIFIER_BYTES: usize = 20 + 1;

/// The collection name a sub-record carries, at most: a qualified name the
/// catalog admitted, as `str`.
const COLLECTION_NAME_RESERVE: usize = MAX_COLLECTION_NAME_BYTES + QUALIFIER_BYTES + HEADER_32;

/// The schema witness a part carries for one row, at most, as the catalog
/// admitted it.
const SCHEMA_WITNESS_RESERVE: usize = MAX_SCHEMA_WITNESS_BYTES;

/// Bytes reserved for the fixed fields of a row sub-record: its tag, field
/// names, surrogates, version, schema index, chain head and valid time.
const ENTRY_FRAMING_RESERVE: usize = 4 * 1024;

/// The largest part cost of one installed document row: its id, its
/// accepted and stored bodies and its new event image at the row limit, its
/// old event image at the old-image limit, each as `bin` or `str`.
const MAX_INSTALLED_ROW_PART_COST: usize = 4 * (MAX_ROW_IMAGE_BYTES + HEADER_32)
    + (MAX_OLD_EVENT_IMAGE_BYTES + HEADER_32)
    + COLLECTION_NAME_RESERVE
    + SCHEMA_WITNESS_RESERVE
    + ENTRY_FRAMING_RESERVE
    + SUB_RECORD_OVERHEAD;

/// The largest part cost of one edge version: its source, label and
/// destination as `str` and its properties as `bin`, each at the limit.
const MAX_EDGE_PART_COST: usize = 4 * (MAX_ROW_IMAGE_BYTES + HEADER_32)
    + COLLECTION_NAME_RESERVE
    + ENTRY_FRAMING_RESERVE
    + SUB_RECORD_OVERHEAD;

const _: () = assert!(
    MAX_COLLECTION_NAME_BYTES <= nodedb_wal::MAX_COLLECTION_NAME_LEN,
    "a collection the catalog admits must fit a WAL collection tombstone"
);

const _: () = assert!(
    MAX_INSTALLED_ROW_PART_COST <= part_budget(MAX_RECORD_PAYLOAD),
    "an installed row at the row image limit must fit one write-set part"
);

const _: () = assert!(
    MAX_EDGE_PART_COST <= part_budget(MAX_RECORD_PAYLOAD),
    "an edge at the row image limit must fit one write-set part"
);

#[cfg(test)]
#[path = "row_image_bound_tests.rs"]
mod tests;
