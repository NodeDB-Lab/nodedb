// SPDX-License-Identifier: BUSL-1.1

//! Columnar base insert handler.
//!
//! Writes rows to `nodedb-columnar`'s `MutationEngine`. Accepts msgpack payload
//! (array of objects). Creates the engine on first insert with schema inferred
//! from the first row.

pub mod flush;
pub mod geometry_index;
pub mod geometry_remove;
pub mod insert;
pub mod read_prior;
pub mod row_ingest;
pub mod schema;
pub mod spatial;

pub(in crate::data::executor) use geometry_index::GeometryIndexDelta;
pub(in crate::data::executor) use geometry_remove::{RemovedSpatialEntry, schema_has_geometry};
pub(in crate::data::executor) use insert::ColumnarInsertParams;
pub(in crate::data::executor) use schema::{ndb_field_to_value, row_values_to_object};
// `ensure_columnar_engine_schema` is an inherent `CoreLoop` method (defined
// in `schema.rs`), called via `self.` — no re-export needed.
// `flush_columnar_memtable_if_needed`, `index_columnar_geometry_columns`, and
// `insert_columnar_rows` are inherent `CoreLoop` methods (defined in
// `flush.rs`, `geometry_index.rs`, `row_ingest.rs`), called via `self.` — no
// re-export needed.
