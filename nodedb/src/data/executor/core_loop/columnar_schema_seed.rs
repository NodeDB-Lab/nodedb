// SPDX-License-Identifier: BUSL-1.1

//! Boot-time seeding of columnar-family (`columnar` / `timeseries` /
//! `spatial`) `MutationEngine`s with their real catalog schema.
//!
//! WAL redo replay (`CoreLoop::replay_all_wal`) runs synchronously on the
//! Data Plane core thread, before the core ever processes an SPSC request.
//! `replay_columnar_payload` calls `execute_columnar_insert` with an empty
//! `schema_bytes`, so on a fresh `columnar_engines` map
//! `ensure_columnar_engine_schema` falls back to inferring the schema from
//! the first replayed row (`infer_schema_from_value`), which recognizes
//! only Float/Int/Bool/String. Declared types such as Geometry, Timestamp,
//! Decimal, Bytes, and Uuid are lost — for a spatial collection the
//! geometry column silently degrades to `String`, and the R-tree restore
//! (`restore_columnar_geometry_indexes`, which filters on
//! `column_type == Geometry`) never runs.
//!
//! [`CoreLoop::seed_columnar_schemas`] closes that gap: called with the
//! catalog-sourced schema for every columnar-family collection (built by
//! `crate::bootstrap::data_plane::load_columnar_schema_seed`) immediately
//! before `replay_all_wal`, so `ensure_columnar_engine_schema` finds an
//! already-registered engine and returns its real schema instead of ever
//! inferring — the same fix shape as `seed_doc_configs` for strict
//! document collections.

use crate::types::{DatabaseId, TenantId};

use super::state::CoreLoop;

impl CoreLoop {
    /// Pre-register a `MutationEngine` for every `(tenant, collection,
    /// schema)` entry, skipping any engine that already exists (e.g. one
    /// created by an earlier seed step or a previous call). Called once at
    /// core startup, after `seed_doc_configs` (so `is_bitemporal` sees the
    /// right flag) and before `replay_all_wal`.
    pub fn seed_columnar_schemas(
        &mut self,
        entries: &[(
            DatabaseId,
            TenantId,
            String,
            nodedb_types::columnar::ColumnarSchema,
        )],
    ) {
        let flush_threshold = self.query_tuning.columnar_flush_threshold;
        for (db, tid, collection, schema) in entries {
            let engine_key = (*db, *tid, collection.clone());
            if self.columnar_engines.contains_key(&engine_key) {
                continue;
            }
            let bitemporal = self.is_bitemporal(db.as_u64(), tid.as_u64(), collection);
            let schema = if bitemporal {
                crate::data::executor::handlers::columnar_write::schema::prepend_bitemporal_columns(
                    schema.clone(),
                )
            } else {
                schema.clone()
            };
            self.columnar_engines.insert(
                engine_key,
                nodedb_columnar::MutationEngine::with_flush_threshold(
                    collection.clone(),
                    schema,
                    flush_threshold,
                ),
            );
        }
    }
}
