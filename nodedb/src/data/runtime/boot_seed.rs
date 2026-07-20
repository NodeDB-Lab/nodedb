// SPDX-License-Identifier: BUSL-1.1

//! Boot stage 2 of 3: seed the catalog-sourced configuration WAL replay needs.
//!
//! Runs after the checkpoint restore and before WAL replay. See
//! `seed_catalog_state` for why that order is the only sound one.

use crate::data::executor::core_loop::CoreLoop;

/// Seed `doc_configs`, vector-index params, and columnar-family schemas from the
/// durable catalog.
///
/// # Ordering (load-bearing)
///
/// Runs AFTER `load_boot_checkpoints` and BEFORE `replay_wal_and_rebuild_indexes`:
/// `seed_columnar_schemas` skips any collection that already has an engine, so
/// running before the restore would leave an empty seeded engine in place of a
/// restored one; running after replay would be too late for every seed here,
/// which exists precisely so replay finds the real schema instead of inferring it.
pub(super) fn seed_catalog_state(
    core: &mut CoreLoop,
    doc_config_seed: &[crate::data::executor::core_loop::DocConfigSeedEntry],
    vector_index_param_seed: &[nodedb_types::StoredVectorIndexParams],
    columnar_schema_seed: &[(
        crate::types::DatabaseId,
        crate::types::TenantId,
        String,
        nodedb_types::columnar::ColumnarSchema,
    )],
) {
    // Seed `doc_configs` from the durable catalog BEFORE WAL
    // replay. `doc_configs` is otherwise only populated by
    // `DocumentOp::Register` broadcasts processed in the event
    // loop — too late for redo replay, which runs
    // synchronously on this thread before the core ever drains
    // an SPSC request. Without this seed, strict (Binary Tuple)
    // collections replay through the schemaless fallback and get
    // re-persisted as raw MessagePack, corrupting the strict
    // store's O(1) field layout.
    core.seed_doc_configs(doc_config_seed);

    // Seed vector-index config from the durable catalog BEFORE
    // WAL replay. The `CREATE VECTOR INDEX` parameters otherwise
    // arrive only as a WAL `VectorParams` record, which is not
    // crash-durable: a `kill -9` before the group-commit flush loses
    // it, so the core would not know the collection carries a vector
    // index. Seeding here (and rebuilding after replay) makes vector search
    // survive a hard crash.
    core.seed_vector_index_params(vector_index_param_seed);

    // Seed columnar-family (`columnar` / `timeseries` /
    // `spatial`) MutationEngine schemas from the durable catalog
    // BEFORE WAL replay. `replay_columnar_payload` replays redo
    // records with an empty `schema_bytes`; on a fresh engine map
    // that falls back to inferring the schema from the first
    // replayed row, which loses declared types like Geometry,
    // Timestamp, and Decimal. Pre-registering here means the
    // replay path finds an existing engine and reuses its real
    // schema instead of ever inferring. Must run after
    // `seed_doc_configs` so `is_bitemporal` sees the right flag.
    core.seed_columnar_schemas(columnar_schema_seed);
}
