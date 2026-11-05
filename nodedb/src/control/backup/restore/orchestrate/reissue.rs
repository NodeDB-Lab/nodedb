// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of the vector-index configuration drained from one
//! database's merged backup snapshot (see [`super::restore_tenant`]).
//!
//! Every section key names a collection as the source Data Plane stored it.
//! Each re-issue resolves it through the [`DatabaseTarget`]: the plan names
//! the destination-qualified collection, and the write routes by the bare
//! name in the destination database.

use crate::Error;
use crate::control::state::SharedState;
use crate::engine::vector::index_config::IndexConfig;
use crate::types::TenantId;

use super::super::target::DatabaseTarget;

/// Decode and durably re-issue every restored vector-index (collection,
/// field) HNSW/PQ/IVF configuration as a `VectorOp::SetParams`.
///
/// MUST run before the vector rows re-issue: `get_or_create_vector_index`
/// (`handlers/vector.rs`) lazily creates the Data Plane HNSW index from
/// `self.vector_params` on the FIRST `VectorOp::Insert` it sees for a
/// (collection, field) — falling back to `HnswParams::default()` when no
/// `SetParams` has landed yet. Re-issuing params after inserts is a
/// no-op for the already-created index.
///
/// `params` are `("{db}:{tid}:{coll_key}", msgpack)` pairs decoding to
/// `HnswParams` (the `TenantDataSnapshot::vector_params` wire shape);
/// `index_configs` are the same key shape decoding to `IndexConfig` (the
/// superset — HNSW params + index type + PQ/IVF params). A (collection,
/// field) present in both is re-issued once using the `IndexConfig` entry
/// (the superset); a (collection, field) present only in `params` is
/// re-issued with the rest of `IndexConfig` left at its default (matching
/// what `execute_set_vector_params` does for unspecified fields). Returns the
/// number of (collection, field) configs re-issued. Any failure is fatal — no
/// warn-and-continue.
pub(super) async fn reissue_vector_params(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    params: Vec<(String, Vec<u8>)>,
    index_configs: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let mut resolved: std::collections::HashMap<String, IndexConfig> =
        std::collections::HashMap::new();
    let mut keys_in_order: Vec<String> = Vec::new();

    for (key, bytes) in index_configs {
        let coll_key = target.scoped_rest(&key, tenant_id)?.to_owned();
        let cfg: IndexConfig = zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: deserialize IndexConfig for '{coll_key}': {e}"),
        })?;
        keys_in_order.push(coll_key.clone());
        resolved.insert(coll_key, cfg);
    }

    for (key, bytes) in params {
        let coll_key = target.scoped_rest(&key, tenant_id)?.to_owned();
        if resolved.contains_key(&coll_key) {
            // Superseded by a full IndexConfig entry for the same (collection,
            // field) — the two sections always describe the same DDL state,
            // so skip the narrower one.
            continue;
        }
        let hnsw: nodedb_types::hnsw::HnswParams =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("restore reissue: deserialize HnswParams for '{coll_key}': {e}"),
            })?;
        keys_in_order.push(coll_key.clone());
        resolved.insert(
            coll_key,
            IndexConfig {
                hnsw,
                ..IndexConfig::default()
            },
        );
    }

    let mut reissued = 0usize;
    for coll_key in keys_in_order {
        let Some(config) = resolved.remove(&coll_key) else {
            // Already re-issued (duplicate key across sections).
            continue;
        };
        let (collection, field_name) =
            super::super::vector_reissue::split_vector_coll_key(&coll_key);
        let name = target.resolve(collection)?;

        let plan = super::super::vector_reissue::build_vector_set_params_plan(
            name.stored.clone(),
            field_name,
            &config,
        );
        super::super::durable::reissue_plan_durably(
            state,
            TenantId::new(tenant_id),
            target,
            &name.bare,
            plan,
        )
        .await?;
        reissued += 1;
    }
    Ok(reissued)
}
