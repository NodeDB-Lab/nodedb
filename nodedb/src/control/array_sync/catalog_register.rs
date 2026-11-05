// SPDX-License-Identifier: BUSL-1.1

//! Register a Lite-synced array in the replicated array catalog.
//!
//! A synced schema lands in `array_sync_schemas` on the replicas of its data
//! group. Its catalog row goes through `PutArray` on the metadata group, the
//! entry SQL DDL uses, so every node can open the array and a SQL DROP or
//! re-CREATE of the same identity orders against it by the same incarnation
//! fence. A row that already exists stays: a re-synced schema never replaces
//! a definition.

use nodedb_array::schema::ArraySchema;
use nodedb_array::sync::SchemaDoc;
use nodedb_array::sync::hlc::Hlc as SchemaHlc;
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::types::ArrayId;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArraySchemaSyncMsg};
use tracing::warn;

use super::inbound::OriginArrayInbound;
use super::reject::engine_reject;
use crate::control::array_catalog::entry::ArrayCatalogEntry;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

impl OriginArrayInbound {
    /// Put the synced array in the replicated catalog, so every node can
    /// open it. A failure reaches the sync sender: reporting
    /// `SchemaImported` for an array no node can open hides the error.
    pub(super) async fn register_in_catalog(
        &self,
        msg: &ArraySchemaSyncMsg,
        remote_hlc: SchemaHlc,
    ) -> Result<(), ArrayRejectMsg> {
        register_array_catalog_entry(
            self.shared(),
            self.tenant_id(),
            self.database_id(),
            &msg.array,
            &msg.snapshot_payload,
            remote_hlc,
        )
        .await
        .map_err(|e| {
            warn!(array = %msg.array, error = %e, "array_inbound: catalog registration failed");
            engine_reject(&msg.array, remote_hlc, "catalog registration error", &e)
        })
    }
}

/// Put the array a synced schema snapshot defines in the replicated catalog,
/// unless the catalog already holds the identity.
///
/// Returns only once this node applied the entry, so the Data Plane here can
/// open the array for the cell ops that follow.
async fn register_array_catalog_entry(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    array: &str,
    snapshot: &[u8],
    schema_hlc: SchemaHlc,
) -> crate::Result<()> {
    crate::control::relation_name::check_relation_name("array", array)?;
    let catalog = state.credentials.catalog();
    if catalog
        .get_array_in_database(tenant_id, database_id, array)?
        .is_some()
    {
        return Ok(());
    }
    let entry = catalog_entry_of(tenant_id, database_id, array, snapshot, schema_hlc)?;
    crate::control::array_catalog::ddl::propose_array_entries(state, |catalog| {
        // Read again under the serialization `propose_array_entries` holds:
        // a concurrent CREATE ARRAY or sync of the same identity wins.
        if catalog
            .get_array_in_database(tenant_id, database_id, array)?
            .is_some()
        {
            return Ok(Vec::new());
        }
        Ok(vec![CatalogEntry::PutArray(Box::new(entry))])
    })
    .await
}

/// The catalog row a schema snapshot defines. The snapshot decodes on its
/// own, so a node that does not replicate the array's data group builds the
/// same row.
pub(crate) fn catalog_entry_of(
    tenant_id: TenantId,
    database_id: DatabaseId,
    array: &str,
    snapshot: &[u8],
    schema_hlc: SchemaHlc,
) -> crate::Result<ArrayCatalogEntry> {
    let schema = schema_of_snapshot(array, snapshot, schema_hlc)?;
    let schema_msgpack = zerompk::to_msgpack_vec(&schema).map_err(|e| crate::Error::Internal {
        detail: format!("synced array '{array}': schema encode: {e}"),
    })?;
    Ok(ArrayCatalogEntry {
        array_id: ArrayId::in_database(tenant_id, database_id, array),
        name: array.to_string(),
        schema_msgpack,
        schema_hash: 0,
        created_at_ms: 0,
        prefix_bits: 8,
        audit_retain_ms: None,
        minimum_audit_retain_ms: None,
        // Frozen by the proposer's stamp.
        modification_hlc: nodedb_types::Hlc::ZERO,
        incarnation: nodedb_types::Hlc::ZERO,
    })
}

fn schema_of_snapshot(
    array: &str,
    snapshot: &[u8],
    schema_hlc: SchemaHlc,
) -> crate::Result<ArraySchema> {
    let mut doc = SchemaDoc::new(ReplicaId(0));
    doc.import_snapshot_replicated(snapshot, schema_hlc)
        .and_then(|()| doc.to_schema())
        .map_err(|e| crate::Error::Internal {
            detail: format!("synced array '{array}': schema snapshot decode: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn snapshot() -> (Vec<u8>, SchemaHlc) {
        let schema = ArraySchemaBuilder::new("synced")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .expect("schema");
        let hlc_gen = nodedb_array::sync::hlc::HlcGenerator::new(ReplicaId(7));
        let doc = SchemaDoc::from_schema(ReplicaId(7), &schema, &hlc_gen).expect("doc");
        (doc.export_snapshot().expect("snapshot"), doc.schema_hlc())
    }

    /// Two nodes that decode the same snapshot build the same row, so the
    /// entry a sync proposes does not depend on which node received it.
    #[test]
    fn a_snapshot_decodes_to_one_catalog_row() {
        let (bytes, hlc) = snapshot();
        let first = catalog_entry_of(TenantId::new(9), DatabaseId::new(4), "synced", &bytes, hlc)
            .expect("decode");
        let second = catalog_entry_of(TenantId::new(9), DatabaseId::new(4), "synced", &bytes, hlc)
            .expect("decode");
        assert_eq!(first, second);
        assert_eq!(
            first.array_id,
            ArrayId::in_database(TenantId::new(9), DatabaseId::new(4), "synced")
        );
    }

    #[test]
    fn a_corrupt_snapshot_is_refused() {
        let (_, hlc) = snapshot();
        assert!(
            catalog_entry_of(
                TenantId::new(9),
                DatabaseId::new(4),
                "synced",
                b"garbage",
                hlc
            )
            .is_err()
        );
    }
}
