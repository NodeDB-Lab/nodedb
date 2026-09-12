// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION execution: catalog read, Data Plane re-encode, catalog write.
//!
//! Accepted targets are `document_schemaless`, `document_strict` and `kv`.
//! The columnar, timeseries and spatial engines are creation-time choices and
//! are rejected here.

use nodedb_types::DatabaseId;
use std::time::Duration;

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::catalog_entry::persist_collection_replicated;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sync_dispatch::{
    SystemReason, SystemTask, dispatch_system,
};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};
use super::column_defs::parse_convert_sql;
use super::support::err;
use super::typeguard_columns::typeguards_to_column_defs;

/// CONVERT COLLECTION <name> TO <target_type> [(<col_defs>)]
pub async fn convert_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, target_type, explicit_columns) = parse_convert_sql(sql)?;
    let tenant_id = identity.tenant_id;

    // Validate collection exists.
    let catalog = state.credentials.catalog();

    let mut coll = catalog
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{collection}' does not exist")))?;

    // Build columns before dispatch — needed for both Data Plane and catalog.
    let columns: Option<Vec<nodedb_types::columnar::ColumnDef>> = match target_type.as_str() {
        "document_strict" | "kv" => {
            let cols = if let Some(cols) = explicit_columns {
                cols
            } else if !coll.type_guards.is_empty() {
                typeguards_to_column_defs(&coll.type_guards)?
            } else {
                return Err(err(
                    "42601",
                    "CONVERT TO strict requires column definitions or active typeguards",
                ));
            };
            Some(cols)
        }
        _ => None,
    };

    let schema_json_for_dp = if let Some(ref cols) = columns {
        sonic_rs::to_string(cols).map_err(|e| err("XX000", format!("schema serialization: {e}")))?
    } else {
        String::new()
    };

    // Resolve the SOURCE storage mode from the catalog row read above, before
    // this DDL mutates `coll.collection_type`. Mirrors the exhaustive match in
    // `build_doc_config_from_stored`, so the Data Plane handler decodes the
    // scanned rows the same way the collection's own register path would.
    let source_storage_mode = match &coll.collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            nodedb_physical::physical_plan::StorageMode::Strict {
                schema: schema.clone(),
            }
        }
        nodedb_types::CollectionType::KeyValue(config) => {
            nodedb_physical::physical_plan::StorageMode::Strict {
                schema: config.schema.clone(),
            }
        }
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        | nodedb_types::CollectionType::Columnar(_) => {
            nodedb_physical::physical_plan::StorageMode::Schemaless
        }
    };

    // Dispatch to Data Plane: re-encode if needed (strict = Binary Tuple).
    let plan = PhysicalPlan::Meta(MetaOp::ConvertCollection {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        target_type: target_type.clone(),
        schema_json: schema_json_for_dp,
        source_storage_mode,
    });

    dispatch_system(
        state,
        SystemTask::new(
            SystemReason::DdlApply,
            tenant_id,
            database_id,
            &collection,
            plan,
        ),
        Duration::from_secs(60),
    )
    .await
    .map_err(|e| err("XX000", format!("conversion failed: {e}")))?;

    // Update catalog collection type.
    let new_type = match target_type.as_str() {
        "document_schemaless" => nodedb_types::CollectionType::document(),
        "document_strict" | "kv" => {
            let columns = columns.expect(
                "invariant: columns is Some for document_strict/kv targets, validated above",
            );
            let schema = nodedb_types::columnar::StrictSchema {
                columns,
                version: 1,
                dropped_columns: Vec::new(),
                bitemporal: false,
            };
            if target_type == "kv" {
                nodedb_types::CollectionType::kv(schema)
            } else {
                nodedb_types::CollectionType::strict(schema)
            }
        }
        _ => {
            return Err(err(
                "42601",
                format!("unsupported target type: {target_type}"),
            ));
        }
    };

    coll.collection_type = new_type;

    // CONVERT TO document_strict: if collection had typeguards, carry over CHECK constraints
    // and drop typeguard definitions (strict schema subsumes type checking).
    if target_type == "document_strict" && !coll.type_guards.is_empty() {
        for guard in &coll.type_guards {
            if let Some(ref check_expr) = guard.check_expr {
                // Avoid duplicate names.
                let name = format!("_guard_{}", guard.field);
                if !coll.check_constraints.iter().any(|c| c.name == name) {
                    coll.check_constraints.push(
                        crate::control::security::catalog::types::CheckConstraintDef {
                            name,
                            check_sql: check_expr.clone(),
                            has_subquery: false,
                        },
                    );
                }
            }
        }
        coll.type_guards.clear();
    }

    persist_collection_replicated(state, database_id, &coll)
        .map_err(|e| err("XX000", e.to_string()))?;

    // Refresh this node's Data Plane `doc_configs` entry to the NEW storage
    // mode. Without this, every later read of the collection resolves its
    // body format from the pre-conversion entry until the process restarts.
    crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored(
        state, &coll,
    )
    .await
    .map_err(|e| err("XX000", e.to_string()))?;

    tracing::info!(
        %collection,
        target_type,
        tenant = tenant_id.as_u64(),
        "collection converted"
    );

    Ok(vec![DdlResult::Status {
        command: "CONVERT COLLECTION".to_string(),
        rows_affected: None,
    }])
}
