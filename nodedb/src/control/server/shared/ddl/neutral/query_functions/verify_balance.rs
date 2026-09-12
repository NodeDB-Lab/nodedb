// SPDX-License-Identifier: BUSL-1.1

//! `SELECT VERIFY_BALANCE('collection', 'column')`
//!
//! Full integrity check: recomputes each materialized balance from source rows,
//! compares to the stored balance, and reports discrepancies.

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{
    clean_arg, err, extract_function_args, json_to_decimal, single_result, unwrap_scan_docs,
};

/// `SELECT VERIFY_BALANCE('collection', 'column')`
///
/// For each row in the target collection, recomputes the materialized sum
/// from all source rows and compares to the stored balance.
pub async fn verify_balance(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "VERIFY_BALANCE")?;
    if args.len() < 2 {
        return Err(err("42601", "VERIFY_BALANCE requires (collection, column)"));
    }

    let collection = clean_arg(args[0]);
    let column = clean_arg(args[1]);

    // Both scans below name collections the caller supplied or the catalog
    // resolved from them, so each is authorized and row-filtered here. The
    // reported discrepancy count is derived from `column` and from the source
    // rows, so a redaction rule over either side is refused: a count computed
    // from masked values would call a consistent ledger broken.
    let gate = CollectionReadGate::open(state, identity, database_id, &collection)?;
    gate.require_document_engine(&collection, "VERIFY_BALANCE")?;
    gate.refuse_if_field_redacted(&collection, &column, "the balance verification")?;

    // Find the materialized sum definition.
    let catalog = state.credentials.catalog();
    let coll = catalog
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .map_err(|e| err("XX000", &e.to_string()))?
        .ok_or_else(|| err("42P01", &format!("collection '{collection}' not found")))?;

    let Some(mat_def) = coll
        .materialized_sums
        .iter()
        .find(|m| m.target_column == column)
    else {
        return Err(err(
            "42704",
            &format!("no MATERIALIZED_SUM defined for column '{column}' on '{collection}'"),
        ));
    };

    gate.authorize(&mat_def.source_collection)?;
    gate.require_document_engine(&mat_def.source_collection, "VERIFY_BALANCE")?;
    gate.refuse_if_any_redaction(&mat_def.source_collection, "the balance verification")?;

    // Scan all target rows.
    let target_vshard = VShardId::from_collection_in_database(database_id, &collection);
    let mut target_scan =
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            limit: usize::MAX,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        });
    gate.inject_rls(&mut target_scan)?;
    let target_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        target_vshard,
        target_scan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", &format!("target scan failed: {e}")))?;
    let target_json =
        crate::data::executor::response_codec::decode_payload_to_json(&target_resp.payload);
    let target_docs: Vec<serde_json::Value> = sonic_rs::from_str(&target_json)
        .map_err(|e| err("22P02", &format!("invalid JSON in target scan: {e}")))?;
    // Unwrap the `{"id", "data"}` scan envelope so matching reads the stored
    // fields, not the wire wrapper.
    let target_docs = unwrap_scan_docs(target_docs)?;

    // Scan all source rows.
    let source_vshard =
        VShardId::from_collection_in_database(database_id, &mat_def.source_collection);
    let mut source_scan =
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                database_id,
                &mat_def.source_collection,
            ),
            limit: usize::MAX,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        });
    gate.inject_rls(&mut source_scan)?;
    let source_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        source_vshard,
        source_scan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", &format!("source scan failed: {e}")))?;
    let source_json =
        crate::data::executor::response_codec::decode_payload_to_json(&source_resp.payload);
    let source_docs: Vec<serde_json::Value> = sonic_rs::from_str(&source_json)
        .map_err(|e| err("22P02", &format!("invalid JSON in source scan: {e}")))?;
    // Unwrap the `{"id", "data"}` scan envelope so matching and `value_expr`
    // evaluation read the stored fields, not the wire wrapper.
    let source_docs = unwrap_scan_docs(source_docs)?;

    // For each target row, recompute balance from source rows.
    let mut discrepancies = 0u64;
    let mut checked = 0u64;

    for target_doc in &target_docs {
        let doc_id = target_doc
            .get("id")
            .or_else(|| target_doc.get("_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if doc_id.is_empty() {
            continue;
        }

        let stored_balance = target_doc
            .get(&column)
            .and_then(json_to_decimal)
            .unwrap_or(rust_decimal::Decimal::ZERO);

        // Sum value_expr for all source rows where join_column == doc_id.
        let mut computed = rust_decimal::Decimal::ZERO;
        for src_doc in &source_docs {
            let join_val = src_doc.get(&mat_def.join_column).and_then(|v| v.as_str());
            if join_val != Some(doc_id) {
                continue;
            }
            let src_val = nodedb_types::Value::from(serde_json::Value::Object(src_doc.clone()));
            let delta =
                serde_json::Value::from(mat_def.value_expr.eval(&src_val).map_err(|e| {
                    err(
                        nodedb_types::error::sqlstate::DIVISION_BY_ZERO,
                        &format!("VERIFY_BALANCE: value_expr failed to evaluate: {e}"),
                    )
                })?);
            if let Some(d) = json_to_decimal(&delta) {
                computed += d;
            }
        }

        if stored_balance != computed {
            discrepancies += 1;
        }
        checked += 1;
    }

    let result = serde_json::json!({
        "collection": collection,
        "column": column,
        "rows_checked": checked,
        "discrepancies": discrepancies,
        "valid": discrepancies == 0,
    });

    Ok(single_result(&result.to_string()))
}
