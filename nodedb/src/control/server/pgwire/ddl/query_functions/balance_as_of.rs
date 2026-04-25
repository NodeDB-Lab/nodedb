//! `SELECT BALANCE_AS_OF('collection', 'key', 'column', 'timestamp')`
//!
//! Returns `current_balance - SUM(value_expr over source rows WHERE created_at > timestamp)`.
//! Fast: only scans recent rows, not full history.

use pgwire::error::PgWireResult;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::VShardId;

use super::super::super::types::sqlstate_error;
use super::helpers::{
    clean_arg, extract_function_args, json_to_decimal, parse_timestamp_secs, return_single_value,
};

pub async fn balance_as_of(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<pgwire::api::results::Response>> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "BALANCE_AS_OF")?;
    if args.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "BALANCE_AS_OF requires (collection, key, column, timestamp)",
        ));
    }

    let collection = clean_arg(args[0]);
    let key = clean_arg(args[1]);
    let column = clean_arg(args[2]);
    let as_of_str = clean_arg(args[3]);

    let as_of_secs = parse_timestamp_secs(&as_of_str)?;

    // Read current balance from the target document.
    let vshard = VShardId::from_collection(&collection);
    let pk_bytes = key.as_bytes().to_vec();
    let surrogate = state
        .surrogate_assigner
        .lookup(&collection, &pk_bytes)
        .map_err(|e| sqlstate_error("XX000", &format!("surrogate lookup failed: {e}")))?
        .unwrap_or(nodedb_types::Surrogate::ZERO);
    let get_plan = PhysicalPlan::Document(crate::bridge::physical_plan::DocumentOp::PointGet {
        collection: collection.clone(),
        document_id: key.clone(),
        surrogate,
        pk_bytes,
        rls_filters: Vec::new(),
        system_as_of_ms: None,
        valid_at_ms: None,
    });

    let get_resp = dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard, get_plan, 0)
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("point get failed: {e}")))?;

    let doc_json = crate::data::executor::response_codec::decode_payload_to_json(&get_resp.payload);
    let doc: serde_json::Value = sonic_rs::from_str(&doc_json).unwrap_or(serde_json::Value::Null);

    let current_balance = doc
        .get(&column)
        .and_then(json_to_decimal)
        .unwrap_or(rust_decimal::Decimal::ZERO);

    // Find materialized sum definitions to know the source collection and value_expr.
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };
    let coll = catalog
        .get_collection(tenant_id.as_u32(), &collection)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{collection}' not found")))?;

    let Some(mat_def) = coll
        .materialized_sums
        .iter()
        .find(|m| m.target_column == column)
    else {
        return return_single_value(&current_balance.to_string());
    };

    // Scan the source collection for rows where join_column = key AND created_at > as_of.
    let source_vshard = VShardId::from_collection(&mat_def.source_collection);
    let source_scan = PhysicalPlan::Document(crate::bridge::physical_plan::DocumentOp::Scan {
        collection: mat_def.source_collection.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_as_of_ms: None,
        valid_at_ms: None,
    });

    let source_resp =
        dispatch_utils::dispatch_to_data_plane(state, tenant_id, source_vshard, source_scan, 0)
            .await
            .map_err(|e| sqlstate_error("XX000", &format!("source scan failed: {e}")))?;

    let source_json =
        crate::data::executor::response_codec::decode_payload_to_json(&source_resp.payload);
    let source_docs: Vec<serde_json::Value> = sonic_rs::from_str(&source_json)
        .map_err(|e| sqlstate_error("22P02", &format!("invalid JSON in source scan: {e}")))?;

    // Sum value_expr for source rows where join_column = key AND created_at > as_of.
    let mut recent_sum = rust_decimal::Decimal::ZERO;
    for src_doc in &source_docs {
        let obj = match src_doc.as_object() {
            Some(o) => o,
            None => continue,
        };

        let join_val = obj.get(&mat_def.join_column).and_then(|v| v.as_str());
        if join_val != Some(&key) {
            continue;
        }

        let created_at = crate::data::executor::enforcement::retention::extract_created_at_secs(
            &sonic_rs::to_vec(src_doc)
                .map_err(|e| sqlstate_error("XX000", &format!("serialization failed: {e}")))?,
        );
        if let Some(ts) = created_at {
            if ts <= as_of_secs {
                continue;
            }
        } else {
            continue;
        }

        let src_val = nodedb_types::Value::from(src_doc.clone());
        let delta_val = serde_json::Value::from(mat_def.value_expr.eval(&src_val));
        if let Some(d) = json_to_decimal(&delta_val) {
            recent_sum += d;
        }
    }

    let as_of_balance = current_balance - recent_sum;
    return_single_value(&as_of_balance.to_string())
}
