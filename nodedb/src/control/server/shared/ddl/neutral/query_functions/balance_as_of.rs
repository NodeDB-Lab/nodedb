// SPDX-License-Identifier: BUSL-1.1

//! `SELECT BALANCE_AS_OF('collection', 'key', 'column', 'timestamp')`
//!
//! Returns `current_balance - SUM(value_expr over source rows WHERE created_at > timestamp)`.
//! Fast: only scans recent rows, not full history.

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{
    clean_arg, err, extract_function_args, json_to_decimal, parse_timestamp_secs, single_result,
    unwrap_scan_docs,
};

pub async fn balance_as_of(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "BALANCE_AS_OF")?;
    if args.len() < 4 {
        return Err(err(
            "42601",
            "BALANCE_AS_OF requires (collection, key, column, timestamp)",
        ));
    }

    let collection = clean_arg(args[0]);
    let key = clean_arg(args[1]);
    let column = clean_arg(args[2]);
    let as_of_str = clean_arg(args[3]);

    let as_of_secs = parse_timestamp_secs(&as_of_str)?;

    // `collection` is a caller argument, so the read it names is authorized and
    // row-filtered here. The returned balance is arithmetic over `column`, so a
    // redaction rule on that column has no honest answer — masking it would
    // report a number no row holds.
    let gate = CollectionReadGate::open(state, identity, database_id, &collection)?;
    gate.require_document_engine(&collection, "BALANCE_AS_OF")?;
    gate.refuse_if_field_redacted(&collection, &column, "the as-of balance")?;

    // Read current balance from the target document.
    let vshard = VShardId::from_collection_in_database(database_id, &collection);
    let pk_bytes = key.as_bytes().to_vec();
    let surrogate = state
        .surrogate_assigner
        .lookup(database_id, tenant_id, &collection, &pk_bytes)
        .map_err(|e| err("XX000", &format!("surrogate lookup failed: {e}")))?
        .unwrap_or(nodedb_types::Surrogate::ZERO);
    let mut get_plan =
        PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            document_id: key.clone(),
            surrogate,
            pk_bytes,
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
    gate.inject_rls(&mut get_plan)?;

    let get_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        get_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", &format!("point get failed: {e}")))?;

    let doc_json = crate::data::executor::response_codec::decode_payload_to_json(&get_resp.payload);
    let doc: serde_json::Value = sonic_rs::from_str(&doc_json).unwrap_or(serde_json::Value::Null);

    let current_balance = doc
        .get(&column)
        .and_then(json_to_decimal)
        .unwrap_or(rust_decimal::Decimal::ZERO);

    // Find materialized sum definitions to know the source collection and value_expr.
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
        return Ok(single_result(&current_balance.to_string()));
    };

    // The source collection is a second read, resolved from the catalog rather
    // than the argument list, and it needs its own grant: a caller who may read
    // the balance is not thereby entitled to the ledger it was summed from.
    // `value_expr` can name any of its columns, so a redaction rule anywhere on
    // it is refused rather than silently summed over hidden values.
    gate.authorize(&mat_def.source_collection)?;
    gate.require_document_engine(&mat_def.source_collection, "BALANCE_AS_OF")?;
    gate.refuse_if_any_redaction(&mat_def.source_collection, "the as-of balance")?;

    // Scan the source collection for rows where join_column = key AND created_at > as_of.
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

    // Sum value_expr for source rows where join_column = key AND created_at > as_of.
    let mut recent_sum = rust_decimal::Decimal::ZERO;
    for obj in &source_docs {
        let join_val = obj.get(&mat_def.join_column).and_then(|v| v.as_str());
        if join_val != Some(&key) {
            continue;
        }

        let src_doc = serde_json::Value::Object(obj.clone());
        let created_at = crate::data::executor::enforcement::retention::extract_created_at_secs(
            &sonic_rs::to_vec(&src_doc)
                .map_err(|e| err("XX000", &format!("serialization failed: {e}")))?,
        );
        if let Some(ts) = created_at {
            if ts <= as_of_secs {
                continue;
            }
        } else {
            continue;
        }

        let src_val = nodedb_types::Value::from(src_doc.clone());
        let delta_val =
            serde_json::Value::from(mat_def.value_expr.eval(&src_val).map_err(|e| {
                err(
                    nodedb_types::error::sqlstate::DIVISION_BY_ZERO,
                    &format!("BALANCE_AS_OF: value_expr failed to evaluate: {e}"),
                )
            })?);
        if let Some(d) = json_to_decimal(&delta_val) {
            recent_sum += d;
        }
    }

    let as_of_balance = current_balance - recent_sum;
    Ok(single_result(&as_of_balance.to_string()))
}
