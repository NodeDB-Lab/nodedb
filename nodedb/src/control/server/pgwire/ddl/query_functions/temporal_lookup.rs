//! `SELECT TEMPORAL_LOOKUP('table', 'key_value', 'as_of', 'key_column', 'time_column')`
//!
//! Returns the row with latest `time_column <= as_of` for the given key.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::VShardId;

use super::super::super::types::{sqlstate_error, text_field};
use super::helpers::{clean_arg, extract_function_args};

pub async fn temporal_lookup(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "TEMPORAL_LOOKUP")?;
    if args.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "TEMPORAL_LOOKUP requires (table, key_value, as_of, key_column, time_column)",
        ));
    }

    let table = clean_arg(args[0]);
    let key_value = clean_arg(args[1]);
    let as_of = clean_arg(args[2]);
    let key_column = clean_arg(args[3]);
    let time_column = clean_arg(args[4]);

    // Scan the table.
    let vshard = VShardId::from_collection(&table);
    let scan_plan = PhysicalPlan::Document(crate::bridge::physical_plan::DocumentOp::Scan {
        collection: table.clone(),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
    });

    let scan_resp = dispatch_utils::dispatch_to_data_plane(state, tenant_id, vshard, scan_plan, 0)
        .await
        .map_err(|e| sqlstate_error("XX000", &format!("scan failed: {e}")))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = serde_json::from_str(&payload_json)
        .map_err(|e| sqlstate_error("22P02", &format!("invalid JSON in scan response: {e}")))?;

    // Find the row with latest time_column <= as_of for the given key.
    let mut best_doc: Option<&serde_json::Value> = None;
    let mut best_time = String::new();

    for doc in &docs {
        let obj = match doc.as_object() {
            Some(o) => o,
            None => continue,
        };

        let key_val = obj.get(&key_column).and_then(|v| v.as_str());
        if key_val != Some(key_value.as_str()) {
            continue;
        }

        let time_val = obj.get(&time_column).and_then(|v| v.as_str()).unwrap_or("");
        if time_val.is_empty() || time_val > as_of.as_str() {
            continue;
        }

        if time_val > best_time.as_str() {
            best_time = time_val.to_string();
            best_doc = Some(doc);
        }
    }

    match best_doc {
        Some(doc) => {
            let schema = Arc::new(vec![text_field("result")]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder
                .encode_field(&doc.to_string())
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(vec![Ok(encoder.take_row())]),
            ))])
        }
        None => {
            let schema = Arc::new(vec![text_field("result")]);
            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::empty(),
            ))])
        }
    }
}
