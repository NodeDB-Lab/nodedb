// SPDX-License-Identifier: BUSL-1.1

//! `SELECT TEMPORAL_LOOKUP('table', 'key_value', 'as_of', 'key_column', 'time_column')`
//!
//! Returns the row with latest `time_column <= as_of` for the given key.

use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{
    clean_arg, empty_result, err, extract_function_args, single_result, unwrap_scan_docs,
};

pub async fn temporal_lookup(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let args = extract_function_args(sql, "TEMPORAL_LOOKUP")?;
    if args.len() < 5 {
        return Err(err(
            "42601",
            "TEMPORAL_LOOKUP requires (table, key_value, as_of, key_column, time_column)",
        ));
    }

    let table = clean_arg(args[0]);
    let key_value = clean_arg(args[1]);
    let as_of = clean_arg(args[2]);
    let key_column = clean_arg(args[3]);
    let time_column = clean_arg(args[4]);

    // `table` came out of the caller's argument list, so the read it names is
    // authorized, row-filtered, and redacted here — nothing downstream of the
    // hand-built plan does any of it.
    let gate = CollectionReadGate::open(state, identity, database_id, &table)?;
    gate.require_document_engine(&table, "TEMPORAL_LOOKUP")?;

    // Scan the table.
    let vshard = VShardId::from_collection_in_database(database_id, &table);
    let mut scan_plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(database_id, &table),
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
    gate.inject_rls(&mut scan_plan)?;

    let scan_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        scan_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", &format!("scan failed: {e}")))?;

    let payload_json =
        crate::data::executor::response_codec::decode_payload_to_json(&scan_resp.payload);
    let docs: Vec<serde_json::Value> = sonic_rs::from_str(&payload_json)
        .map_err(|e| err("22P02", &format!("invalid JSON in scan response: {e}")))?;
    // The raw document-scan codec wraps each row as `{"id": .., "data": {..}}`;
    // unwrap it so matching and redaction operate on the stored fields, not
    // the wire wrapper.
    let docs = unwrap_scan_docs(docs)?;

    // Find the row with latest time_column <= as_of for the given key.
    let mut best_doc: Option<&serde_json::Map<String, serde_json::Value>> = None;
    let mut best_time = String::new();

    for obj in &docs {
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
            best_doc = Some(obj);
        }
    }

    // The matched row is returned verbatim (as the stored fields, not the
    // `{id, data}` wire wrapper), so it goes through the same column
    // redaction a `SELECT` on this table would apply. Redaction runs on the
    // chosen row rather than on the whole scan: the key / time columns are
    // matched against their stored values, exactly as a `WHERE` clause is,
    // and only the delivered row is rewritten.
    match best_doc {
        Some(obj) => {
            let mut doc = serde_json::Value::Object(obj.clone());
            let redaction = gate.redaction_for([table.as_str()]);
            gate.redact(&redaction, &mut doc);
            Ok(single_result(&doc.to_string()))
        }
        None => Ok(empty_result()),
    }
}
