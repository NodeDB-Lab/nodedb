// SPDX-License-Identifier: BUSL-1.1

//! `SELECT TEMPORAL_LOOKUP('table', 'key_value', 'as_of', 'key_column', 'time_column')`
//!
//! Returns the row with latest `time_column <= as_of` for the given key.

use nodedb_sql::parser::query_function::TemporalLookupArgs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::server::response_shape::cell::row_to_wire_json;
use crate::control::server::response_shape::types::ShapedRow;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::RowContext;
use crate::types::{DatabaseId, TraceId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{empty_result, scan_rows, single_result, value_text};

pub async fn temporal_lookup(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    args: &TemporalLookupArgs,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let table = args.table.clone();
    let key_value = args.key_value.clone();
    let as_of = args.as_of.clone();
    let key_column = args.key_column.clone();
    let time_column = args.time_column.clone();

    // `table` came out of the caller's argument list, so the read it names is
    // authorized, row-filtered, and redacted here — nothing downstream of the
    // hand-built plan does any of it.
    let gate = CollectionReadGate::open(state, identity, database_id, &table)?;
    gate.require_document_engine(&table, "TEMPORAL_LOOKUP")?;

    // Scan the table.
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &table).vshard();
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
    .map_err(|e| DdlError::from_error_in_context("scan failed", &e))?;

    // The raw document-scan codec wraps each row as `{"id": .., "data": {..}}`;
    // `scan_rows` unwraps it so matching and redaction operate on the stored
    // fields, typed, not the wire wrapper.
    let docs = scan_rows(&scan_resp.payload)?;

    // Find the row with latest time_column <= as_of for the given key.
    let mut best_doc: Option<&ShapedRow> = None;
    let mut best_time = String::new();

    for obj in &docs {
        let key_val = obj.get(&key_column).and_then(value_text);
        if key_val.as_deref() != Some(key_value.as_str()) {
            continue;
        }

        let time_val = obj
            .get(&time_column)
            .and_then(value_text)
            .unwrap_or_default();
        if time_val.is_empty() || time_val.as_str() > as_of.as_str() {
            continue;
        }

        if time_val > best_time {
            best_time = time_val;
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
            // The client sees the row as every typed row renders.
            let mut doc = serde_json::Value::Object(row_to_wire_json(obj));
            let redaction = gate.redaction_for([table.as_str()]);
            gate.redact(&redaction, RowContext::Flat, &mut doc);
            Ok(single_result(&doc.to_string()))
        }
        None => Ok(empty_result()),
    }
}
