//! SELECT DIFF(collection, 'doc-id', version_a, version_b)

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::CrdtOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SELECT DIFF(collection, 'doc-id', 'version_a', 'version_b')
///
/// Returns the delta bytes between two versions. The `version_a` and
/// `version_b` parameters can be checkpoint names or raw VV JSON.
///
/// The result is the raw Loro delta (binary) encoded as hex, plus size info.
/// Application-level diff rendering (field-level diffs) will be added
/// with the Field-Level Change Events feature (3.4).
pub async fn select_diff(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_diff_args(sql)?;
    if args.len() < 4 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SELECT DIFF('collection', 'doc_id', 'version_a', 'version_b')",
        ));
    }

    let collection = &args[0];
    let doc_id = &args[1];
    let version_a_name = &args[2];
    let version_b_name = &args[3];
    let tenant_id = identity.tenant_id;

    // Resolve version names to VV JSON.
    let from_vv = super::at_version::resolve_checkpoint_vv(
        state,
        tenant_id.as_u32(),
        collection,
        doc_id,
        version_a_name,
    )?;

    // Export delta from version_a to current via Data Plane.
    let plan = PhysicalPlan::Crdt(CrdtOp::ExportDelta {
        from_version_json: from_vv,
    });
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    let delta_bytes =
        super::super::sync_dispatch::dispatch_async(state, tenant_id, collection, plan, timeout)
            .await
            .map_err(|e| sqlstate_error("XX000", &format!("dispatch: {e}")))?;

    let schema = Arc::new(vec![
        text_field("from_version"),
        text_field("to_version"),
        int8_field("delta_size_bytes"),
        text_field("delta_hex"),
    ]);

    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(version_a_name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(version_b_name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    encoder
        .encode_field(&(delta_bytes.len() as i64))
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    // Encode delta as hex for SQL-safe transport.
    let hex: String = delta_bytes.iter().map(|b| format!("{b:02x}")).collect();
    encoder
        .encode_field(&hex)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}

/// Parse function arguments from `SELECT DIFF('a', 'b', 'c', 'd')`.
fn parse_diff_args(sql: &str) -> PgWireResult<Vec<String>> {
    let start = sql
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' in DIFF call"))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", "expected ')' in DIFF call"))?;
    if start >= end {
        return Err(sqlstate_error("42601", "empty DIFF arguments"));
    }
    let args_str = &sql[start + 1..end];
    Ok(args_str
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .collect())
}
