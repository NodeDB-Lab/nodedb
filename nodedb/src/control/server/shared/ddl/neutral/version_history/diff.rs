// SPDX-License-Identifier: BUSL-1.1

//! SELECT DIFF(collection, 'doc-id', version_a, version_b)

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;

use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::dispatch::dispatch_authorized_read;

/// Names the delta export in the refusal a read policy raises: the delta is the
/// oplog the document's states were built from, returned as opaque bytes with
/// no row for a filter to apply to — what `rls_injection::crdt` concludes for
/// `CrdtOp::ExportDelta`.
const DIFF_WHAT: &str = "a version diff, which returns the CRDT oplog delta between two versions";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

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
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_diff_args(sql)?;
    if args.len() < 4 {
        return Err(err(
            "42601",
            "syntax: SELECT DIFF('collection', 'doc_id', 'version_a', 'version_b')".to_string(),
        ));
    }

    let collection = &args[0];
    let doc_id = &args[1];
    let version_a_name = &args[2];
    let version_b_name = &args[3];
    let tenant_id = identity.tenant_id;

    // The delta is stored document content in oplog form, so it carries the
    // collection's read grant, and a read policy refuses it: the bytes come
    // back as one payload with no row a filter could be evaluated against. The
    // checkpoint lookup below already discloses that a named version exists for
    // this document, so the gate runs before it.
    RefusingReadGate::open(state, identity, database_id, collection, DIFF_WHAT)?;

    // Resolve version names to VV JSON.
    let from_vv = super::at_version::resolve_checkpoint_vv(
        state,
        crate::control::security::catalog::types::CheckpointDoc::new(
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection,
            doc_id,
        ),
        version_a_name,
    )?;

    // Export delta from version_a to current through the authorized door —
    // this is user SQL, so the plan that reaches storage is the one
    // authorization approved.
    let plan = PhysicalPlan::Crdt(CrdtOp::ExportDelta {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        from_version_json: from_vv,
    });
    let delta_bytes =
        dispatch_authorized_read(state, identity, database_id, collection, plan).await?;

    let columns = vec![
        "from_version".to_string(),
        "to_version".to_string(),
        "delta_size_bytes".to_string(),
        "delta_hex".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    // Encode delta as hex for SQL-safe transport.
    let hex: String = delta_bytes.iter().map(|b| format!("{b:02x}")).collect();

    let mut row = Map::new();
    row.insert(
        "from_version".to_string(),
        JsonValue::String(version_a_name.clone()),
    );
    row.insert(
        "to_version".to_string(),
        JsonValue::String(version_b_name.clone()),
    );
    row.insert(
        "delta_size_bytes".to_string(),
        JsonValue::String((delta_bytes.len() as i64).to_string()),
    );
    row.insert("delta_hex".to_string(), JsonValue::String(hex));

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        vec![row],
    ))])
}

/// Parse function arguments from `SELECT DIFF('a', 'b', 'c', 'd')`.
fn parse_diff_args(sql: &str) -> Result<Vec<String>, DdlError> {
    let start = sql
        .find('(')
        .ok_or_else(|| err("42601", "expected '(' in DIFF call".to_string()))?;
    let end = sql
        .rfind(')')
        .ok_or_else(|| err("42601", "expected ')' in DIFF call".to_string()))?;
    if start >= end {
        return Err(err("42601", "empty DIFF arguments".to_string()));
    }
    let args_str = &sql[start + 1..end];
    Ok(args_str
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .collect())
}
