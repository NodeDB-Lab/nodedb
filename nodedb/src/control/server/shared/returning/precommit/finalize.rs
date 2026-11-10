// SPDX-License-Identifier: BUSL-1.1

//! Evaluate and freeze one write response before the source commits.

use std::collections::HashSet;

use nodedb_types::{NativeCell, Value};

use crate::control::server::response_shape::project::cell_keys;
use crate::control::server::response_shape::returning::retype_cell;
use crate::control::server::response_shape::stamp::substitute::resolve_accessors;
use crate::control::server::response_shape::types::{ShapedRow, ShapedRows};
use crate::data::executor::response_codec::RowsPayload;

use super::columns::{compile_items, source_columns};
use super::limits::{encode_rows, limit};
use super::types::{FrozenReturningReply, PrecommitReturning, PreparedReturningRow};
use crate::control::server::shared::returning::transcript::{
    AccessorTranscript, EvaluationBinding,
};

/// Evaluate read-visible actual rows in mutation order.
/// The caller closes the source writer before invoking this function.
/// Retain the transcript even when evaluation or encoding returns an error.
pub(crate) fn finalize_returning(
    request: &PrecommitReturning<'_>,
    rows: Vec<PreparedReturningRow>,
    transcript: &AccessorTranscript,
) -> crate::Result<FrozenReturningReply> {
    limit("rows", rows.len(), request.limits.rows)?;
    let mut identities = HashSet::with_capacity(rows.len());
    for row in &rows {
        if row.identity.collection != request.redaction.collection()
            || row.identity.stable_row_key.is_empty()
            || !identities.insert(&row.identity)
        {
            return Err(crate::Error::PlanError {
                detail:
                    "RETURNING requires unique prepared identities in the authenticated collection"
                        .into(),
            });
        }
    }
    let mut masked_rows: Vec<ShapedRow> = rows.iter().map(|row| row.fields.clone()).collect();
    for row in &mut masked_rows {
        request.redaction.apply(row);
    }
    let source_columns = source_columns(request.source_columns, &masked_rows);
    let items = compile_items(request.spec, &source_columns)?;
    limit("columns", items.len(), request.limits.columns)?;
    let cells = rows.len().checked_mul(items.len()).ok_or_else(|| {
        crate::Error::ExecutionLimitExceeded {
            detail: "RETURNING cell count overflow".into(),
        }
    })?;
    limit("cells", cells, request.limits.cells)?;
    let columns: Vec<String> = items.iter().map(|(name, _)| name.clone()).collect();
    let keys = cell_keys(&columns);
    let types = match request
        .projection
        .filter(|schema| !schema.is_star && !schema.columns.is_empty())
    {
        Some(schema) => {
            if schema.columns.len() != columns.len()
                || schema
                    .columns
                    .iter()
                    .zip(&columns)
                    .any(|(column, name)| column.display_name != *name)
            {
                return Err(crate::Error::PlanError {
                    detail: "RETURNING columns differ from the announced response".into(),
                });
            }
            schema.columns.iter().map(|column| column.ty).collect()
        }
        None => ShapedRows::text_types(columns.len()),
    };
    transcript.bind(EvaluationBinding {
        rows,
        expressions: items.clone(),
        masked_rows: masked_rows.clone(),
        policy_version: request.redaction.version(),
        tenant_id: request.redaction.tenant_id(),
        collection: request.redaction.collection().to_owned(),
        roles: request.redaction.roles().to_vec(),
    })?;
    let access = transcript.begin(request.sequences);
    let crs = nodedb_crs::CrsContext::new(request.crs_set);
    let mut encoded_rows = Vec::with_capacity(masked_rows.len());
    let mut output_rows = Vec::with_capacity(masked_rows.len());
    for (row_index, row) in masked_rows.iter().enumerate() {
        let document = Value::Object(
            row.iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        );
        let mut cells = Vec::with_capacity(items.len());
        let mut output = ShapedRow::new();
        for (index, (_, expression)) in items.iter().enumerate() {
            access.set_occurrence(row_index, index);
            let resolved = resolve_accessors(expression, &document, &access, &crs)?;
            let mut value = resolved.eval(&document, &crs)?;
            retype_cell(types[index], &mut value);
            output.insert(keys[index].clone(), value.clone());
            cells.push(NativeCell(value));
        }
        output_rows.push(output);
        encoded_rows.push(cells);
    }
    access.finish()?;
    let payload = encode_rows(
        &RowsPayload {
            columns: columns.clone(),
            rows: encoded_rows,
        },
        request.limits.payload_bytes,
    )?;
    Ok(FrozenReturningReply {
        payload,
        shaped: ShapedRows::from_rows(columns, types, output_rows),
        sequence_effects: transcript.sequence_effects(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_query::expr::{BinaryOp, SqlExpr};
    use nodedb_types::{DatabaseId, TenantId};

    use super::super::fixtures::{
        call, computed, finalize, limits, literal, prepared_rows, registry,
    };
    use super::super::types::PreparedRowIdentity;
    use super::*;
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::control::sequence::{SequenceAccess, SessionSequenceAccess, SessionSequenceValues};
    use crate::control::server::shared::returning::{
        ReturningColumns, ReturningItem, ReturningSpec,
    };

    #[test]
    fn aliases_and_computed_fields_read_masked_sources() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "mask".into(),
            tenant_id: 7,
            collection: "users".into(),
            display_collection: "users".into(),
            for_role: "reader".into(),
            rules: vec![RedactionRule {
                field: "secret".into(),
                mode: RedactionMode::Mask("hidden".into()),
            }],
        });
        let snapshot = store.snapshot_rules(TenantId::new(7), "users", &["reader".into()]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Column {
                    name: "secret".into(),
                    alias: Some("alias".into()),
                },
                computed("computed", SqlExpr::Column("secret".into())),
            ]),
            precommit: None,
        };
        let crs = nodedb_crs::CustomCrsSet::empty();
        let request = PrecommitReturning {
            spec: &spec,
            source_columns: &[],
            redaction: &snapshot,
            sequences: &crate::control::server::response_shape::stamp::NoSequenceAccess,
            projection: None,
            crs_set: crs,
            limits: limits(),
        };
        let result = finalize(
            &request,
            vec![ShapedRow::from([(
                "secret".into(),
                Value::String("raw".into()),
            )])],
            &AccessorTranscript::default(),
        )
        .expect("finalize");
        assert_eq!(result.shaped.columns, ["alias", "computed"]);
        assert_eq!(
            result.shaped.rows[0]["alias"],
            Value::String("hidden".into())
        );
        assert_eq!(
            result.shaped.rows[0]["computed"],
            Value::String("hidden".into())
        );
        let decoded: crate::data::executor::response_codec::ReturningRowsReply =
            zerompk::from_msgpack(&result.payload).expect("decode rows");
        assert_eq!(decoded.columns, result.shaped.columns);
        assert_eq!(decoded.rows[0][0].0, result.shaped.rows[0]["alias"]);
    }

    #[test]
    fn nested_accessors_and_row_names_use_real_session_state_in_order() {
        let registry = registry();
        let session = Arc::new(SessionSequenceValues::new());
        let access = SessionSequenceAccess::new(
            &registry,
            Some(session),
            DatabaseId::new(1),
            TenantId::new(7),
        );
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let seq = || SqlExpr::Column("seq".into());
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                computed(
                    "positioned",
                    call(
                        "setval",
                        vec![
                            seq(),
                            SqlExpr::BinaryOp {
                                left: Box::new(call("nextval", vec![seq()])),
                                op: BinaryOp::Add,
                                right: Box::new(literal(Value::Integer(10))),
                            },
                        ],
                    ),
                ),
                computed("current", call("currval", vec![seq()])),
                computed("next", call("nextval", vec![seq()])),
            ]),
            precommit: None,
        };
        let crs = nodedb_crs::CustomCrsSet::empty();
        let request = PrecommitReturning {
            spec: &spec,
            source_columns: &[],
            redaction: &snapshot,
            sequences: &access,
            projection: None,
            crs_set: crs,
            limits: limits(),
        };
        let rows: Vec<_> = ["s", "other"]
            .into_iter()
            .map(|name| ShapedRow::from([("seq".into(), Value::String(name.into()))]))
            .collect();
        let transcript = AccessorTranscript::default();
        let first = finalize(&request, rows.clone(), &transcript).expect("finalize");
        assert_eq!(first.shaped.columns, ["positioned", "current", "next"]);
        for row in &first.shaped.rows {
            assert_eq!(row["positioned"], Value::Integer(11));
            assert_eq!(row["current"], Value::Integer(1));
            assert_eq!(row["next"], Value::Integer(12));
        }
        let replay = finalize(&request, rows, &transcript).expect("replay");
        assert_eq!(replay.payload, first.payload);
        assert_eq!(access.nextval("s").expect("next after replay"), 13);
        let changed = vec![ShapedRow::from([(
            "seq".into(),
            Value::String("other".into()),
        )])];
        assert!(matches!(
            finalize(&request, changed, &transcript),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        assert_eq!(access.nextval("other").expect("next after mismatch"), 13);
    }

    #[test]
    fn replacement_reorder_membership_and_expression_changes_conflict_before_sequence_access() {
        let registry = registry();
        let access =
            SessionSequenceAccess::new(&registry, None, DatabaseId::new(1), TenantId::new(7));
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let next = computed(
            "n",
            call("nextval", vec![literal(Value::String("s".into()))]),
        );
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![next.clone()]),
            precommit: None,
        };
        let mut request = PrecommitReturning {
            spec: &spec,
            source_columns: &[],
            redaction: &snapshot,
            sequences: &access,
            projection: None,
            crs_set: nodedb_crs::CustomCrsSet::empty(),
            limits: limits(),
        };
        let rows = prepared_rows(vec![ShapedRow::new(), ShapedRow::new()]);
        let transcript = AccessorTranscript::default();
        let first =
            super::finalize_returning(&request, rows.clone(), &transcript).expect("first input");
        let replay =
            super::finalize_returning(&request, rows.clone(), &transcript).expect("same input");
        assert_eq!(first.payload, replay.payload);
        let mut reordered = rows.clone();
        reordered.reverse();
        let mut replacement = rows.clone();
        replacement[0].identity.stable_row_key = b"replacement".to_vec();
        let removed = rows[..1].to_vec();
        let mut added = rows.clone();
        added.push(PreparedReturningRow {
            identity: PreparedRowIdentity {
                collection: "users".into(),
                stable_row_key: b"added".to_vec(),
                operation_index: 2,
            },
            fields: ShapedRow::new(),
        });
        let mut changed_image = rows.clone();
        changed_image[0]
            .fields
            .insert("value".into(), Value::Integer(3));
        for changed in [reordered, replacement, removed, added, changed_image] {
            assert!(matches!(
                super::finalize_returning(&request, changed, &transcript),
                Err(crate::Error::CalvinSerializationConflict)
            ));
        }
        let changed_spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                computed("constant", literal(Value::Integer(0))),
                next,
            ]),
            precommit: None,
        };
        request.spec = &changed_spec;
        assert!(matches!(
            super::finalize_returning(&request, rows, &transcript),
            Err(crate::Error::CalvinSerializationConflict)
        ));
        assert_eq!(
            access.nextval("s").expect("no new calls during conflicts"),
            3
        );
    }
}
