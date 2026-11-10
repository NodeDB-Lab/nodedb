// SPDX-License-Identifier: BUSL-1.1

//! Finalize a write response after source preparation and before source commit.

use std::collections::HashSet;

use nodedb_query::expr::SqlExpr;
use nodedb_types::{NativeCell, Value};

use crate::control::security::redaction::snapshot_rules::RedactionSnapshot;
use crate::control::sequence::SequenceAccess;
use crate::control::server::response_shape::project::cell_keys;
use crate::control::server::response_shape::returning::retype_cell;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::stamp::substitute::resolve_accessors;
use crate::control::server::response_shape::types::{ShapedRow, ShapedRows};
use crate::data::executor::response_codec::RowsPayload;

use super::transcript::{AccessorTranscript, EvaluationBinding};
use super::{ReturningColumns, ReturningItem, ReturningSpec};

/// Source identity supplied by the checked preparation coordinator.
/// This identity never comes from user-visible source fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PreparedRowIdentity {
    pub collection: String,
    pub stable_row_key: Vec<u8>,
    pub operation_index: u64,
}

/// One actual prepared source image and its independent stable identity.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedReturningRow {
    pub identity: PreparedRowIdentity,
    pub fields: ShapedRow,
}

/// Hard response limits checked before source commit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReturningLimits {
    pub rows: usize,
    pub columns: usize,
    pub cells: usize,
    pub payload_bytes: usize,
}

/// Immutable request scope and response contract for one prepared source.
pub(crate) struct PrecommitReturning<'a> {
    pub spec: &'a ReturningSpec,
    /// Source field order, including declared fields when no row matches.
    pub source_columns: &'a [String],
    pub redaction: &'a RedactionSnapshot,
    pub sequences: &'a dyn SequenceAccess,
    pub projection: Option<&'a OutputSchema>,
    pub crs_set: &'a nodedb_crs::CustomCrsSet,
    pub limits: ReturningLimits,
}

/// Frozen response bytes and their exact protocol-neutral shape.
/// Transport retries reuse this value without evaluating expressions again.
#[derive(Debug)]
pub(crate) struct FrozenReturningReply {
    pub payload: Vec<u8>,
    pub shaped: ShapedRows,
}

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
    })
}

fn encode_rows(rows: &RowsPayload, maximum: usize) -> crate::Result<Vec<u8>> {
    let mut output = BoundedOutput {
        bytes: Vec::new(),
        maximum,
        exceeded: false,
    };
    if let Err(error) = zerompk::write_msgpack(&mut output, rows) {
        if output.exceeded {
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!("RETURNING payload bytes exceed limit {maximum}"),
            });
        }
        return Err(crate::Error::Codec {
            detail: format!("RETURNING response serialization: {error}"),
        });
    }
    Ok(output.bytes)
}

struct BoundedOutput {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "RETURNING payload byte limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn limit(name: &str, actual: usize, maximum: usize) -> crate::Result<()> {
    if actual > maximum {
        return Err(crate::Error::ExecutionLimitExceeded {
            detail: format!("RETURNING {name} {actual} exceeds limit {maximum}"),
        });
    }
    Ok(())
}

fn source_columns(declared: &[String], rows: &[ShapedRow]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    for name in declared
        .iter()
        .chain(rows.iter().flat_map(|row| row.keys()))
    {
        if seen.insert(name.as_str()) {
            columns.push(name.clone());
        }
    }
    columns
}

fn compile_items(spec: &ReturningSpec, source: &[String]) -> crate::Result<Vec<(String, SqlExpr)>> {
    let expand_star = || {
        source
            .iter()
            .map(|name| (name.clone(), SqlExpr::Column(name.clone())))
    };
    match &spec.columns {
        ReturningColumns::Star => Ok(expand_star().collect()),
        ReturningColumns::Named(items) => {
            let mut compiled = Vec::new();
            for item in items {
                match item {
                    ReturningItem::Star => compiled.extend(expand_star()),
                    ReturningItem::Column { name, alias } => compiled.push((
                        alias.as_ref().unwrap_or(name).clone(),
                        SqlExpr::Column(name.clone()),
                    )),
                    ReturningItem::Computed { expression, name } => {
                        let expression =
                            zerompk::from_msgpack::<SqlExpr>(expression).map_err(|error| {
                                crate::Error::Codec {
                                    detail: format!(
                                        "RETURNING expression {name:?} decode: {error}"
                                    ),
                                }
                            })?;
                        compiled.push((name.clone(), expression));
                    }
                }
            }
            Ok(compiled)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::control::sequence::{
        SequenceRegistry, SessionSequenceAccess, SessionSequenceValues,
    };
    use nodedb_query::expr::BinaryOp;
    use nodedb_types::{DatabaseId, TenantId};

    fn prepared_rows(rows: Vec<ShapedRow>) -> Vec<PreparedReturningRow> {
        rows.into_iter()
            .enumerate()
            .map(|(index, fields)| PreparedReturningRow {
                identity: PreparedRowIdentity {
                    collection: "users".into(),
                    stable_row_key: index.to_be_bytes().to_vec(),
                    operation_index: index as u64,
                },
                fields,
            })
            .collect()
    }

    fn finalize(
        request: &PrecommitReturning<'_>,
        rows: Vec<ShapedRow>,
        transcript: &AccessorTranscript,
    ) -> crate::Result<FrozenReturningReply> {
        super::finalize_returning(request, prepared_rows(rows), transcript)
    }

    fn limits() -> ReturningLimits {
        ReturningLimits {
            rows: 100,
            columns: 100,
            cells: 1000,
            payload_bytes: 100_000,
        }
    }

    fn computed(name: &str, expression: SqlExpr) -> ReturningItem {
        ReturningItem::Computed {
            name: name.into(),
            expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
        }
    }

    fn call(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
        SqlExpr::Function {
            name: name.into(),
            args,
        }
    }

    fn literal(value: Value) -> SqlExpr {
        SqlExpr::Literal(value)
    }

    fn registry() -> SequenceRegistry {
        let registry = SequenceRegistry::new();
        for name in ["s", "other"] {
            registry
                .create(StoredSequence::new(1, 7, name.into(), "alice".into()))
                .expect("create sequence");
        }
        registry
    }

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
    fn late_evaluation_and_encoded_size_errors_remain_typed() {
        let registry = registry();
        let access =
            SessionSequenceAccess::new(&registry, None, DatabaseId::new(1), TenantId::new(7));
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                computed(
                    "next",
                    call("nextval", vec![literal(Value::String("s".into()))]),
                ),
                computed(
                    "bad",
                    SqlExpr::BinaryOp {
                        left: Box::new(literal(Value::Integer(1))),
                        op: BinaryOp::Div,
                        right: Box::new(literal(Value::Integer(0))),
                    },
                ),
            ]),
        };
        let crs = nodedb_crs::CustomCrsSet::empty();
        let mut request = PrecommitReturning {
            spec: &spec,
            source_columns: &[],
            redaction: &snapshot,
            sequences: &access,
            projection: None,
            crs_set: crs,
            limits: limits(),
        };
        let transcript = AccessorTranscript::default();
        assert!(matches!(
            finalize(&request, vec![ShapedRow::new()], &transcript),
            Err(crate::Error::DivisionByZero)
        ));
        assert_eq!(transcript.entries().len(), 1);
        assert!(matches!(
            finalize(&request, vec![ShapedRow::new()], &transcript),
            Err(crate::Error::DivisionByZero)
        ));
        assert_eq!(access.nextval("s").expect("next"), 2);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![computed(
                "value",
                literal(Value::String("large".into())),
            )]),
        };
        request.spec = &spec;
        request.limits.payload_bytes = 1;
        assert!(matches!(
            finalize(
                &request,
                vec![ShapedRow::new()],
                &AccessorTranscript::default()
            ),
            Err(crate::Error::ExecutionLimitExceeded { .. })
        ));
    }

    #[test]
    fn zero_rows_and_duplicate_names_keep_positional_columns() {
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Column {
                    name: "b".into(),
                    alias: Some("same".into()),
                },
                ReturningItem::Column {
                    name: "a".into(),
                    alias: Some("same".into()),
                },
                ReturningItem::Star,
            ]),
        };
        let source = vec!["b".into(), "a".into()];
        let crs = nodedb_crs::CustomCrsSet::empty();
        let request = PrecommitReturning {
            spec: &spec,
            source_columns: &source,
            redaction: &snapshot,
            sequences: &crate::control::server::response_shape::stamp::NoSequenceAccess,
            projection: None,
            crs_set: crs,
            limits: limits(),
        };
        let empty =
            finalize(&request, Vec::new(), &AccessorTranscript::default()).expect("empty rows");
        assert_eq!(empty.shaped.columns, ["same", "same", "b", "a"]);
        assert!(empty.shaped.rows.is_empty());
        let row = ShapedRow::from([
            ("a".into(), Value::Integer(1)),
            ("b".into(), Value::Integer(2)),
        ]);
        let result = finalize(&request, vec![row], &AccessorTranscript::default()).expect("rows");
        let keys = result.shaped.cell_keys();
        assert_eq!(result.shaped.rows[0][&keys[0]], Value::Integer(2));
        assert_eq!(result.shaped.rows[0][&keys[1]], Value::Integer(1));
        let decoded: crate::data::executor::response_codec::ReturningRowsReply =
            zerompk::from_msgpack(&result.payload).expect("decode rows");
        assert_eq!(
            decoded.rows[0]
                .iter()
                .map(|cell| cell.0.clone())
                .collect::<Vec<_>>(),
            [
                Value::Integer(2),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(1)
            ]
        );
    }
    #[test]
    fn output_aliases_never_replace_source_columns_expressions_sequences_or_star() {
        let registry = registry();
        let access = SessionSequenceAccess::new(
            &registry,
            Some(Arc::new(SessionSequenceValues::new())),
            DatabaseId::new(1),
            TenantId::new(7),
        );
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                ReturningItem::Column {
                    name: "a".into(),
                    alias: Some("b".into()),
                },
                ReturningItem::Column {
                    name: "b".into(),
                    alias: None,
                },
                computed(
                    "b",
                    SqlExpr::BinaryOp {
                        left: Box::new(SqlExpr::Column("a".into())),
                        op: BinaryOp::Add,
                        right: Box::new(literal(Value::Integer(1))),
                    },
                ),
                computed("read_b", SqlExpr::Column("b".into())),
                computed("seq", literal(Value::String("other".into()))),
                computed("n", call("nextval", vec![SqlExpr::Column("seq".into())])),
                ReturningItem::Star,
            ]),
        };
        let source = vec!["a".into(), "b".into(), "seq".into()];
        let request = PrecommitReturning {
            spec: &spec,
            source_columns: &source,
            redaction: &snapshot,
            sequences: &access,
            projection: None,
            crs_set: nodedb_crs::CustomCrsSet::empty(),
            limits: limits(),
        };
        let result = finalize(
            &request,
            vec![ShapedRow::from([
                ("a".into(), Value::Integer(9)),
                ("b".into(), Value::Integer(2)),
                ("seq".into(), Value::String("s".into())),
            ])],
            &AccessorTranscript::default(),
        )
        .expect("finalize collisions");
        let decoded: crate::data::executor::response_codec::ReturningRowsReply =
            zerompk::from_msgpack(&result.payload).expect("decode collisions");
        assert_eq!(
            decoded.rows[0]
                .iter()
                .map(|cell| cell.0.clone())
                .collect::<Vec<_>>(),
            [
                Value::Integer(9),
                Value::Integer(2),
                Value::Integer(10),
                Value::Integer(2),
                Value::String("other".into()),
                Value::Integer(1),
                Value::Integer(9),
                Value::Integer(2),
                Value::String("s".into()),
            ]
        );
        assert_eq!(access.currval("s").expect("original sequence"), 1);
        assert_eq!(access.nextval("other").expect("unreferenced sequence"), 1);
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
