// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge UPDATE lifecycle: diff the implicit edges surfaced by the OLLP
//! pre-execution scan against the SET-clause overrides and append the minimal
//! `GraphOp::EdgeDelete` / `GraphOp::EdgePut` tasks to keep the mirrored graph
//! edges transactionally consistent with the document write.
//!
//! The edge identity for the diff is `(from, to, label)` — the same identity
//! the INSERT (`append_implicit_edge_tasks`) and DELETE
//! (`append_implicit_edge_delete_tasks`) paths key on. Edge `weight` is NOT part
//! of that identity, but it IS carried through the recon→diff→EdgePut ripple so a
//! moved or relabeled edge is re-created with the doc's (unchanged) weight rather
//! than silently reverting to the default unit weight — the locked invariant is
//! that an implicit edge must be IDENTICAL to the edge the matching INSERT
//! created.
//!
//! # Weight refresh for a weight-only change
//!
//! A SET that touches `weight` but leaves `_from`/`_to`/`_type` unchanged is
//! identity-equal, so the diff would otherwise be a no-op and leave the stored
//! edge weight stale. The CSR `add_edge_internal` DEDUPS on `(src,label,dst)`
//! and returns early WITHOUT updating the weight, so a bare re-`EdgePut` of the
//! same identity does NOT refresh the CSR weight. We therefore emit an explicit
//! `EdgeDelete(old) + EdgePut(new_weight)` pair for the weight-only case.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_physical::physical_task::PhysicalTask;

use super::extract::resolve_edge_label;
use super::routed::{EdgeRouteCtx, push_edge_delete, push_edge_put};
use crate::control::planner::calvin::preexec::ScannedEdge;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// How a single reserved edge field (`_from` / `_to` / `_type`) is affected by
/// an UPDATE's SET clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldUpdate {
    /// Field not present in the SET clause — its value is preserved.
    Unchanged,
    /// Field SET to a literal string value.
    Set(String),
    /// Field SET to null (or a non-string literal) — the edge field is removed.
    Cleared,
}

/// How the numeric `weight` field is affected by an UPDATE's SET clause.
///
/// Weight is numeric (not a string), so it gets its own enum rather than reusing
/// [`FieldUpdate`]: `Set` carries an `f64`, and `Cleared` (null / non-numeric)
/// reverts the edge to the default unit weight.
#[derive(Debug, Clone, PartialEq)]
pub enum WeightUpdate {
    /// `weight` not present in the SET clause — its value is preserved.
    Unchanged,
    /// `weight` SET to a finite numeric literal.
    Set(f64),
    /// `weight` SET to null (or a non-numeric / non-finite literal) — the edge
    /// reverts to the default unit weight.
    Cleared,
}

/// The reserved-edge-field overrides parsed from an UPDATE's SET clause.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeFieldOverrides {
    pub from: FieldUpdate,
    pub to: FieldUpdate,
    pub label: FieldUpdate,
    pub weight: WeightUpdate,
}

/// Parse the reserved-edge-field overrides (`_from`, `_to`, `_type`) from an
/// UPDATE's SET assignments.
///
/// For each reserved string field (`_from`/`_to`/`_type`) found: a `Literal`
/// that decodes (as `extract_edge` reads an edge field) to a string yields
/// [`FieldUpdate::Set`]; a null or non-string literal yields
/// [`FieldUpdate::Cleared`]; an absent field stays [`FieldUpdate::Unchanged`].
/// The numeric `weight` field is decoded the way `extract_edge` decodes it: a
/// finite f64 (from a Float, Integer or Decimal literal) yields
/// [`WeightUpdate::Set`]; a null / non-numeric / non-finite literal yields
/// [`WeightUpdate::Cleared`]; an absent field stays [`WeightUpdate::Unchanged`].
/// An `Expr` assignment to one of the three reserved STRING fields returns a
/// typed internal error — defensively, since the `convert_update` planner gate
/// rejects it at plan time, this is never expected to fire but must never panic.
pub fn parse_edge_field_overrides(
    updates: &[(String, UpdateValue)],
) -> crate::Result<EdgeFieldOverrides> {
    let mut overrides = EdgeFieldOverrides {
        from: FieldUpdate::Unchanged,
        to: FieldUpdate::Unchanged,
        label: FieldUpdate::Unchanged,
        weight: WeightUpdate::Unchanged,
    };
    for (field, val) in updates {
        // `weight` is numeric — decode it into its own override enum, not the
        // string-typed `FieldUpdate` slots.
        if field == "weight" {
            overrides.weight = match val {
                UpdateValue::Literal(bytes) => match decode_literal_weight(bytes) {
                    Some(w) => WeightUpdate::Set(w),
                    None => WeightUpdate::Cleared,
                },
                // An expression `weight` update can't be reconciled into the
                // mirrored edge here; reject defensively (the planner gate
                // already blocks it at plan time).
                UpdateValue::Expr(_) => {
                    return Err(crate::Error::BadRequest {
                        detail: "expression updates to the reserved edge field 'weight' are not \
                                 supported on edge-bearing collections; use a literal value"
                            .to_string(),
                    });
                }
            };
            continue;
        }
        let slot = match field.as_str() {
            "_from" => &mut overrides.from,
            "_to" => &mut overrides.to,
            "_type" => &mut overrides.label,
            _ => continue,
        };
        *slot = match val {
            UpdateValue::Literal(bytes) => match decode_literal_string(bytes) {
                Some(s) => FieldUpdate::Set(s),
                None => FieldUpdate::Cleared,
            },
            UpdateValue::Expr(_) => {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "expression updates to reserved edge fields (_from, _to, _type) \
                         are not supported on edge-bearing collections (field '{field}'); \
                         use a literal value"
                    ),
                });
            }
        };
    }
    Ok(overrides)
}

/// Decode a standard-msgpack literal value, returning `Some(s)` only when it is
/// a string, as `extract_edge` reads an edge field.
fn decode_literal_string(bytes: &[u8]) -> Option<String> {
    match nodedb_types::value_from_msgpack(bytes).ok()? {
        nodedb_types::Value::String(s) => Some(s),
        _ => None,
    }
}

/// Decode a standard-msgpack literal value into a FINITE f64, returning `None`
/// for null / non-numeric / non-finite. Reads the weight as `extract_edge`
/// does (`nodedb_graph::property_weight`: Integer, Float or Decimal).
fn decode_literal_weight(bytes: &[u8]) -> Option<f64> {
    let decoded = nodedb_types::value_from_msgpack(bytes).ok()?;
    nodedb_graph::property_weight(&decoded).filter(|w| w.is_finite())
}

/// The canonical edge identity used for the diff: `(from, to, resolved-label)`.
type EdgeIdentity = (String, String, String);

/// Append the minimal `EdgeDelete` / `EdgePut` tasks to reconcile the mirrored
/// graph edges with an UPDATE's SET clause.
///
/// `edges` are the implicit edges of the matched edge documents (surfaced by
/// the OLLP recon scan); `all_surrogates` is the full matched-document surrogate
/// set (a superset of the edge docs — it also contains non-edge docs that an
/// endpoint-setting UPDATE may turn INTO edges); `overrides` is the parsed SET
/// clause. Emitted ops are deduped so identical `EdgePut` / `EdgeDelete` are not
/// repeated when several rows map to the same edge identity.
///
/// Tenancy/collection identity shared by every task
/// [`append_implicit_edge_update_tasks`] appends.
pub struct EdgeUpdateCtx<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub trace_id: TraceId,
    pub collection: &'a str,
}

pub async fn append_implicit_edge_update_tasks(
    ctx: EdgeUpdateCtx<'_>,
    out: &mut Vec<PhysicalTask>,
    edges: &[ScannedEdge],
    all_surrogates: &[u32],
    overrides: &EdgeFieldOverrides,
) -> crate::Result<()> {
    let EdgeUpdateCtx {
        state,
        tenant_id,
        database_id,
        trace_id,
        collection,
    } = ctx;
    let old_by_surrogate: HashMap<u32, &ScannedEdge> =
        edges.iter().map(|e| (e.surrogate, e)).collect();

    // Dedup ledgers so identical ops are emitted at most once.
    let mut deleted: HashSet<EdgeIdentity> = HashSet::new();
    let mut put: HashSet<EdgeIdentity> = HashSet::new();

    // 1. Existing edges: apply overrides, diff old-vs-new identity.
    for old in edges {
        let new_from = apply_override(&overrides.from, Some(old.from.as_str()));
        let new_to = apply_override(&overrides.to, Some(old.to.as_str()));
        // Label: Unchanged preserves the doc's raw `_type` (which may be absent
        // → default "edge"); Set overrides it; Cleared falls back to default.
        let new_label_raw = match &overrides.label {
            FieldUpdate::Unchanged => old.label.clone(),
            FieldUpdate::Set(s) => Some(s.clone()),
            FieldUpdate::Cleared => None,
        };
        // Weight is NOT part of the identity, but the re-created edge must carry
        // it: Unchanged keeps the doc's (scanned) weight; Set overrides it;
        // Cleared reverts to default unit weight (`None`).
        let new_weight: Option<f64> = match &overrides.weight {
            WeightUpdate::Unchanged => old.weight,
            WeightUpdate::Set(w) => Some(*w),
            WeightUpdate::Cleared => None,
        };

        let old_identity: EdgeIdentity = (
            old.from.clone(),
            old.to.clone(),
            resolve_edge_label(old.label.as_deref()),
        );

        // The new edge exists only when BOTH endpoints survive.
        let new_identity: Option<EdgeIdentity> = match (new_from, new_to) {
            (Some(f), Some(t)) => Some((f, t, resolve_edge_label(new_label_raw.as_deref()))),
            _ => None,
        };

        if new_identity.as_ref() == Some(&old_identity) {
            // Identity unchanged. If `weight` changed (Set/Cleared), the stored
            // edge weight is now stale and a bare re-EdgePut would NOT refresh it
            // (the CSR dedups on identity and ignores the second put's
            // properties), so emit an explicit EdgeDelete(old)+EdgePut(new) pair
            // to force the weight through. A truly unchanged weight is a no-op.
            let weight_changed = !matches!(overrides.weight, WeightUpdate::Unchanged);
            if weight_changed && deleted.insert(old_identity.clone()) {
                push_edge_delete(
                    EdgeRouteCtx {
                        state,
                        tenant_id,
                        database_id,
                        trace_id,
                        collection,
                        src: &old_identity.0,
                        dst: &old_identity.1,
                    },
                    out,
                    old_identity.2.clone(),
                )
                .await?;
                put.insert(old_identity.clone());
                push_edge_put(
                    EdgeRouteCtx {
                        state,
                        tenant_id,
                        database_id,
                        trace_id,
                        collection,
                        src: &old_identity.0,
                        dst: &old_identity.1,
                    },
                    out,
                    old_identity.2.clone(),
                    new_weight,
                )
                .await?;
            }
            continue;
        }

        // Identity changed (or the edge disappeared): retract the old edge.
        if deleted.insert(old_identity.clone()) {
            push_edge_delete(
                EdgeRouteCtx {
                    state,
                    tenant_id,
                    database_id,
                    trace_id,
                    collection,
                    src: &old_identity.0,
                    dst: &old_identity.1,
                },
                out,
                old_identity.2.clone(),
            )
            .await?;
        }

        // Install the new edge when it still exists, carrying the doc's weight.
        if let Some(new_identity) = new_identity
            && put.insert(new_identity.clone())
        {
            push_edge_put(
                EdgeRouteCtx {
                    state,
                    tenant_id,
                    database_id,
                    trace_id,
                    collection,
                    src: &new_identity.0,
                    dst: &new_identity.1,
                },
                out,
                new_identity.2.clone(),
                new_weight,
            )
            .await?;
        }
    }

    // 2. Non-edge docs becoming edges: only when BOTH endpoints are SET to a
    //    literal string (a clean base doc carries neither `_from` nor `_to`, so
    //    `Unchanged`/`Cleared` cannot synthesize an endpoint). The base doc had
    //    no `_type`, so the label is the SET value or the default.
    if let (FieldUpdate::Set(f), FieldUpdate::Set(t)) = (&overrides.from, &overrides.to) {
        let new_label_raw = match &overrides.label {
            FieldUpdate::Set(s) => Some(s.as_str()),
            // Cleared / Unchanged on a clean base doc → default "edge".
            FieldUpdate::Cleared | FieldUpdate::Unchanged => None,
        };
        let identity: EdgeIdentity = (f.clone(), t.clone(), resolve_edge_label(new_label_raw));

        // This is the ONLY spot where the prior weight cannot be recovered: the
        // base doc was not projected as an edge by the recon scan (it had no
        // `_from`/`_to`), so no `ScannedEdge.weight` exists. Use the SET
        // `weight` value when present; otherwise default to unit weight.
        let new_weight: Option<f64> = match &overrides.weight {
            WeightUpdate::Set(w) => Some(*w),
            WeightUpdate::Unchanged | WeightUpdate::Cleared => None,
        };

        let has_new_edge_doc = all_surrogates
            .iter()
            .any(|s| !old_by_surrogate.contains_key(s));

        if has_new_edge_doc && put.insert(identity.clone()) {
            push_edge_put(
                EdgeRouteCtx {
                    state,
                    tenant_id,
                    database_id,
                    trace_id,
                    collection,
                    src: &identity.0,
                    dst: &identity.1,
                },
                out,
                identity.2.clone(),
                new_weight,
            )
            .await?;
        }
    }

    Ok(())
}

/// Resolve a single endpoint field under its override: `Unchanged` keeps the
/// current value, `Set` replaces it, `Cleared` removes it (`None`).
fn apply_override(update: &FieldUpdate, current: Option<&str>) -> Option<String> {
    match update {
        FieldUpdate::Unchanged => current.map(str::to_string),
        FieldUpdate::Set(s) => Some(s.clone()),
        FieldUpdate::Cleared => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit_str(s: &str) -> UpdateValue {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv::Value::String(s.into())).expect("encode");
        UpdateValue::Literal(buf)
    }

    fn lit_null() -> UpdateValue {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv::Value::Nil).expect("encode");
        UpdateValue::Literal(buf)
    }

    fn lit_f64(w: f64) -> UpdateValue {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv::Value::F64(w)).expect("encode");
        UpdateValue::Literal(buf)
    }

    fn lit_int(i: i64) -> UpdateValue {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv::Value::Integer(i.into())).expect("encode");
        UpdateValue::Literal(buf)
    }

    #[test]
    fn parse_set_endpoints_and_label() {
        let updates = vec![
            ("_from".to_string(), lit_str("x")),
            ("_to".to_string(), lit_str("y")),
            ("_type".to_string(), lit_str("ROAD")),
            ("other".to_string(), lit_str("z")),
        ];
        let ov = parse_edge_field_overrides(&updates).expect("parse");
        assert_eq!(ov.from, FieldUpdate::Set("x".to_string()));
        assert_eq!(ov.to, FieldUpdate::Set("y".to_string()));
        assert_eq!(ov.label, FieldUpdate::Set("ROAD".to_string()));
        assert_eq!(ov.weight, WeightUpdate::Unchanged);
    }

    #[test]
    fn parse_null_is_cleared_and_absent_is_unchanged() {
        let updates = vec![("_from".to_string(), lit_null())];
        let ov = parse_edge_field_overrides(&updates).expect("parse");
        assert_eq!(ov.from, FieldUpdate::Cleared);
        assert_eq!(ov.to, FieldUpdate::Unchanged);
        assert_eq!(ov.label, FieldUpdate::Unchanged);
        assert_eq!(ov.weight, WeightUpdate::Unchanged);
    }

    #[test]
    fn parse_weight_set_float_and_int() {
        let ov =
            parse_edge_field_overrides(&[("weight".to_string(), lit_f64(2.5))]).expect("parse");
        assert_eq!(ov.weight, WeightUpdate::Set(2.5));

        let ov = parse_edge_field_overrides(&[("weight".to_string(), lit_int(3))]).expect("parse");
        assert_eq!(ov.weight, WeightUpdate::Set(3.0));
    }

    #[test]
    fn parse_weight_null_or_nonnumeric_is_cleared() {
        let ov = parse_edge_field_overrides(&[("weight".to_string(), lit_null())]).expect("parse");
        assert_eq!(ov.weight, WeightUpdate::Cleared);

        // A non-numeric literal (string) for `weight` clears it.
        let ov =
            parse_edge_field_overrides(&[("weight".to_string(), lit_str("heavy"))]).expect("parse");
        assert_eq!(ov.weight, WeightUpdate::Cleared);
    }

    #[test]
    fn parse_weight_expr_is_rejected() {
        use nodedb_query::expr::SqlExpr;
        let updates = vec![(
            "weight".to_string(),
            UpdateValue::Expr(SqlExpr::Column("other".to_string())),
        )];
        assert!(parse_edge_field_overrides(&updates).is_err());
    }

    #[test]
    fn parse_expr_on_edge_field_is_rejected() {
        use nodedb_query::expr::SqlExpr;
        let updates = vec![(
            "_to".to_string(),
            UpdateValue::Expr(SqlExpr::Column("other".to_string())),
        )];
        assert!(parse_edge_field_overrides(&updates).is_err());
    }

    #[test]
    fn parse_expr_on_non_edge_field_is_ignored() {
        use nodedb_query::expr::SqlExpr;
        let updates = vec![(
            "score".to_string(),
            UpdateValue::Expr(SqlExpr::Column("other".to_string())),
        )];
        let ov = parse_edge_field_overrides(&updates).expect("parse");
        assert_eq!(ov.from, FieldUpdate::Unchanged);
    }

    #[test]
    fn apply_override_semantics() {
        assert_eq!(
            apply_override(&FieldUpdate::Unchanged, Some("a")),
            Some("a".to_string())
        );
        assert_eq!(
            apply_override(&FieldUpdate::Set("b".to_string()), Some("a")),
            Some("b".to_string())
        );
        assert_eq!(apply_override(&FieldUpdate::Cleared, Some("a")), None);
    }
}
