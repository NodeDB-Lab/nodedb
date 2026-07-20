// SPDX-License-Identifier: BUSL-1.1

//! Aggregate result-cache key derivation.

use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};

/// Serialize group-key specs into a deterministic, distinct cache-key fragment.
/// Each spec contributes `output_name=field` (empty field for a computed key),
/// so distinct group-key shapes never collide.
fn group_specs_key(group_by: &[GroupKeySpec]) -> String {
    group_by
        .iter()
        .map(|spec| {
            format!(
                "{}={}",
                spec.output_name,
                spec.field.as_deref().unwrap_or("")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn aggregate_cache_key(
    database_id: u64,
    tid: u64,
    collection: &str,
    group_by: &[GroupKeySpec],
    aggregates: &[AggregateSpec],
    sub_group_by: &[String],
    sub_aggregates: &[AggregateSpec],
) -> (crate::types::DatabaseId, crate::types::TenantId, String) {
    use std::fmt::Write;
    let mut rest = format!(
        "{collection}\0{}\0{}",
        group_specs_key(group_by),
        aggregates
            .iter()
            .map(|agg| {
                if agg.expr.is_some() {
                    format!("{}(expr)->{}", agg.function, agg.alias)
                } else {
                    format!("{}({})->{}", agg.function, agg.field, agg.alias)
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    );
    if !sub_group_by.is_empty() || !sub_aggregates.is_empty() {
        let _ = write!(
            rest,
            "\0sub:{}\0{}",
            sub_group_by.join(","),
            sub_aggregates
                .iter()
                .map(|agg| {
                    if agg.expr.is_some() {
                        format!("{}(expr)->{}", agg.function, agg.alias)
                    } else {
                        format!("{}({})->{}", agg.function, agg.field, agg.alias)
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    (
        crate::types::DatabaseId::new(database_id),
        crate::types::TenantId::new(tid),
        rest,
    )
}

pub(super) fn legacy_aggregate_pairs(
    aggregates: &[AggregateSpec],
) -> Option<Vec<(String, String)>> {
    aggregates
        .iter()
        .map(|agg| {
            if agg.expr.is_some() {
                None
            } else {
                Some((agg.function.clone(), agg.field.clone()))
            }
        })
        .collect()
}
