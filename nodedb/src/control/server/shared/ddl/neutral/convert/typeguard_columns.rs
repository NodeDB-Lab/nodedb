// SPDX-License-Identifier: BUSL-1.1

//! Typeguard field definitions turned into strict-schema columns.
//!
//! `CONVERT COLLECTION <name> TO document_strict` with no explicit column list
//! reads the collection's active typeguards instead.

use super::super::super::result::DdlError;
use super::super::column_default::validate_column_default;
use super::support::err;
use super::type_map::typeguard_type_to_column_type;

/// Convert typeguard field definitions to strict schema column definitions.
///
/// Each typeguard field becomes a column. The type expression is mapped to ColumnType.
/// REQUIRED fields become NOT NULL. DEFAULT expressions carry over.
///
/// A carried-over expression becomes a column `DEFAULT`, so it passes the gate
/// every declared column `DEFAULT` passes. An unregistered function name
/// raises `42883`; every other rejection raises `42601`.
pub(super) fn typeguards_to_column_defs(
    guards: &[nodedb_types::TypeGuardFieldDef],
) -> Result<Vec<nodedb_types::columnar::ColumnDef>, DdlError> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    // Always include an `id` column as primary key.
    let mut columns = vec![ColumnDef::required("id", ColumnType::String).with_primary_key()];

    for guard in guards {
        // Skip dot-path fields (nested) — strict schema is flat.
        if guard.field.contains('.') {
            continue;
        }
        // Skip if already added (e.g., "id").
        if guard.field == "id" || columns.iter().any(|c| c.name == guard.field) {
            continue;
        }

        let ct = typeguard_type_to_column_type(&guard.type_expr).map_err(|e| {
            err(
                &e.sqlstate,
                format!("field '{}': {}", guard.field, e.message),
            )
        })?;
        let mut col = if guard.required {
            ColumnDef::required(guard.field.clone(), ct)
        } else {
            ColumnDef::nullable(guard.field.clone(), ct)
        };
        // A guard carries either DEFAULT or VALUE, never both. Strict schema
        // has one materialization slot, so both land on the column `DEFAULT`.
        if let Some(expr) = guard.default_expr.clone().or(guard.value_expr.clone()) {
            validate_column_default(&col.name, &expr)?;
            col.default = Some(expr);
        }
        columns.push(col);
    }

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard(field: &str, type_expr: &str) -> nodedb_types::TypeGuardFieldDef {
        nodedb_types::TypeGuardFieldDef {
            field: field.to_string(),
            type_expr: type_expr.to_string(),
            required: false,
            check_expr: None,
            default_expr: None,
            value_expr: None,
        }
    }

    /// An unresolvable guard type stops CONVERT instead of storing a column
    /// type the guard never declared.
    #[test]
    fn typeguards_to_column_defs_refuses_an_unresolvable_guard_type() {
        let guards = vec![guard("gadget", "WIDGET")];
        let error =
            typeguards_to_column_defs(&guards).expect_err("an unresolvable guard type must refuse");
        assert_eq!(error.sqlstate, "42601");
        assert!(
            error.message.contains("gadget"),
            "the error must name the field: {}",
            error.message
        );
    }

    /// A guard DEFAULT the column path can evaluate reaches the column.
    #[test]
    fn typeguards_to_column_defs_carry_a_literal_default() {
        let mut status = guard("status", "STRING");
        status.default_expr = Some("'draft'".to_string());
        let columns = typeguards_to_column_defs(&[status]).expect("a literal DEFAULT must convert");
        assert_eq!(columns[1].name, "status");
        assert_eq!(columns[1].default.as_deref(), Some("'draft'"));
    }

    /// A guard VALUE lands on the column `DEFAULT`, the one strict-schema slot
    /// that materializes an omitted field.
    #[test]
    fn typeguards_to_column_defs_carry_a_value_expression() {
        let mut computed = guard("computed", "STRING");
        computed.value_expr = Some("'server'".to_string());
        let columns =
            typeguards_to_column_defs(&[computed]).expect("a VALUE expression must convert");
        assert_eq!(columns[1].default.as_deref(), Some("'server'"));
    }

    /// A carried-over expression naming an unregistered function stops CONVERT
    /// with the SQLSTATE every other DEFAULT source raises for it.
    #[test]
    fn typeguards_to_column_defs_refuse_an_unevaluable_default() {
        let mut status = guard("status", "STRING");
        status.default_expr = Some("no_such_function()".to_string());
        let error = typeguards_to_column_defs(&[status])
            .expect_err("an unevaluable carried-over DEFAULT must refuse");
        assert_eq!(error.sqlstate, "42883");
    }
}
