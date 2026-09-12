// SPDX-License-Identifier: BUSL-1.1

//! Enforcement-option parsing, validation, and projection helpers:
//!
//! - [`parse_balanced_clause`]        — WITH BALANCED ON (...) parser
//! - [`validate_balanced_columns`]    — DDL-time check that the columns a
//!   BALANCED declaration names exist and carry types the commit-time check
//!   can actually read; [`parse_and_validate_balanced_clause`] is the
//!   parse-then-check entry point DDL uses
//! - [`validate_hash_chain_flags`]    — HASH_CHAIN implies an append-only
//!   collection
//! - [`find_materialized_sum_bindings`] — cross-collection
//!   materialized_sum lookup
//! - [`build_generated_column_specs`] — extract generated-column
//!   specs from a `StoredCollection`'s schema JSON

use crate::bootstrap::constraint_reconcile::CollectionSource;
use nodedb_types::DatabaseId;
use sonic_rs;

use crate::control::security::catalog::{BalancedConstraintDef, StoredCollection};

/// Everything that can be wrong with a collection's enforcement declaration
/// at DDL time.
///
/// Each variant carries the ANSI SQLSTATE the DDL layer reports for it via
/// [`EnforcementDeclError::sqlstate`], so a caller never has to re-classify a
/// message string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnforcementDeclError {
    #[error("BALANCED ON requires parenthesized options: (group_key = col, ...)")]
    MissingOpenParen,

    #[error("BALANCED ON: missing closing parenthesis")]
    MissingCloseParen,

    #[error("BALANCED ON: unknown option '{option}'")]
    UnknownOption { option: String },

    #[error("BALANCED ON: missing {field}")]
    MissingField { field: &'static str },

    #[error("BALANCED ON: {field} must be a valid column name, got '{column}'")]
    InvalidColumnName { field: &'static str, column: String },

    #[error("BALANCED ON: {field} column '{column}' is not declared on the collection")]
    UnknownColumn { field: &'static str, column: String },

    /// The commit-time check reads `group_key` and `entry_type` as strings; a
    /// column of any other type yields no entry at all, so the constraint
    /// would silently never fire.
    #[error(
        "BALANCED ON: {field} column '{column}' must be a text column for the \
         balance check to read it, got '{declared_type}'"
    )]
    NonTextKeyColumn {
        field: &'static str,
        column: String,
        declared_type: String,
    },

    /// The commit-time check parses `amount` as a decimal (from a JSON number
    /// or a numeric string); any other type yields no entry, silently
    /// disabling the constraint.
    #[error(
        "BALANCED ON: amount column '{column}' must be a numeric or text column, \
         got '{declared_type}'"
    )]
    NonNumericAmountColumn {
        column: String,
        declared_type: String,
    },

    /// A hash chain exists to make retroactive modification detectable, and
    /// `verify_chain` walks entries in order: removing or rewriting a chained
    /// row reports the SUCCESSOR's link as broken, blaming an untampered row.
    /// The chain is only meaningful on a collection that cannot be modified.
    #[error("HASH_CHAIN requires APPEND_ONLY")]
    HashChainRequiresAppendOnly,
}

impl EnforcementDeclError {
    /// ANSI SQLSTATE reported for this rejection.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::MissingOpenParen
            | Self::MissingCloseParen
            | Self::UnknownOption { .. }
            | Self::MissingField { .. }
            | Self::InvalidColumnName { .. }
            | Self::HashChainRequiresAppendOnly => "42601",
            Self::UnknownColumn { .. } => "42703",
            Self::NonTextKeyColumn { .. } | Self::NonNumericAmountColumn { .. } => "42804",
        }
    }
}

impl From<EnforcementDeclError> for crate::Error {
    fn from(error: EnforcementDeclError) -> Self {
        crate::Error::BadRequest {
            detail: error.to_string(),
        }
    }
}

/// `HASH_CHAIN` is only sound on an append-only collection.
///
/// Rejecting the contradictory combination is deliberate: silently switching
/// on `APPEND_ONLY` would impose a restriction the user never asked for.
pub fn validate_hash_chain_flags(
    hash_chain: bool,
    append_only: bool,
) -> Result<(), EnforcementDeclError> {
    if hash_chain && !append_only {
        return Err(EnforcementDeclError::HashChainRequiresAppendOnly);
    }
    Ok(())
}

/// Parse `BALANCED ON (group_key = col, debit = 'DEBIT',
/// credit = 'CREDIT', amount = col)` from the uppercase SQL
/// string. Returns `None` if not present.
pub fn parse_balanced_clause(
    upper: &str,
) -> Result<Option<BalancedConstraintDef>, EnforcementDeclError> {
    let Some(pos) = upper.find("BALANCED ON") else {
        return Ok(None);
    };
    let after = &upper[pos + "BALANCED ON".len()..];
    let after = after.trim_start();
    let Some(paren_start) = after.find('(') else {
        return Err(EnforcementDeclError::MissingOpenParen);
    };
    let Some(paren_end) = after.find(')') else {
        return Err(EnforcementDeclError::MissingCloseParen);
    };
    let inner = &after[paren_start + 1..paren_end];

    let mut group_key = None;
    let mut entry_type = None;
    let mut debit = None;
    let mut credit = None;
    let mut amount = None;

    for part in inner.split(',') {
        let part = part.trim();
        if let Some((key, value)) = part.split_once('=') {
            let key = key.trim().to_uppercase();
            let value = value.trim().trim_matches('\'').trim_matches('"');
            match key.as_str() {
                "GROUP_KEY" => group_key = Some(value.to_lowercase()),
                "ENTRY_TYPE" => entry_type = Some(value.to_lowercase()),
                "DEBIT" => debit = Some(value.to_string()),
                "CREDIT" => credit = Some(value.to_string()),
                "AMOUNT" => amount = Some(value.to_lowercase()),
                other => {
                    return Err(EnforcementDeclError::UnknownOption {
                        option: other.to_string(),
                    });
                }
            }
        }
    }

    let group_key = group_key.ok_or(EnforcementDeclError::MissingField { field: "group_key" })?;
    let debit = debit.ok_or(EnforcementDeclError::MissingField { field: "debit" })?;
    let credit = credit.ok_or(EnforcementDeclError::MissingField { field: "credit" })?;
    let amount = amount.ok_or(EnforcementDeclError::MissingField { field: "amount" })?;
    let entry_type = entry_type.unwrap_or_else(|| "entry_type".to_string());

    // Validate column names are safe identifiers (alphanumeric + underscore).
    for (label, col) in [
        ("group_key", group_key.as_str()),
        ("entry_type", entry_type.as_str()),
        ("amount", amount.as_str()),
    ] {
        if col.is_empty() || !col.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(EnforcementDeclError::InvalidColumnName {
                field: label,
                column: col.to_string(),
            });
        }
    }

    Ok(Some(BalancedConstraintDef {
        group_key_column: group_key,
        entry_type_column: entry_type,
        debit_value: debit,
        credit_value: credit,
        amount_column: amount,
    }))
}

/// Parse a `BALANCED ON` clause from a pre-extracted raw inner string.
///
/// The raw string is the content inside the outer parens:
/// `"group_key = txn_type, debit = 'DEBIT', credit = 'CREDIT', amount = amount"`.
///
/// Called by typed-AST handlers that receive `balanced_raw: Option<String>`.
pub fn parse_balanced_clause_from_raw(
    raw: &str,
) -> Result<Option<BalancedConstraintDef>, EnforcementDeclError> {
    if raw.trim().is_empty() {
        return Ok(None);
    }
    // Reconstruct a minimal uppercase string for `parse_balanced_clause`.
    let pseudo = format!("BALANCED ON ({raw})");
    parse_balanced_clause(&pseudo.to_uppercase())
}

/// Parse a `BALANCED ON` clause and check it against the collection's
/// declared `(name, type)` columns in one step, so no caller can persist a
/// parsed-but-unchecked definition.
///
/// See [`parse_balanced_clause_from_raw`] and [`validate_balanced_columns`].
pub fn parse_and_validate_balanced_clause(
    raw: &str,
    columns: &[(String, String)],
) -> Result<Option<BalancedConstraintDef>, EnforcementDeclError> {
    let parsed = parse_balanced_clause_from_raw(raw)?;
    if let Some(def) = &parsed {
        validate_balanced_columns(def, columns)?;
    }
    Ok(parsed)
}

/// Reduce a declared SQL type string to its bare type name: `"DECIMAL(10,2)
/// NOT NULL"` → `"DECIMAL"`.
fn base_type_name(type_str: &str) -> String {
    let head = type_str.split_whitespace().next().unwrap_or(type_str);
    let head = head.split('(').next().unwrap_or(head);
    head.to_ascii_uppercase()
}

/// Types the commit-time balance check can read as a string key.
fn is_text_type(base: &str) -> bool {
    matches!(
        base,
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER" | "BPCHAR" | "STRING" | "NAME" | "UUID"
    )
}

/// Types the commit-time balance check can parse into a decimal amount.
fn is_numeric_type(base: &str) -> bool {
    matches!(
        base,
        "INT"
            | "INT2"
            | "INT4"
            | "INT8"
            | "INTEGER"
            | "SMALLINT"
            | "BIGINT"
            | "DECIMAL"
            | "NUMERIC"
            | "MONEY"
            | "REAL"
            | "FLOAT"
            | "FLOAT4"
            | "FLOAT8"
            | "DOUBLE"
            | "SERIAL"
            | "SMALLSERIAL"
            | "BIGSERIAL"
    )
}

/// Check a `BALANCED ON` declaration against the collection's declared
/// columns.
///
/// The commit-time check reads `group_key` and `entry_type` with a string
/// accessor and parses `amount` as a decimal; a row that fails any of those
/// three reads contributes no entry, and a constraint with no entries always
/// passes. A declaration naming a missing column — or a column whose type the
/// check cannot read — is therefore silently unenforced, so it is refused
/// here instead.
///
/// `columns` is the declared `(name, type)` list. An empty list means the
/// collection is schemaless: nothing is declared, so nothing is checkable and
/// the declaration is accepted as-is.
pub fn validate_balanced_columns(
    def: &BalancedConstraintDef,
    columns: &[(String, String)],
) -> Result<(), EnforcementDeclError> {
    if columns.is_empty() {
        return Ok(());
    }

    let declared_type = |column: &str| -> Option<String> {
        columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, type_str)| base_type_name(type_str))
    };

    for (field, column) in [
        ("group_key", def.group_key_column.as_str()),
        ("entry_type", def.entry_type_column.as_str()),
    ] {
        let base = declared_type(column).ok_or_else(|| EnforcementDeclError::UnknownColumn {
            field,
            column: column.to_string(),
        })?;
        if !is_text_type(&base) {
            return Err(EnforcementDeclError::NonTextKeyColumn {
                field,
                column: column.to_string(),
                declared_type: base,
            });
        }
    }

    let amount = def.amount_column.as_str();
    let base = declared_type(amount).ok_or_else(|| EnforcementDeclError::UnknownColumn {
        field: "amount",
        column: amount.to_string(),
    })?;
    // A text amount is read through the decimal-from-string path, so it is
    // enforced; only a type neither branch can read is refused.
    if !is_numeric_type(&base) && !is_text_type(&base) {
        return Err(EnforcementDeclError::NonNumericAmountColumn {
            column: amount.to_string(),
            declared_type: base,
        });
    }

    Ok(())
}

/// Find all materialized sum bindings where
/// `source_collection == collection_name`.
///
/// Scans all collections for the tenant and extracts bindings
/// from their `materialized_sums` definitions. These are placed
/// on the SOURCE collection's `EnforcementOptions` so the Data
/// Plane fires the trigger on INSERT.
pub fn find_materialized_sum_bindings<S: CollectionSource + ?Sized>(
    catalog: &S,
    tenant_id: u64,
    collection_name: &str,
    database_id: DatabaseId,
) -> Vec<nodedb_physical::physical_plan::MaterializedSumBinding> {
    let all_collections = catalog
        .collections_for_tenant(database_id, tenant_id)
        .unwrap_or_default();

    let mut bindings = Vec::new();
    for target_coll in &all_collections {
        for def in &target_coll.materialized_sums {
            if def.source_collection == collection_name {
                bindings.push(nodedb_physical::physical_plan::MaterializedSumBinding {
                    target_collection: def.target_collection.clone(),
                    target_column: def.target_column.clone(),
                    join_column: def.join_column.clone(),
                    value_expr: def.value_expr.clone(),
                    // `target_coll` is the collection the binding writes into,
                    // so its declared key names the target row.
                    declared_primary_key: target_coll.declared_primary_key.clone(),
                });
            }
        }
    }
    bindings
}

/// Build generated column specs from the stored collection's
/// schema. Checks both strict-schema `ColumnDef` entries (via
/// `timeseries_config`, reused for schema storage) and schemaless
/// `FieldDefinition` entries (via `field_defs`).
pub fn build_generated_column_specs(
    coll: &StoredCollection,
) -> Vec<nodedb_physical::physical_plan::GeneratedColumnSpec> {
    let mut specs = Vec::new();

    let schema_json = coll.timeseries_config.as_deref().unwrap_or("");
    if let Ok(schema) = sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(schema_json) {
        for col in &schema.columns {
            if let Some(ref expr_json) = col.generated_expr
                && let Ok(expr) = sonic_rs::from_str::<crate::bridge::expr_eval::SqlExpr>(expr_json)
            {
                specs.push(nodedb_physical::physical_plan::GeneratedColumnSpec {
                    name: col.name.clone(),
                    expr,
                    depends_on: col.generated_deps.clone(),
                });
            }
        }
    }

    for field_def in &coll.field_defs {
        if field_def.is_generated
            && !field_def.value_expr.is_empty()
            && let Ok(expr) =
                sonic_rs::from_str::<crate::bridge::expr_eval::SqlExpr>(&field_def.value_expr)
            && !specs.iter().any(|s| s.name == field_def.name)
        {
            specs.push(nodedb_physical::physical_plan::GeneratedColumnSpec {
                name: field_def.name.clone(),
                expr,
                depends_on: field_def.generated_deps.clone(),
            });
        }
    }

    specs
}

/// Replace user-defined type names with their physical storage type (`TEXT`)
/// so that `build_collection_type` can parse a valid strict schema.
///
/// Enum and composite values are stored physically as `TEXT`. The original
/// type names are preserved in `StoredCollection.fields` for drop-protection
/// and SHOW COLLECTIONS output.
pub(super) fn resolve_custom_type_columns(
    columns: &[(String, String)],
    state: &crate::control::state::SharedState,
    database_id: u64,
    tenant_id: u64,
) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|(col_name, type_str)| {
            // Extract the bare type name (before any modifier like NOT NULL).
            let bare = type_str.split_whitespace().next().unwrap_or(type_str);
            if state
                .custom_type_registry
                .exists(database_id, tenant_id, &bare.to_lowercase())
            {
                // Replace the custom type name with TEXT, preserving any
                // trailing modifiers (e.g. "priority NOT NULL" → "TEXT NOT NULL").
                let rest = type_str[bare.len()..].trim();
                let resolved = if rest.is_empty() {
                    "TEXT".to_string()
                } else {
                    format!("TEXT {rest}")
                };
                (col_name.clone(), resolved)
            } else {
                (col_name.clone(), type_str.clone())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger_columns() -> Vec<(String, String)> {
        vec![
            ("journal_id".to_string(), "TEXT".to_string()),
            ("entry_type".to_string(), "VARCHAR(8) NOT NULL".to_string()),
            ("amount".to_string(), "DECIMAL(18,2)".to_string()),
            ("posted_at".to_string(), "TIMESTAMP".to_string()),
        ]
    }

    fn ledger_def() -> BalancedConstraintDef {
        BalancedConstraintDef {
            group_key_column: "journal_id".to_string(),
            entry_type_column: "entry_type".to_string(),
            debit_value: "DEBIT".to_string(),
            credit_value: "CREDIT".to_string(),
            amount_column: "amount".to_string(),
        }
    }

    #[test]
    fn hash_chain_without_append_only_is_rejected() {
        let error = validate_hash_chain_flags(true, false)
            .expect_err("HASH_CHAIN without APPEND_ONLY must be refused");
        assert_eq!(error, EnforcementDeclError::HashChainRequiresAppendOnly);
        assert_eq!(error.sqlstate(), "42601");
    }

    #[test]
    fn hash_chain_with_append_only_is_accepted() {
        assert!(validate_hash_chain_flags(true, true).is_ok());
    }

    #[test]
    fn append_only_without_hash_chain_is_accepted() {
        assert!(validate_hash_chain_flags(false, true).is_ok());
        assert!(validate_hash_chain_flags(false, false).is_ok());
    }

    #[test]
    fn balanced_on_declared_columns_is_accepted() {
        assert!(validate_balanced_columns(&ledger_def(), &ledger_columns()).is_ok());
    }

    #[test]
    fn balanced_on_text_amount_is_accepted() {
        let mut columns = ledger_columns();
        columns[2] = ("amount".to_string(), "TEXT".to_string());
        assert!(validate_balanced_columns(&ledger_def(), &columns).is_ok());
    }

    #[test]
    fn balanced_on_schemaless_collection_is_accepted() {
        assert!(validate_balanced_columns(&ledger_def(), &[]).is_ok());
    }

    #[test]
    fn balanced_on_missing_column_is_rejected() {
        let mut def = ledger_def();
        def.group_key_column = "ledger_id".to_string();
        let error = validate_balanced_columns(&def, &ledger_columns())
            .expect_err("undeclared group_key column must be refused");
        assert_eq!(
            error,
            EnforcementDeclError::UnknownColumn {
                field: "group_key",
                column: "ledger_id".to_string(),
            }
        );
        assert_eq!(error.sqlstate(), "42703");
    }

    #[test]
    fn balanced_on_non_text_group_key_is_rejected() {
        let mut columns = ledger_columns();
        columns[0] = ("journal_id".to_string(), "BIGINT".to_string());
        let error = validate_balanced_columns(&ledger_def(), &columns)
            .expect_err("non-text group_key column must be refused");
        assert_eq!(
            error,
            EnforcementDeclError::NonTextKeyColumn {
                field: "group_key",
                column: "journal_id".to_string(),
                declared_type: "BIGINT".to_string(),
            }
        );
        assert_eq!(error.sqlstate(), "42804");
    }

    #[test]
    fn balanced_on_unreadable_amount_is_rejected() {
        let mut columns = ledger_columns();
        columns[2] = ("amount".to_string(), "BOOLEAN".to_string());
        let error = validate_balanced_columns(&ledger_def(), &columns)
            .expect_err("non-numeric amount column must be refused");
        assert_eq!(
            error,
            EnforcementDeclError::NonNumericAmountColumn {
                column: "amount".to_string(),
                declared_type: "BOOLEAN".to_string(),
            }
        );
        assert_eq!(error.sqlstate(), "42804");
    }

    #[test]
    fn balanced_clause_parses_and_reports_typed_errors() {
        let parsed = parse_balanced_clause_from_raw(
            "group_key = journal_id, debit = 'DEBIT', credit = 'CREDIT', amount = amount",
        )
        .expect("well-formed BALANCED ON parses")
        .expect("a definition is produced");
        assert_eq!(parsed.group_key_column, "journal_id");
        assert_eq!(parsed.entry_type_column, "entry_type");
        assert_eq!(parsed.amount_column, "amount");

        let error = parse_balanced_clause_from_raw("debit = 'DEBIT', credit = 'CREDIT'")
            .expect_err("missing group_key must be refused");
        assert_eq!(
            error,
            EnforcementDeclError::MissingField { field: "group_key" }
        );
        assert_eq!(error.sqlstate(), "42601");

        assert!(
            parse_balanced_clause_from_raw("")
                .expect("an absent clause parses")
                .is_none()
        );
    }

    #[test]
    fn parse_and_validate_runs_both_halves() {
        let raw = "group_key = journal_id, debit = 'DEBIT', credit = 'CREDIT', amount = amount";
        assert!(
            parse_and_validate_balanced_clause(raw, &ledger_columns())
                .expect("a declaration matching the columns is accepted")
                .is_some()
        );

        let mut columns = ledger_columns();
        columns[0] = ("journal_id".to_string(), "INTEGER".to_string());
        let error = parse_and_validate_balanced_clause(raw, &columns)
            .expect_err("a declaration the balance check cannot read is refused");
        assert_eq!(error.sqlstate(), "42804");
    }
}
