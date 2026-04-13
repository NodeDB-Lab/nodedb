//! Enforcement-option parsing and projection helpers:
//!
//! - [`parse_balanced_clause`]        — WITH BALANCED ON (...) parser
//! - [`find_materialized_sum_bindings`] — cross-collection
//!   materialized_sum lookup
//! - [`build_generated_column_specs`] — extract generated-column
//!   specs from a `StoredCollection`'s schema JSON

use sonic_rs;

use crate::control::security::catalog::{
    BalancedConstraintDef, StoredCollection, types::SystemCatalog,
};

/// Parse `BALANCED ON (group_key = col, debit = 'DEBIT',
/// credit = 'CREDIT', amount = col)` from the uppercase SQL
/// string. Returns `None` if not present.
pub fn parse_balanced_clause(upper: &str) -> Result<Option<BalancedConstraintDef>, String> {
    let Some(pos) = upper.find("BALANCED ON") else {
        return Ok(None);
    };
    let after = &upper[pos + "BALANCED ON".len()..];
    let after = after.trim_start();
    let Some(paren_start) = after.find('(') else {
        return Err("BALANCED ON requires parenthesized options: (group_key = col, ...)".into());
    };
    let Some(paren_end) = after.find(')') else {
        return Err("BALANCED ON: missing closing parenthesis".into());
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
                other => return Err(format!("BALANCED ON: unknown option '{other}'")),
            }
        }
    }

    let group_key = group_key.ok_or("BALANCED ON: missing group_key")?;
    let debit = debit.ok_or("BALANCED ON: missing debit")?;
    let credit = credit.ok_or("BALANCED ON: missing credit")?;
    let amount = amount.ok_or("BALANCED ON: missing amount")?;
    let entry_type = entry_type.unwrap_or_else(|| "entry_type".to_string());

    // Validate column names are safe identifiers (alphanumeric + underscore).
    for (label, col) in [
        ("group_key", group_key.as_str()),
        ("entry_type", entry_type.as_str()),
        ("amount", amount.as_str()),
    ] {
        if col.is_empty() || !col.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "BALANCED ON: {label} must be a valid column name, got '{col}'"
            ));
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

/// Find all materialized sum bindings where
/// `source_collection == collection_name`.
///
/// Scans all collections for the tenant and extracts bindings
/// from their `materialized_sums` definitions. These are placed
/// on the SOURCE collection's `EnforcementOptions` so the Data
/// Plane fires the trigger on INSERT.
pub fn find_materialized_sum_bindings(
    catalog: &SystemCatalog,
    tenant_id: u32,
    collection_name: &str,
) -> Vec<crate::bridge::physical_plan::MaterializedSumBinding> {
    let all_collections = catalog
        .load_collections_for_tenant(tenant_id)
        .unwrap_or_default();

    let mut bindings = Vec::new();
    for target_coll in &all_collections {
        for def in &target_coll.materialized_sums {
            if def.source_collection == collection_name {
                bindings.push(crate::bridge::physical_plan::MaterializedSumBinding {
                    target_collection: def.target_collection.clone(),
                    target_column: def.target_column.clone(),
                    join_column: def.join_column.clone(),
                    value_expr: def.value_expr.clone(),
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
) -> Vec<crate::bridge::physical_plan::GeneratedColumnSpec> {
    let mut specs = Vec::new();

    let schema_json = coll.timeseries_config.as_deref().unwrap_or("");
    if let Ok(schema) = sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(schema_json) {
        for col in &schema.columns {
            if let Some(ref expr_json) = col.generated_expr
                && let Ok(expr) = sonic_rs::from_str::<crate::bridge::expr_eval::SqlExpr>(expr_json)
            {
                specs.push(crate::bridge::physical_plan::GeneratedColumnSpec {
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
            specs.push(crate::bridge::physical_plan::GeneratedColumnSpec {
                name: field_def.name.clone(),
                expr,
                depends_on: field_def.generated_deps.clone(),
            });
        }
    }

    specs
}
