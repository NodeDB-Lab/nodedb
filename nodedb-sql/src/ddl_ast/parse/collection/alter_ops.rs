// SPDX-License-Identifier: Apache-2.0

//! Parse ALTER COLLECTION sub-operations.

use crate::ddl_ast::statement::AlterCollectionOp;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

pub(super) fn parse_alter_operation(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
    collection_name: &str,
) -> Option<AlterCollectionOp> {
    // Operations handled exclusively by the collaborative dispatcher (raw-SQL path).
    // Return None so try_parse returns Ok(None), letting the router fall through.
    if upper.contains("ADD CONSTRAINT")
        || upper.contains("ADD PERIOD LOCK")
        || upper.contains("DROP PERIOD LOCK")
        || upper.contains("SET PERMISSION_TREE")
        || upper.contains("ADD TRANSITION CHECK")
    {
        return None;
    }

    // MATERIALIZED_SUM takes priority over ADD COLUMN.
    if upper.contains("MATERIALIZED_SUM") {
        return parse_materialized_sum(parts, trimmed, collection_name);
    }

    if upper.contains("ADD COLUMN") || (upper.contains(" ADD ") && !upper.contains("MATERIALIZED"))
    {
        return parse_add_column(parts);
    }
    if upper.contains("DROP COLUMN") {
        return parse_drop_column(parts);
    }
    if upper.contains("RENAME COLUMN") {
        return parse_rename_column(parts);
    }
    if upper.contains("ALTER COLUMN") && upper.contains(" TYPE ") {
        return parse_alter_column_type(parts);
    }
    if upper.contains("OWNER TO") {
        let new_owner = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("TO"))
            .and_then(|i| parts.get(i + 1))
            .map(|s| s.to_string())?;
        return Some(AlterCollectionOp::OwnerTo { new_owner });
    }
    if upper.contains("SET RETENTION") {
        let value = extract_set_value(upper, "RETENTION")?;
        return Some(AlterCollectionOp::SetRetention { value });
    }
    if upper.contains("SET APPEND_ONLY") {
        return Some(AlterCollectionOp::SetAppendOnly);
    }
    if upper.contains("LAST_VALUE_CACHE") {
        let enabled =
            upper.contains("LAST_VALUE_CACHE = TRUE") || upper.contains("LAST_VALUE_CACHE=TRUE");
        return Some(AlterCollectionOp::SetLastValueCache { enabled });
    }
    if upper.contains("LEGAL_HOLD") {
        let enabled = upper.contains("LEGAL_HOLD = TRUE") || upper.contains("LEGAL_HOLD=TRUE");
        let tag = extract_tag_value(upper)?;
        return Some(AlterCollectionOp::SetLegalHold { enabled, tag });
    }
    None
}

/// Parse `ALTER COLLECTION <name> ADD [COLUMN] <col> ... MATERIALIZED_SUM SOURCE <src>
/// ON <join> VALUE <expr>` into a typed [`AlterCollectionOp::AddMaterializedSum`].
///
/// Returns `None` if any required keyword is absent — the router will surface a
/// parse error to the client.
fn parse_materialized_sum(
    parts: &[&str],
    trimmed: &str,
    collection_name: &str,
) -> Option<AlterCollectionOp> {
    // Target column: token after ADD [COLUMN].
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .or_else(|| parts.iter().position(|p| p.eq_ignore_ascii_case("ADD")))?;
    let target_column = parts.get(col_idx + 1)?.to_lowercase();

    // Source collection: token after SOURCE.
    let source_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("SOURCE"))?;
    let source_collection = parts.get(source_idx + 1)?.to_lowercase();

    // Join column: extract from ON clause `source.col = target.id`.
    let on_pos = find_ascii_case_insensitive(trimmed, " ON ")?;
    let after_on = &trimmed[on_pos + 4..];
    let join_column = extract_join_column(after_on, &source_collection)?;

    // Value expression: token(s) after VALUE keyword.
    let value_pos = find_ascii_case_insensitive(trimmed, " VALUE ")?;
    let value_expr_raw = trimmed[value_pos + 7..].trim().trim_end_matches(';');
    let value_expr = extract_value_expr(value_expr_raw, &source_collection)?;

    Some(AlterCollectionOp::AddMaterializedSum {
        target_collection: collection_name.to_lowercase(),
        target_column,
        source_collection,
        join_column,
        value_expr,
    })
}

/// Extract the join column from `source.col = target.id` — returns the source side column.
fn extract_join_column(join_clause: &str, source_coll: &str) -> Option<String> {
    let eq_parts: Vec<&str> = join_clause.splitn(2, '=').collect();
    if eq_parts.len() != 2 {
        return None;
    }
    let left = eq_parts[0].trim().to_lowercase();
    let right = eq_parts[1].trim().to_lowercase();

    let prefix = format!("{source_coll}.");
    let col = if left.starts_with(&prefix) {
        left.strip_prefix(&prefix).unwrap_or(&left).to_string()
    } else if right.starts_with(&prefix) {
        right.strip_prefix(&prefix).unwrap_or(&right).to_string()
    } else {
        left.split('.').next_back().unwrap_or(&left).to_string()
    };

    Some(col.split_whitespace().next().unwrap_or(&col).to_string())
}

/// Extract value expression — simple column reference or qualified `source.column`.
/// Returns `None` for complex expressions that require a pre-computed column.
fn extract_value_expr(expr_str: &str, source_coll: &str) -> Option<String> {
    let lower = expr_str.trim().to_lowercase();
    let prefix = format!("{source_coll}.");
    let col_name = if lower.starts_with(&prefix) {
        lower.strip_prefix(&prefix).unwrap_or(&lower).to_string()
    } else {
        lower.to_string()
    };

    if col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Some(col_name)
    } else {
        // Complex expression — caller must use a pre-computed column.
        None
    }
}

fn parse_add_column(parts: &[&str]) -> Option<AlterCollectionOp> {
    // ALTER {COLLECTION|TABLE} <name> ADD [COLUMN] <col_name> <col_type> [NOT NULL] [DEFAULT expr]
    let add_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("ADD"))?;
    let col_start = if parts
        .get(add_idx + 1)
        .map(|p| p.eq_ignore_ascii_case("COLUMN"))
        .unwrap_or(false)
    {
        add_idx + 2
    } else {
        add_idx + 1
    };
    let column_name = parts.get(col_start)?.to_lowercase();
    let column_type = parts.get(col_start + 1)?.to_string();
    let not_null = parts[col_start..]
        .windows(2)
        .any(|w| w[0].eq_ignore_ascii_case("NOT") && w[1].eq_ignore_ascii_case("NULL"));
    let default_expr = parts[col_start..]
        .iter()
        .position(|p| p.eq_ignore_ascii_case("DEFAULT"))
        .and_then(|i| parts.get(col_start + i + 1))
        .map(|s| s.trim_end_matches(';').to_string());
    Some(AlterCollectionOp::AddColumn {
        column_name,
        column_type,
        not_null,
        default_expr,
    })
}

fn parse_drop_column(parts: &[&str]) -> Option<AlterCollectionOp> {
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))?;
    let column_name = parts.get(col_idx + 1)?.trim_end_matches(';').to_lowercase();
    Some(AlterCollectionOp::DropColumn { column_name })
}

fn parse_rename_column(parts: &[&str]) -> Option<AlterCollectionOp> {
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))?;
    let old_name = parts.get(col_idx + 1)?.to_lowercase();
    // Expect TO keyword.
    let to_tok = parts.get(col_idx + 2)?;
    if !to_tok.eq_ignore_ascii_case("TO") {
        return None;
    }
    let new_name = parts.get(col_idx + 3)?.trim_end_matches(';').to_lowercase();
    Some(AlterCollectionOp::RenameColumn { old_name, new_name })
}

fn parse_alter_column_type(parts: &[&str]) -> Option<AlterCollectionOp> {
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))?;
    let column_name = parts.get(col_idx + 1)?.to_lowercase();
    let type_idx = parts[col_idx..]
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TYPE"))
        .map(|i| col_idx + i)?;
    let new_type = parts.get(type_idx + 1)?.trim_end_matches(';').to_string();
    Some(AlterCollectionOp::AlterColumnType {
        column_name,
        new_type,
    })
}

fn extract_set_value(upper: &str, key: &str) -> Option<String> {
    let pattern = format!("{key} =");
    let pos = upper
        .find(&pattern)
        .or_else(|| upper.find(&format!("{key}=")))?;
    let after = upper[pos..].split('=').nth(1)?.trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .unwrap_or(value.len());
    let result = value[..end].to_string();
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

fn extract_tag_value(upper: &str) -> Option<String> {
    let pos = upper.find("TAG ")?;
    let after = upper[pos + 4..].trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .or_else(|| value.find(' '))
        .unwrap_or(value.len());
    if end == 0 {
        return None;
    }
    Some(value[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_sum_keywords_after_unicode_name_preserve_original_offsets() {
        let sql = "ALTER COLLECTION totalsﬀﬀ ADD COLUMN total INT MATERIALIZED_SUM SOURCE orders ON orders.customer_id = totalsﬀﬀ.id VALUE orders.amount";
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let op =
            parse_materialized_sum(&parts, sql, "totalsﬀﬀ").expect("materialized sum should parse");
        assert!(matches!(
            op,
            AlterCollectionOp::AddMaterializedSum {
                target_collection,
                source_collection,
                join_column,
                value_expr,
                ..
            } if target_collection == "totalsﬀﬀ"
                && source_collection == "orders"
                && join_column == "customer_id"
                && value_expr == "amount"
        ));
    }
}
