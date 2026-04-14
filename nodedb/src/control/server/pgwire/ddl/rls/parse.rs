//! `CREATE RLS POLICY` argument parser.
//!
//! Responsible only for turning the split `parts: &[&str]` token
//! stream into a validated `ParsedRlsCreate`. No side effects, no
//! store access. Shared by `create_rls_policy` and tests.

use pgwire::error::PgWireResult;

use crate::control::security::deny::{self, DenyMode};
use crate::control::security::predicate::{PolicyMode, RlsPredicate};
use crate::control::security::predicate_parser::{parse_predicate, validate_auth_refs};
use crate::control::security::rls::PolicyType;

use super::super::super::types::sqlstate_error;

/// Fully validated arguments to `create_rls_policy`. No defaults
/// or post-processing remaining — every field is the literal value
/// that will land in the `StoredRlsPolicy` record.
pub struct ParsedRlsCreate {
    pub name: String,
    pub collection: String,
    pub tenant_id: u32,
    pub policy_type: PolicyType,
    pub policy_type_label: String,
    pub predicate: Vec<u8>,
    pub compiled_predicate: Option<RlsPredicate>,
    pub mode: PolicyMode,
    pub is_restrictive: bool,
    pub on_deny: DenyMode,
}

pub fn parse_create_rls_policy(
    parts: &[&str],
    default_tenant_id: u32,
) -> PgWireResult<ParsedRlsCreate> {
    // CREATE RLS POLICY <name> ON <collection> FOR <type> USING (<predicate>)
    if parts.len() < 9 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE RLS POLICY <name> ON <collection> FOR <read|write|all> USING (<predicate>)",
        ));
    }

    let name = parts[3].to_string();
    let collection = parts[5].to_string();
    let policy_type_label = parts[7].to_uppercase();
    let policy_type = match policy_type_label.as_str() {
        "READ" => PolicyType::Read,
        "WRITE" => PolicyType::Write,
        "ALL" => PolicyType::All,
        other => {
            return Err(sqlstate_error(
                "42601",
                &format!("invalid policy type: {other}. Expected READ, WRITE, or ALL"),
            ));
        }
    };

    let using_idx = parts
        .iter()
        .position(|p: &&str| p.to_uppercase() == "USING")
        .ok_or_else(|| sqlstate_error("42601", "missing USING clause"))?;

    let pred_end = parts[using_idx + 1..]
        .iter()
        .position(|p: &&str| {
            let upper = p.to_uppercase();
            upper == "RESTRICTIVE" || upper == "TENANT" || upper == "ON"
        })
        .map(|i| using_idx + 1 + i)
        .unwrap_or(parts.len());

    let predicate_str = parts[using_idx + 1..pred_end]
        .join(" ")
        .trim_matches(|c: char| c == '(' || c == ')')
        .to_string();

    let is_restrictive = parts[pred_end..]
        .iter()
        .any(|p: &&str| p.to_uppercase() == "RESTRICTIVE");
    let mode = if is_restrictive {
        PolicyMode::Restrictive
    } else {
        PolicyMode::Permissive
    };

    let tenant_id = parts
        .iter()
        .position(|p: &&str| p.to_uppercase() == "TENANT")
        .and_then(|i| parts.get(i + 1))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default_tenant_id);

    let has_rich_syntax = predicate_str.contains("$auth")
        || predicate_str.to_uppercase().contains("CONTAINS")
        || predicate_str.to_uppercase().contains("INTERSECTS")
        || predicate_str.to_uppercase().contains(" AND ")
        || predicate_str.to_uppercase().contains(" OR ")
        || predicate_str.to_uppercase().contains("NOT ");

    let (predicate, compiled_predicate) = if has_rich_syntax {
        let compiled = parse_predicate(&predicate_str)
            .map_err(|e| sqlstate_error("42601", &format!("predicate parse error: {e}")))?;
        validate_auth_refs(&compiled).map_err(|e| sqlstate_error("42601", &e))?;
        (Vec::new(), Some(compiled))
    } else {
        let pred_parts: Vec<&str> = predicate_str.split_whitespace().collect();
        if pred_parts.len() < 3 {
            return Err(sqlstate_error(
                "42601",
                "USING predicate must be: (<field> <op> <value>) or a rich expression with $auth.*",
            ));
        }

        let field = pred_parts[0];
        let op = pred_parts[1];
        let value_str = pred_parts[2..].join(" ").trim_matches('\'').to_string();

        let filter = crate::bridge::scan_filter::ScanFilter {
            field: field.to_string(),
            op: op.into(),
            value: nodedb_types::Value::String(value_str),
            clauses: Vec::new(),
            expr: None,
        };
        let predicate = zerompk::to_msgpack_vec(&vec![filter])
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

        (predicate, None)
    };

    let on_deny = {
        let deny_parts: Vec<&str> = parts[pred_end..]
            .iter()
            .copied()
            .skip_while(|p: &&str| p.to_uppercase() != "ON")
            .skip(1)
            .take_while(|p: &&str| {
                let u = p.to_uppercase();
                u != "RESTRICTIVE" && u != "TENANT"
            })
            .collect();

        if deny_parts.first().map(|s: &&str| s.to_uppercase()) == Some("DENY".into()) {
            deny::parse_on_deny(&deny_parts[1..]).map_err(|e| sqlstate_error("42601", &e))?
        } else {
            DenyMode::default()
        }
    };

    Ok(ParsedRlsCreate {
        name,
        collection,
        tenant_id,
        policy_type,
        policy_type_label,
        predicate,
        compiled_predicate,
        mode,
        is_restrictive,
        on_deny,
    })
}
