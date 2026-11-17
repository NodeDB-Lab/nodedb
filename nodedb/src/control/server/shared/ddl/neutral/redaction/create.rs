// SPDX-License-Identifier: BUSL-1.1

//! `CREATE REDACTION POLICY` — the DDL that installs a column-redaction rule
//! set for one `(tenant, collection, for_role)` triple.
//!
//! Shaped after the sibling `neutral::rls` create handler: authorize, validate,
//! pre-check the duplicate, and propose `CatalogEntry::PutRedactionPolicy`.

use nodedb_sql::ddl_ast::statement::RedactionRuleSpec;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredRedactionPolicy;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::redaction::{
    MAX_REDACTION_MASK_BYTES, RedactionMode, RedactionPolicy, RedactionRule,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::scope::{authorize_redaction_scope, reject_array_collection, status};

/// Parsed `CREATE REDACTION POLICY` request. Fields mirror the typed
/// `PolicyStmt::CreateRedactionPolicy` AST variant.
#[derive(Clone, Copy)]
pub struct CreateRedactionPolicyRequest<'a> {
    pub name: &'a str,
    pub collection: &'a str,
    pub for_role: &'a str,
    pub rules: &'a [RedactionRuleSpec],
    pub if_not_exists: bool,
    pub tenant_id_override: Option<u64>,
    /// Database the statement ran against. Column redaction is enforced
    /// against `db_qualified(database_id, collection)` (the same key
    /// physical plan ops carry), so the policy must be stored there too.
    pub database_id: DatabaseId,
}

/// Translate the AST rule specs into runtime [`RedactionRule`]s.
fn compile_rules(specs: &[RedactionRuleSpec]) -> Result<Vec<RedactionRule>, DdlError> {
    if specs.is_empty() {
        return Err(DdlError::new(
            "42601",
            "CREATE REDACTION POLICY requires at least one field rule",
        ));
    }
    specs
        .iter()
        .map(|spec| {
            let mode = match spec.mode_raw.as_str() {
                "HASH" => RedactionMode::Hash,
                "NULL" => RedactionMode::Null,
                "MASK" => {
                    let literal = spec.mask.clone().ok_or(DdlError::new(
                        "42601",
                        "MASK requires a replacement literal",
                    ))?;
                    if literal.len() > MAX_REDACTION_MASK_BYTES {
                        return Err(DdlError::new(
                            "54000",
                            format!(
                                "MASK literal for field '{}' is {} bytes, over the limit of \
                                 {MAX_REDACTION_MASK_BYTES} bytes; use a shorter mask",
                                spec.field,
                                literal.len()
                            ),
                        ));
                    }
                    RedactionMode::Mask(literal)
                }
                other => {
                    return Err(DdlError::new(
                        "42601",
                        format!("invalid redaction mode '{other}'. Expected MASK, HASH, or NULL"),
                    ));
                }
            };
            Ok(RedactionRule {
                field: spec.field.clone(),
                mode,
            })
        })
        .collect()
}

/// `CREATE REDACTION POLICY [IF NOT EXISTS] <name> ON <collection>
///     FOR ROLE <role> (<field> <MODE> [, ...]) [TENANT <id>]`
///
/// All fields are pre-parsed by the `nodedb-sql` AST layer; this handler only
/// validates the modes, refuses array targets, and mutates the catalog.
pub async fn create_redaction_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateRedactionPolicyRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateRedactionPolicyRequest {
        name,
        collection,
        for_role,
        rules,
        if_not_exists,
        tenant_id_override,
        database_id,
    } = *req;
    let tenant_id = authorize_redaction_scope(identity, tenant_id_override)?;
    reject_array_collection(state, tenant_id, collection)?;

    // Store and key under the same qualified name enforcement looks up
    // (`db_qualified`, applied to every op's `collection` field). Keep the
    // bare, user-typed name only for display (`SHOW REDACTION POLICIES`).
    let qualified_collection = db_qualified(database_id, collection);

    let rules = compile_rules(rules)?;
    let rule_count = rules.len();

    // Pre-check the duplicate so the proposing node fails fast with a clean
    // SQLSTATE instead of going through raft only to be a silent overwrite.
    if state
        .redaction
        .policy_exists(tenant_id, &qualified_collection, for_role)
    {
        if if_not_exists {
            return Ok(status("CREATE REDACTION POLICY"));
        }
        return Err(DdlError::new(
            "42710",
            format!("a redaction policy already exists on '{collection}' for role '{for_role}'"),
        ));
    }

    let policy = RedactionPolicy {
        name: name.to_string(),
        tenant_id,
        collection: qualified_collection,
        display_collection: collection.to_string(),
        for_role: for_role.to_string(),
        rules,
    };

    let stored = StoredRedactionPolicy::from_runtime(&policy)
        .map_err(|e| DdlError::from_error_in_context("redaction serialize", &e))?;

    let entry = CatalogEntry::PutRedactionPolicy(Box::new(stored.clone()));
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(crate::types::TenantId::new(tenant_id)),
        &identity.username,
        &format!(
            "redaction policy '{name}' created on '{collection}' for role '{for_role}' \
             ({rule_count} field rule(s))"
        ),
    );

    Ok(status("CREATE REDACTION POLICY"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(field: &str, mode: &str, mask: Option<&str>) -> RedactionRuleSpec {
        RedactionRuleSpec {
            field: field.into(),
            mode_raw: mode.into(),
            mask: mask.map(str::to_string),
        }
    }

    #[test]
    fn compile_rules_maps_every_mode() {
        let rules = compile_rules(&[
            spec("email", "MASK", Some("***")),
            spec("ssn", "HASH", None),
            spec("notes", "NULL", None),
        ])
        .expect("rules compile");
        assert!(matches!(&rules[0].mode, RedactionMode::Mask(m) if m == "***"));
        assert!(matches!(rules[1].mode, RedactionMode::Hash));
        assert!(matches!(rules[2].mode, RedactionMode::Null));
    }

    #[test]
    fn compile_rules_rejects_an_empty_list_and_an_unknown_mode() {
        assert_eq!(compile_rules(&[]).unwrap_err().sqlstate, "42601");
        assert_eq!(
            compile_rules(&[spec("email", "SCRAMBLE", None)])
                .unwrap_err()
                .sqlstate,
            "42601"
        );
        assert_eq!(
            compile_rules(&[spec("email", "MASK", None)])
                .unwrap_err()
                .sqlstate,
            "42601"
        );
    }

    #[test]
    fn a_mask_over_the_cap_is_refused() {
        let at_cap = "*".repeat(MAX_REDACTION_MASK_BYTES);
        let rules = compile_rules(&[spec("email", "MASK", Some(&at_cap))]).expect("at the cap");
        assert!(
            matches!(&rules[0].mode, RedactionMode::Mask(m) if m.len() == MAX_REDACTION_MASK_BYTES)
        );
        let over = "*".repeat(MAX_REDACTION_MASK_BYTES + 1);
        let error = compile_rules(&[spec("email", "MASK", Some(&over))]).unwrap_err();
        assert_eq!(error.sqlstate, "54000");
    }
}
