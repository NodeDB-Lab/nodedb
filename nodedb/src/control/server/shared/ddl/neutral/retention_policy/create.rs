// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE RETENTION POLICY` DDL handler.
//!
//! The collection, duplicate-name, and per-collection checks run on the
//! leader, before the row is proposed as a replicated catalog entry. A failed
//! continuous-aggregate auto-wire rolls the policy back through the same
//! replicated path, so no node keeps a half-wired policy.
//!
//! Syntax:
//! ```sql
//! CREATE RETENTION POLICY <name> ON <collection> (
//!     RAW RETAIN '<duration>',
//!     DOWNSAMPLE TO '<interval>'
//!         AGGREGATE (func(col) [AS alias], ...)
//!         RETAIN '<duration>',
//!     ...
//!     [ARCHIVE TO '<s3_url>']
//! ) [WITH (EVAL_INTERVAL = '<duration>')]
//! ```

use nodedb_types::{DatabaseId, quote_ident, quote_literal};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::require_tenant_admin;
use super::RETENTION_POLICIES_CRDT_COLLECTION;
use super::parse::parse_create_retention_policy;
use super::replicate::{propose_delete, propose_put};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// Refuse `def` when the continuous aggregate name of a downsample tier is
/// over the collection name limit.
fn check_tier_names(def: &RetentionPolicyDef) -> crate::Result<()> {
    def.downsample_tiers().iter().try_for_each(|tier| {
        crate::control::catalog_entry::collection_limits::check_collection_name(
            &def.aggregate_name(tier.tier_index),
        )
    })
}

fn assemble_retention_parser_input(
    name: &str,
    collection: &str,
    body_raw: &str,
    eval_interval_raw: Option<&str>,
) -> String {
    let prefix = format!(
        "CREATE RETENTION POLICY {} ON {} ({body_raw})",
        quote_ident(name),
        quote_ident(collection)
    );
    match eval_interval_raw {
        Some(eval) => format!("{prefix} WITH (EVAL_INTERVAL = {})", quote_literal(eval)),
        None => prefix,
    }
}

/// Handle `CREATE RETENTION POLICY` from typed AST fields extracted by nodedb-sql parser.
///
/// `body_raw` is the raw text between the outer parentheses.
/// `eval_interval_raw` is the optional EVAL_INTERVAL string from the WITH clause.
pub async fn create_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    collection: &str,
    body_raw: &str,
    eval_interval_raw: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create retention policies")?;

    // Reconstruct minimal SQL for the existing complex parser. The body is
    // already structured retention-policy grammar; only externally named SQL
    // tokens are re-emitted here.
    let reconstructed =
        assemble_retention_parser_input(name, collection, body_raw, eval_interval_raw);
    // reconstructed-sql: parser-only reparses strict retention grammar into typed policy fields
    let parsed = parse_create_retention_policy(&reconstructed)?;
    let tenant_id = identity.tenant_id.as_u64();

    // Validate collection exists and is timeseries.
    {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(database_id, tenant_id, &parsed.collection) {
            Ok(Some(coll)) if coll.collection_type.is_timeseries() => {}
            Ok(Some(_)) => {
                return Err(err(
                    "42809",
                    format!("'{}' is not a timeseries collection", parsed.collection),
                ));
            }
            _ => {
                return Err(err(
                    "42P01",
                    format!("collection '{}' does not exist", parsed.collection),
                ));
            }
        }
    }

    // Check for duplicate policy name.
    if state
        .retention_policy_registry
        .get(database_id.as_u64(), tenant_id, &parsed.name)
        .is_some()
    {
        return Err(err(
            "42710",
            format!("retention policy '{}' already exists", parsed.name),
        ));
    }

    // Check no other policy already targets this collection.
    if state
        .retention_policy_registry
        .get_for_collection(database_id.as_u64(), tenant_id, &parsed.collection)
        .is_some()
    {
        return Err(err(
            "42710",
            format!(
                "collection '{}' already has a retention policy",
                parsed.collection
            ),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError::internal("system clock error"))?
        .as_secs();

    let def = RetentionPolicyDef {
        database_id: database_id.as_u64(),
        tenant_id,
        name: parsed.name.clone(),
        collection: parsed.collection.clone(),
        tiers: parsed.tiers,
        auto_tier: false,
        enabled: true,
        eval_interval_ms: parsed.eval_interval_ms,
        owner: identity.username.clone(),
        created_at: now,
    };

    // Every tier's continuous aggregate name fits the collection name limit
    // before anything is proposed.
    check_tier_names(&def)?;

    // Replicated: every node writes the row and installs the definition in
    // its own `RetentionPolicyRegistry` via post-apply.
    propose_put(state, &def).await?;

    // Definition notice for the tenant admins' Lite sessions.
    super::super::system_notice::SystemNotice {
        state,
        database_id,
        tenant_id,
        collection: RETENTION_POLICIES_CRDT_COLLECTION,
        document_id: def.name.clone(),
    }
    .upsert(&def);

    // Auto-wire continuous aggregates for each downsample tier.
    if !def.downsample_tiers().is_empty()
        && let Err(e) =
            crate::engine::timeseries::retention_policy::autowire::register_tiers(state, &def).await
    {
        // Roll back through the same replicated path that created it, so the
        // policy disappears on every node, not only on this one.
        if let Err(rollback) = propose_delete(state, &def).await {
            return Err(DdlError::from_error_in_context(
                &format!(
                    "rollback left the policy in place: {}; failed to auto-wire aggregates",
                    rollback.message
                ),
                &e,
            ));
        }
        return Err(DdlError::from_error_in_context(
            "failed to auto-wire aggregates",
            &e,
        ));
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "CREATE RETENTION POLICY {} ON {}",
            parsed.name, parsed.collection
        ),
    );

    tracing::info!(
        name = parsed.name,
        collection = parsed.collection,
        tiers = parsed.tier_count,
        "retention policy created"
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE RETENTION POLICY".to_string(),
        rows_affected: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::TierDef;
    use nodedb_types::MAX_COLLECTION_NAME_BYTES;

    fn tier(tier_index: u32, resolution_ms: u64) -> TierDef {
        TierDef {
            tier_index,
            resolution_ms,
            aggregates: Vec::new(),
            retain_ms: 0,
            archive: None,
        }
    }

    fn policy(name: String) -> RetentionPolicyDef {
        RetentionPolicyDef {
            database_id: 0,
            tenant_id: 1,
            name,
            collection: "metrics".into(),
            tiers: vec![tier(0, 0), tier(1, 60_000)],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: 0,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    /// A policy whose tier aggregate name passes the collection name limit
    /// refuses with SQLSTATE 54000. One whose name fits passes.
    #[test]
    fn a_tier_name_over_the_collection_name_limit_refuses() {
        let fits = policy("p".repeat(16));
        check_tier_names(&fits).expect("a short policy name");
        let derived = fits.aggregate_name(1).len() - 16;
        let over = policy("p".repeat(MAX_COLLECTION_NAME_BYTES - derived + 1));
        let refused = check_tier_names(&over).expect_err("refused");
        assert!(
            matches!(
                refused,
                crate::Error::DataPlane(
                    crate::bridge::envelope::ErrorCode::ProgramLimitExceeded { .. }
                )
            ),
            "{refused:?}"
        );
        let at = policy("p".repeat(MAX_COLLECTION_NAME_BYTES - derived));
        check_tier_names(&at).expect("a tier name at the limit");
    }

    #[test]
    fn reconstructed_sql_quotes_identifier_and_interval_inputs() {
        let sql = assemble_retention_parser_input(
            "policy\"; DROP TABLE audit_log; --",
            "metrics\"; DELETE FROM audit_log; --",
            "RAW RETAIN '7d'",
            Some("1h'; DELETE FROM audit_log; --"),
        );

        assert_eq!(
            sql,
            "CREATE RETENTION POLICY \"policy\"\"; DROP TABLE audit_log; --\" ON \"metrics\"\"; DELETE FROM audit_log; --\" (RAW RETAIN '7d') WITH (EVAL_INTERVAL = '1h''; DELETE FROM audit_log; --')"
        );
        assert!(super::parse_create_retention_policy(&sql).is_err());
    }

    #[test]
    fn reconstructed_sql_preserves_structured_body_without_eval_interval() {
        let sql = assemble_retention_parser_input(
            "Policy Name",
            "Metrics \"Primary\"",
            "RAW RETAIN '7d'",
            None,
        );
        assert_eq!(
            sql,
            "CREATE RETENTION POLICY \"Policy Name\" ON \"Metrics \"\"Primary\"\"\" (RAW RETAIN '7d')"
        );
        let parsed = super::parse_create_retention_policy(&sql).expect("reconstructed SQL parses");
        assert_eq!(parsed.name, "Policy Name");
        assert_eq!(parsed.collection, "Metrics \"Primary\"");
    }
}
