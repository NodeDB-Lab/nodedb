//! The `create_function` pgwire handler.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::StoredFunction;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::{require_admin, sqlstate_error};
use super::super::validate::validate_function_body;
use super::deps::extract_dependencies;
use super::parse::{ParsedCreateFunction, parse_create_function};

/// Handle `CREATE [OR REPLACE] FUNCTION <name>(<params>) RETURNS
/// <type> [IMMUTABLE|STABLE|VOLATILE] AS <body>`.
///
/// Requires superuser or tenant_admin — function bodies are SQL
/// expressions that can reference any collection, so creation is
/// a privileged operation.
pub fn create_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create functions")?;

    let parsed = parse_create_function(sql)?;
    let tenant_id = identity.tenant_id.as_u32();

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function(tenant_id, &parsed.name)
    {
        return Err(sqlstate_error(
            "42723",
            &format!("function '{}' already exists", parsed.name),
        ));
    }

    // Detect body kind and compile/validate accordingly.
    use crate::control::planner::procedural::ast::BodyKind;
    let compiled_body_sql = match BodyKind::detect(&parsed.body_sql) {
        BodyKind::Expression => {
            validate_function_body(&parsed)?;
            None
        }
        BodyKind::Procedural => {
            let block = crate::control::planner::procedural::parse_block(&parsed.body_sql)
                .map_err(|e| sqlstate_error("42601", &format!("procedural parse error: {e}")))?;

            crate::control::planner::procedural::validate_function_block(&block)
                .map_err(|e| sqlstate_error("42601", &format!("procedural validation: {e}")))?;

            let compiled = crate::control::planner::procedural::compile_to_sql(&block)
                .map_err(|e| sqlstate_error("42601", &format!("procedural compile: {e}")))?;

            // Validate the compiled expression via DataFusion.
            let compiled_parsed = ParsedCreateFunction {
                or_replace: parsed.or_replace,
                name: parsed.name.clone(),
                parameters: parsed.parameters.clone(),
                return_type: parsed.return_type.clone(),
                volatility: parsed.volatility,
                body_sql: compiled.clone(),
            };
            validate_function_body(&compiled_parsed)?;

            Some(compiled)
        }
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| sqlstate_error("XX000", "system clock before UNIX epoch"))?
        .as_secs();

    let stored = StoredFunction {
        tenant_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: parsed.body_sql,
        compiled_body_sql,
        volatility: parsed.volatility,
        security: crate::control::security::catalog::FunctionSecurity::default(),
        language: crate::control::security::catalog::function_types::FunctionLanguage::Sql,
        wasm_hash: None,
        wasm_fuel: 1_000_000,
        wasm_memory: 16 * 1024 * 1024,
        owner: identity.username.clone(),
        created_at: now,
    };

    // Propose through the metadata raft group. Every node's applier
    // writes the function record to local redb and clears the
    // parsed block cache so subsequent calls re-parse the new body.
    // (The WASM binary itself, if any, stays on the proposing node
    // only until a future batch adds replicated WASM distribution.)
    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        catalog
            .put_function(&stored)
            .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
    }

    // Ownership replicates through the parent `PutFunction`
    // post_apply on every node — `stored.owner` carries the
    // creator and `post_apply::function::put` installs the owner
    // record cluster-wide.

    // Extract and store dependencies (referenced functions in the body).
    let deps = extract_dependencies(&stored);
    if !deps.is_empty() {
        let _ = catalog.put_dependencies("function", tenant_id, &stored.name, &deps);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE FUNCTION {}", stored.name),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE FUNCTION"))])
}
