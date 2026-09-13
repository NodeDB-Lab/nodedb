// SPDX-License-Identifier: BUSL-1.1

//! String-recognized schema DDL arms: procedures, functions, constraints,
//! permission trees, period locks, and typeguards.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::constraint;
use super::super::function;
use super::super::period_lock;
use super::super::permission_tree;
use super::super::procedure;
use super::super::typeguard;

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Stored procedures. None of `CREATE [OR REPLACE] PROCEDURE`, `DROP
    // PROCEDURE`, `SHOW PROCEDURES`, or `CALL <procedure>(...)` parse into any
    // typed AST variant — the pgwire router dispatched all of them by string
    // prefix from the raw SQL / token slice. Replicate that exactly here, before
    // the parse gate, so the prefix recognition and syntax messages stay
    // byte-identical.
    if upper.starts_with("CREATE OR REPLACE PROCEDURE ") || upper.starts_with("CREATE PROCEDURE ") {
        return Some(procedure::create_procedure(state, identity, sql));
    }
    if upper.starts_with("DROP PROCEDURE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(procedure::drop_procedure(state, identity, &parts));
    }
    if upper == "SHOW PROCEDURES" || upper.starts_with("SHOW PROCEDURES") {
        return Some(procedure::show_procedures(state, identity, database_id));
    }
    if upper.starts_with("CALL ") {
        return Some(procedure::call_procedure(state, identity, database_id, sql).await);
    }

    // User-defined functions. None of `CREATE [OR REPLACE] [AGGREGATE] FUNCTION`,
    // `DROP FUNCTION`, `ALTER FUNCTION`, or `SHOW FUNCTIONS` parse into any typed
    // AST variant — the pgwire router dispatched all of them by string prefix
    // from the raw SQL / token slice. Replicate that exactly here, before the
    // parse gate, so the prefix recognition, `LANGUAGE WASM` branch, and syntax
    // messages stay byte-identical.
    if upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ")
        || upper.starts_with("CREATE AGGREGATE FUNCTION ")
    {
        return Some(function::create_wasm_aggregate(state, identity, sql));
    }
    if upper.starts_with("CREATE OR REPLACE FUNCTION ") || upper.starts_with("CREATE FUNCTION ") {
        if upper.contains("LANGUAGE WASM") {
            return Some(function::create_wasm_function(state, identity, sql));
        }
        return Some(function::create_function(state, identity, sql));
    }
    if upper.starts_with("DROP FUNCTION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(function::drop_function(state, identity, &parts));
    }
    if upper.starts_with("ALTER FUNCTION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(function::alter_function(state, identity, &parts));
    }
    if upper == "SHOW FUNCTIONS" || upper.starts_with("SHOW FUNCTIONS") {
        return Some(function::show_functions(state, identity, database_id));
    }

    // Constraint DDL. `ALTER COLLECTION ... ADD CONSTRAINT` / `ADD TRANSITION
    // CHECK` do not parse into any typed AST variant (the `parse_alter_operation`
    // path returns `None` for them, so `ddl_ast::parse` yields `None`), and
    // `DROP CONSTRAINT` / `SHOW CONSTRAINTS ON` were dispatched by string prefix
    // from the pgwire collaborative router. Replicate that exactly here, before
    // the parse gate, so the prefix recognition and syntax messages stay
    // byte-identical. Guard ordering (TRANSITIONS before the general CHECK arm,
    // which excludes both TRANSITIONS and TRANSITION CHECK) is preserved verbatim.
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("TRANSITIONS")
    {
        return Some(constraint::add_state_constraint(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD TRANSITION CHECK") {
        return Some(constraint::add_transition_check(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("CHECK")
        && !upper.contains("TRANSITIONS")
        && !upper.contains("TRANSITION CHECK")
    {
        return Some(constraint::add_check_constraint(state, identity, sql));
    }
    if upper.starts_with("SHOW CONSTRAINTS ON ") {
        return Some(constraint::show_constraints(state, identity, sql));
    }
    if upper.starts_with("DROP CONSTRAINT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(constraint::drop_constraint(state, identity, &parts));
    }

    // Permission tree management. `ALTER COLLECTION … SET PERMISSION_TREE` and
    // `… DROP PERMISSION_TREE` do not parse into any typed AST variant (the
    // `parse_alter_operation` path returns `None` for both, so `ddl_ast::parse`
    // yields `None`) — the pgwire collaborative router dispatched both from the
    // raw SQL by string prefix + `contains`. Replicate that exactly here, before
    // the parse gate, so the recognition and syntax messages stay byte-identical.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET PERMISSION_TREE") {
        return Some(permission_tree::set_permission_tree(state, identity, sql).await);
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERMISSION_TREE") {
        return Some(permission_tree::drop_permission_tree(state, identity, sql).await);
    }

    // Period lock management. `ALTER COLLECTION … ADD PERIOD LOCK` and `… DROP
    // PERIOD LOCK` do not parse into any typed AST variant (the
    // `parse_alter_operation` path returns `None` for both, so `ddl_ast::parse`
    // yields `None`) — the pgwire collaborative router dispatched both from the
    // raw SQL by string prefix + `contains`. Replicate that exactly here, before
    // the parse gate, so the recognition and syntax messages stay byte-identical.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD PERIOD LOCK") {
        return Some(period_lock::add_period_lock(state, identity, sql));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERIOD LOCK") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(period_lock::drop_period_lock(state, identity, &parts));
    }

    // TYPEGUARD DDL. None of these statements are dispatched from a typed AST
    // variant — the pgwire router recognized all of them by string prefix from
    // the raw SQL (the `SHOW TYPEGUARD…` prefix does parse into a typed
    // `MiscStmt::ShowTypeGuards`, but the pgwire string dispatch claimed it
    // before the parse gate). Replicate that exactly here, before the parse
    // gate, so the prefix recognition and syntax messages stay byte-identical.
    if upper.starts_with("CREATE TYPEGUARD ") || upper.starts_with("CREATE OR REPLACE TYPEGUARD ") {
        return Some(typeguard::create_typeguard(
            state,
            identity,
            database_id,
            sql,
        ));
    }
    if upper.starts_with("ALTER TYPEGUARD ") {
        return Some(typeguard::alter_typeguard(
            state,
            identity,
            database_id,
            sql,
        ));
    }
    if upper.starts_with("DROP TYPEGUARD ") {
        return Some(typeguard::drop_typeguard(state, identity, database_id, sql));
    }
    if upper.starts_with("VALIDATE TYPEGUARD ON ") {
        return Some(typeguard::validate_typeguard(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SHOW TYPEGUARD ON ") {
        return Some(typeguard::show_typeguard(state, identity, database_id, sql));
    }
    if upper == "SHOW TYPEGUARDS" || upper.starts_with("SHOW TYPEGUARDS") {
        return Some(typeguard::show_typeguards(
            state,
            identity,
            database_id,
            sql,
        ));
    }

    None
}
