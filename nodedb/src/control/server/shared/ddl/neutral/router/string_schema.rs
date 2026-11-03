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
use super::route_tokens::{RouteTokens, mentions};

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Stored procedures. None of `CREATE [OR REPLACE] PROCEDURE`, `DROP
    // PROCEDURE`, `SHOW PROCEDURES`, or `CALL <procedure>(...)` parse into any
    // typed AST variant — the router dispatches all of them by string
    // prefix from the raw SQL / token slice, before the parse gate.
    if upper.starts_with("CREATE OR REPLACE PROCEDURE ") || upper.starts_with("CREATE PROCEDURE ") {
        return Some(procedure::create_procedure(state, identity, database_id, sql).await);
    }
    if upper.starts_with("DROP PROCEDURE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(procedure::drop_procedure(state, identity, database_id, &parts).await);
    }
    if upper == "SHOW PROCEDURES" || upper.starts_with("SHOW PROCEDURES") {
        return Some(procedure::show_procedures(state, identity, database_id));
    }
    if upper.starts_with("CALL ") {
        return Some(procedure::call_procedure(state, identity, database_id, sql).await);
    }

    // User-defined functions. None of `CREATE [OR REPLACE] [AGGREGATE] FUNCTION`,
    // `DROP FUNCTION`, `ALTER FUNCTION`, or `SHOW FUNCTIONS` parse into any typed
    // AST variant — the router dispatches all of them by string prefix
    // from the raw SQL / token slice, before the parse gate (including the
    // `LANGUAGE WASM` branch).
    if upper.starts_with("CREATE OR REPLACE AGGREGATE FUNCTION ")
        || upper.starts_with("CREATE AGGREGATE FUNCTION ")
    {
        return Some(function::create_wasm_aggregate(state, identity, database_id, sql).await);
    }
    if upper.starts_with("CREATE OR REPLACE FUNCTION ") || upper.starts_with("CREATE FUNCTION ") {
        if is_wasm_function(sql) {
            return Some(function::create_wasm_function(state, identity, database_id, sql).await);
        }
        return Some(function::create_function(state, identity, database_id, sql).await);
    }
    if upper.starts_with("DROP FUNCTION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(function::drop_function(state, identity, database_id, &parts).await);
    }
    if upper.starts_with("ALTER FUNCTION ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(function::alter_function(state, identity, database_id, &parts).await);
    }
    if upper == "SHOW FUNCTIONS" || upper.starts_with("SHOW FUNCTIONS") {
        return Some(function::show_functions(state, identity, database_id));
    }

    // `ALTER COLLECTION` forms that parse into no typed AST variant (the
    // `parse_alter_operation` path returns `None` for them): constraints,
    // permission trees, and period locks. Each is recognized by the clause
    // that follows the collection name, before the parse gate.
    if let Some(arm) = alter_collection_arm(sql) {
        return Some(match arm {
            AlterCollectionArm::StateConstraint => {
                constraint::add_state_constraint(state, identity, database_id, sql).await
            }
            AlterCollectionArm::TransitionCheck => {
                constraint::add_transition_check(state, identity, database_id, sql).await
            }
            AlterCollectionArm::CheckConstraint => {
                constraint::add_check_constraint(state, identity, database_id, sql).await
            }
            AlterCollectionArm::SetPermissionTree => {
                permission_tree::set_permission_tree(state, identity, database_id, sql).await
            }
            AlterCollectionArm::DropPermissionTree => {
                permission_tree::drop_permission_tree(state, identity, database_id, sql).await
            }
            AlterCollectionArm::AddPeriodLock => {
                period_lock::add_period_lock(state, identity, database_id, sql).await
            }
            AlterCollectionArm::DropPeriodLock => {
                let parts: Vec<&str> = sql.split_whitespace().collect();
                period_lock::drop_period_lock(state, identity, database_id, &parts).await
            }
        });
    }

    // `DROP CONSTRAINT` / `SHOW CONSTRAINTS ON` are dispatched by string
    // prefix here, before the parse gate.
    if upper.starts_with("SHOW CONSTRAINTS ON ") {
        return Some(constraint::show_constraints(
            state,
            identity,
            database_id,
            sql,
        ));
    }
    if upper.starts_with("DROP CONSTRAINT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(constraint::drop_constraint(state, identity, database_id, &parts).await);
    }

    // TYPEGUARD DDL. None of these statements are dispatched from a typed AST
    // variant — the router recognizes all of them by string prefix from
    // the raw SQL (the `SHOW TYPEGUARD…` prefix does parse into a typed
    // `MiscStmt::ShowTypeGuards`, but string dispatch claims it before the
    // parse gate).
    if upper.starts_with("CREATE TYPEGUARD ") || upper.starts_with("CREATE OR REPLACE TYPEGUARD ") {
        return Some(typeguard::create_typeguard(state, identity, database_id, sql).await);
    }
    if upper.starts_with("ALTER TYPEGUARD ") {
        return Some(typeguard::alter_typeguard(state, identity, database_id, sql).await);
    }
    if upper.starts_with("DROP TYPEGUARD ") {
        return Some(typeguard::drop_typeguard(state, identity, database_id, sql).await);
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

/// An `ALTER COLLECTION` form this family handles.
#[derive(Debug, PartialEq, Eq)]
enum AlterCollectionArm {
    /// `ADD CONSTRAINT <name> ON COLUMN <col> TRANSITIONS (...)`.
    StateConstraint,
    /// `ADD TRANSITION CHECK <name> (...)`.
    TransitionCheck,
    /// `ADD CONSTRAINT <name> CHECK (...)`.
    CheckConstraint,
    /// `SET PERMISSION_TREE = '...'`.
    SetPermissionTree,
    /// `DROP PERMISSION_TREE`.
    DropPermissionTree,
    /// `ADD PERIOD LOCK ON <col> REFERENCES ...`.
    AddPeriodLock,
    /// `DROP PERIOD LOCK`.
    DropPeriodLock,
}

/// Recognize an `ALTER COLLECTION <name> <clause>` form by the clause that
/// follows the name. A keyword inside the name, a literal, or a parenthesized
/// predicate never selects an arm.
fn alter_collection_arm(sql: &str) -> Option<AlterCollectionArm> {
    if !mentions(sql, "ALTER") {
        return None;
    }
    let tokens = RouteTokens::new(sql);
    if !tokens.leads_with(&["ALTER", "COLLECTION"]) {
        return None;
    }
    let clause = tokens.skip_name(2);
    if tokens.phrase_at(clause, &["ADD", "CONSTRAINT"]) {
        let after_constraint_name = tokens.skip_name(clause + 2);
        if tokens.has_word_from(after_constraint_name, "TRANSITIONS") {
            return Some(AlterCollectionArm::StateConstraint);
        }
        if tokens.has_word_from(after_constraint_name, "CHECK") {
            return Some(AlterCollectionArm::CheckConstraint);
        }
        return None;
    }
    [
        (
            &["ADD", "TRANSITION", "CHECK"][..],
            AlterCollectionArm::TransitionCheck,
        ),
        (
            &["SET", "PERMISSION_TREE"][..],
            AlterCollectionArm::SetPermissionTree,
        ),
        (
            &["DROP", "PERMISSION_TREE"][..],
            AlterCollectionArm::DropPermissionTree,
        ),
        (
            &["ADD", "PERIOD", "LOCK"][..],
            AlterCollectionArm::AddPeriodLock,
        ),
        (
            &["DROP", "PERIOD", "LOCK"][..],
            AlterCollectionArm::DropPeriodLock,
        ),
    ]
    .into_iter()
    .find(|(clause_words, _)| tokens.phrase_at(clause, clause_words))
    .map(|(_, arm)| arm)
}

/// Whether a `CREATE FUNCTION` declares `LANGUAGE WASM` in its header, before
/// its `AS` body.
fn is_wasm_function(sql: &str) -> bool {
    mentions(sql, "WASM") && RouteTokens::new(sql).has_phrase_before(&["LANGUAGE", "WASM"], "AS")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_alter_collection_clause_selects_its_arm() {
        let cases = [
            (
                "ALTER COLLECTION orders ADD CONSTRAINT flow ON COLUMN status \
                 TRANSITIONS ('a' -> 'b')",
                AlterCollectionArm::StateConstraint,
            ),
            (
                "ALTER COLLECTION orders ADD TRANSITION CHECK guard (NEW.n > OLD.n)",
                AlterCollectionArm::TransitionCheck,
            ),
            (
                "ALTER COLLECTION orders ADD CONSTRAINT positive CHECK (n > 0)",
                AlterCollectionArm::CheckConstraint,
            ),
            (
                "ALTER COLLECTION docs SET PERMISSION_TREE = '{}'",
                AlterCollectionArm::SetPermissionTree,
            ),
            (
                "ALTER COLLECTION docs DROP PERMISSION_TREE",
                AlterCollectionArm::DropPermissionTree,
            ),
            (
                "ALTER COLLECTION journal ADD PERIOD LOCK ON period REFERENCES periods(id)",
                AlterCollectionArm::AddPeriodLock,
            ),
            (
                "alter collection journal drop period lock",
                AlterCollectionArm::DropPeriodLock,
            ),
        ];
        for (sql, arm) in cases {
            assert_eq!(alter_collection_arm(sql), Some(arm), "{sql}");
        }
    }

    /// A keyword spelled as a collection name, a constraint name, or inside a
    /// literal selects nothing.
    #[test]
    fn a_keyword_in_a_name_or_literal_selects_no_arm() {
        // The constraint is named `transitions`: it is a CHECK constraint.
        assert_eq!(
            alter_collection_arm(
                "ALTER COLLECTION orders ADD CONSTRAINT transitions CHECK (n > 0)"
            ),
            Some(AlterCollectionArm::CheckConstraint)
        );
        // The collection is named `period`, the clause is a permission tree.
        assert_eq!(
            alter_collection_arm("ALTER COLLECTION \"drop period lock\" DROP PERMISSION_TREE"),
            Some(AlterCollectionArm::DropPermissionTree)
        );
        // The JSON literal names a period lock; the clause sets a tree.
        assert_eq!(
            alter_collection_arm(
                "ALTER COLLECTION docs SET PERMISSION_TREE = '{\"ADD PERIOD LOCK\": 1}'"
            ),
            Some(AlterCollectionArm::SetPermissionTree)
        );
        // `ADD COLUMN transitions` is no clause of this family.
        assert_eq!(
            alter_collection_arm("ALTER COLLECTION orders ADD COLUMN transitions TEXT"),
            None
        );
    }

    #[test]
    fn wasm_is_recognized_in_the_header_only() {
        assert!(is_wasm_function(
            "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='"
        ));
        assert!(!is_wasm_function(
            "CREATE FUNCTION f(x INT) RETURNS TEXT AS 'LANGUAGE WASM'"
        ));
    }
}
