use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Pub/Sub: SUBSCRIBE TO (legacy).
    if upper.starts_with("SUBSCRIBE TO ") {
        return Some(super::super::pubsub::subscribe_to(
            state, identity, sql, parts,
        ));
    }

    // State transition constraints and transition checks.
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("TRANSITIONS")
    {
        return Some(super::super::constraint::add_state_constraint(
            state, identity, sql,
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD TRANSITION CHECK") {
        return Some(super::super::constraint::add_transition_check(
            state, identity, sql,
        ));
    }
    // General CHECK constraint (not TRANSITIONS, not TRANSITION CHECK).
    if upper.starts_with("ALTER COLLECTION ")
        && upper.contains("ADD CONSTRAINT")
        && upper.contains("CHECK")
        && !upper.contains("TRANSITIONS")
        && !upper.contains("TRANSITION CHECK")
    {
        return Some(super::super::constraint::add_check_constraint(
            state, identity, sql,
        ));
    }
    if upper.starts_with("DROP CONSTRAINT ") {
        return Some(super::super::constraint::drop_constraint(
            state, identity, parts,
        ));
    }

    // Materialized sum: ALTER COLLECTION x ADD COLUMN ... AS MATERIALIZED_SUM ...
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("MATERIALIZED_SUM") {
        return Some(super::super::collection::alter::add_materialized_sum(
            state, identity, sql,
        ));
    }

    // Period lock management.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("ADD PERIOD LOCK") {
        return Some(super::super::period_lock::add_period_lock(
            state, identity, sql,
        ));
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERIOD LOCK") {
        return Some(super::super::period_lock::drop_period_lock(
            state, identity, parts,
        ));
    }

    // Permission tree management.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("SET PERMISSION_TREE") {
        return Some(
            super::super::permission_tree::set_permission_tree(state, identity, sql).await,
        );
    }
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("DROP PERMISSION_TREE") {
        return Some(
            super::super::permission_tree::drop_permission_tree(state, identity, sql).await,
        );
    }

    // ── Version History ──────────────────────────────────────────────
    if upper.starts_with("CREATE CHECKPOINT ") {
        return Some(
            super::super::version_history::checkpoint::create_checkpoint(state, identity, sql)
                .await,
        );
    }
    if upper.starts_with("DROP CHECKPOINT ") {
        return Some(
            super::super::version_history::checkpoint::drop_checkpoint(state, identity, sql).await,
        );
    }
    if upper.starts_with("SHOW VERSIONS OF ") {
        return Some(super::super::version_history::show_versions::show_versions(
            state, identity, sql,
        ));
    }
    if upper.contains("AT VERSION") && upper.starts_with("SELECT") {
        return Some(
            super::super::version_history::at_version::select_at_version(state, identity, sql)
                .await,
        );
    }
    if upper.starts_with("SELECT DIFF(") || upper.starts_with("SELECT DIFF (") {
        return Some(super::super::version_history::diff::select_diff(state, identity, sql).await);
    }
    if upper.starts_with("RESTORE ") && upper.contains("SET VERSION") {
        return Some(
            super::super::version_history::restore::restore_version(state, identity, sql).await,
        );
    }
    if upper.starts_with("COMPACT HISTORY ON ") {
        return Some(
            super::super::version_history::compact::compact_history(state, identity, sql).await,
        );
    }

    None
}
