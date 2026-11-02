// SPDX-License-Identifier: BUSL-1.1

//! String-recognized versioning DDL arms: version history, maintenance,
//! cluster management, vector-index lifecycle, vector-model metadata, and
//! graph index / tree operations.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::cluster;
use super::super::collection;
use super::super::maintenance;
use super::super::tree_ops;
use super::super::version_history;
use super::route_tokens::{RouteTokens, mentions};

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Version history. None of `CREATE CHECKPOINT`, `DROP CHECKPOINT`, `SHOW
    // VERSIONS OF`, `SELECT … AT VERSION`, `SELECT DIFF(…)`, `RESTORE … SET
    // VERSION`, or `COMPACT HISTORY ON` parse into any typed AST variant — the
    // router dispatches all of them by string prefix from the raw SQL,
    // before the parse gate. The `RESTORE … SET VERSION` guard keeps
    // `RESTORE TENANT` / `RESTORE DATABASE` on the typed path.
    if upper.starts_with("CREATE CHECKPOINT ") {
        return Some(
            version_history::checkpoint::create_checkpoint(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("DROP CHECKPOINT ") {
        return Some(
            version_history::checkpoint::drop_checkpoint(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("SHOW VERSIONS OF ") {
        return Some(version_history::show_versions::show_versions(
            state,
            identity,
            database_id,
            sql,
        ));
    }
    if is_select_at_version(sql) {
        return Some(
            version_history::at_version::select_at_version(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("SELECT DIFF(") || upper.starts_with("SELECT DIFF (") {
        return Some(version_history::diff::select_diff(state, identity, database_id, sql).await);
    }
    if is_restore_set_version(sql) {
        if restore_forbidden_in_transaction(txn_ctx) {
            return Some(Err(DdlError::from_error(
                &crate::Error::CrdtApplyForbiddenInTransaction,
            )));
        }
        return Some(
            version_history::restore::restore_version(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("COMPACT HISTORY ON ") {
        return Some(
            version_history::compact::compact_history(state, identity, database_id, sql).await,
        );
    }

    // Maintenance: ANALYZE / COMPACT / SHOW STORAGE / SHOW COMPACTION STATUS.
    // These parse into typed `ClusterStmt` variants, but the router
    // dispatches all four by string prefix from the raw SQL / token slice,
    // before the parse gate (trailing space on `ANALYZE ` / `COMPACT `, and the
    // `SHOW COMPACTION STATUS` exact / prefix forms; names come from `parts`).
    // The `COMPACT ` prefix is placed after the version-history
    // `COMPACT HISTORY ON` guard above, so `COMPACT HISTORY ON …` routes to
    // version_history.
    if upper.starts_with("ANALYZE ") {
        return Some(maintenance::handle_analyze(state, identity, sql, database_id).await);
    }
    if upper.starts_with("COMPACT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(maintenance::handle_compact(
            state,
            identity,
            &parts,
            database_id,
        ));
    }
    if upper.starts_with("SHOW STORAGE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(maintenance::handle_show_storage(
            state,
            identity,
            &parts,
            database_id,
        ));
    }
    if upper == "SHOW COMPACTION STATUS" || upper.starts_with("SHOW COMPACTION STATUS ") {
        return Some(maintenance::handle_show_compaction_status(state, identity));
    }

    // Cluster management & observability: SHOW CLUSTER, SHOW RAFT GROUPS,
    // SHOW RAFT GROUP <id>, SHOW MIGRATIONS, REBALANCE, SHOW PEER HEALTH,
    // SHOW NODES, SHOW NODE <id>, REMOVE NODE <id>, SHOW RANGES, SHOW
    // ROUTING, SHOW SCHEMA VERSION. All of these parse into typed
    // `ClusterStmt` variants, but the router dispatches them by
    // string prefix from the raw SQL / token slice, before the parse gate.
    // Order matters: `SHOW RAFT GROUPS` comes before `SHOW RAFT GROUP `.
    // `ALTER RAFT GROUP` is dispatched via the typed match below.
    if upper.starts_with("SHOW CLUSTER") {
        return Some(cluster::show_cluster(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUPS") {
        return Some(cluster::show_raft_groups(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUP ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(cluster::show_raft_group(state, identity, &parts));
    }
    if upper.starts_with("SHOW MIGRATIONS") {
        return Some(cluster::show_migrations(state, identity));
    }
    if upper.starts_with("REBALANCE") {
        return Some(cluster::rebalance(state, identity));
    }
    if upper.starts_with("SHOW PEER HEALTH") {
        return Some(cluster::show_peer_health(state, identity));
    }
    if upper.starts_with("SHOW NODES") {
        return Some(cluster::show_nodes(state, identity));
    }
    if upper.starts_with("SHOW NODE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(cluster::show_node(state, identity, &parts));
    }
    if upper.starts_with("REMOVE NODE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(cluster::remove_node(state, identity, &parts));
    }
    if upper.starts_with("SHOW RANGES") {
        return Some(cluster::show_ranges(state, identity));
    }
    if upper.starts_with("SHOW ROUTING") {
        return Some(cluster::show_routing(state, identity));
    }
    if upper.starts_with("SHOW SCHEMA VERSION") {
        return Some(cluster::show_schema_version(state, identity));
    }
    if upper == "CREATE RESTORE POINT" || upper.starts_with("CREATE RESTORE POINT ") {
        return Some(cluster::create_restore_point(state, identity).await);
    }
    if upper.starts_with("SHOW RESTORE POINTS") {
        return Some(cluster::show_restore_points(state, identity));
    }

    // Vector index lifecycle: SHOW VECTOR INDEX / ALTER VECTOR INDEX. None of
    // these are dispatched from a typed AST arm — the router recognizes all
    // four from the raw SQL, before the parse gate. An `ALTER VECTOR INDEX
    // ON <collection>.<column>` form is chosen by the clause after the target.
    if upper.starts_with("SHOW VECTOR INDEX ") {
        return Some(
            maintenance::handle_show_vector_index(state, identity, database_id, sql).await,
        );
    }
    if let Some(clause) = alter_vector_index_clause(sql) {
        return Some(match clause {
            VectorIndexClause::Seal => {
                maintenance::handle_alter_vector_index_seal(state, identity, database_id, sql).await
            }
            VectorIndexClause::Compact => {
                maintenance::handle_alter_vector_index_compact(state, identity, database_id, sql)
                    .await
            }
            VectorIndexClause::Set => {
                maintenance::handle_alter_vector_index_set(state, identity, database_id, sql).await
            }
        });
    }

    // Vector model metadata. None of these are dispatched from a typed AST arm —
    // `ALTER COLLECTION ... SET VECTOR METADATA ON` parses into no
    // `AlterCollectionOp` variant, and `SHOW VECTOR MODELS` / `SELECT
    // VECTOR_METADATA(...)` parse into no typed DDL AST at all. The router
    // recognizes all three by string prefix from the raw SQL, before the parse
    // gate. The `SET VECTOR METADATA ON` guard precedes the typed parse gate
    // below, so the typed `AlterCollection` dispatch never shadows it.
    if is_set_vector_metadata(sql) {
        return Some(
            collection::handle_set_vector_metadata(state, identity, sql, database_id).await,
        );
    }
    if upper.starts_with("SHOW VECTOR MODELS") {
        return Some(collection::handle_show_vector_models(
            state,
            identity,
            database_id,
        ));
    }
    if upper.starts_with("SELECT VECTOR_METADATA(") || upper.starts_with("SELECT VECTOR_METADATA (")
    {
        let inner = sql
            .find('(')
            .and_then(|start| sql.rfind(')').map(|end| &sql[start + 1..end]));
        if let Some(args_str) = inner {
            let args: Vec<&str> = args_str
                .split(',')
                .map(|s| s.trim().trim_matches('\'').trim_matches('"'))
                .collect();
            if args.len() >= 2 && !args[0].is_empty() && !args[1].is_empty() {
                return Some(collection::handle_vector_metadata_query(
                    state,
                    identity,
                    database_id,
                    &args[0].to_lowercase(),
                    &args[1].to_lowercase(),
                ));
            }
        }
        return Some(Err(DdlError::new(
            "42601",
            "usage: SELECT VECTOR_METADATA('collection', 'column')",
        )));
    }

    // Graph index and tree operations: CREATE GRAPH INDEX / TREE_SUM /
    // TREE_CHILDREN. None of these are dispatched from a typed AST arm — the
    // router recognizes all three by string prefix from the raw SQL, before the
    // parse gate (the `SELECT TREE_SUM` / bare `TREE_SUM` and `SELECT
    // TREE_CHILDREN` / bare `TREE_CHILDREN` forms never parse into a typed DDL
    // AST).
    if upper.starts_with("CREATE GRAPH INDEX ") {
        return Some(tree_ops::create_graph_index(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SELECT TREE_SUM") || upper.starts_with("TREE_SUM") {
        return Some(
            tree_ops::tree_sum(
                state,
                identity,
                database_id,
                sql,
                txn_ctx.linearizable_reads(),
            )
            .await,
        );
    }
    if upper.starts_with("SELECT TREE_CHILDREN") || upper.starts_with("TREE_CHILDREN") {
        return Some(
            tree_ops::tree_children(
                state,
                identity,
                database_id,
                sql,
                txn_ctx.linearizable_reads(),
            )
            .await,
        );
    }

    None
}

fn restore_forbidden_in_transaction(txn_ctx: &DmlTxnCtx<'_>) -> bool {
    txn_ctx.sessions.transaction_state(txn_ctx.session_id) != TransactionState::Idle
}

/// `SELECT ... FROM <collection> AT VERSION '<checkpoint>' ...`: the clause
/// follows the collection name.
fn is_select_at_version(sql: &str) -> bool {
    if !mentions(sql, "VERSION") {
        return false;
    }
    let tokens = RouteTokens::new(sql);
    tokens.leads_with(&["SELECT"])
        && tokens
            .top_level_word_from(1, "FROM")
            .is_some_and(|from| tokens.phrase_at(tokens.skip_name(from + 1), &["AT", "VERSION"]))
}

/// `RESTORE <collection> SET VERSION = '<checkpoint>' ...`.
fn is_restore_set_version(sql: &str) -> bool {
    if !mentions(sql, "RESTORE") {
        return false;
    }
    let tokens = RouteTokens::new(sql);
    tokens.leads_with(&["RESTORE"]) && tokens.phrase_at(tokens.skip_name(1), &["SET", "VERSION"])
}

/// `ALTER COLLECTION <collection> SET VECTOR METADATA ON <column> ...`.
fn is_set_vector_metadata(sql: &str) -> bool {
    if !mentions(sql, "METADATA") {
        return false;
    }
    let tokens = RouteTokens::new(sql);
    tokens.leads_with(&["ALTER", "COLLECTION"])
        && tokens.phrase_at(tokens.skip_name(2), &["SET", "VECTOR", "METADATA", "ON"])
}

/// The clause of an `ALTER VECTOR INDEX ON <collection>.<column>` form.
#[derive(Debug, PartialEq, Eq)]
enum VectorIndexClause {
    Seal,
    Compact,
    Set,
}

/// The index name between `INDEX` and `ON` is optional: `ALTER VECTOR INDEX
/// [<name>] ON <target> <clause>`. The handler resolves the index from the
/// target either way.
fn alter_vector_index_clause(sql: &str) -> Option<VectorIndexClause> {
    if !mentions(sql, "VECTOR") {
        return None;
    }
    let tokens = RouteTokens::new(sql);
    if !tokens.leads_with(&["ALTER", "VECTOR", "INDEX"]) {
        return None;
    }
    let on = if tokens.phrase_at(3, &["ON"]) {
        3
    } else {
        tokens.skip_name(3)
    };
    if !tokens.phrase_at(on, &["ON"]) {
        return None;
    }
    let clause = tokens.skip_name(on + 1);
    [
        ("SEAL", VectorIndexClause::Seal),
        ("COMPACT", VectorIndexClause::Compact),
        ("SET", VectorIndexClause::Set),
    ]
    .into_iter()
    .find(|(word, _)| tokens.phrase_at(clause, &[word]))
    .map(|(_, clause)| clause)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{SessionId, SessionStore};

    #[test]
    fn at_version_is_routed_on_the_clause_after_the_collection() {
        assert!(is_select_at_version(
            "SELECT * FROM docs AT VERSION 'ckpt' WHERE id = 'd1'"
        ));
        // The phrase inside a literal is data.
        assert!(!is_select_at_version(
            "SELECT * FROM docs WHERE note = 'AT VERSION 3'"
        ));
        // A column named `at` aliased `version` is no clause.
        assert!(!is_select_at_version("SELECT at version FROM docs"));
    }

    #[test]
    fn restore_is_routed_on_the_clause_after_the_collection() {
        assert!(is_restore_set_version(
            "RESTORE docs SET VERSION = 'ckpt' WHERE id = 'd1'"
        ));
        assert!(!is_restore_set_version("RESTORE DATABASE 'set version'"));
    }

    #[test]
    fn set_vector_metadata_is_routed_on_the_clause_after_the_collection() {
        assert!(is_set_vector_metadata(
            "ALTER COLLECTION docs SET VECTOR METADATA ON emb (model = 'm')"
        ));
        assert!(!is_set_vector_metadata(
            "ALTER COLLECTION docs SET (note = 'SET VECTOR METADATA ON x')"
        ));
    }

    #[test]
    fn vector_index_clauses_are_routed_after_the_target() {
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX ON docs.emb SEAL"),
            Some(VectorIndexClause::Seal)
        );
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX ON docs.emb COMPACT"),
            Some(VectorIndexClause::Compact)
        );
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX ON docs.emb SET (ef_search = 64)"),
            Some(VectorIndexClause::Set)
        );
        // A collection named `seal` sets parameters; it does not seal.
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX ON seal.emb SET (ef_search = 64)"),
            Some(VectorIndexClause::Set)
        );
        // A named index routes on the clause after its target too.
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX idx ON docs SET (m = 32)"),
            Some(VectorIndexClause::Set)
        );
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX idx ON docs.emb SEAL"),
            Some(VectorIndexClause::Seal)
        );
        assert_eq!(
            alter_vector_index_clause("ALTER VECTOR INDEX idx SEAL"),
            None
        );
    }

    #[test]
    fn restore_is_forbidden_in_active_or_failed_transactions() {
        let sessions = SessionStore::new();
        let addr = "127.0.0.1:5400".parse().expect("test address");
        sessions.ensure_session(addr);
        let ctx = DmlTxnCtx {
            sessions: &sessions,
            session_id: SessionId::from(&addr),
        };
        assert!(!restore_forbidden_in_transaction(&ctx));
        sessions.begin(addr).expect("begin");
        assert!(restore_forbidden_in_transaction(&ctx));
        sessions.fail_transaction(addr);
        assert!(restore_forbidden_in_transaction(&ctx));
    }
}
