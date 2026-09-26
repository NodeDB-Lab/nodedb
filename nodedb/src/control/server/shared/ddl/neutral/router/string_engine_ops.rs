// SPDX-License-Identifier: BUSL-1.1

//! String-recognized engine-ops DDL arms: weighted pick, rate gates, transfers,
//! sorted-index / atomic KV functions, timeseries, last-value cache,
//! materialized views, continuous aggregates, convert, retention policies, DSL
//! index extensions, CRDT ops, chunk-text / show-changes / estimate-count,
//! field definitions, and explain tiers.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::chunk_text;
use super::super::continuous_agg;
use super::super::convert;
use super::super::crdt_ops;
use super::super::dsl;
use super::super::estimate_count;
use super::super::explain_tiers;
use super::super::field_def;
use super::super::kv_atomic;
use super::super::kv_sorted_index;
use super::super::last_value;
use super::super::materialized_view;
use super::super::rate_gate;
use super::super::retention_policy;
use super::super::show_changes;
use super::super::spatial;
use super::super::timeseries;
use super::super::transfer;
use super::super::weighted_pick;
use super::helpers::{extract_last_value_args, extract_last_values_arg};

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Engine-ops SQL functions and DDL. None of these are dispatched from a
    // typed AST arm — the pgwire engine_ops router recognized all of them by
    // string prefix from the raw SQL (these keywords do not appear in the DDL
    // AST grammar, so `ddl_ast::parse` returns `None` for them). Replicate that
    // exactly here, before the parse gate, so the prefix recognition, guard
    // ordering, and syntax messages stay byte-identical. The three vector
    // model / metadata forms (`ALTER COLLECTION … SET VECTOR METADATA ON`,
    // `SHOW VECTOR MODELS`, `SELECT VECTOR_METADATA(…)`) are routed by the
    // string-prefix arms above (alongside the vector-index lifecycle forms).
    //
    // `CREATE TIMESERIES` / `ALTER TIMESERIES` / `REWRITE PARTITIONS` are
    // routed here, but `SHOW PARTITIONS ` is intentionally NOT — it is already
    // claimed by the consumer-group handler above (which ran before engine_ops
    // on the pgwire path too), so the timeseries `show_partitions` handler stays
    // shadowed exactly as it was.

    // Weighted random selection — anchored to the statement prefix so a
    // doc-object UPSERT body carrying the token never reaches this arm.
    if upper.starts_with("SELECT * FROM WEIGHTED_PICK(")
        || upper.starts_with("SELECT * FROM WEIGHTED_PICK (")
    {
        return Some(weighted_pick::weighted_pick(state, identity, sql).await);
    }

    // Rate gate / cooldown functions.
    if upper.starts_with("SELECT RATE_CHECK(") || upper.starts_with("SELECT RATE_CHECK (") {
        return Some(rate_gate::rate_check(state, identity, sql).await);
    }
    if upper.starts_with("SELECT RATE_REMAINING(") || upper.starts_with("SELECT RATE_REMAINING (") {
        return Some(rate_gate::rate_remaining(state, identity, sql).await);
    }
    if upper.starts_with("SELECT RATE_RESET(") || upper.starts_with("SELECT RATE_RESET (") {
        return Some(rate_gate::rate_reset(state, identity, sql).await);
    }

    // Atomic transfer functions.
    if upper.starts_with("SELECT TRANSFER(") || upper.starts_with("SELECT TRANSFER (") {
        return Some(transfer::transfer(state, identity, sql, txn_ctx).await);
    }
    if upper.starts_with("SELECT TRANSFER_ITEM(") || upper.starts_with("SELECT TRANSFER_ITEM (") {
        return Some(transfer::transfer_item(state, identity, sql, txn_ctx).await);
    }

    // Sorted index DDL.
    if upper.starts_with("CREATE SORTED INDEX ") {
        return Some(kv_sorted_index::create_sorted_index(state, identity, database_id, sql).await);
    }
    if upper.starts_with("DROP SORTED INDEX ") {
        return Some(kv_sorted_index::drop_sorted_index(state, identity, database_id, sql).await);
    }

    // Sorted index query functions, anchored to the statement prefix: a
    // doc-object UPSERT body, string literal, or comment carrying the token
    // can never reach these arms.
    if upper.starts_with("SELECT RANK(") || upper.starts_with("SELECT RANK (") {
        return Some(
            kv_sorted_index::select_rank(state, identity, database_id, sql, txn_ctx).await,
        );
    }
    if upper.starts_with("SELECT TOPK(")
        || upper.starts_with("SELECT TOPK (")
        || upper.starts_with("SELECT * FROM TOPK(")
        || upper.starts_with("SELECT * FROM TOPK (")
    {
        return Some(
            kv_sorted_index::select_topk(state, identity, database_id, sql, txn_ctx).await,
        );
    }
    if upper.starts_with("SELECT SORTED_COUNT(") || upper.starts_with("SELECT SORTED_COUNT (") {
        return Some(
            kv_sorted_index::select_sorted_count(state, identity, database_id, sql, txn_ctx).await,
        );
    }
    // RANGE as a sorted index function (check it's not a standard SQL RANGE).
    if (upper.starts_with("SELECT * FROM RANGE(") || upper.starts_with("SELECT * FROM RANGE ("))
        && !upper.contains(" BETWEEN ")
    {
        return Some(
            kv_sorted_index::select_range(state, identity, database_id, sql, txn_ctx).await,
        );
    }

    // KV_INCR / KV_DECR / KV_INCR_FLOAT / KV_CAS / KV_GETSET — atomic KV operations.
    if upper.starts_with("SELECT KV_INCR(") || upper.starts_with("SELECT KV_INCR (") {
        return Some(kv_atomic::kv_incr(state, identity, sql, false, txn_ctx).await);
    }
    if upper.starts_with("SELECT KV_DECR(") || upper.starts_with("SELECT KV_DECR (") {
        return Some(kv_atomic::kv_incr(state, identity, sql, true, txn_ctx).await);
    }
    if upper.starts_with("SELECT KV_INCR_FLOAT(") || upper.starts_with("SELECT KV_INCR_FLOAT (") {
        return Some(kv_atomic::kv_incr_float(state, identity, sql, txn_ctx).await);
    }
    if upper.starts_with("SELECT KV_CAS(") || upper.starts_with("SELECT KV_CAS (") {
        return Some(kv_atomic::kv_cas(state, identity, sql, txn_ctx).await);
    }
    if upper.starts_with("SELECT KV_GETSET(") || upper.starts_with("SELECT KV_GETSET (") {
        return Some(kv_atomic::kv_getset(state, identity, sql, txn_ctx).await);
    }

    // Timeseries: CREATE TIMESERIES, ALTER TIMESERIES, REWRITE PARTITIONS.
    // (SHOW PARTITIONS is shadowed by consumer_group above, as noted.)
    if upper.starts_with("CREATE TIMESERIES ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(timeseries::create_timeseries(
            state,
            identity,
            &parts,
            database_id,
        ));
    }
    if upper.starts_with("ALTER TIMESERIES ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(timeseries::alter_timeseries(state, identity, &parts));
    }
    if upper.starts_with("REWRITE PARTITIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(timeseries::rewrite_partitions(state, identity, &parts));
    }

    // Last-value cache queries.
    if upper.starts_with("SELECT LAST_VALUES(") {
        // SELECT LAST_VALUES('collection_name')
        if let Some(collection) = extract_last_values_arg(sql) {
            return Some(
                last_value::query_last_values(state, identity, database_id, &collection).await,
            );
        }
    }
    if upper.starts_with("SELECT LAST_VALUE(") && !upper.starts_with("SELECT LAST_VALUES(") {
        // SELECT LAST_VALUE('collection_name', series_id)
        if let Some((collection, series_id)) = extract_last_value_args(sql) {
            return Some(
                last_value::query_last_value(state, identity, database_id, &collection, series_id)
                    .await,
            );
        }
    }

    // Materialized views (HTAP). `REFRESH MATERIALIZED VIEW` parses into no typed
    // AST variant, and `SHOW MATERIALIZED VIEWS` parses into a typed
    // `StreamViewStmt::ShowMaterializedViews` but the pgwire admin router
    // dispatched it from the raw token slice by string prefix (the `SHOW
    // MATERIALIZED VIEW` prefix, trailing-space-less, captures both the plural
    // `SHOW MATERIALIZED VIEWS` and the bare-singular input). Replicate both here,
    // before the parse gate, so the prefix recognition and the `parts`-based name
    // extraction stay byte-identical. `CREATE` / `DROP MATERIALIZED VIEW` are
    // handled in the typed match below (they parse into typed StreamView variants).
    if upper.starts_with("REFRESH MATERIALIZED VIEW") {
        return Some(
            materialized_view::refresh_materialized_view(state, identity, database_id, sql).await,
        );
    }
    if upper.starts_with("SHOW MATERIALIZED VIEW") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(materialized_view::show_materialized_views(
            state,
            identity,
            database_id,
            &parts,
        ));
    }

    // Continuous aggregates (timeseries). `SHOW CONTINUOUS AGGREGATES [FOR
    // <source>]` parses into a typed `StreamViewStmt::ShowContinuousAggregates`
    // but the pgwire admin router dispatched it from the raw token slice by
    // string prefix (the `SHOW CONTINUOUS AGGREGATE` prefix, trailing-space-less,
    // captures both the plural `SHOW CONTINUOUS AGGREGATES` and the bare-singular
    // input). Replicate that here, before the parse gate, so the prefix
    // recognition and the `parts`-based `FOR <source>` extraction stay
    // byte-identical. `CREATE` / `DROP CONTINUOUS AGGREGATE` are handled in the
    // typed match below (they parse into typed StreamView variants).
    if upper.starts_with("SHOW CONTINUOUS AGGREGATE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(
            continuous_agg::show_continuous_aggregates(state, identity, database_id, &parts).await,
        );
    }

    // CONVERT COLLECTION between storage modes. `CONVERT COLLECTION <name> TO
    // <target>` parses into no typed AST variant — the pgwire admin router
    // dispatched it by string prefix from the raw SQL. Replicate that exactly
    // here, before the parse gate, so the prefix recognition (the
    // `CONVERT COLLECTION ` form plus the broader `CONVERT ... TO ...` form, in
    // that `||`/`&&` precedence) and the parse / syntax messages stay
    // byte-identical.
    if upper.starts_with("CONVERT COLLECTION ")
        || upper.starts_with("CONVERT ") && upper.contains(" TO ")
    {
        return Some(convert::convert_collection(state, identity, database_id, sql).await);
    }

    // Retention policies (timeseries). `SHOW RETENTION POLICIES` parses into a
    // typed `PolicyStmt::ShowRetentionPolicies`, but the pgwire admin router
    // dispatched it from the raw token slice by the `SHOW RETENTION POLIC`
    // prefix (trailing-space-less, captures both the plural `SHOW RETENTION
    // POLICIES` and the singular `SHOW RETENTION POLICY ON <collection>`).
    // Replicate that exactly here, before the parse gate, so the prefix
    // recognition and the `parts`-based `ON <collection>` filter stay
    // byte-identical. `CREATE` / `ALTER` / `DROP RETENTION POLICY` are handled in
    // the typed match below (they parse into typed Policy variants).
    if upper.starts_with("SHOW RETENTION POLIC") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(retention_policy::show_retention_policy(
            state,
            identity,
            database_id,
            &parts,
        ));
    }

    // DSL extensions (custom SQL-like surfaces). None of these are dispatched
    // from a typed AST arm — the pgwire dsl router recognized all six by string
    // prefix from the raw SQL. Replicate that exactly here, before the parse
    // gate, so the prefix recognition and syntax messages stay byte-identical.
    // `SEARCH ... USING FUSION` must precede the parse gate because it would
    // otherwise parse into a typed graph statement and be captured by the graph
    // dispatch below. `SEARCH ... USING VECTOR(...)` never reaches here — it is
    // preprocessor-rewritten to a canonical `SELECT ... vector_distance(...)`.
    if upper.starts_with("SEARCH ") && upper.contains("USING FUSION") {
        return Some(dsl::search_fusion(state, identity, database_id, sql).await);
    }
    if upper.starts_with("CREATE VECTOR INDEX ") {
        return Some(dsl::create_vector_index(state, identity, database_id, sql).await);
    }
    if upper.starts_with("CREATE FULLTEXT INDEX ") {
        return Some(dsl::create_fulltext_index(state, identity, database_id, sql).await);
    }
    if upper.starts_with("CREATE SEARCH INDEX ") {
        return Some(dsl::create_search_index(state, identity, database_id, sql).await);
    }
    if upper.starts_with("CREATE SPARSE INDEX ") {
        return Some(dsl::create_sparse_index(state, identity, database_id, sql));
    }
    // CREATE SPATIAL INDEX — string-recognized (no typed AST variant); the pgwire
    // schema string router dispatched it from the raw SQL. Replicate that
    // exactly here, before the parse gate.
    if upper.starts_with("CREATE SPATIAL INDEX ") {
        return Some(spatial::create_spatial_index(
            state,
            identity,
            database_id,
            sql,
        ));
    }
    if upper.starts_with("CRDT MERGE ") {
        if crdt_apply_forbidden_in_transaction(txn_ctx) {
            return Some(Err(crdt_transaction_error()));
        }
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(dsl::crdt_merge(state, identity, database_id, &parts).await);
    }
    // `SELECT crdt_state(...)` / `SELECT crdt_apply(...)` CRDT DSL functions —
    // string-recognized (they parse into no typed DDL variant). The pgwire dsl
    // string router recognized both by prefix from the raw SQL; replicate that
    // exactly here, before the parse gate.
    if upper.starts_with("SELECT CRDT_STATE(") || upper.starts_with("SELECT CRDT_STATE (") {
        return Some(crdt_ops::crdt_state(state, identity, database_id, sql).await);
    }
    if upper.starts_with("SELECT CRDT_APPLY(") || upper.starts_with("SELECT CRDT_APPLY (") {
        if crdt_apply_forbidden_in_transaction(txn_ctx) {
            return Some(Err(crdt_transaction_error()));
        }
        return Some(crdt_ops::crdt_apply(state, identity, database_id, sql).await);
    }

    // `NDB_CHUNK_TEXT(...)` table-valued function, `SHOW CHANGES FOR …`
    // change-stream query, and `SELECT ESTIMATE_COUNT(…)` — string-recognized
    // (none parse into a typed DDL variant). Anchored to the statement prefix
    // so a string literal or doc-object UPSERT body carrying the token never
    // reaches this arm.
    if upper.starts_with("SELECT * FROM NDB_CHUNK_TEXT(")
        || upper.starts_with("SELECT * FROM NDB_CHUNK_TEXT (")
        || upper.starts_with("SELECT NDB_CHUNK_TEXT(")
        || upper.starts_with("SELECT NDB_CHUNK_TEXT (")
    {
        return Some(chunk_text::execute_chunk_text(sql));
    }
    if upper.starts_with("SHOW CHANGES ") {
        return Some(show_changes::show_changes(
            state,
            identity,
            database_id,
            sql,
        ));
    }
    if upper.starts_with("SELECT ESTIMATE_COUNT(") || upper.starts_with("SELECT ESTIMATE_COUNT (") {
        return Some(estimate_count::estimate_count(state, identity, database_id, sql).await);
    }

    // `DEFINE FIELD …` / `DEFINE EVENT …` / `REMOVE EVENT …` —
    // string-recognized (no typed DDL variant), before the parse gate.
    if upper.starts_with("DEFINE FIELD ") {
        return Some(field_def::define_field(state, identity, database_id, sql));
    }
    if upper.starts_with("DEFINE EVENT ") {
        return Some(field_def::define_event(state, identity, database_id, sql));
    }
    if upper.starts_with("REMOVE EVENT ") {
        return Some(field_def::remove_event(state, identity, database_id, sql));
    }

    // `EXPLAIN TIERS ON <collection> [RANGE …]` — string-recognized (no typed
    // DDL variant); the pgwire admin string router dispatched it from the raw
    // token slice. Replicate that exactly here, before the parse gate.
    if upper.starts_with("EXPLAIN TIERS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(explain_tiers::explain_tiers(
            state,
            identity,
            database_id,
            &parts,
        ));
    }

    None
}

fn crdt_apply_forbidden_in_transaction(txn_ctx: &DmlTxnCtx<'_>) -> bool {
    txn_ctx.sessions.transaction_state(txn_ctx.session_id)
        != crate::control::server::shared::session::TransactionState::Idle
}

fn crdt_transaction_error() -> DdlError {
    DdlError::new(
        "25001",
        crate::Error::CrdtApplyForbiddenInTransaction.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{SessionId, SessionStore};

    #[test]
    fn crdt_apply_and_merge_are_forbidden_in_active_or_failed_transactions() {
        let sessions = SessionStore::new();
        let addr = "127.0.0.1:5399".parse().expect("test address");
        sessions.ensure_session(addr);
        let ctx = DmlTxnCtx {
            sessions: &sessions,
            session_id: SessionId::from(&addr),
        };
        assert!(!crdt_apply_forbidden_in_transaction(&ctx));

        sessions
            .begin(addr, crate::types::Lsn::new(1), 0)
            .expect("begin");
        assert!(crdt_apply_forbidden_in_transaction(&ctx));
        sessions.fail_transaction(addr);
        assert!(crdt_apply_forbidden_in_transaction(&ctx));

        let error = crdt_transaction_error();
        assert_eq!(error.sqlstate, "25001");
        assert_eq!(
            error.message,
            crate::Error::CrdtApplyForbiddenInTransaction.to_string()
        );
    }
}
