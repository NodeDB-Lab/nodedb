use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::bridge::physical_plan::DocumentOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // TYPEGUARD DDL.
    if upper.starts_with("CREATE TYPEGUARD ") || upper.starts_with("CREATE OR REPLACE TYPEGUARD ") {
        return Some(super::super::typeguard::create_typeguard(
            state, identity, sql,
        ));
    }
    if upper.starts_with("ALTER TYPEGUARD ") {
        return Some(super::super::typeguard::alter_typeguard(
            state, identity, sql,
        ));
    }
    if upper.starts_with("DROP TYPEGUARD ") {
        return Some(super::super::typeguard::drop_typeguard(
            state, identity, sql,
        ));
    }
    if upper.starts_with("VALIDATE TYPEGUARD ON ") {
        return Some(super::super::typeguard::validate_typeguard(state, identity, sql).await);
    }
    if upper.starts_with("SHOW TYPEGUARD ON ") {
        return Some(super::super::typeguard::show_typeguard(
            state, identity, sql,
        ));
    }
    if upper == "SHOW TYPEGUARDS" || upper.starts_with("SHOW TYPEGUARDS") {
        return Some(super::super::typeguard::show_typeguards(
            state, identity, sql,
        ));
    }

    // CHUNK_TEXT table-valued function: SELECT * FROM CHUNK_TEXT(...).
    if (upper.starts_with("SELECT ") && upper.contains("CHUNK_TEXT("))
        || upper.starts_with("SELECT CHUNK_TEXT(")
    {
        return Some(super::helpers::execute_chunk_text(sql));
    }

    // DSL: SEARCH commands (async — dispatches to Data Plane).
    if upper.starts_with("SEARCH ") && upper.contains("USING VECTOR") {
        return Some(super::super::dsl::search_vector(state, identity, sql).await);
    }
    if upper.starts_with("SEARCH ") && upper.contains("USING FUSION") {
        return Some(super::super::dsl::search_fusion(state, identity, sql).await);
    }

    // DSL: CREATE VECTOR INDEX / CREATE FULLTEXT INDEX.
    if upper.starts_with("CREATE VECTOR INDEX ") {
        return Some(super::super::dsl::create_vector_index(state, identity, parts).await);
    }
    if upper.starts_with("CREATE FULLTEXT INDEX ") {
        return Some(super::super::dsl::create_fulltext_index(
            state, identity, parts,
        ));
    }
    // CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]
    if upper.starts_with("CREATE SEARCH INDEX ") {
        return Some(super::super::dsl::create_search_index(state, identity, sql));
    }
    if upper.starts_with("CREATE SPARSE INDEX ") {
        return Some(super::super::dsl::create_sparse_index(
            state, identity, parts,
        ));
    }

    // DSL: CRDT MERGE INTO (async — dispatches to Data Plane).
    if upper.starts_with("CRDT MERGE ") {
        return Some(super::super::dsl::crdt_merge(state, identity, parts).await);
    }

    // CRDT operations via SQL-like syntax (async).
    if upper.starts_with("SELECT CRDT_STATE(") || upper.starts_with("SELECT CRDT_STATE (") {
        return Some(super::super::crdt_ops::crdt_state(state, identity, sql).await);
    }
    if upper.starts_with("SELECT CRDT_APPLY(") || upper.starts_with("SELECT CRDT_APPLY (") {
        return Some(super::super::crdt_ops::crdt_apply(state, identity, sql).await);
    }

    // Graph DSL (`GRAPH ...`) and `MATCH` flow through the typed
    // AST. Parsing is done by `nodedb_sql::ddl_ast::graph_parse`,
    // which is quote- and brace-aware — handlers never see raw SQL.
    if (upper.starts_with("GRAPH ")
        || upper.starts_with("MATCH ")
        || upper.starts_with("OPTIONAL MATCH "))
        && let Some(stmt) = nodedb_sql::ddl_ast::parse(sql)
    {
        if matches!(
            stmt,
            nodedb_sql::ddl_ast::NodedbStatement::MatchQuery { .. }
        ) {
            return Some(super::super::match_ops::match_query(state, identity, sql).await);
        }
        if let Some(resp) = super::super::graph_ops::dispatch_typed(state, identity, stmt).await {
            return Some(resp);
        }
    }

    // COPY FROM file.
    if upper.starts_with("COPY ") && upper.contains(" FROM ") {
        return Some(super::super::bulk::copy_from(state, identity, parts).await);
    }

    // INSERT INTO x { } — object literal syntax; intercept for trigger/sequence handling.
    if upper.starts_with("INSERT INTO ") {
        let after_into = &sql["INSERT INTO ".len()..].trim_start();
        let coll_end = after_into
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after_into.len());
        if after_into[coll_end..].trim_start().starts_with('{')
            && let Some(result) =
                super::super::collection::insert_document(state, identity, sql).await
        {
            return Some(result);
        }
    }

    // UPSERT INTO — same as INSERT but merges into existing document if it exists.
    // Handles both (cols) VALUES (vals) and { } object literal forms.
    if upper.starts_with("UPSERT INTO ")
        && (upper.contains("VALUES") || {
            let after_into = &sql["UPSERT INTO ".len()..].trim_start();
            let coll_end = after_into
                .find(|c: char| c.is_whitespace())
                .unwrap_or(after_into.len());
            after_into[coll_end..].trim_start().starts_with('{')
        })
        && let Some(result) = super::super::collection::upsert_document(state, identity, sql).await
    {
        return Some(result);
    }

    // SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]
    if upper.starts_with("SHOW CHANGES ") {
        if let Some(coll_name) = super::super::sql_parse::extract_collection_after(sql, " FOR ") {
            let since_ms: u64 = if let Some(since_pos) = upper.find(" SINCE ") {
                let since_str = sql[since_pos + 7..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("0");
                match super::super::sql_parse::parse_since_timestamp(since_str) {
                    Ok(ms) => ms,
                    Err(msg) => {
                        return Some(Err(super::super::super::types::sqlstate_error(
                            "22007",
                            &msg.to_string(),
                        )));
                    }
                }
            } else {
                // Default: last 24 hours of changes.
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                now_ms.saturating_sub(86_400 * 1000)
            };

            let limit = upper
                .find(" LIMIT ")
                .and_then(|pos| sql[pos + 7..].split_whitespace().next())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1000);

            let changes = state
                .change_stream
                .query_changes(Some(&coll_name), since_ms, limit);

            use futures::stream;
            use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};

            let schema = std::sync::Arc::new(vec![
                super::super::super::types::text_field("collection"),
                super::super::super::types::text_field("operation"),
                super::super::super::types::text_field("document_id"),
                super::super::super::types::text_field("timestamp_ms"),
                super::super::super::types::text_field("lsn"),
            ]);

            let mut rows = Vec::with_capacity(changes.len());
            for change in &changes {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let _ = encoder.encode_field(&change.collection);
                let _ = encoder.encode_field(&change.operation.as_str().to_string());
                let _ = encoder.encode_field(&change.document_id);
                let _ = encoder.encode_field(&change.timestamp_ms.to_string());
                let _ = encoder.encode_field(&change.lsn.as_u64().to_string());
                rows.push(Ok(encoder.take_row()));
            }

            return Some(Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(rows),
            ))]));
        }
        return Some(Err(super::super::super::types::sqlstate_error(
            "42601",
            "syntax: SHOW CHANGES FOR <collection> [SINCE <timestamp>]",
        )));
    }

    // ESTIMATE_COUNT('collection', 'field') — fast approximate count from HLL stats.
    if upper.starts_with("SELECT ESTIMATE_COUNT(") || upper.starts_with("SELECT ESTIMATE_COUNT (") {
        // Parse: SELECT ESTIMATE_COUNT('collection', 'field')
        let inner = sql
            .find('(')
            .and_then(|start| sql.rfind(')').map(|end| &sql[start + 1..end]));
        if let Some(args_str) = inner {
            let args: Vec<&str> = args_str
                .split(',')
                .map(|s| s.trim().trim_matches('\''))
                .collect();
            if args.len() >= 2 {
                let coll = args[0].to_lowercase();
                let field = args[1].to_string();
                let tenant_id = identity.tenant_id;
                let vshard = crate::types::VShardId::from_collection(&coll);
                let plan =
                    crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::EstimateCount {
                        collection: coll,
                        field,
                    });
                match crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    state, tenant_id, vshard, plan, 0,
                )
                .await
                {
                    Ok(resp) => {
                        let payload_text =
                            crate::data::executor::response_codec::decode_payload_to_json(
                                &resp.payload,
                            );
                        use futures::stream;
                        use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
                        let schema =
                            std::sync::Arc::new(vec![super::super::super::types::text_field(
                                "estimate_count",
                            )]);
                        let mut encoder = DataRowEncoder::new(schema.clone());
                        let _ = encoder.encode_field(&payload_text);
                        let row = encoder.take_row();
                        return Some(Ok(vec![Response::Query(QueryResponse::new(
                            schema,
                            stream::iter(vec![Ok(row)]),
                        ))]));
                    }
                    Err(e) => {
                        return Some(Err(super::super::super::types::sqlstate_error(
                            "XX000",
                            &e.to_string(),
                        )));
                    }
                }
            }
        }
        return Some(Err(super::super::super::types::sqlstate_error(
            "42601",
            "usage: SELECT ESTIMATE_COUNT('collection', 'field')",
        )));
    }

    // TRUNCATE — handled by SQL planner (plan_truncate_stmt → DocumentOp::Truncate).

    None
}
