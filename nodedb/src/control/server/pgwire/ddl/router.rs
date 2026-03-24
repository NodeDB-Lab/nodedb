use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Try to handle a SQL statement as a Control Plane DDL command.
///
/// These execute directly on the Control Plane without going through
/// DataFusion or the Data Plane. Returns `None` if not recognized.
///
/// Async because DSL commands (SEARCH, CRDT) dispatch to the Data Plane
/// and must await the response without blocking the Tokio runtime.
pub async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    let upper = sql.to_uppercase();
    let parts: Vec<&str> = sql.split_whitespace().collect();

    // User management.
    if upper.starts_with("CREATE USER ") {
        return Some(super::user::create_user(state, identity, &parts));
    }
    if upper.starts_with("ALTER USER ") {
        return Some(super::user::alter_user(state, identity, &parts));
    }
    if upper.starts_with("DROP USER ") {
        return Some(super::user::drop_user(state, identity, &parts));
    }

    // Service accounts.
    if upper.starts_with("CREATE SERVICE ACCOUNT ") {
        return Some(super::service_account::create_service_account(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DROP SERVICE ACCOUNT ") {
        return Some(super::service_account::drop_service_account(
            state, identity, &parts,
        ));
    }

    // Tenant management.
    if upper.starts_with("CREATE TENANT ") {
        return Some(super::tenant::create_tenant(state, identity, &parts));
    }
    if upper.starts_with("ALTER TENANT ") {
        return Some(super::tenant::alter_tenant(state, identity, &parts));
    }
    if upper.starts_with("DROP TENANT ") {
        return Some(super::tenant::drop_tenant(state, identity, &parts));
    }

    // GRANT / REVOKE.
    if upper.starts_with("GRANT ") {
        return Some(super::grant::handle_grant(state, identity, &parts));
    }
    if upper.starts_with("REVOKE ") {
        return Some(super::grant::handle_revoke(state, identity, &parts));
    }

    // Role management.
    if upper.starts_with("CREATE ROLE ") {
        return Some(super::role::create_role(state, identity, &parts));
    }
    if upper.starts_with("ALTER ROLE ") {
        return Some(super::role::alter_role(state, identity, &parts));
    }
    if upper.starts_with("DROP ROLE ") {
        return Some(super::role::drop_role(state, identity, &parts));
    }

    // Backup / Restore.
    if upper.starts_with("BACKUP TENANT ") {
        return Some(super::backup::backup_tenant(state, identity, &parts));
    }
    if upper.starts_with("RESTORE TENANT ") {
        if upper.ends_with(" DRY RUN") || upper.ends_with(" DRYRUN") {
            return Some(super::backup::restore_tenant_dry_run(
                state, identity, &parts,
            ));
        }
        return Some(super::backup::restore_tenant(state, identity, &parts));
    }

    // Schema introspection.
    if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
        return Some(super::collection::describe_collection(
            state, identity, &parts,
        ));
    }

    // Collection management.
    if upper.starts_with("CREATE COLLECTION ") {
        return Some(super::collection::create_collection(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("DROP COLLECTION ") {
        return Some(super::collection::drop_collection(state, identity, &parts));
    }
    if upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
        return Some(super::collection::show_collections(state, identity));
    }
    // Timeseries: CREATE TIMESERIES, SHOW PARTITIONS, ALTER TIMESERIES.
    if upper.starts_with("CREATE TIMESERIES ") {
        return Some(super::timeseries_ddl::create_timeseries(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        return Some(super::timeseries_ddl::show_partitions(
            state, identity, &parts,
        ));
    }
    if upper.starts_with("ALTER TIMESERIES ") {
        return Some(super::timeseries_ddl::alter_timeseries(
            state, identity, &parts,
        ));
    }

    // Pub/Sub: CREATE TOPIC, DROP TOPIC, SHOW TOPICS, PUBLISH TO, SUBSCRIBE TO.
    if upper.starts_with("CREATE TOPIC ") {
        return Some(super::pubsub::create_topic(state, identity, &parts));
    }
    if upper.starts_with("DROP TOPIC ") {
        return Some(super::pubsub::drop_topic(state, identity, &parts));
    }
    if upper == "SHOW TOPICS" || upper.starts_with("SHOW TOPICS") {
        return Some(super::pubsub::show_topics(state));
    }
    if upper.starts_with("PUBLISH TO ") {
        return Some(super::pubsub::publish_to(state, identity, sql, &parts));
    }
    if upper.starts_with("SUBSCRIBE TO ") {
        return Some(super::pubsub::subscribe_to(state, identity, sql, &parts));
    }

    if upper.starts_with("CREATE INDEX ") || upper.starts_with("CREATE UNIQUE INDEX ") {
        return Some(super::collection::create_index(
            state, identity, &parts, sql,
        ));
    }
    if upper.starts_with("DROP INDEX ") {
        return Some(super::collection::drop_index(state, identity, &parts));
    }
    if upper.starts_with("SHOW INDEXES") || upper.starts_with("SHOW INDEX") {
        return Some(super::collection::show_indexes(state, identity, &parts));
    }

    // Ownership transfer.
    if upper.starts_with("ALTER COLLECTION ") && upper.contains("OWNER TO") {
        return Some(super::ownership::alter_collection_owner(
            state, identity, &parts,
        ));
    }

    // RLS policies.
    if upper.starts_with("CREATE RLS POLICY ") {
        return Some(super::rls::create_rls_policy(state, identity, &parts));
    }
    if upper.starts_with("DROP RLS POLICY ") {
        return Some(super::rls::drop_rls_policy(state, identity, &parts));
    }
    if upper.starts_with("SHOW RLS POLICIES") || upper.starts_with("SHOW RLS POLICY") {
        return Some(super::rls::show_rls_policies(state, identity, &parts));
    }

    // API keys.
    if upper.starts_with("CREATE API KEY ") {
        return Some(super::apikey::create_api_key(state, identity, &parts));
    }
    if upper.starts_with("REVOKE API KEY ") {
        return Some(super::apikey::revoke_api_key(state, identity, &parts));
    }
    if upper.starts_with("LIST API KEYS") {
        return Some(super::apikey::list_api_keys(state, identity, &parts));
    }

    // Cluster management & observability.
    if upper.starts_with("SHOW CLUSTER") {
        return Some(super::cluster::show_cluster(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUPS") {
        return Some(super::cluster::show_raft_groups(state, identity));
    }
    if upper.starts_with("SHOW RAFT GROUP ") {
        return Some(super::cluster::show_raft_group(state, identity, &parts));
    }
    if upper.starts_with("ALTER RAFT GROUP ") {
        return Some(super::cluster::alter_raft_group(state, identity, &parts));
    }
    if upper.starts_with("SHOW MIGRATIONS") {
        return Some(super::cluster::show_migrations(state, identity));
    }
    if upper.starts_with("REBALANCE") {
        return Some(super::cluster::rebalance(state, identity));
    }
    if upper.starts_with("SHOW PEER HEALTH") {
        return Some(super::cluster::show_peer_health(state, identity));
    }
    if upper.starts_with("SHOW NODES") {
        return Some(super::cluster::show_nodes(state, identity));
    }
    if upper.starts_with("SHOW NODE ") {
        return Some(super::cluster::show_node(state, identity, &parts));
    }
    if upper.starts_with("REMOVE NODE ") {
        return Some(super::cluster::remove_node(state, identity, &parts));
    }

    // Introspection.
    if upper.starts_with("SHOW USERS") {
        return Some(super::inspect::show_users(state, identity));
    }
    if upper.starts_with("SHOW TENANTS") {
        return Some(super::inspect::show_tenants(state, identity));
    }
    if upper.starts_with("SHOW SESSION") {
        return Some(super::inspect::show_session(identity));
    }
    if upper.starts_with("TRUNCATE AUDIT")
        || upper.starts_with("DELETE AUDIT")
        || upper.starts_with("CLEAR AUDIT")
    {
        return Some(Err(super::super::types::sqlstate_error(
            "42501",
            "audit log cannot be manually truncated. Entries are pruned automatically by the retention policy (audit_retention_days in config).",
        )));
    }
    if upper.starts_with("EXPORT AUDIT") {
        return Some(super::inspect::export_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUDIT LOG") || upper.starts_with("SHOW AUDIT_LOG") {
        return Some(super::inspect::show_audit_log(state, identity, &parts));
    }
    if upper.starts_with("SHOW PERMISSIONS") {
        return Some(super::inspect::show_permissions(state, identity, &parts));
    }
    if upper.starts_with("SHOW GRANTS") {
        return Some(super::inspect::show_grants(state, identity, &parts));
    }

    // DSL: SEARCH commands (async — dispatches to Data Plane).
    if upper.starts_with("SEARCH ") && upper.contains("USING VECTOR") {
        return Some(super::dsl::search_vector(state, identity, sql).await);
    }
    if upper.starts_with("SEARCH ") && upper.contains("USING FUSION") {
        return Some(super::dsl::search_fusion(state, identity, sql).await);
    }

    // DSL: CREATE VECTOR INDEX / CREATE FULLTEXT INDEX.
    if upper.starts_with("CREATE VECTOR INDEX ") {
        return Some(super::dsl::create_vector_index(state, identity, &parts));
    }
    if upper.starts_with("CREATE FULLTEXT INDEX ") {
        return Some(super::dsl::create_fulltext_index(state, identity, &parts));
    }

    // DSL: CRDT MERGE INTO (async — dispatches to Data Plane).
    if upper.starts_with("CRDT MERGE ") {
        return Some(super::dsl::crdt_merge(state, identity, &parts).await);
    }

    // CRDT operations via SQL-like syntax (async).
    if upper.starts_with("SELECT CRDT_STATE(") || upper.starts_with("SELECT CRDT_STATE (") {
        return Some(super::crdt_ops::crdt_state(state, identity, sql).await);
    }
    if upper.starts_with("SELECT CRDT_APPLY(") || upper.starts_with("SELECT CRDT_APPLY (") {
        return Some(super::crdt_ops::crdt_apply(state, identity, sql).await);
    }

    // DSL: Graph operations (async — dispatches to Data Plane).
    if upper.starts_with("GRAPH INSERT EDGE ") {
        return Some(super::graph_ops::insert_edge(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH DELETE EDGE ") {
        return Some(super::graph_ops::delete_edge(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH TRAVERSE ") {
        return Some(super::graph_ops::traverse(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH NEIGHBORS ") {
        return Some(super::graph_ops::neighbors(state, identity, &parts, sql).await);
    }
    if upper.starts_with("GRAPH PATH ") {
        return Some(super::graph_ops::shortest_path(state, identity, &parts, sql).await);
    }

    // COPY FROM file.
    if upper.starts_with("COPY ") && upper.contains(" FROM ") {
        return Some(super::bulk::copy_from(state, identity, &parts).await);
    }

    // INSERT INTO — intercept for schemaless collections (DataFusion rejects
    // columns not in the Arrow schema, but NodeDB collections are document stores).
    if upper.starts_with("INSERT INTO ")
        && upper.contains("VALUES")
        && let Some(result) = super::collection_insert::insert_document(state, identity, sql).await
    {
        return Some(result);
    }

    // UPSERT INTO — same as INSERT but merges into existing document if it exists.
    if upper.starts_with("UPSERT INTO ")
        && upper.contains("VALUES")
        && let Some(result) = super::collection_insert::upsert_document(state, identity, sql).await
    {
        return Some(result);
    }

    // SHOW CHANGES FOR <collection> [SINCE <timestamp>] [LIMIT <n>]
    if upper.starts_with("SHOW CHANGES ") {
        if let Some(coll_name) = super::sql_parse::extract_collection_after(sql, " FOR ") {
            let since_ms: u64 = if let Some(since_pos) = upper.find(" SINCE ") {
                let since_str = sql[since_pos + 7..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("0");
                match super::sql_parse::parse_since_timestamp(since_str) {
                    Ok(ms) => ms,
                    Err(msg) => {
                        return Some(Err(super::super::types::sqlstate_error(
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
                super::super::types::text_field("collection"),
                super::super::types::text_field("operation"),
                super::super::types::text_field("document_id"),
                super::super::types::text_field("timestamp_ms"),
                super::super::types::text_field("lsn"),
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
        return Some(Err(super::super::types::sqlstate_error(
            "42601",
            "syntax: SHOW CHANGES FOR <collection> [SINCE <timestamp>]",
        )));
    }

    // LIVE SELECT ... FROM <collection> [WHERE ...]
    //
    // Registers a subscription on the change stream and returns the subscription
    // ID + LISTEN channel name. Changes are delivered via PostgreSQL async
    // notifications (same as LISTEN/NOTIFY). The client receives:
    //   1. A query result with the subscription_id and channel name
    //   2. Async NOTIFY messages for each matching change event
    if upper.starts_with("LIVE SELECT ") {
        if let Some(coll_name) = super::sql_parse::extract_collection_after(sql, " FROM ") {
            let tenant_id = identity.tenant_id;
            let sub = state
                .change_stream
                .subscribe(Some(coll_name.clone()), Some(tenant_id));
            let sub_id = sub.id;

            // The channel name for LISTEN/NOTIFY delivery.
            let channel = format!("live_{coll_name}");

            use futures::stream;
            use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
            let schema = std::sync::Arc::new(vec![
                super::super::types::text_field("subscription_id"),
                super::super::types::text_field("channel"),
                super::super::types::text_field("collection"),
                super::super::types::text_field("status"),
            ]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(&sub_id.to_string());
            let _ = encoder.encode_field(&channel);
            let _ = encoder.encode_field(&coll_name);
            let _ = encoder.encode_field(&"active");
            let row = encoder.take_row();
            return Some(Ok(vec![Response::Query(QueryResponse::new(
                schema,
                stream::iter(vec![Ok(row)]),
            ))]));
        }
        return Some(Err(super::super::types::sqlstate_error(
            "42601",
            "syntax: LIVE SELECT [*|fields] FROM <collection> [WHERE ...]",
        )));
    }

    // DEFINE FIELD <name> ON <collection> [TYPE <type>] [DEFAULT <expr>] [VALUE <expr>] [ASSERT <expr>] [READONLY]
    if upper.starts_with("DEFINE FIELD ") {
        return Some(super::field_def::define_field(state, identity, sql));
    }

    // DEFINE EVENT <name> ON <collection> WHEN <condition> THEN <action>
    if upper.starts_with("DEFINE EVENT ") {
        return Some(super::field_def::define_event(state, identity, sql));
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
                let plan = crate::bridge::envelope::PhysicalPlan::EstimateCount {
                    collection: coll,
                    field,
                };
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
                        let schema = std::sync::Arc::new(vec![super::super::types::text_field(
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
                        return Some(Err(super::super::types::sqlstate_error(
                            "XX000",
                            &e.to_string(),
                        )));
                    }
                }
            }
        }
        return Some(Err(super::super::types::sqlstate_error(
            "42601",
            "usage: SELECT ESTIMATE_COUNT('collection', 'field')",
        )));
    }

    // TRUNCATE <collection> — fast delete-all without filter scan.
    if upper.starts_with("TRUNCATE ") {
        let coll_name = parts.get(1).map(|s| s.to_lowercase()).unwrap_or_default();
        if coll_name.is_empty() {
            return Some(Err(super::super::types::sqlstate_error(
                "42601",
                "TRUNCATE requires a collection name",
            )));
        }
        let tenant_id = identity.tenant_id;
        let vshard = crate::types::VShardId::from_collection(&coll_name);
        let plan = crate::bridge::envelope::PhysicalPlan::Truncate {
            collection: coll_name,
        };
        if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state, tenant_id, vshard, plan, 0,
        )
        .await
        {
            return Some(Err(super::super::types::sqlstate_error(
                "XX000",
                &e.to_string(),
            )));
        }
        return Some(Ok(vec![pgwire::api::results::Response::Execution(
            pgwire::api::results::Tag::new("TRUNCATE"),
        )]));
    }

    None
}
