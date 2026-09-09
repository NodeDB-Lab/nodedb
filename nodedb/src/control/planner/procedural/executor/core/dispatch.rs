// SPDX-License-Identifier: BUSL-1.1

//! DML dispatch and transaction control for the statement executor.

use super::super::transaction::ProcedureTransactionCtx;
use super::StatementExecutor;
use super::sql_literal_concat::fold_literal_string_concat;
use crate::control::planner::procedural::ast::SqlExpr;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::eval;
use crate::types::TraceId;

impl<'a> StatementExecutor<'a> {
    // ── ASSIGN handling ─────────────────────────────────────────────────

    pub(super) async fn execute_assign(
        &self,
        target: &str,
        expr: &SqlExpr,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let target_upper = target.to_uppercase();
        if let Some(field_name) = target_upper.strip_prefix("NEW.") {
            let bound_expr = bindings.substitute(&expr.sql);
            let value = eval::evaluate_to_value(self.state, self.tenant_id, &bound_expr).await?;
            let mut guard = self.new_mutations.lock().unwrap_or_else(|p| p.into_inner());
            guard.insert(field_name.to_lowercase(), value);
        }
        Ok(())
    }

    // ── RETURN handling ─────────────────────────────────────────────────

    pub(super) async fn execute_return(
        &self,
        expr: &SqlExpr,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let bound_expr = bindings.substitute(&expr.sql);
        let value = eval::evaluate_to_value(self.state, self.tenant_id, &bound_expr).await?;
        let mut guard = self.out_values.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert("__return".to_string(), value);
        Ok(())
    }

    // ── SQL dispatch ────────────────────────────────────────────────────

    pub(super) async fn execute_sql(&self, sql: &str, bindings: &RowBindings) -> crate::Result<()> {
        let bound_sql = fold_literal_string_concat(&bindings.substitute(sql));

        // First, attempt unified dispatch for NodeDB SQL extensions (PUBLISH TO,
        // topic/consumer-group DDL, etc.). If the SQL is not an extension,
        // `dispatch_sql` returns None and we fall through to plan_sql with
        // transaction-buffer semantics preserved exactly as before.
        if let Some(_outcome) = crate::control::sql_dispatch::dispatch_sql_in_database(
            self.state,
            &self.identity_for_dispatch(),
            self.database_id,
            &bound_sql,
        )
        .await?
        {
            return Ok(());
        }

        // Not a NodeDB extension: plan with descriptor versions so admission
        // occurs before this internal path buffers, WAL-appends, or dispatches.
        // Stored procedures are trusted internal execution, so this deliberately
        // does not add user authorization beyond their existing semantics.
        //
        // Planning and lease admission run as ONE retried unit: the lease that
        // pins the planned descriptor version is acquired after the catalog
        // read, so a descriptor drain starting in between would otherwise fail
        // the whole procedure. Re-planning is pure, and admission fails closed
        // before granting anything, so an absorbed attempt reads nothing.
        let ctx = crate::control::planner::context::QueryContext::for_state(self.state);
        let ctx = &ctx;
        let bound_sql = &bound_sql;
        // A stored-procedure body is server-defined code, not a client
        // statement, and runs SECURITY DEFINER exactly as a trigger body does —
        // so it plans as the system rather than under the invoker's scope. The
        // context is built once and borrowed by the retry closure, which may run
        // it several times.
        let security = crate::control::planner::context::SystemPlanSecurity::new(
            self.tenant_id,
            "_system_procedure",
        );
        let security = &security;
        let (tasks, lease_scope) =
            crate::control::server::shared::retry::retry_on_schema_change(move || async move {
                let (tasks, _output_schema, versions, _) = ctx
                    .plan_sql_with_rls_and_versions(
                        bound_sql,
                        self.tenant_id,
                        self.database_id,
                        &security.context(self.state),
                        None,
                    )
                    .await?;
                let lease_scope = self.state.acquire_plan_lease_scope(&versions)?;
                Ok::<_, crate::Error>((tasks, lease_scope))
            })
            .await?;

        if let Some(ref tx_ctx) = self.tx_ctx {
            let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            guard.buffer_statement(tasks, lease_scope);
        } else {
            // Keep the scope through every route decision, WAL append, and
            // Data-Plane dispatch; errors also release it via Drop.
            let _lease_scope = lease_scope;
            for task in tasks {
                // Cross-shard trigger origination: when this executor carries a
                // source-write origin (Event-Plane AFTER-trigger fire path) AND
                // the node is clustered, a task whose target collection is homed
                // on a remote node must be dispatched to that node via the
                // cross-shard event subsystem — NOT written to the local core
                // (the historical silent mis-write). Stored procedures and
                // normal client SQL carry no origin, so `route` is `None` and
                // they always take the unchanged local path below.
                if let Some(origin) = self.cross_shard_origin.as_ref() {
                    let route = {
                        let routing_guard = self
                            .state
                            .cluster_routing
                            .as_ref()
                            .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
                        routing_guard.as_deref().map(|routing| {
                            crate::control::gateway::router::resolve_decision(
                                task.vshard_id.as_u32(),
                                self.state.node_id,
                                Some(routing),
                                None,
                            )
                        })
                    };

                    match route {
                        // Single-node (no routing table) or this node owns the
                        // target vShard: fall through to the local write path.
                        None | Some(crate::control::gateway::RouteDecision::Local) => {}
                        Some(crate::control::gateway::RouteDecision::Remote {
                            node_id, ..
                        }) => {
                            self.enqueue_cross_shard_write(
                                node_id,
                                origin,
                                task.vshard_id.as_u32(),
                                bound_sql,
                            )?;
                            continue;
                        }
                        Some(crate::control::gateway::RouteDecision::LeaderUnknown {
                            vshard_id,
                        }) => {
                            return Err(crate::Error::NotLeader {
                                vshard_id: crate::types::VShardId::new(vshard_id as u32),
                                leader_node: 0,
                                leader_addr: String::new(),
                            });
                        }
                        Some(crate::control::gateway::RouteDecision::Broadcast { .. }) => {
                            // `resolve_decision` resolves a single vShard and
                            // never returns Broadcast; treat as an invariant
                            // violation rather than silently mis-routing.
                            return Err(crate::Error::Internal {
                                detail: "cross-shard trigger: resolve_decision returned \
                                         Broadcast for a single vShard"
                                    .into(),
                            });
                        }
                    }
                }

                let outcome = crate::control::server::wal_dispatch::wal_append_if_write(
                    &self.state.wal,
                    task.tenant_id,
                    task.vshard_id,
                    task.database_id,
                    &task.plan,
                )?;

                crate::control::server::dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
                    self.state,
                    crate::control::server::dispatch_utils::WriteDispatch {
                        tenant_id: task.tenant_id,
                        database_id: task.database_id,
                        vshard_id: task.vshard_id,
                        plan: task.plan,
                        trace_id: TraceId::ZERO,
                        event_source: self.event_source,
                        txn_id: None,
                        wal_lsn: outcome.lsn,
                        resolved_now_ms: outcome.resolved_now_ms,
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Enqueue a trigger-originated write for delivery to the vShard's owning
    /// node via the cross-shard event dispatcher.
    ///
    /// Event-Plane safe: this only performs a bounded in-memory push (the
    /// dispatcher's per-target queue). The durable write happens on the target
    /// node's `CrossShardReceiver`, which WAL-appends and dispatches there. No
    /// storage I/O or remote DML executes inline here.
    fn enqueue_cross_shard_write(
        &self,
        target_node: u64,
        origin: &super::CrossShardOrigin,
        target_vshard: u32,
        bound_sql: &str,
    ) -> crate::Result<()> {
        let request = crate::event::cross_shard::types::CrossShardWriteRequest {
            sql: bound_sql.to_string(),
            tenant_id: self.tenant_id.as_u64(),
            database_id: self.database_id.as_u64(),
            source_vshard: origin.source_vshard,
            source_lsn: origin.source_lsn,
            source_sequence: origin.source_sequence,
            cascade_depth: self.cascade_depth(),
            source_collection: origin.source_collection.clone(),
            target_vshard,
        };

        let dispatcher =
            self.state
                .cross_shard_dispatcher
                .as_ref()
                .ok_or(crate::Error::Dispatch {
                    detail: "cross-shard dispatcher not initialised for trigger origination"
                        .to_string(),
                })?;

        if !dispatcher.enqueue(target_node, request) {
            return Err(crate::Error::Dispatch {
                detail: format!("cross-shard send queue full for target node {target_node}"),
            });
        }

        Ok(())
    }

    /// Return the procedural session's identity for use when dispatching SQL extensions.
    fn identity_for_dispatch(&self) -> crate::control::security::identity::AuthenticatedIdentity {
        self.identity.clone()
    }

    // ── Transaction control ─────────────────────────────────────────────

    pub(super) async fn execute_commit(&self) -> crate::Result<()> {
        self.flush_transaction_buffer().await
    }

    pub(super) fn execute_rollback(&self) -> crate::Result<()> {
        self.with_tx_ctx("ROLLBACK", |ctx| {
            ctx.rollback();
            Ok(())
        })
    }

    pub(super) fn execute_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("SAVEPOINT", |ctx| {
            ctx.savepoint(name);
            Ok(())
        })
    }

    pub(super) fn execute_rollback_to(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("ROLLBACK TO", |ctx| ctx.rollback_to(name))
    }

    pub(super) fn execute_release_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("RELEASE SAVEPOINT", |ctx| ctx.release_savepoint(name))
    }

    fn with_tx_ctx(
        &self,
        stmt_name: &str,
        f: impl FnOnce(&mut ProcedureTransactionCtx) -> crate::Result<()>,
    ) -> crate::Result<()> {
        match self.tx_ctx {
            Some(ref tx_ctx) => {
                let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
                f(&mut guard)
            }
            None => Err(crate::Error::BadRequest {
                detail: format!("{stmt_name} is only valid inside stored procedures"),
            }),
        }
    }

    /// Flush the procedure transaction buffer: WAL append + dispatch as batch.
    pub(super) async fn flush_transaction_buffer(&self) -> crate::Result<()> {
        let (tasks, _lease_scopes) = if let Some(ref tx_ctx) = self.tx_ctx {
            let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            guard.take_buffered()
        } else {
            return Ok(());
        };

        // `_lease_scopes` owns every statement's descriptor admission through
        // all WAL appends and the complete batch dispatch below. It is dropped
        // only after this function returns, including on an execution error.
        if tasks.is_empty() {
            return Ok(());
        }

        // Each task's WAL record has its own LSN; the batch dispatch below
        // carries the highest so the Data Plane's write-version floor advances
        // past every write it applies. Same approximation for the resolved TTL
        // instant: a single scalar can't represent one-per-task resolved
        // instants for a heterogeneous multi-statement batch, so it is only
        // threaded through when the buffer holds exactly one task (below);
        // resolving that properly for N>1 would need `MetaOp::TransactionBatch`
        // to carry a per-plan `Vec<Option<u64>>`, a separate, wider change to
        // the procedural batch-flush path, not this KV-write fix.
        let mut max_wal_lsn: Option<crate::types::Lsn> = None;
        let mut single_task_resolved_now_ms: Option<u64> = None;
        for task in &tasks {
            let outcome = crate::control::server::wal_dispatch::wal_append_if_write(
                &self.state.wal,
                task.tenant_id,
                task.vshard_id,
                task.database_id,
                &task.plan,
            )?;
            if let Some(lsn) = outcome.lsn {
                max_wal_lsn = Some(max_wal_lsn.map_or(lsn, |cur| cur.max(lsn)));
            }
            single_task_resolved_now_ms = outcome.resolved_now_ms;
        }

        if tasks.len() == 1 {
            if let Some(task) = tasks.into_iter().next() {
                crate::control::server::dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
                    self.state,
                    crate::control::server::dispatch_utils::WriteDispatch {
                        tenant_id: task.tenant_id,
                        database_id: task.database_id,
                        vshard_id: task.vshard_id,
                        plan: task.plan,
                        trace_id: TraceId::ZERO,
                        event_source: self.event_source,
                        txn_id: None,
                        wal_lsn: max_wal_lsn,
                        resolved_now_ms: single_task_resolved_now_ms,
                    },
                )
                .await?;
            }
        } else {
            let tenant_id = tasks[0].tenant_id;
            let database_id = tasks[0].database_id;
            let vshard_id = tasks[0].vshard_id;
            let plans: Vec<_> = tasks.into_iter().map(|t| t.plan).collect();
            let batch_plan = crate::bridge::envelope::PhysicalPlan::Meta(
                nodedb_physical::physical_plan::MetaOp::TransactionBatch {
                    plans,
                    txn_id: None,
                },
            );
            crate::control::server::dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
                self.state,
                crate::control::server::dispatch_utils::WriteDispatch {
                    tenant_id,
                    database_id,
                    vshard_id,
                    plan: batch_plan,
                    trace_id: TraceId::ZERO,
                    event_source: self.event_source,
                    txn_id: None,
                    wal_lsn: max_wal_lsn,
                    // N>1 batch: no single instant represents every task's
                    // resolved TTL — see the comment above the WAL-append loop.
                    resolved_now_ms: None,
                },
            )
            .await?;
        }

        Ok(())
    }
}

/// Deterministic coverage for the cross-shard trigger ORIGINATION logic
/// (the `execute_sql` routing branch above). A full-cluster e2e test cannot
/// cover this: that harness cannot place different vShards' Raft leadership
/// on different nodes, so a same-node "remote" route never arises there.
/// Here the routing table is built directly, so both `Local` and `Remote`
/// decisions are reachable without a cluster.
///
/// The send/receive path (dispatcher retry/DLQ/HWM-dedup, wire
/// serialization, receiver apply) is covered separately by
/// `nodedb/tests/event_cross_shard.rs` and
/// `nodedb/src/event/cross_shard/dispatcher.rs`'s own unit tests; this
/// module only proves the origination gate in `execute_sql`.
#[cfg(test)]
mod cross_shard_origination_tests {
    use std::sync::{Arc, RwLock};

    use nodedb_cluster::RoutingTable;
    use nodedb_types::DatabaseId;

    use crate::control::planner::procedural::executor::bindings::RowBindings;
    use crate::control::planner::procedural::executor::core::{
        CrossShardOrigin, StatementExecutor,
    };
    use crate::control::security::identity::AuthenticatedIdentity;
    use crate::control::server::shared::ddl::neutral::collection::create::handler::create_collection;
    use crate::control::server::shared::ddl::neutral::collection::create::request::CreateCollectionRequest;
    use crate::control::state::SharedState;
    use crate::event::cross_shard::{CrossShardDispatcher, CrossShardMetrics};
    use crate::types::TenantId;
    use crate::wal::WalManager;

    /// This node's id in every fixture below.
    const LOCAL_NODE: u64 = 1;
    /// The other cluster member every fixture routes remote writes to.
    const REMOTE_NODE: u64 = 2;

    fn test_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            1,
            "cross_shard_origin_test",
            TenantId::new(1),
            vec![],
            true,
            None,
            AuthenticatedIdentity::default_database_set(true),
        )
    }

    /// Build a `SharedState` wired for cross-shard trigger origination: a
    /// 2-group routing table (data group 1 led by `LOCAL_NODE`, data group 2
    /// led by `REMOTE_NODE`) plus a live `CrossShardDispatcher`. Under
    /// `RoutingTable::uniform`, even vShards map to group 1 (local) and odd
    /// vShards map to group 2 (remote). Also creates `coll_name` as a
    /// `document_strict` collection with `id TEXT PRIMARY KEY` so `INSERT`
    /// plans against it.
    async fn build_state_with_collection(
        dir: &tempfile::TempDir,
        coll_name: &str,
    ) -> Arc<SharedState> {
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 16);
        let mut state = SharedState::new(dispatcher, wal).unwrap();

        {
            let s = Arc::get_mut(&mut state)
                .expect("sole owner: no clone has been taken yet in this fixture");
            s.node_id = LOCAL_NODE;
            let routing = RoutingTable::uniform(2, &[LOCAL_NODE, REMOTE_NODE], 1);
            s.cluster_routing = Some(Arc::new(RwLock::new(routing)));
            s.cross_shard_dispatcher = Some(Arc::new(CrossShardDispatcher::new(
                LOCAL_NODE,
                Arc::new(CrossShardMetrics::new()),
            )));
        }

        let identity = test_identity();
        let columns = vec![
            ("id".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("val".to_string(), "INT".to_string()),
        ];
        let req = CreateCollectionRequest {
            name: coll_name,
            engine: Some("document_strict"),
            columns: &columns,
            options: &[],
            flags: &[],
            balanced_raw: None,
        };
        create_collection(&state, &identity, &req, DatabaseId::DEFAULT)
            .await
            .unwrap_or_else(|e| panic!("create_collection({coll_name}) failed: {e:?}"));

        state
    }

    /// Build an unclustered state with a collection in an explicit database.
    /// This keeps procedural DML buffered while the test verifies planning
    /// scope, and lets procedural PUBLISH complete locally.
    async fn build_unrouted_state_with_collection(
        dir: &tempfile::TempDir,
        coll_name: &str,
        database_id: DatabaseId,
    ) -> Arc<SharedState> {
        let wal_path = dir.path().join("test-unrouted.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 16);
        let state = SharedState::new(dispatcher, wal).unwrap();
        let identity = test_identity();
        let columns = vec![
            ("id".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("val".to_string(), "INT".to_string()),
        ];
        let req = CreateCollectionRequest {
            name: coll_name,
            engine: Some("document_strict"),
            columns: &columns,
            options: &[],
            flags: &[],
            balanced_raw: None,
        };
        create_collection(&state, &identity, &req, database_id)
            .await
            .unwrap_or_else(|e| panic!("create_collection({coll_name}) failed: {e:?}"));
        state
    }

    /// Procedural PUBLISH and DML must both retain the executor's explicit
    /// database instead of falling back to `DatabaseId::DEFAULT`.
    #[tokio::test]
    async fn procedural_publish_and_dml_use_explicit_non_default_database() {
        let dir = tempfile::tempdir().unwrap();
        let database_id = DatabaseId::new(9);
        let state = build_unrouted_state_with_collection(&dir, "scoped_orders", database_id).await;
        let topic = crate::event::topic::TopicDef {
            tenant_id: 1,
            name: "scoped_events".into(),
            retention: crate::event::cdc::stream_def::RetentionConfig::default(),
            owner: "cross_shard_origin_test".into(),
            created_at: 0,
            database_id,
            last_sequence: 0,
            last_lsn: 0,
        };
        // A topic exists only once it is durable: PUBLISH revalidates the
        // catalog row under the lifecycle lock before it accepts a message,
        // so registering the runtime definition alone is not a live topic.
        state
            .credentials
            .catalog()
            .put_ep_topic(&topic)
            .expect("persist topic");
        state.ep_topic_registry.register(topic);

        let executor = StatementExecutor::with_source_in_database(
            &state,
            test_identity(),
            TenantId::new(1),
            database_id,
            0,
            crate::event::EventSource::User,
        )
        .with_transaction_context();

        executor
            .execute_sql(
                "PUBLISH TO scoped_events '{\"kind\":\"created\"}'",
                &RowBindings::empty(),
            )
            .await
            .expect("PUBLISH must resolve the topic in the executor database");
        executor
            .execute_sql(
                "INSERT INTO scoped_orders (id, val) VALUES ('scoped', 1)",
                &RowBindings::empty(),
            )
            .await
            .expect("DML must plan against the executor database");
    }

    /// Find a `{prefix}_<i>` collection name whose vShard is homed on
    /// `REMOTE_NODE` under the routing table `build_state_with_collection`
    /// installs (odd vShard → data group 2 → `REMOTE_NODE`).
    fn remote_homed_name(prefix: &str) -> String {
        for i in 0..4096u32 {
            let name = format!("{prefix}_{i}");
            let vshard = nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, &name);
            if vshard % 2 == 1 {
                return name;
            }
        }
        panic!("could not find a remote-homed collection name for prefix {prefix}");
    }

    /// Find a `{prefix}_<i>` collection name whose vShard is homed on
    /// `LOCAL_NODE` (even vShard → data group 1 → `LOCAL_NODE`).
    fn local_homed_name(prefix: &str) -> String {
        for i in 0..4096u32 {
            let name = format!("{prefix}_{i}");
            let vshard = nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, &name);
            if vshard.is_multiple_of(2) {
                return name;
            }
        }
        panic!("could not find a local-homed collection name for prefix {prefix}");
    }

    /// WHEN `cross_shard_origin` is set AND the write's target vShard
    /// resolves to a REMOTE node, THEN `execute_sql` enqueues a
    /// `CrossShardWriteRequest` to that node's dispatcher queue instead of
    /// taking the local write path. Non-vacuous: reverting the `Some(origin)`
    /// branch in `execute_sql` back to always-local would make this test
    /// enqueue nothing and fail the `total_pending() == 1` assertion.
    #[tokio::test]
    async fn trigger_write_to_remote_homed_collection_enqueues_cross_shard() {
        let dir = tempfile::tempdir().unwrap();
        let tgt = remote_homed_name("cs_origin_remote");
        let state = build_state_with_collection(&dir, &tgt).await;

        let executor = StatementExecutor::with_source(
            &state,
            test_identity(),
            TenantId::new(1),
            0,
            crate::event::EventSource::Trigger,
        )
        .with_cross_shard_origin(CrossShardOrigin {
            source_lsn: 100,
            source_sequence: 7,
            source_vshard: 999,
            source_collection: "src_probe".to_string(),
        });

        let sql = format!("INSERT INTO {tgt} (id, val) VALUES ('fired', 1)");
        executor
            .execute_sql(&sql, &RowBindings::empty())
            .await
            .expect("execute_sql should enqueue the remote write, not fail");

        let dispatcher = state
            .cross_shard_dispatcher
            .as_ref()
            .expect("dispatcher configured by build_state_with_collection");
        assert_eq!(
            dispatcher.total_pending(),
            1,
            "exactly one cross-shard write must be enqueued"
        );
        assert_eq!(
            dispatcher.active_targets(),
            vec![REMOTE_NODE],
            "the write must be enqueued to the target's owning node"
        );

        let pending = dispatcher.peek_pending(REMOTE_NODE);
        assert_eq!(pending.len(), 1);
        let req = &pending[0];
        assert_eq!(req.sql, sql);
        assert_eq!(req.source_lsn, 100);
        assert_eq!(req.source_sequence, 7);
        assert_eq!(req.source_vshard, 999);
        assert_eq!(req.source_collection, "src_probe");
        assert_eq!(req.cascade_depth, 0);
        assert_eq!(
            req.target_vshard,
            nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, &tgt)
        );
    }

    /// Companion gate assertion: with the SAME `cross_shard_origin` set, a
    /// write whose target vShard resolves to `Local` (this node owns it)
    /// must NOT be enqueued to the cross-shard dispatcher — proving the gate
    /// is routing-driven, not "always enqueue when origin is set". No Data
    /// Plane core is running to drain the SPSC bridge in this fixture, so the
    /// local path's `dispatch_write_to_data_plane` await never resolves on
    /// its own; bounding it with a timeout is enough to observe that the
    /// cross-shard dispatcher was never touched before that await blocks.
    #[tokio::test]
    async fn trigger_write_to_locally_homed_collection_never_enqueues_cross_shard() {
        let dir = tempfile::tempdir().unwrap();
        let tgt = local_homed_name("cs_origin_local");
        let state = build_state_with_collection(&dir, &tgt).await;

        let executor = StatementExecutor::with_source(
            &state,
            test_identity(),
            TenantId::new(1),
            0,
            crate::event::EventSource::Trigger,
        )
        .with_cross_shard_origin(CrossShardOrigin {
            source_lsn: 1,
            source_sequence: 1,
            source_vshard: 0,
            source_collection: "src_probe".to_string(),
        });

        let sql = format!("INSERT INTO {tgt} (id, val) VALUES ('local', 1)");
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            executor.execute_sql(&sql, &RowBindings::empty()),
        )
        .await;

        let dispatcher = state
            .cross_shard_dispatcher
            .as_ref()
            .expect("dispatcher configured by build_state_with_collection");
        assert_eq!(
            dispatcher.total_pending(),
            0,
            "a Local-routed write must never be enqueued to the cross-shard dispatcher"
        );
    }
}
