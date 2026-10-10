// SPDX-License-Identifier: BUSL-1.1

//! DML dispatch and transaction control for the statement executor.

use super::super::transaction::{ProcedureTransactionCtx, RemoteWrite};
use super::StatementExecutor;
use super::route::StatementRoute;
use super::sql_literal_concat::fold_literal_string_concat;
use crate::control::planner::procedural::ast::SqlExpr;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::eval;
use crate::control::system_txn::{OpenSystemTxn, SystemTxnStatement};

/// Whether a body statement goes to the DDL router before the planner.
/// `INSERT` and `UPSERT` go to the planner: the router's object-literal and
/// `UPSERT INTO` forms fire the target's triggers again.
fn routes_through_ddl_router(sql: &str) -> bool {
    let head = sql.trim_start();
    let starts_with = |word: &str| {
        head.get(..word.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(word))
    };
    !(starts_with("INSERT") || starts_with("UPSERT"))
}

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

        // A NodeDB SQL extension (`PUBLISH TO`) is checked now and sent only
        // after the transaction commits, so a rolled-back body publishes
        // nothing.
        if crate::control::sql_dispatch::is_sql_extension(&bound_sql) {
            // A shipped block carries only the DML its origin routed here:
            // the origin publishes from its own commit.
            if matches!(self.body, Some(super::AtomicBody::CrossShardApply)) {
                return Err(crate::Error::BadRequest {
                    detail: "PUBLISH is not accepted in a cross-shard trigger write".into(),
                });
            }
            let publish = crate::control::sql_dispatch::prepare_publish(
                self.state,
                &self.identity_for_dispatch(),
                self.database_id,
                &bound_sql,
            )?;
            let mut guard = self.tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            return guard.buffer_publish(publish);
        }

        // DDL and the other router-owned statements run on the open
        // transaction's session, as in a client transaction: DDL buffers and
        // commits with the body's writes.
        if routes_through_ddl_router(bound_sql.as_str()) {
            let mut txn = self.txn.lock().await;
            let open = self.open_txn(&mut txn).await?;
            let txn_ctx = open.txn_ctx()?;
            if let Some(result) = crate::control::server::shared::ddl::dispatch(
                self.state,
                &self.identity,
                &bound_sql,
                self.database_id,
                &txn_ctx,
            )
            .await
            {
                return result.map(drop).map_err(crate::Error::from);
            }
        }

        // Plan with descriptor versions so admission occurs before this
        // internal path stages anything. Stored procedures are trusted
        // internal execution, so this deliberately does not add user
        // authorization beyond their existing semantics.
        //
        // Planning and lease admission run as ONE retried unit: the lease that
        // pins the planned descriptor version is acquired after the catalog
        // read, so a descriptor drain starting in between will otherwise fail
        // the whole procedure. Re-planning is pure, and admission fails closed
        // before granting anything, so an absorbed attempt reads nothing.
        let ctx = crate::control::planner::context::QueryContext::for_state(self.state);
        let ctx = &ctx;
        let bound_sql = &bound_sql;
        // A stored-procedure body is server-defined code, not a client
        // statement, and runs SECURITY DEFINER exactly as a trigger body does —
        // so it plans as the system rather than under the invoker's scope. The
        // context is built once and borrowed by the retry closure, which can run
        // it several times.
        let security = crate::control::planner::context::SystemPlanSecurity::new(
            self.tenant_id,
            "_system_procedure",
        );
        let security = &security;
        // The derived writes (implicit edges, materialized sums, period
        // locks) join the statement as they do for a client statement. The
        // plan never carries a Calvin OLLP prediction or a resolved write:
        // the planner emits neither, so every task takes the staging gate.
        // They read their source rows through the transaction this statement
        // stages into, so they see its earlier statements.
        let read_txn = self.read_txn().await?;
        let (tasks, planned, lease_scope, sum_target_reads) =
            crate::control::server::shared::retry::retry_on_schema_change(
                &self.state.lease_drain,
                move || async move {
                    let (mut tasks, _output_schema, versions, _) = ctx
                        .plan_sql_with_rls_and_versions(
                            bound_sql,
                            self.tenant_id,
                            self.database_id,
                            &security.context(self.state),
                            None,
                        )
                        .await?;
                    let planned = tasks.len();
                    let sum_target_reads =
                        crate::control::server::shared::plan_admission::append_derived_tasks(
                            self.state,
                            &mut tasks,
                            self.tenant_id,
                            self.database_id,
                            read_txn,
                            crate::types::TraceId::ZERO,
                        )
                        .await?;
                    let lease_scope = self.state.acquire_plan_lease_scope(&versions).await?;
                    Ok::<_, crate::Error>((tasks, planned, lease_scope, sum_target_reads))
                },
            )
            .await?;
        // A lease this node lost ends the procedure with a retryable error
        // before the statement stages.
        lease_scope.check_not_revoked()?;

        // A statement led by another node ships whole and waits for the local
        // COMMIT. That node plans it again and derives every write it makes,
        // on every vShard, so the statement and its derived writes commit in
        // one transaction there, or not at all. Every other statement stages
        // into the open transaction now, so the next statement sees its
        // writes. A derived write on another shard stages on that shard's
        // leader, so placement reads the statement's own tasks only.
        match self.statement_route(tasks.get(..planned).unwrap_or(&tasks))? {
            StatementRoute::Remote { vshard_id, .. } => {
                let mut guard = self.tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
                guard.buffer_remote(RemoteWrite {
                    target_vshard: vshard_id,
                    sql: bound_sql.to_string(),
                })
            }
            StatementRoute::Local => self.stage_tasks(tasks, sum_target_reads, lease_scope).await,
        }
    }

    /// Stage `tasks` and record `reads` in the open transaction, beginning it
    /// when none is open.
    async fn stage_tasks(
        &self,
        tasks: Vec<nodedb_physical::physical_task::PhysicalTask>,
        reads: Vec<crate::control::server::shared::session::read_set::ReadSetEntry>,
        lease_scope: crate::control::lease::QueryLeaseScope,
    ) -> crate::Result<()> {
        let mut txn = self.txn.lock().await;
        let open = self.open_txn(&mut txn).await?;
        open.record_reads(reads)?;
        open.stage(SystemTxnStatement {
            tasks,
            lease_scope: std::sync::Arc::new(lease_scope),
        })
        .await
        .map_err(crate::Error::from)
    }

    /// The id of the transaction the next statement stages into: the open
    /// one, else the joined statement's. `None` when neither exists yet, so
    /// the transaction the statement opens has staged nothing.
    async fn read_txn(&self) -> crate::Result<Option<crate::types::TxnId>> {
        let txn = self.txn.lock().await;
        if let Some(open) = txn.as_ref() {
            let ctx = open.txn_ctx()?;
            return Ok(ctx.sessions.tx_id(ctx.session_id));
        }
        Ok(self
            .joined
            .and_then(|ctx| ctx.sessions.tx_id(ctx.session_id)))
    }

    /// The open transaction, begun now when none is open. A joined body
    /// joins its statement's transaction instead.
    async fn open_txn<'g>(
        &self,
        txn: &'g mut Option<OpenSystemTxn<'a>>,
    ) -> crate::Result<&'g OpenSystemTxn<'a>> {
        if txn.is_none() {
            let open = match self.joined {
                Some(ctx) => {
                    OpenSystemTxn::join(self.state, self.identity.clone(), ctx, self.tenant_id)
                        .await?
                }
                None => {
                    let mut open =
                        OpenSystemTxn::begin(self.state, self.identity.clone(), self.event_source)?;
                    if let Some((key, target_vshard)) = self.commit_key() {
                        open.set_applied_key(key, target_vshard);
                    }
                    open
                }
            };
            *txn = Some(open);
        }
        txn.as_ref().ok_or(crate::Error::Internal {
            detail: "the procedural transaction did not open".into(),
        })
    }

    /// Return the procedural session's identity for use when dispatching SQL extensions.
    fn identity_for_dispatch(&self) -> crate::control::security::identity::AuthenticatedIdentity {
        self.identity.clone()
    }

    // ── Transaction control ─────────────────────────────────────────────

    pub(super) async fn execute_commit(&self) -> crate::Result<()> {
        self.refuse_in_atomic_body("COMMIT")?;
        self.flush_transaction_buffer().await
    }

    pub(super) async fn execute_rollback(&self) -> crate::Result<()> {
        self.refuse_in_atomic_body("ROLLBACK")?;
        self.discard_transaction_buffer().await;
        Ok(())
    }

    pub(super) async fn execute_savepoint(&self, name: &str) -> crate::Result<()> {
        let mut txn = self.txn.lock().await;
        self.open_txn(&mut txn)
            .await?
            .savepoint(self.tenant_id, name)
            .await?;
        self.with_tx_ctx(|ctx| {
            ctx.savepoint(name);
            Ok(())
        })
    }

    pub(super) async fn execute_rollback_to(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx(|ctx| ctx.rollback_to(name))?;
        let txn = self.txn.lock().await;
        match txn.as_ref() {
            Some(open) => open.rollback_to(self.tenant_id, name).await,
            None => Err(crate::Error::BadRequest {
                detail: format!("savepoint '{name}' does not exist"),
            }),
        }
    }

    pub(super) async fn execute_release_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx(|ctx| ctx.release_savepoint(name))?;
        let txn = self.txn.lock().await;
        match txn.as_ref() {
            Some(open) => open.release(name),
            None => Err(crate::Error::BadRequest {
                detail: format!("savepoint '{name}' does not exist"),
            }),
        }
    }

    /// A server-run body commits once, at its end, so it refuses statements
    /// that end its transaction early.
    fn refuse_in_atomic_body(&self, statement: &str) -> crate::Result<()> {
        match self.body {
            Some(ref body) => Err(body.refuse_transaction_control(statement)),
            None => Ok(()),
        }
    }

    fn with_tx_ctx(
        &self,
        f: impl FnOnce(&mut ProcedureTransactionCtx) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let mut guard = self.tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut guard)
    }

    /// Roll back the open transaction and drop its held effects.
    pub(super) async fn discard_transaction_buffer(&self) {
        {
            let mut guard = self.tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            guard.rollback();
        }
        let open = self.txn.lock().await.take();
        if let Some(open) = open {
            open.rollback().await;
        }
    }

    /// Commit the open transaction: every staged and buffered write lands in
    /// one redo record that installs all of them or none. Restart replay
    /// installs that same record. Each statement's descriptor leases stay on
    /// the tasks it buffered until COMMIT has checked them.
    ///
    /// Held publishes commit in a redo record: a joined body's in its
    /// statement's, every other body's in this one, so a publish and its
    /// transaction commit together. Held cross-node writes are queued once
    /// the commit succeeds, and a queueing error leaves a durable retry
    /// record. All of them drop with the commit when it fails.
    pub(super) async fn flush_transaction_buffer(&self) -> crate::Result<()> {
        let mut effects = {
            let mut guard = self.tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            guard.take_effects()
        };
        if let Some(ctx) = self.joined {
            let open = self.txn.lock().await.take();
            if let Some(open) = open {
                open.commit().await.map_err(crate::Error::from)?;
            }
            return self.defer_to_statement(ctx, effects);
        }
        // The body's messages and its cross-node requests commit in its redo
        // record, with its writes.
        let mut publishes = self.redo_publishes(std::mem::take(&mut effects.publishes));
        publishes.extend(self.outbox_messages(std::mem::take(&mut effects.remote))?);
        if !publishes.is_empty() {
            let mut txn = self.txn.lock().await;
            self.open_txn(&mut txn).await?.hold_publishes(publishes)?;
        }
        let open = self.txn.lock().await.take();
        if let Some(open) = open {
            open.commit().await.map_err(crate::Error::from)?;
        }
        Ok(())
    }
}

/// A trigger body's local writes on a one-node cluster: staging, rollback,
/// transaction control, and committed publishes. The cluster applies its
/// Raft entries through a fake Data-Plane core, so tests observe what is
/// staged, WAL-appended and queued at each statement.
///
/// The cross-node half of origination runs on the multi-node cluster harness
/// (`trigger_cross_shard_origination` and `trigger_body_atomic_cross_node`
/// in the cluster test suite), where a real remote node exists. The
/// send/receive path (dispatcher retry/DLQ, dedup, wire serialization,
/// receiver apply) is covered by `nodedb/tests/inproc/cases/event_cross_shard.rs`
/// and the `event::cross_shard` unit tests. Read-your-own-writes against a
/// real Data Plane is covered by `nodedb/tests/wire/cases/procedural_txn_visibility.rs`.
#[cfg(test)]
mod origination_tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use nodedb_physical::physical_plan::MetaOp;
    use nodedb_types::DatabaseId;

    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide};
    use crate::bridge::envelope::{ErrorCode, Payload, PhysicalPlan, Response, Status};
    use crate::control::cluster::test_one_node::{OneNodeCluster, boot_with_core};
    use crate::control::planner::procedural::executor::bindings::RowBindings;
    use crate::control::planner::procedural::executor::core::{
        AtomicBody, CrossShardOrigin, StatementExecutor,
    };
    use crate::control::security::identity::AuthenticatedIdentity;
    use crate::control::server::shared::ddl::neutral::collection::create::handler::create_collection;
    use crate::control::server::shared::ddl::neutral::collection::create::request::CreateCollectionRequest;
    use crate::control::state::SharedState;
    use crate::types::{Lsn, TenantId};

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

    /// Create `name` as a `document_strict` collection with
    /// `id TEXT PRIMARY KEY, val INT` in `database_id`.
    async fn create_strict(state: &Arc<SharedState>, name: &str, database_id: DatabaseId) {
        let columns = vec![
            ("id".to_string(), "TEXT PRIMARY KEY".to_string()),
            ("val".to_string(), "INT".to_string()),
        ];
        let req = CreateCollectionRequest {
            name,
            engine: Some("document_strict"),
            columns: &columns,
            options: &[],
            flags: &[],
            balanced_raw: None,
        };
        create_collection(state, &test_identity(), &req, database_id)
            .await
            .unwrap_or_else(|e| panic!("create_collection({name}) failed: {e:?}"));
    }

    /// How the fake core answers everything that is not a staged write.
    #[derive(Clone, Copy)]
    enum OtherRequests {
        Succeed,
        Fail,
    }

    /// A fake Data-Plane core the one-node cluster applies its Raft entries
    /// through. A staged write answers with one affected row. It records the
    /// staged writes and overlay releases it sees, in order.
    #[derive(Clone)]
    struct FakeCore {
        seen: Arc<Mutex<Vec<&'static str>>>,
        fail_other: Arc<AtomicBool>,
    }

    impl FakeCore {
        fn new() -> Self {
            Self {
                seen: Arc::new(Mutex::new(Vec::new())),
                fail_other: Arc::new(AtomicBool::new(false)),
            }
        }

        fn seen(&self) -> Vec<&'static str> {
            self.seen.lock().unwrap_or_else(|p| p.into_inner()).clone()
        }

        /// Answer every later request that is not a staged write as `other`.
        fn answer_other(&self, other: OtherRequests) {
            self.fail_other
                .store(matches!(other, OtherRequests::Fail), Ordering::Relaxed);
        }

        fn spawn(
            &self,
            state: Arc<SharedState>,
            side: CoreChannelDataSide,
        ) -> tokio::task::JoinHandle<()> {
            tokio::spawn(answer(self.clone(), state, side))
        }
    }

    async fn answer(core: FakeCore, state: Arc<SharedState>, mut side: CoreChannelDataSide) {
        loop {
            while let Ok(request) = side.request_rx.try_pop() {
                let request = request.inner;
                let kind = match &request.plan {
                    PhysicalPlan::Meta(MetaOp::StageWrite { .. }) => Some("stage"),
                    PhysicalPlan::Meta(MetaOp::DropTxnOverlay { .. }) => Some("drop_overlay"),
                    _ => None,
                };
                if let Some(kind) = kind {
                    core.seen
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .push(kind);
                }
                let (status, payload, error_code) = match kind {
                    Some("stage") => (
                        Status::Ok,
                        crate::data::executor::response_codec::encode_count("affected", 1)
                            .expect("count payload"),
                        None,
                    ),
                    _ if core.fail_other.load(Ordering::Relaxed) => (
                        Status::Error,
                        Vec::new(),
                        Some(Box::new(ErrorCode::Internal {
                            detail: "fake core refuses".into(),
                        })),
                    ),
                    _ => (Status::Ok, Vec::new(), None),
                };
                let response = Response {
                    request_id: request.request_id,
                    status,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(payload),
                    watermark_lsn: Lsn::ZERO,
                    error_code,
                    stage_vote: None,
                    read_version_lsn: Lsn::ZERO,
                    write_set: Vec::new(),
                };
                side.response_tx
                    .try_push(BridgeResponse { inner: response })
                    .expect("fake core response queue has capacity");
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
    }

    /// A one-node cluster whose Data Plane is `core`, with every name in
    /// `collections` created in the default database.
    async fn node_with_collections(core: &FakeCore, collections: &[&str]) -> OneNodeCluster {
        let spawner = core.clone();
        let cluster = boot_with_core(|_| {}, move |state, side| spawner.spawn(state, side)).await;
        for name in collections {
            create_strict(&cluster.state, name, DatabaseId::DEFAULT).await;
        }
        cluster
    }

    /// The WAL's data records at or above `from`: row writes and committed
    /// transaction redo.
    fn data_records_since(state: &SharedState, from: Lsn) -> usize {
        use nodedb_wal::record::RecordType;
        state.wal.sync().expect("sync wal");
        state
            .wal
            .replay()
            .expect("read wal")
            .into_iter()
            .filter(|record| record.header.lsn >= from.as_u64())
            .filter(|record| {
                matches!(
                    RecordType::from_raw(record.logical_record_type()),
                    Some(RecordType::Put | RecordType::Delete | RecordType::TransactionRedo)
                )
            })
            .count()
    }

    /// Register `name` as a durable topic in `database_id`.
    fn register_topic(state: &SharedState, name: &str, database_id: DatabaseId) {
        let topic = crate::event::topic::TopicDef {
            tenant_id: 1,
            name: name.into(),
            retention: crate::event::cdc::stream_def::RetentionConfig::default(),
            owner: "cross_shard_origin_test".into(),
            created_at: 0,
            database_id,
            last_sequence: 0,
            last_lsn: 0,
            last_epoch: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
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
    }

    /// A trigger-body executor carrying a source-write origin.
    fn trigger_executor(state: &SharedState) -> StatementExecutor<'_> {
        StatementExecutor::with_source(
            state,
            test_identity(),
            TenantId::new(1),
            0,
            crate::event::EventSource::Trigger,
        )
        .with_atomic_body(AtomicBody::trigger("probe"))
        .with_cross_shard_origin(CrossShardOrigin {
            source_lsn: 100,
            source_sequence: 7,
            source_vshard: 999,
            source_collection: "src_probe".to_string(),
        })
    }

    fn parse(sql: &str) -> crate::control::planner::procedural::ast::ProceduralBlock {
        crate::control::planner::procedural::parse_block(sql)
            .unwrap_or_else(|e| panic!("parse {sql}: {e}"))
    }

    /// Writes this node's cross-shard dispatcher holds for another node. A
    /// one-node cluster homes every collection here, so it never holds one.
    fn pending(state: &SharedState) -> usize {
        state
            .cross_shard_dispatcher
            .as_ref()
            .expect("the cluster wiring installs the dispatcher")
            .total_pending()
    }

    /// PUBLISH and DML both keep the executor's explicit database instead of
    /// falling back to `DatabaseId::DEFAULT`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn procedural_publish_and_dml_use_explicit_non_default_database() {
        let core = FakeCore::new();
        let cluster = node_with_collections(&core, &[]).await;
        let state = &cluster.state;
        let database_id = DatabaseId::new(9);
        let mut database = crate::control::security::catalog::DatabaseDescriptor::default_db();
        database.id = database_id;
        database.name = "scoped_database".into();
        state
            .credentials
            .catalog()
            .put_database(&database)
            .expect("add the scoped database");
        create_strict(state, "scoped_orders", database_id).await;
        register_topic(state, "scoped_events", database_id);

        let executor = StatementExecutor::with_source_in_database(
            state,
            test_identity(),
            TenantId::new(1),
            database_id,
            0,
            crate::event::EventSource::User,
        );

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
            .expect("DML must plan and stage against the executor database");
        executor.discard_transaction_buffer().await;
        drop(executor);
        cluster.shutdown().await;
    }

    /// A local write stages into the overlay at its statement, before the next
    /// statement plans. Nothing reaches the WAL until COMMIT.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_write_stages_at_its_statement() {
        let core = FakeCore::new();
        let cluster = node_with_collections(&core, &["cs_stage_local"]).await;
        let state = &cluster.state;
        let lsn_before = state.wal.next_lsn();
        let executor = trigger_executor(state);

        executor
            .execute_sql(
                "INSERT INTO cs_stage_local (id, val) VALUES ('local', 1)",
                &RowBindings::empty(),
            )
            .await
            .expect("the local write stages");

        assert_eq!(
            core.seen(),
            vec!["stage"],
            "staged before the statement returns"
        );
        assert_eq!(
            data_records_since(state, lsn_before),
            0,
            "nothing is WAL-appended"
        );
        assert_eq!(pending(state), 0, "a local write is never queued remotely");
        executor.discard_transaction_buffer().await;
        drop(executor);
        cluster.shutdown().await;
    }

    /// An executor dropped with its transaction open (its future cancelled)
    /// releases the staging overlay on a spawned rollback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_open_transaction_releases_its_overlay() {
        let core = FakeCore::new();
        // The spawned rollback takes its owned handle to the state through
        // the node's gateway.
        let cluster = node_with_collections(&core, &["cs_drop_local"]).await;
        let state = &cluster.state;
        let tgt = "cs_drop_local";

        let executor = trigger_executor(state);
        executor
            .execute_sql(
                &format!("INSERT INTO {tgt} (id, val) VALUES ('dropped', 1)"),
                &RowBindings::empty(),
            )
            .await
            .expect("the local write stages");
        drop(executor);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !core.seen().contains(&"drop_overlay") {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the dropped transaction never released its overlay: {:?}",
                core.seen()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cluster.shutdown().await;
    }

    /// A body whose local COMMIT fails reports the refusal and queues
    /// nothing for another node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_local_commit_fails_the_body() {
        let core = FakeCore::new();
        let cluster = node_with_collections(&core, &["cs_wait_local"]).await;
        let state = &cluster.state;
        let executor = trigger_executor(state);
        executor
            .execute_sql(
                "INSERT INTO cs_wait_local (id, val) VALUES ('l', 1)",
                &RowBindings::empty(),
            )
            .await
            .expect("the local write stages");

        core.answer_other(OtherRequests::Fail);
        let committed =
            tokio::time::timeout(Duration::from_secs(5), executor.flush_transaction_buffer())
                .await
                .expect("the refused COMMIT returns");
        core.answer_other(OtherRequests::Succeed);
        assert!(committed.is_err(), "the fake core refuses the COMMIT");
        assert_eq!(pending(state), 0, "a refused body queues nothing");
        drop(executor);
        cluster.shutdown().await;
    }

    /// A body whose second statement fails WAL-appends nothing and rolls its
    /// staged write back out of the overlay.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_body_leaves_no_local_write() {
        let core = FakeCore::new();
        let cluster = node_with_collections(&core, &["cs_fail_local"]).await;
        let state = &cluster.state;
        let lsn_before = state.wal.next_lsn();

        let executor = trigger_executor(state);
        let block = parse(
            "BEGIN INSERT INTO cs_fail_local (id, val) VALUES ('a', 1); \
             INSERT INTO cs_fail_missing (id, val) VALUES ('b', 2); END",
        );
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            executor.execute_block(&block, &RowBindings::empty()),
        )
        .await
        .expect("a failed body returns");
        assert!(
            result.is_err(),
            "the second statement's collection is missing"
        );

        assert_eq!(
            data_records_since(state, lsn_before),
            0,
            "nothing is WAL-appended"
        );
        let seen = core.seen();
        assert_eq!(
            seen.first(),
            Some(&"stage"),
            "the first write staged: {seen:?}"
        );
        assert!(
            seen.contains(&"drop_overlay"),
            "the failed body releases its overlay: {seen:?}"
        );
        assert_eq!(pending(state), 0);
        drop(executor);
        cluster.shutdown().await;
    }

    /// COMMIT and ROLLBACK are refused inside a trigger body. SAVEPOINT is
    /// allowed, and a stored procedure still accepts COMMIT.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_control_is_refused_in_trigger_bodies() {
        let core = FakeCore::new();
        let cluster = node_with_collections(&core, &[]).await;
        let state = &cluster.state;

        for statement in ["COMMIT", "ROLLBACK"] {
            let result = trigger_executor(state)
                .execute_block(
                    &parse(&format!("BEGIN {statement}; END")),
                    &RowBindings::empty(),
                )
                .await;
            assert!(
                matches!(result, Err(crate::Error::NotInTransactionBlock { .. })),
                "{statement} in a trigger body must be refused, got {result:?}"
            );
        }

        let savepoints = trigger_executor(state);
        savepoints
            .execute_savepoint("sp1")
            .await
            .expect("SAVEPOINT is allowed in a trigger body");
        savepoints
            .execute_rollback_to("sp1")
            .await
            .expect("ROLLBACK TO is allowed in a trigger body");
        savepoints
            .execute_release_savepoint("sp1")
            .await
            .expect("RELEASE SAVEPOINT is allowed in a trigger body");
        savepoints.discard_transaction_buffer().await;
        drop(savepoints);

        StatementExecutor::with_source(
            state,
            test_identity(),
            TenantId::new(1),
            0,
            crate::event::EventSource::User,
        )
        .execute_block(&parse("BEGIN COMMIT; END"), &RowBindings::empty())
        .await
        .expect("a stored procedure accepts COMMIT");
        cluster.shutdown().await;
    }

    /// A shipped block never carries PUBLISH: its origin publishes from its
    /// own commit. One that does is refused before anything applies, so the
    /// receiver never holds a publish it owes after its commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shipped_block_refuses_publish() {
        let core = FakeCore::new();
        let cluster = node_with_collections(&core, &["shipped_rows"]).await;
        let state = &cluster.state;
        register_topic(state, "shipped_events", DatabaseId::DEFAULT);

        let result = StatementExecutor::with_source(
            state,
            test_identity(),
            TenantId::new(1),
            1,
            crate::event::EventSource::Trigger,
        )
        .with_atomic_body(AtomicBody::CrossShardApply)
        .execute_block(
            &parse("BEGIN PUBLISH TO shipped_events 'x'; END"),
            &RowBindings::empty(),
        )
        .await;
        assert!(
            matches!(result, Err(crate::Error::BadRequest { .. })),
            "{result:?}"
        );
        cluster.shutdown().await;
    }

    /// A PUBLISH in a body that fails is never sent. The same PUBLISH in a
    /// body that succeeds is sent once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publish_in_a_failed_body_is_never_sent() {
        let core = FakeCore::new();
        // One node homes every collection, so the body commits on this
        // node's own apply.
        let cluster = node_with_collections(&core, &["body_rows"]).await;
        let state = &cluster.state;
        register_topic(state, "body_events", DatabaseId::DEFAULT);
        let mut messages = state
            .ep_topic_registry
            .sender(DatabaseId::DEFAULT, 1, "body_events")
            .expect("topic sender")
            .subscribe();

        let failed = trigger_executor(state)
            .execute_block(
                &parse("BEGIN PUBLISH TO body_events 'rolled back'; RAISE EXCEPTION 'boom'; END"),
                &RowBindings::empty(),
            )
            .await;
        assert!(failed.is_err(), "the body raises");
        assert!(
            committed_publishes(state).is_empty(),
            "a rolled-back body commits no message"
        );

        trigger_executor(state)
            .execute_block(
                &parse("BEGIN PUBLISH TO body_events 'committed'; END"),
                &RowBindings::empty(),
            )
            .await
            .expect("the body commits");
        let committed = committed_publishes(state);
        assert_eq!(committed.len(), 1, "the message commits once");
        assert_eq!(committed[0].topic, "body_events");
        assert_eq!(committed[0].payload, "committed");
        assert!(
            messages.try_recv().is_err(),
            "the body sends nothing itself: the Event Plane delivers the committed message"
        );
        drop(messages);
        cluster.shutdown().await;
    }

    /// Every `PUBLISH TO` message the node's WAL holds in a committed redo
    /// record, in WAL order.
    fn committed_publishes(state: &SharedState) -> Vec<crate::wal::RedoPublish> {
        state.wal.sync().expect("sync wal");
        state
            .wal
            .replay()
            .expect("read wal")
            .into_iter()
            .filter(|record| {
                nodedb_wal::record::RecordType::from_raw(record.logical_record_type())
                    == Some(nodedb_wal::record::RecordType::TransactionRedo)
            })
            .flat_map(|record| {
                crate::wal::RedoRecord::from_bytes(&record.payload)
                    .expect("decode redo record")
                    .publishes
            })
            .collect()
    }
}
