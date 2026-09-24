//! StatementExecutor struct, construction, and cross-shard/mutation state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::transaction::ProcedureTransactionCtx;

/// Maximum trigger cascade depth (trigger A fires trigger B fires trigger A).
pub const MAX_CASCADE_DEPTH: u32 = 16;

/// Source-write context propagated into a trigger body's executor so that DML
/// targeting a remote-homed collection is dispatched to the owning node via the
/// cross-shard event subsystem instead of being silently mis-written to the
/// local Data-Plane core.
///
/// Populated ONLY by the Event-Plane AFTER-trigger fire path (from the source
/// `WriteEvent`). Stored procedures and normal client SQL leave it `None`; its
/// presence is the gate that enables cross-shard routing in `execute_sql`.
#[derive(Debug, Clone)]
pub struct CrossShardOrigin {
    /// LSN of the source write that fired the trigger (target-side HWM dedup key).
    pub source_lsn: u64,
    /// Sequence number of the source write (monotonic per core/collection).
    pub source_sequence: u64,
    /// vShard that owns the source collection (dedup key on the target).
    pub source_vshard: u32,
    /// Collection whose write fired the trigger.
    pub source_collection: String,
}

/// Statement executor: steps through procedural SQL blocks with DML.
pub struct StatementExecutor<'a> {
    pub(super) state: &'a SharedState,
    pub(super) identity: AuthenticatedIdentity,
    pub(super) tenant_id: TenantId,
    /// Database scope fixed for this executor's lifetime.
    pub(super) database_id: DatabaseId,
    pub(super) cascade_depth: u32,
    pub(super) event_source: crate::event::EventSource,
    /// Arc<Mutex> required (not RefCell) because execute_statement returns `+ Send` futures.
    pub(super) new_mutations: Arc<Mutex<HashMap<String, nodedb_types::Value>>>,
    pub(super) tx_ctx: Option<Arc<Mutex<ProcedureTransactionCtx>>>,
    pub(super) out_values: Arc<Mutex<HashMap<String, nodedb_types::Value>>>,
    /// Cross-shard origin context; `Some` only in the Event-Plane trigger fire
    /// path. Gates remote-write dispatch in `execute_sql`.
    pub(super) cross_shard_origin: Option<CrossShardOrigin>,
}

/// Control flow signal from statement execution.
pub(in crate::control::planner::procedural::executor) enum Flow {
    Continue,
    Break,
    LoopContinue,
}

impl<'a> StatementExecutor<'a> {
    pub fn new(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
    ) -> Self {
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self::with_source_in_database(
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            crate::event::EventSource::User,
        )
    }

    pub fn with_source(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
        event_source: crate::event::EventSource,
    ) -> Self {
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self::with_source_in_database(
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            event_source,
        )
    }

    /// Construct an executor in an explicit database scope when the caller
    /// carries definition or event database context independent of identity.
    pub fn with_source_in_database(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        database_id: DatabaseId,
        cascade_depth: u32,
        event_source: crate::event::EventSource,
    ) -> Self {
        Self {
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            event_source,
            new_mutations: Arc::new(Mutex::new(HashMap::new())),
            tx_ctx: None,
            out_values: Arc::new(Mutex::new(HashMap::new())),
            cross_shard_origin: None,
        }
    }

    /// Enable procedure transaction context for COMMIT/ROLLBACK/SAVEPOINT.
    pub fn with_transaction_context(mut self) -> Self {
        self.tx_ctx = Some(Arc::new(Mutex::new(ProcedureTransactionCtx::new())));
        self
    }

    /// Attach cross-shard origin context (Event-Plane AFTER-trigger fire path).
    ///
    /// When set, `execute_sql` route-resolves every write task: a task homed on
    /// a remote node is dispatched to that node via the cross-shard dispatcher
    /// instead of being written to the local core.
    pub fn with_cross_shard_origin(mut self, origin: CrossShardOrigin) -> Self {
        self.cross_shard_origin = Some(origin);
        self
    }

    pub fn take_new_mutations(&self) -> HashMap<String, nodedb_types::Value> {
        let mut guard = self.new_mutations.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn take_out_values(&self) -> HashMap<String, nodedb_types::Value> {
        let mut guard = self.out_values.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn cascade_depth(&self) -> u32 {
        self.cascade_depth
    }
}
