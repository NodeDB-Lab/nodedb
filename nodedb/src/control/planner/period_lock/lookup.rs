// SPDX-License-Identifier: BUSL-1.1

//! Shared lookups every period-lock resolution path in this module uses.

use crate::control::server::surrogate_exchange::lookup_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};

/// Request scope shared by every per-row period-lock resolution call within
/// one [`resolve_period_lock_targets`](super::resolve::resolve_period_lock_targets)
/// pass — bundled so the resolver functions stay within a sane argument
/// count.
pub(super) struct PeriodLockScope<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The open transaction the statement runs in, `None` outside a
    /// transaction block. Every stored-row read goes through its staging
    /// overlay, so the period a write gates on is the row as this transaction
    /// sees it.
    pub read_txn: Option<TxnId>,
    pub trace_id: TraceId,
}

/// Resolve one period value to its reference row's surrogate, `None` when no
/// reference row names it.
///
/// `lookup_surrogate_routed`, never `assign_surrogate_routed`: a period value
/// that names no existing reference row is an unknown period, not a row to
/// mint identity for.
pub(super) async fn lookup_period_surrogate(
    state: &SharedState,
    ref_table: &str,
    period_key: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Option<nodedb_types::Surrogate>> {
    lookup_surrogate_routed(
        state,
        nodedb_types::CollectionKey::from_bare(database_id, ref_table),
        tenant_id,
        period_key.as_bytes(),
        trace_id,
    )
    .await
}

/// Strip the `"<db_id>/"` prefix a planned collection name carries, yielding
/// the catalog name a collection lookup is keyed on.
pub(super) fn strip_db_prefix(database_id: DatabaseId, qualified: &str) -> &str {
    if database_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", database_id.as_u64());
    qualified.strip_prefix(prefix.as_str()).unwrap_or(qualified)
}
