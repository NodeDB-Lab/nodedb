//! `_system.dropped_collections` virtual view.
//!
//! Surfaces every soft-deleted collection that is still within its
//! retention window. Operators use this to audit what the Event-Plane
//! GC sweeper will hard-delete, and to confirm whether `UNDROP` is
//! still possible.
//!
//! Columns:
//! - `tenant_id` — numeric tenant id (as `int8`).
//! - `name` — collection name.
//! - `owner` — preserved owner username.
//! - `engine_type` — storage engine slug (`"document"`, `"strict"`,
//!   `"columnar"`, `"timeseries"`, `"columnar:spatial"`, `"kv"`),
//!   resolved from `StoredCollection.collection_type.as_str()`.
//! - `deactivated_at_ns` — HLC wall-clock nanoseconds when
//!   `is_active` flipped to false (from `StoredCollection.modification_hlc`).
//! - `retention_expires_at_ns` — same unit, wall-clock ns.
//!
//! Column names match the field names on `nodedb_types::DroppedCollection`
//! so the `NodeDb::list_dropped_collections` client trait decoder can
//! round-trip rows without an alias mapping.
//!
//! Visibility: tenant_admin and superuser see every tenant's entries;
//! regular users see only their own tenant. Enforced in-handler to
//! avoid leaking cross-tenant names.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::server::pgwire::types::{int8_field, text_field};
use crate::control::state::SharedState;

/// Row generator for `_system.dropped_collections`.
pub async fn dropped_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> PgWireResult<Vec<Response>> {
    let schema = Arc::new(vec![
        int8_field("tenant_id"),
        text_field("name"),
        text_field("owner"),
        text_field("engine_type"),
        int8_field("deactivated_at_ns"),
        int8_field("retention_expires_at_ns"),
        int8_field("size_bytes_estimate"),
    ]);

    let Some(catalog) = state.credentials.catalog() else {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::iter(Vec::<Result<_, pgwire::error::PgWireError>>::new()),
        ))]);
    };

    let dropped = catalog
        .load_dropped_collections()
        .map_err(|e| pgwire::error::PgWireError::ApiError(Box::new(e)))?;

    // Live retention window — reads the same cell the GC sweeper
    // reads on each tick, so the displayed expiry matches the actual
    // sweeper behavior after `ALTER SYSTEM SET
    // deactivated_collection_retention_days = ...`.
    let retention = state
        .retention_settings
        .read()
        .map(|r| r.retention_window())
        .unwrap_or_else(|_| crate::config::server::RetentionSettings::default().retention_window());
    let retention_ns = retention.as_nanos() as u64;

    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);
    let caller_tenant = identity.tenant_id.as_u32();

    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(schema.clone());
    for coll in &dropped {
        if !is_admin && coll.tenant_id != caller_tenant {
            continue;
        }
        let deactivated_ns = coll.modification_hlc.wall_ns;
        let expires_ns = deactivated_ns.saturating_add(retention_ns);
        let engine_type = coll.collection_type.as_str();

        // Size estimate: prefer the cached value on the catalog row
        // if present; otherwise dispatch `MetaOp::QueryCollectionSize`
        // to core 0 for a live one-core estimate. Per-row dispatch is
        // acceptable — soft-deleted collection counts are always
        // small, and the dispatch is bounded by a 500ms timeout.
        let size_estimate = if coll.size_bytes_estimate > 0 {
            coll.size_bytes_estimate
        } else {
            query_collection_size(state, coll.tenant_id, &coll.name)
                .await
                .unwrap_or(0)
        };

        encoder.encode_field(&(coll.tenant_id as i64))?;
        encoder.encode_field(&coll.name.as_str())?;
        encoder.encode_field(&coll.owner.as_str())?;
        encoder.encode_field(&engine_type)?;
        encoder.encode_field(&(deactivated_ns as i64))?;
        encoder.encode_field(&(expires_ns as i64))?;
        encoder.encode_field(&(size_estimate as i64))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Dispatch `MetaOp::QueryCollectionSize` to core 0 and return the
/// byte-sum response. Returns `None` on dispatch or timeout errors;
/// the caller falls back to 0 so the view always renders.
async fn query_collection_size(
    state: &SharedState,
    tenant_id: u32,
    collection: &str,
) -> Option<u64> {
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::bridge::physical_plan::MetaOp;
    use crate::types::{ReadConsistency, TenantId, VShardId};

    let request_id = state.next_request_id();
    let timeout = std::time::Duration::from_millis(500);

    let request = Request {
        request_id,
        tenant_id: TenantId::new(tenant_id),
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Meta(MetaOp::QueryCollectionSize {
            tenant_id,
            name: collection.to_string(),
        }),
        deadline: std::time::Instant::now() + timeout,
        priority: Priority::Background,
        trace_id: 0,
        consistency: ReadConsistency::Eventual,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
    };
    let rx = state.tracker.register_oneshot(request_id);
    {
        let mut d = state.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        if d.dispatch_to_core(0, request).is_err() {
            state.tracker.cancel(&request_id);
            return None;
        }
    }
    let resp = tokio::time::timeout(timeout, rx).await.ok()?.ok()?;
    if resp.status != Status::Ok {
        return None;
    }
    let bytes = resp.payload.as_ref();
    if bytes.len() < 8 {
        return None;
    }
    Some(u64::from_le_bytes(bytes[..8].try_into().ok()?))
}
