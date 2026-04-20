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
pub fn dropped_collections(
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

    // Default retention (7 days). When the per-tenant / system
    // override path becomes plumbable through SharedState, swap in
    // the resolved value — for now the displayed expiry reflects the
    // default, which is the sweeper's effective behavior out of the
    // box.
    let retention = crate::config::server::RetentionSettings::default().retention_window();
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
        encoder.encode_field(&(coll.tenant_id as i64))?;
        encoder.encode_field(&coll.name.as_str())?;
        encoder.encode_field(&coll.owner.as_str())?;
        encoder.encode_field(&engine_type)?;
        encoder.encode_field(&(deactivated_ns as i64))?;
        encoder.encode_field(&(expires_ns as i64))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
