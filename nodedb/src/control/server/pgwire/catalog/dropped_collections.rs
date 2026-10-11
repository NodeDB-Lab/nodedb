// SPDX-License-Identifier: BUSL-1.1

//! `_system.dropped_collections` row producer.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::tables::collections::encode_row;

pub async fn dropped_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let catalog = state.credentials.catalog();

    let dropped = catalog
        .load_dropped_collections(DatabaseId::DEFAULT)
        .map_err(|e| crate::Error::Storage {
            engine: "catalog".to_string(),
            detail: e.to_string(),
        })?;

    let retention = state
        .retention_settings
        .read()
        .map(|r| r.retention_window())
        .unwrap_or_else(|_| crate::config::server::RetentionSettings::default().retention_window());
    let retention_ns = retention.as_nanos() as u64;

    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);
    let caller_tenant = identity.tenant_id.as_u64();

    let mut rows = Vec::new();
    for coll in &dropped {
        if !is_admin && coll.tenant_id != caller_tenant {
            continue;
        }
        // `deactivated_at_ns == 0` is the field's own documented "unknown"
        // sentinel (pre-upgrade row, or not yet adopted by the GC sweeper's
        // `AdoptDeactivationTime` path) — surface both columns as `0` rather
        // than deriving a misleadingly-near expiry from `0 + retention`.
        let deactivated_ns = coll.deactivated_at_ns;
        let expires_ns = if deactivated_ns == 0 {
            0
        } else {
            deactivated_ns.saturating_add(retention_ns)
        };
        let engine_type = coll.collection_type.as_str().to_string();

        let size_estimate = if coll.size_bytes_estimate > 0 {
            coll.size_bytes_estimate
        } else {
            query_collection_size(state, coll.tenant_id, &coll.name)
                .await
                .unwrap_or(0)
        };

        let mut r: HashMap<String, Value> = HashMap::with_capacity(8);
        r.insert("tenant_id".into(), Value::Integer(coll.tenant_id as i64));
        r.insert("name".into(), Value::String(coll.name.clone()));
        r.insert("owner".into(), Value::String(coll.owner.clone()));
        r.insert("engine_type".into(), Value::String(engine_type));
        r.insert(
            "deactivated_at_ns".into(),
            Value::Integer(deactivated_ns as i64),
        );
        r.insert(
            "retention_expires_at_ns".into(),
            Value::Integer(expires_ns as i64),
        );
        r.insert(
            "size_bytes_estimate".into(),
            Value::Integer(size_estimate as i64),
        );
        r.insert(
            "partition_strategy".into(),
            Value::String(coll.partition_strategy.as_str().into()),
        );
        rows.push(encode_row(r)?);
    }
    Ok(rows)
}

async fn query_collection_size(
    state: &SharedState,
    tenant_id: u64,
    collection: &str,
) -> Option<u64> {
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::MetaOp;

    let request_id = state.next_request_id();
    let timeout = std::time::Duration::from_millis(500);

    let request = Request {
        request_id,
        tenant_id: TenantId::new(tenant_id),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Meta(MetaOp::QueryCollectionSize {
            tenant_id,
            name: collection.to_string(),
        }),
        deadline: std::time::Instant::now() + timeout,
        priority: Priority::Background,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Eventual,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };
    let mut rx = state.tracker.register(request_id);
    {
        let mut d = state.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        if d.dispatch_to_core(0, request).is_err() {
            state.tracker.cancel(&request_id);
            return None;
        }
    }
    let resp = tokio::time::timeout(timeout, async { rx.recv().await.ok_or(()) })
        .await
        .ok()?
        .ok()?;
    if resp.status != Status::Ok {
        return None;
    }
    let bytes = resp.payload.as_ref();
    if bytes.len() < 8 {
        return None;
    }
    Some(u64::from_le_bytes(bytes[..8].try_into().ok()?))
}
