// SPDX-License-Identifier: BUSL-1.1

//! `_system.l2_cleanup_queue` row producer.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::tables::collections::encode_row;

pub fn l2_cleanup_queue(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let catalog = state.credentials.catalog();
    let queue = catalog
        .load_l2_cleanup_queue()
        .map_err(|e| crate::Error::Storage {
            engine: "catalog".to_string(),
            detail: e.to_string(),
        })?;

    let is_admin = identity.is_superuser || identity.has_role(&Role::TenantAdmin);
    let caller_tenant = identity.tenant_id.as_u64();

    let mut rows = Vec::new();
    for e in &queue {
        if !is_admin && e.tenant_id != caller_tenant {
            continue;
        }
        let mut r: HashMap<String, Value> = HashMap::with_capacity(8);
        r.insert("database_id".into(), Value::Integer(e.database_id as i64));
        r.insert("tenant_id".into(), Value::Integer(e.tenant_id as i64));
        r.insert("name".into(), Value::String(e.name.clone()));
        r.insert("purge_lsn".into(), Value::Integer(e.purge_lsn as i64));
        r.insert(
            "enqueued_at_ns".into(),
            Value::Integer(e.enqueued_at_ns as i64),
        );
        r.insert(
            "bytes_pending".into(),
            Value::Integer(e.bytes_pending as i64),
        );
        r.insert("last_error".into(), Value::String(e.last_error.clone()));
        r.insert("attempts".into(), Value::Integer(e.attempts as i64));
        rows.push(encode_row(r)?);
    }
    Ok(rows)
}
