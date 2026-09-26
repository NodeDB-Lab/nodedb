// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored KV rows.
//!
//! The per-node snapshot install puts a KV row straight into the target
//! node's memtable, with no WAL record and no Raft entry: only that node holds
//! it, and it is gone after a restart. RESTORE re-issues each row as a
//! `KvOp::Put` instead, so every replica of the collection's group applies it
//! and its WAL makes it durable.

use nodedb_physical::physical_plan::KvOp;
use nodedb_types::QualifiedCollection;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// One restored KV table's rows: `(key, value, expire_at_ms)`, the shape the
/// KV snapshot captures. `expire_at_ms` is `0` for a row with no TTL.
type KvRows = Vec<(Vec<u8>, Vec<u8>, u64)>;

/// Split a KV snapshot's stored collection name into its database and bare
/// collection. A collection outside the default database is stored as
/// `{database_id}/{collection}`.
fn split_stored_collection(stored: &str) -> (DatabaseId, &str) {
    match stored.split_once('/') {
        Some((prefix, name)) => match prefix.parse::<u64>() {
            Ok(database_id) => (DatabaseId::new(database_id), name),
            Err(_) => (DatabaseId::DEFAULT, stored),
        },
        None => (DatabaseId::DEFAULT, stored),
    }
}

/// The TTL a restored row keeps at `now_ms`: `Some(0)` for no TTL, the time
/// left for a row that has not expired, `None` for a row already expired.
fn remaining_ttl_ms(expire_at_ms: u64, now_ms: u64) -> Option<u64> {
    match expire_at_ms {
        0 => Some(0),
        at if at > now_ms => Some(at - now_ms),
        _ => None,
    }
}

/// Decode and durably re-issue every restored KV table. Returns the number of
/// rows re-issued.
pub(in crate::control::backup::restore) async fn reissue_kv_tables(
    state: &SharedState,
    tenant_id: u64,
    tables: Vec<(String, Vec<u8>)>,
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for (stored, bytes) in tables {
        let rows: KvRows = zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("restore reissue: deserialize KV table '{stored}': {e}"),
        })?;
        let (database_id, collection) = split_stored_collection(&stored);
        super::durable::log_reissue_step(
            state,
            "kv",
            collection,
            crate::types::VShardId::from_collection_in_database(database_id, collection),
            rows.len(),
        );
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        for (key, value, expire_at_ms) in rows {
            let Some(ttl_ms) = remaining_ttl_ms(expire_at_ms, now_ms) else {
                continue;
            };
            let surrogate = state
                .surrogate_assigner
                .assign(database_id, tenant, &stored, &key)?;
            let plan = PhysicalPlan::Kv(KvOp::Put {
                collection: QualifiedCollection::from_stored(stored.clone()),
                key,
                value,
                ttl_ms,
                surrogate,
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            });
            super::durable::reissue_plan_durably(state, tenant, database_id, collection, plan)
                .await?;
            reissued += 1;
        }
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_name_splits_into_its_database_and_collection() {
        assert_eq!(
            split_stored_collection("orders"),
            (DatabaseId::DEFAULT, "orders")
        );
        assert_eq!(
            split_stored_collection("7/orders"),
            (DatabaseId::new(7), "orders")
        );
    }

    #[test]
    fn an_expired_row_is_not_restored() {
        assert_eq!(remaining_ttl_ms(0, 500), Some(0));
        assert_eq!(remaining_ttl_ms(900, 500), Some(400));
        assert_eq!(remaining_ttl_ms(400, 500), None);
    }
}
