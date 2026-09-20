// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::KvInsert` → `PhysicalTask` lowering.

use nodedb_sql::types::{KvInsertIntent, SqlExpr, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{
    assignments_to_update_values, sql_value_to_bytes, write_msgpack_map_header, write_msgpack_str,
    write_msgpack_value,
};
use super::insert::assign_for_pk;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(in super::super) fn convert_kv_insert(
    collection: &str,
    entries: &[(SqlValue, Vec<(String, SqlValue)>)],
    ttl_secs: u64,
    intent: KvInsertIntent,
    on_conflict_updates: &[(String, SqlExpr)],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let update_values = if on_conflict_updates.is_empty() {
        Vec::new()
    } else {
        assignments_to_update_values(on_conflict_updates)?
    };
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let ttl_ms = ttl_secs * 1000;
    let mut tasks = Vec::with_capacity(entries.len());
    for (key_val, value_cols) in entries {
        // A declared PRIMARY KEY implies NOT NULL. The planner substitutes
        // `SqlValue::Null` for a column the statement omitted, so this also
        // catches an omitted key, not only an explicit `NULL` literal.
        if matches!(key_val, SqlValue::Null) {
            return Err(crate::Error::RejectedConstraint {
                collection: collection.to_string(),
                constraint: "not_null".to_string(),
                detail: "primary key cannot be NULL or omitted".to_string(),
            });
        }
        let key = sql_value_to_bytes(key_val)?;
        let value = if value_cols.len() == 1 && value_cols[0].0 == "value" {
            sql_value_to_bytes(&value_cols[0].1)?
        } else {
            let mut buf = Vec::with_capacity(value_cols.len() * 32);
            write_msgpack_map_header(&mut buf, value_cols.len());
            for (col, val) in value_cols {
                write_msgpack_str(&mut buf, col);
                write_msgpack_value(&mut buf, val);
            }
            buf
        };
        let surrogate = assign_for_pk(ctx, collection, &key)?;
        let op = match intent {
            KvInsertIntent::Insert => KvOp::Insert {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::InsertIfAbsent => KvOp::InsertIfAbsent {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::Put if !update_values.is_empty() => KvOp::InsertOnConflictUpdate {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                updates: update_values.clone(),
                surrogate,
                // Filled by the RLS injection pass, which runs after plan
                // conversion.
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
            KvInsertIntent::Put => KvOp::Put {
                collection: qualified_collection.clone(),
                key,
                value,
                ttl_ms,
                surrogate,
                // Both filled in after conversion: the RETURNING spec by
                // the protocol layer's injection pass, the read filter by the
                // RLS injection pass.
                returning: None,
                rls_filters: Vec::new(),
            },
        };
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Kv(op),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }
    Ok(tasks)
}
