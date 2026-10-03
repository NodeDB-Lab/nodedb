// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::KvInsert` → `PhysicalTask` lowering.

use nodedb_sql::types::{KvInsertIntent, SqlExpr, SqlValue};
use nodedb_types::CollectionType;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{
    assignments_to_update_values, sql_value_to_bytes, sql_value_to_string,
    write_msgpack_map_header, write_msgpack_str, write_msgpack_value,
};
use super::insert::assign_for_pk;
use super::key_assignment::check_assignments_keep_key;
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
    let collection_key = ctx.collection_key(collection);
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    // The conflict branch merges the assignments into the body stored under
    // the row's key, and a named key column is a copy of that key in the
    // body. An assignment to the key column must name the key it keeps.
    let key_column = if on_conflict_updates.is_empty() {
        None
    } else {
        Some(kv_key_column(ctx, collection)?)
    };
    // The built-in `key` column is not in the body, so an assignment that
    // keeps it writes nothing there.
    let update_values = match key_column.as_deref() {
        None => Vec::new(),
        Some(key_column) => {
            let body_assignments: Vec<(String, SqlExpr)> = on_conflict_updates
                .iter()
                .filter(|(field, _)| {
                    !(key_column == KV_KEY_COLUMN && field.eq_ignore_ascii_case(KV_KEY_COLUMN))
                })
                .cloned()
                .collect();
            assignments_to_update_values(&body_assignments)?
        }
    };
    let vshard = collection_key.vshard();
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
        if let Some(key_column) = key_column.as_deref() {
            check_assignments_keep_key(
                collection,
                key_column,
                on_conflict_updates,
                &sql_value_to_string(key_val),
            )?;
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
        let surrogate = assign_for_pk(ctx, collection_key, &key)?;
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
            // A conflict clause whose assignments all keep the built-in key
            // still merges: an empty merge keeps the stored body.
            KvInsertIntent::Put if key_column.is_some() => KvOp::InsertOnConflictUpdate {
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
                provenance: None,
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

/// The column a KV collection's key is read from: its schema's primary-key
/// column, else the built-in `key` column. The SQL planner extracts the key
/// by the same rule.
fn kv_key_column(ctx: &ConvertContext, collection: &str) -> crate::Result<String> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(KV_KEY_COLUMN.to_string());
    };
    let bare =
        crate::control::target_identity::naming::bare_collection_name(ctx.database_id, collection);
    let stored =
        credentials
            .catalog()
            .get_collection(ctx.database_id, ctx.tenant_id.as_u64(), &bare)?;
    let declared = stored.and_then(|stored| match stored.collection_type {
        CollectionType::KeyValue(config) => config
            .schema
            .columns
            .into_iter()
            .find(|column| column.primary_key)
            .map(|column| column.name),
        CollectionType::Document(_) | CollectionType::Columnar(_) => None,
    });
    Ok(declared.unwrap_or_else(|| KV_KEY_COLUMN.to_string()))
}

/// The key column of a KV collection with no declared primary key.
const KV_KEY_COLUMN: &str = "key";
