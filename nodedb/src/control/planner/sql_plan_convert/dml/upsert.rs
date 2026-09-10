// SPDX-License-Identifier: BUSL-1.1

//! `UPSERT` / `INSERT ... ON CONFLICT DO UPDATE` lowering.
//!
//! Split from `insert.rs`, which lowers plain `INSERT`. The two share the row
//! identity helper there (`resolve_doc_identity`) so a row's surrogate is
//! derived identically whichever statement wrote it.

use nodedb_sql::types::{SqlExpr, SqlValue, WriteRoute};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{
    assignments_to_update_values, expand_row_defaults, row_to_msgpack, rows_to_msgpack_array,
};
use super::insert::{
    build_schema_bytes, columnar_row_surrogates, declared_primary_key_name, resolve_doc_identity,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Bundled arguments for [`convert_upsert`].
pub(in super::super) struct ConvertUpsertArgs<'a> {
    pub collection: &'a str,
    /// The lowering these rows take, decided by `nodedb-sql`.
    pub route: WriteRoute,
    pub rows: &'a [Vec<(String, SqlValue)>],
    pub column_defaults: &'a [(String, String)],
    pub column_schema: &'a [(String, String)],
    pub on_conflict_updates: &'a [(String, SqlExpr)],
    pub primary_key: Option<&'a str>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in super::super) fn convert_upsert(
    args: ConvertUpsertArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertUpsertArgs {
        collection,
        route,
        rows,
        column_defaults,
        column_schema,
        on_conflict_updates,
        primary_key,
        tenant_id,
        ctx,
    } = args;
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::new();

    // Detect CRDT document collections once. An explicit `ON CONFLICT DO UPDATE
    // SET ...` cannot be honored: CRDT conflict resolution IS the LWW
    // full-replace `DocUpsert` performs, so a caller-supplied merge clause has
    // no place to run. Reject rather than silently ignore it.
    let is_crdt = super::crdt_gate::document_collection_is_crdt(ctx, collection)?;
    if is_crdt && !on_conflict_updates.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "UPSERT with ON CONFLICT DO UPDATE on CRDT collection '{collection}' is not \
                 supported; CRDT documents converge via last-writer-wins full replace"
            ),
        });
    }

    let on_conflict_values = if on_conflict_updates.is_empty() {
        Vec::new()
    } else {
        assignments_to_update_values(on_conflict_updates)?
    };

    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    // Every engine's rows expand their DEFAULTs here, ahead of identity
    // derivation, so the primary-key NOT NULL gate reads the row the
    // declaration promises — see `expand_row_defaults`.
    let expanded_rows = expand_row_defaults(rows, column_defaults, tenant_id, ctx)?;

    for row in &expanded_rows {
        match route {
            WriteRoute::Document => {
                let value_bytes = row_to_msgpack(row)?;
                let (doc_id, surrogate) = resolve_doc_identity(ctx, collection, primary_key, row)?;
                let plan = if is_crdt {
                    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                        collection: qualified_collection.clone(),
                        document_id: doc_id,
                        fields_json: super::crdt_gate::row_to_fields_json(row)?,
                        surrogate,
                        partial: false,
                        returning: None,
                        rls_filters: Vec::new(),
                    })
                } else {
                    PhysicalPlan::Document(DocumentOp::Upsert {
                        collection: qualified_collection.clone(),
                        document_id: doc_id,
                        value: value_bytes,
                        on_conflict_updates: on_conflict_values.clone(),
                        surrogate,
                        // Filled in by the RLS injection pass, which runs after
                        // conversion.
                        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                        rls_filters: Vec::new(),
                        // Filled in by the protocol layer's RETURNING injection.
                        returning: None,
                        // Filled by the materialized-sum resolution pass.
                        resolved_sum_targets: Vec::new(),
                    })
                };
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    database_id: ctx.database_id,
                    plan,
                    post_set_op: PostSetOp::None,
                    txn_id: None,
                });
            }
            WriteRoute::ColumnarFamily => {
                columnar_rows.push(row);
            }
        }
    }

    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows)?;
        let declared_pk = declared_primary_key_name(ctx, collection)?;
        let surrogates = columnar_row_surrogates(
            ctx,
            collection,
            &columnar_rows,
            primary_key,
            declared_pk.as_deref(),
        )?;
        let schema_bytes = build_schema_bytes(column_schema, declared_pk.as_deref());
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: qualified_collection,
                payload,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Put,
                on_conflict_updates: on_conflict_values,
                surrogates,
                schema_bytes,
                provenance: None,
                wal_lsn: None,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Filled by the later `inject_returning_spec` / row-level-security
                // passes — see the plain-insert site above.
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(tasks)
}
