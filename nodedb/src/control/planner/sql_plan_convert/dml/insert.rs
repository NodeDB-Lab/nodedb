// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{SqlValue, WriteRoute};
use nodedb_types::Surrogate;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{
    expand_row_defaults, row_to_msgpack, rows_to_msgpack_array, sql_value_to_string,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Build a `ColumnarSchema` from raw catalog column-type strings.
///
/// `column_schema` is the list of `(column_name, type_str)` pairs from the
/// DDL catalog (`stored.fields`). Unknown type strings are treated as
/// `ColumnType::String` (matching the memtable's existing fallback).
///
/// The `id` column is treated as the primary key when present; all other
/// columns are treated as nullable.
///
/// Returns `None` when `column_schema` is empty (no catalog schema available
/// — test fixtures and legacy paths) or the resulting schema fails
/// validation.
///
/// This is the single source of truth for turning a catalog's raw
/// `(name, type_str)` field list into a typed `ColumnarSchema` — shared by
/// the live SQL insert path (via [`build_schema_bytes`]) and
/// `bootstrap::data_plane::load_columnar_schema_seed`, which pre-registers
/// each columnar-family collection's real schema before WAL replay so a
/// fresh `MutationEngine` never falls back to type-lossy inference.
pub(crate) fn build_columnar_schema(column_schema: &[(String, String)]) -> Option<ColumnarSchema> {
    if column_schema.is_empty() {
        return None;
    }
    let mut cols = Vec::with_capacity(column_schema.len());
    let mut has_id = false;
    for (name, type_str) in column_schema {
        // `type_str` may contain SQL modifiers such as `NOT NULL` or `PRIMARY KEY`
        // (e.g. "BIGINT NOT NULL"). Strip everything after the first token so that
        // `ColumnType::from_str` receives the bare type name (e.g. "BIGINT").
        let bare_type = type_str
            .split_whitespace()
            .next()
            .unwrap_or(type_str.as_str());
        let col_type = bare_type
            .parse::<ColumnType>()
            .unwrap_or(ColumnType::String);
        let is_id = name == "id" || name == "document_id";
        if is_id {
            has_id = true;
            cols.push(ColumnDef::required(name.clone(), col_type).with_primary_key());
        } else {
            cols.push(ColumnDef::nullable(name.clone(), col_type));
        }
    }
    // If no PK column found in stored.fields, inject a synthetic one.
    if !has_id {
        cols.insert(
            0,
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
        );
    }
    ColumnarSchema::new(cols).ok()
}

/// Build a `ColumnarSchema` from raw catalog column-type strings, then
/// serialize it as MessagePack for the `ColumnarOp::Insert::schema_bytes` field.
///
/// Returns an empty `Vec` when `column_schema` is empty or fails validation
/// — see [`build_columnar_schema`] for the typed builder this wraps.
pub(super) fn build_schema_bytes(column_schema: &[(String, String)]) -> Vec<u8> {
    build_columnar_schema(column_schema)
        .map(|schema| zerompk::to_msgpack_vec(&schema).unwrap_or_default())
        .unwrap_or_default()
}

/// A row's primary-key column as found during identity derivation.
///
/// `Present("")` is the empty string — a real key, not an absence.
pub(super) enum DocId {
    Present(String),
    ExplicitNull,
    Absent,
}

/// Extract the document-id value from a row, keyed off the declared
/// `primary_key` column when present, falling back to the legacy
/// `id`/`document_id`/`key` convention otherwise.
pub(super) fn extract_doc_id(row: &[(String, SqlValue)], primary_key: Option<&str>) -> DocId {
    match row.iter().find(|(k, _)| match primary_key {
        Some(pk) => k == pk,
        None => k == "id" || k == "document_id" || k == "key",
    }) {
        Some((_, SqlValue::Null)) => DocId::ExplicitNull,
        Some((_, v)) => DocId::Present(sql_value_to_string(v)),
        None => DocId::Absent,
    }
}

/// `collection`'s DDL-declared `PRIMARY KEY` column name, if any.
///
/// `primary_key` cannot answer this: schemaless, columnar, and spatial
/// collections resolve it to `id` by convention with nothing declared. The
/// catalog's `declared_primary_key` is set only by the keyword itself, and
/// names the column the keyword applied `NOT NULL` to. A catalog miss reads
/// as not declared — nothing to enforce.
pub(in super::super) fn declared_primary_key_name(
    ctx: &ConvertContext,
    collection: &str,
) -> crate::Result<Option<String>> {
    let Some(credentials) = ctx.credentials.as_ref() else {
        return Ok(None);
    };
    credentials
        .catalog()
        .declared_primary_key(ctx.database_id, ctx.tenant_id.as_u64(), collection)
}

/// Resolve a row's document id + surrogate, refusing a `NULL`/omitted
/// declared primary key first: a declared `PRIMARY KEY` implies `NOT NULL`.
///
/// Enforcement keys on the DDL-declared column, not the resolved
/// `primary_key`: those diverge whenever a natural key sits on a column
/// other than `id` (e.g. `metrics (sku TEXT PRIMARY KEY)` resolves
/// `primary_key` to `id` but declares `sku`). `_rowid` carries no
/// declaration, so it skips the check and mints a surrogate.
///
/// Identity minting then runs on the resolved `primary_key`: an auto-`_rowid`
/// pk or a missing/null key mints a fresh surrogate; a present key
/// content-addresses one via [`assign_for_pk`]. The two steps are one call so
/// no caller can mint an identity without the NOT NULL check running first.
pub(super) fn resolve_doc_identity(
    ctx: &ConvertContext,
    collection: &str,
    primary_key: Option<&str>,
    row: &[(String, SqlValue)],
) -> crate::Result<(String, Surrogate)> {
    if !is_auto_rowid_pk(primary_key)
        && let Some(declared) = declared_primary_key_name(ctx, collection)?
    {
        match extract_doc_id(row, Some(&declared)) {
            DocId::Present(_) => {}
            DocId::ExplicitNull | DocId::Absent => {
                return Err(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "not_null".to_string(),
                    detail: format!("primary key '{declared}' cannot be NULL or omitted"),
                });
            }
        }
    }

    if is_auto_rowid_pk(primary_key) {
        let (s, pk) = assign_fresh(
            ctx,
            collection,
            nodedb_physical::FreshSurrogateKind::AutoRowId,
        )?;
        return Ok((pk, s));
    }
    match extract_doc_id(row, primary_key) {
        DocId::Present(id) => {
            let s = assign_for_pk(ctx, collection, id.as_bytes())?;
            Ok((id, s))
        }
        DocId::ExplicitNull | DocId::Absent => {
            let (s, pk) = assign_fresh(
                ctx,
                collection,
                nodedb_physical::FreshSurrogateKind::DocumentStorageKey,
            )?;
            Ok((pk, s))
        }
    }
}

pub(super) fn assign_for_pk(
    ctx: &ConvertContext,
    collection: &str,
    pk_bytes: &[u8],
) -> crate::Result<Surrogate> {
    ctx.surrogate_for_pk(collection, pk_bytes)
}

/// Allocate a fresh, unique surrogate for a row whose primary key is the
/// auto-generated `_rowid` (no `PRIMARY KEY` declared), or that carries no
/// content primary key at all.
///
/// Content-addressing an empty pk collapses every such row onto one
/// surrogate, a duplicate-key violation on the second insert.
///
/// Returns the identity string `kind` binds. The caller uses it verbatim.
pub(super) fn assign_fresh(
    ctx: &ConvertContext,
    collection: &str,
    kind: nodedb_physical::FreshSurrogateKind,
) -> crate::Result<(Surrogate, String)> {
    ctx.fresh_surrogate(collection, kind)
}

/// Whether a collection's declared primary key is the auto-generated `_rowid`
/// sentinel — injected by strict-schema construction when no `PRIMARY KEY` was
/// declared. Such rows carry no user identity: each needs a fresh surrogate.
pub(super) fn is_auto_rowid_pk(primary_key: Option<&str>) -> bool {
    primary_key == Some("_rowid")
}

/// Mirrors the document-engine identity path (`resolve_doc_identity`) for
/// columnar/spatial rows. The declared `primary_key` — not the legacy
/// `id`/`document_id`/`key` name guess — determines each row's identity, so a
/// natural key on any column (e.g. `sku`) gets its own surrogate. A
/// missing/empty key mints a fresh unique surrogate rather than collapsing
/// onto `Surrogate::ZERO`, which would silently merge distinct rows.
pub(super) fn columnar_row_surrogates(
    ctx: &ConvertContext,
    collection: &str,
    columnar_rows: &[&Vec<(String, SqlValue)>],
    primary_key: Option<&str>,
) -> crate::Result<Vec<Surrogate>> {
    let mut out = Vec::with_capacity(columnar_rows.len());
    for row in columnar_rows {
        let (_, surrogate) = resolve_doc_identity(ctx, collection, primary_key, row)?;
        out.push(surrogate);
    }
    Ok(out)
}

/// Bundled arguments for [`convert_insert`].
pub(in super::super) struct ConvertInsertArgs<'a> {
    pub collection: &'a str,
    /// The lowering these rows take, decided by `nodedb-sql`.
    pub route: WriteRoute,
    pub rows: &'a [Vec<(String, SqlValue)>],
    pub column_defaults: &'a [(String, String)],
    pub column_schema: &'a [(String, String)],
    pub if_absent: bool,
    pub primary_key: Option<&'a str>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in super::super) fn convert_insert(
    args: ConvertInsertArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertInsertArgs {
        collection,
        route,
        rows,
        column_defaults,
        column_schema,
        if_absent,
        primary_key,
        tenant_id,
        ctx,
    } = args;
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::new();
    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    // Both INSERT routing gates, read from the catalog once for the whole
    // statement (never re-hit per row).
    //
    // `IF NOT EXISTS` (ON CONFLICT DO NOTHING → `if_absent`) cannot be honored by
    // `CrdtOp::DocUpsert`, which is an unconditional LWW full-replace: reject.
    let gates = super::balanced_gate::document_collection_write_gates(ctx, collection)?;
    let is_crdt = gates.crdt;
    if is_crdt && if_absent {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "INSERT ... IF NOT EXISTS on CRDT collection '{collection}' is not supported; \
                 CRDT documents converge via last-writer-wins full replace"
            ),
        });
    }

    // A balanced collection's rows are judged as a set, so the statement lowers
    // to ONE page rather than one task per row — see `balanced_gate`.
    let is_balanced = gates.balanced && !is_crdt;
    // `ON CONFLICT DO NOTHING` skips rows whose key already exists, and which
    // rows those are is decided per row at apply time. A journal that silently
    // loses one leg that way is exactly the unbalanced state the constraint
    // exists to refuse, and the page shape cannot express the per-row skip, so
    // the combination is rejected rather than half-honored.
    if is_balanced && if_absent {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "INSERT ... IF NOT EXISTS on BALANCED collection '{collection}' is not \
                 supported; a row skipped on conflict would leave its journal unbalanced"
            ),
        });
    }
    // Rows of a balanced INSERT, accumulated across the loop below and emitted
    // as one `BatchInsert` task after it.
    let mut balanced_documents: Vec<(String, Vec<u8>)> = Vec::new();
    let mut balanced_surrogates: Vec<Surrogate> = Vec::new();

    // Every engine's rows expand their DEFAULTs here, ahead of identity
    // derivation. A DEFAULT materialized after the primary-key NOT NULL gate
    // refuses a key the declaration supplies.
    let expanded_rows = expand_row_defaults(rows, column_defaults, tenant_id, ctx)?;

    for row in &expanded_rows {
        match route {
            WriteRoute::ColumnarFamily => {
                columnar_rows.push(row);
            }
            WriteRoute::Document => {
                let value_bytes = row_to_msgpack(row)?;
                let (doc_id, surrogate) = resolve_doc_identity(ctx, collection, primary_key, row)?;
                // One page for the whole statement: the rows of a balanced
                // INSERT are judged together, so they may not be split across
                // one task — one boundary — per row.
                if is_balanced {
                    balanced_documents.push((doc_id, value_bytes));
                    balanced_surrogates.push(surrogate);
                    continue;
                }
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
                    PhysicalPlan::Document(DocumentOp::PointInsert {
                        collection: qualified_collection.clone(),
                        document_id: doc_id,
                        value: value_bytes,
                        if_absent,
                        surrogate,
                        // Both filled in after conversion: the RETURNING spec
                        // by the protocol layer's injection pass, the read
                        // filter by the RLS injection pass.
                        returning: None,
                        rls_filters: Vec::new(),
                        // Filled by the materialized-sum resolution pass,
                        // which runs after conversion (it needs the catalog
                        // and, in cluster mode, a routed lookup).
                        resolved_sum_targets: Vec::new(),
                        deferred_sum_targets: Vec::new(),
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
        }
    }

    if !balanced_documents.is_empty() {
        tasks.push(super::balanced_gate::balanced_batch_task(
            super::balanced_gate::BalancedBatch {
                collection,
                tenant_id,
                vshard,
                documents: balanced_documents,
                surrogates: balanced_surrogates,
            },
            ctx.database_id,
        ));
    }

    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows)?;
        let intent = if if_absent {
            ColumnarInsertIntent::InsertIfAbsent
        } else {
            ColumnarInsertIntent::Insert
        };
        let surrogates = columnar_row_surrogates(ctx, collection, &columnar_rows, primary_key)?;
        let schema_bytes = build_schema_bytes(column_schema);
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: qualified_collection.clone(),
                payload,
                format: "msgpack".into(),
                intent,
                on_conflict_updates: Vec::new(),
                surrogates,
                schema_bytes,
                provenance: None,
                wal_lsn: None,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                // Both slots are filled by later passes over the built plan —
                // `inject_returning_spec` from the statement's RETURNING list,
                // and the row-level-security injector from the collection's read
                // policy. Filling either here would duplicate a decision that
                // has one owner.
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(tasks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;

    /// Build a `ConvertContext` whose credential store carries a catalog with
    /// three collections under tenant 0 / DEFAULT database: `edges`
    /// (`has_implicit_edges = true`), `plain` (all flags false), and `crdt_coll`
    /// (`crdt = true`). The returned `TempDir` must be kept alive for the lifetime
    /// of the context (it backs the catalog's redb file).
    fn ctx_with_catalog() -> (ConvertContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store");
        {
            let catalog = store.catalog();
            let mut edges = StoredCollection::new(0, "edges", "owner");
            edges.has_implicit_edges = true;
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &edges)
                .expect("put edges collection");
            let plain = StoredCollection::new(0, "plain", "owner");
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &plain)
                .expect("put plain collection");
            let mut crdt_coll = StoredCollection::new(0, "crdt_coll", "owner");
            crdt_coll.crdt = true;
            catalog
                .put_collection(crate::types::DatabaseId::DEFAULT, &crdt_coll)
                .expect("put crdt collection");
        }

        let ctx = ConvertContext {
            purpose: crate::control::planner::sql_plan_convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: Some(Arc::new(store)),
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            sql_catalog: None,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        };
        (ctx, dir)
    }

    fn crdt_row(id: &str) -> Vec<(String, SqlValue)> {
        vec![
            ("id".to_string(), SqlValue::String(id.to_string())),
            ("name".to_string(), SqlValue::String("alice".to_string())),
        ]
    }

    #[test]
    fn insert_into_crdt_collection_routes_doc_upsert() {
        let (ctx, _dir) = ctx_with_catalog();
        let rows = vec![crdt_row("k1")];
        let tasks = convert_insert(ConvertInsertArgs {
            collection: "crdt_coll",
            route: WriteRoute::Document,
            rows: &rows,
            column_defaults: &[],
            column_schema: &[],
            if_absent: false,
            primary_key: Some("id"),
            tenant_id: TenantId::new(0),
            ctx: &ctx,
        })
        .expect("convert_insert");
        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                document_id,
                fields_json,
                partial,
                ..
            }) => {
                assert_eq!(document_id, "k1");
                assert!(!partial, "INSERT must be a full-replace DocUpsert");
                assert!(fields_json.contains("alice"));
            }
            other => panic!("expected CrdtOp::DocUpsert, got {other:?}"),
        }
    }

    #[test]
    fn insert_into_non_crdt_collection_routes_point_insert() {
        let (ctx, _dir) = ctx_with_catalog();
        let rows = vec![crdt_row("k1")];
        let tasks = convert_insert(ConvertInsertArgs {
            collection: "plain",
            route: WriteRoute::Document,
            rows: &rows,
            column_defaults: &[],
            column_schema: &[],
            if_absent: false,
            primary_key: Some("id"),
            tenant_id: TenantId::new(0),
            ctx: &ctx,
        })
        .expect("convert_insert");
        assert_eq!(tasks.len(), 1);
        assert!(
            matches!(
                &tasks[0].plan,
                PhysicalPlan::Document(DocumentOp::PointInsert { .. })
            ),
            "non-crdt INSERT must remain a PointInsert"
        );
    }
}
