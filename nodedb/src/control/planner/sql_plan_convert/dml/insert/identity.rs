// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::SqlValue;
use nodedb_types::Surrogate;

use super::super::super::convert::ConvertContext;
use super::super::super::value::sql_value_to_string;

/// A row's primary-key column as found during identity derivation.
///
/// `Present("")` is the empty string — a real key, not an absence.
pub(super) enum DocId {
    Present(String),
    ExplicitNull,
    Absent,
}

/// Extract the document-id value from a row, keyed off `primary_key`.
pub(super) fn extract_doc_id(row: &[(String, SqlValue)], primary_key: &str) -> DocId {
    match row.iter().find(|(k, _)| k == primary_key) {
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
pub(in super::super::super) fn declared_primary_key_name(
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

/// Resolve a row's document id and surrogate, refusing a NULL or omitted
/// declared primary key first. A declared `PRIMARY KEY` implies `NOT NULL`.
///
/// Enforcement keys on the DDL-declared column, not the resolved
/// `primary_key`. The two diverge whenever a natural key sits on a column
/// other than `id`. `metrics (sku TEXT PRIMARY KEY)` resolves `primary_key`
/// to `id` and declares `sku`.
///
/// Identity minting runs on the same declared name. A present declared key
/// content-addresses a surrogate via [`assign_for_pk`]. With no declared key,
/// minting uses the resolved `primary_key`. An auto-`_rowid` pk or a missing
/// key mints a fresh surrogate.
///
/// The caller resolves `declared` once per statement, so a multi-row INSERT
/// reads the catalog once rather than once per row. Both steps are one call,
/// so no caller mints an identity without the NOT NULL check running first.
pub(in super::super) fn resolve_doc_identity_with_declared(
    ctx: &ConvertContext,
    collection: &str,
    primary_key: &str,
    declared: Option<&str>,
    row: &[(String, SqlValue)],
) -> crate::Result<(String, Surrogate)> {
    if let Some(declared) = declared {
        match extract_doc_id(row, declared) {
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
    let mint_key: &str = declared.unwrap_or(primary_key);
    match extract_doc_id(row, mint_key) {
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

pub(in super::super) fn assign_for_pk(
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
pub(in super::super) fn is_auto_rowid_pk(primary_key: &str) -> bool {
    primary_key == "_rowid"
}

/// Mirrors the document-engine identity path for columnar and spatial rows.
///
/// The declared primary key determines each row's identity, so a natural key
/// on any column gets its own surrogate. A missing or empty key mints a fresh
/// surrogate rather than collapsing onto `Surrogate::ZERO`, which merges
/// distinct rows.
///
/// `declared_pk` is the catalog's declared PK name, resolved once by the
/// caller for the whole statement — not re-read here per row.
pub(in super::super) fn columnar_row_surrogates(
    ctx: &ConvertContext,
    collection: &str,
    columnar_rows: &[&Vec<(String, SqlValue)>],
    primary_key: &str,
    declared_pk: Option<&str>,
) -> crate::Result<Vec<Surrogate>> {
    // `_rowid` carries no declaration.
    let declared = if is_auto_rowid_pk(primary_key) {
        None
    } else {
        declared_pk
    };
    let mut out = Vec::with_capacity(columnar_rows.len());
    for row in columnar_rows {
        let (_, surrogate) =
            resolve_doc_identity_with_declared(ctx, collection, primary_key, declared, row)?;
        out.push(surrogate);
    }
    Ok(out)
}
