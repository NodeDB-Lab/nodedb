// SPDX-License-Identifier: Apache-2.0

//! `SqlCatalog` trait + descriptor-resolution error type.

use nodedb_types::DatabaseId;
use thiserror::Error;

use crate::types::CollectionInfo;
use crate::types_array::{ArrayAttrAst, ArrayDimAst};

/// Normalize the PostgreSQL identifier spelling accepted by `regclass` input.
///
/// Unquoted identifiers fold to lowercase, quoted identifiers preserve case and
/// support doubled quote escapes. NodeDB currently exposes user relations in
/// `public` and catalog relations in `pg_catalog`; those qualifiers resolve to
/// the same canonical relation name used by the catalog adapters.
pub fn normalize_regclass_name(input: &str) -> Option<String> {
    fn parse_part(raw: &str) -> Option<String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        if !raw.starts_with('"') {
            return (!raw.contains('"')).then(|| raw.to_ascii_lowercase());
        }

        let mut chars = raw.char_indices().peekable();
        chars.next();
        let mut out = String::new();
        let mut closed_at = None;
        while let Some((idx, ch)) = chars.next() {
            if ch != '"' {
                out.push(ch);
                continue;
            }
            if chars.peek().is_some_and(|(_, next)| *next == '"') {
                chars.next();
                out.push('"');
            } else {
                closed_at = Some(idx + ch.len_utf8());
                break;
            }
        }
        let end = closed_at?;
        raw[end..].trim().is_empty().then_some(out)
    }

    let mut parts = Vec::new();
    let mut quoted = false;
    let mut start = 0;
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut i = 0;
    while i < chars.len() {
        let (idx, ch) = chars[i];
        if ch == '"' {
            if quoted && i + 1 < chars.len() && chars[i + 1].1 == '"' {
                i += 2;
                continue;
            }
            quoted = !quoted;
        } else if ch == '.' && !quoted {
            parts.push(parse_part(&input[start..idx])?);
            start = idx + 1;
        }
        i += 1;
    }
    if quoted {
        return None;
    }
    parts.push(parse_part(&input[start..])?);

    match parts.as_slice() {
        [name] => Some(name.clone()),
        [schema, name] if schema == "public" || schema == "pg_catalog" => Some(name.clone()),
        _ => None,
    }
}

/// Errors surfaced by `SqlCatalog` implementations.
///
/// Only one variant today — callers pattern-match directly and
/// map the retryable case to `SqlError::RetryableSchemaChanged`
/// via the `From` impl in `error.rs`. The enum shape is kept
/// despite having a single variant so future variants can be
/// added without a breaking change.
#[derive(Debug, Clone, Error)]
pub enum SqlCatalogError {
    /// A DDL drain is in progress on the descriptor at the
    /// version the planner wanted to acquire a lease on. Callers
    /// should retry the whole plan after a short backoff — by
    /// then either the drain has completed (new descriptor
    /// version available in the cache) or the retry budget is
    /// exhausted and a typed error surfaces to the client.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged {
        /// Human-readable identifier for the descriptor, e.g.
        /// `"collection orders"`. Used in log / trace output.
        descriptor: String,
    },

    /// Collection is soft-deleted (`DROP COLLECTION` run, retention
    /// window still active). Distinct from `Ok(None)` = absent so the
    /// planner can surface an actionable error with an `UNDROP`
    /// hint rather than a generic "unknown table".
    #[error(
        "collection '{name}' was dropped and is within its retention window; \
         restore with UNDROP COLLECTION before {retention_expires_at_ns} ns"
    )]
    CollectionDeactivated {
        name: String,
        /// Wall-clock nanoseconds when retention elapses and the
        /// collection is hard-deleted by the GC sweeper.
        retention_expires_at_ns: u64,
    },
}

/// A relation that resolves to planner metadata without a stored collection.
/// Catalog tables (pg_class, _system.*) and array TVFs both fit this shape.
pub trait TableProvider {
    fn schema(&self) -> CollectionInfo;
    fn rel_oid(&self) -> Option<i64> {
        None
    }
}

/// Trait for looking up collection metadata during planning.
///
/// Both Origin (via CredentialStore) and Lite (via the embedded
/// redb catalog) implement this trait.
///
/// The return type is `Result<Option<CollectionInfo>, _>` with
/// a three-way semantics:
///
/// - `Ok(Some(info))` — the collection exists and is usable.
///   An Origin implementation will have acquired a descriptor
///   lease at the current version before returning; subsequent
///   planning against the same collection within the lease
///   window is drain-safe.
/// - `Ok(None)` — the collection does not exist. Callers should
///   surface this as `SqlError::UnknownTable`.
/// - `Err(SqlCatalogError::RetryableSchemaChanged { .. })` —
///   the collection exists but a DDL drain is in progress.
///   Callers propagate this up so the pgwire layer can retry
///   the whole statement.
pub trait SqlCatalog {
    fn get_collection(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError>;

    /// Resolve ANY relation name to planner metadata. Catalog tables override
    /// this to surface synthetic relations by name; the default resolves only
    /// stored collections, identical to `get_collection`.
    fn resolve_relation(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        self.get_collection(database_id, name)
    }

    /// Look up an array by name. Returns `None` if no array with that
    /// name is registered. The default implementation returns `None` so
    /// that catalog adapters predating array support compile without
    /// change — the array DML planner falls back to "array not found"
    /// in that case.
    fn lookup_array(&self, _name: &str) -> Option<ArrayCatalogView> {
        None
    }

    /// Cheap existence check; the default delegates to `lookup_array`.
    fn array_exists(&self, name: &str) -> bool {
        self.lookup_array(name).is_some()
    }

    /// Resolve a relation name to its catalog OID for `'name'::regclass`.
    /// Returns `None` when the name is not found. The default returns `None`
    /// so existing impls compile without change.
    fn resolve_regclass(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        _name: &str,
    ) -> Option<i64> {
        None
    }

    /// Resolve a type name to its catalog OID for `'name'::regtype`.
    /// Returns `None` when the name is not found. The default returns `None`
    /// so existing impls compile without change.
    fn resolve_regtype(&self, _name: &str) -> Option<i64> {
        None
    }

    /// Advance a sequence and return the allocated value, recording it as this
    /// session's `currval`. Scoping mirrors `resolve_regclass`.
    ///
    /// An unknown name returns [`SqlError::UndefinedObject`] (SQLSTATE
    /// `42704`). The default returns "sequence access unavailable" so
    /// implementors with no sequence state compile without change.
    fn sequence_nextval(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        _name: &str,
    ) -> Result<i64, crate::SqlError> {
        Err(sequence_access_unavailable())
    }

    /// Return the last value this session obtained from `sequence_nextval`.
    ///
    /// A sequence this session never advanced returns
    /// [`SqlError::ObjectNotInPrerequisiteState`] (SQLSTATE `55000`).
    fn sequence_currval(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        _name: &str,
    ) -> Result<i64, crate::SqlError> {
        Err(sequence_access_unavailable())
    }

    /// Position a sequence so the next `sequence_nextval` returns
    /// `value + increment`. Returns `value`.
    fn sequence_setval(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        _name: &str,
        _value: i64,
    ) -> Result<i64, crate::SqlError> {
        Err(sequence_access_unavailable())
    }
}

/// The error a catalog with no sequence state returns for every accessor.
fn sequence_access_unavailable() -> crate::SqlError {
    crate::SqlError::ObjectNotInPrerequisiteState {
        object: "sequence".into(),
        detail: "sequence access unavailable: this catalog carries no sequence registry".into(),
    }
}

/// View of a registered array, surfaced to the SQL planner. Decoded by
/// the runtime catalog adapter from its persisted msgpack schema blob;
/// keeps `nodedb-sql` free of any dependency on `nodedb-array`.
#[derive(Debug, Clone)]
pub struct ArrayCatalogView {
    pub name: String,
    pub dims: Vec<ArrayDimAst>,
    pub attrs: Vec<ArrayAttrAst>,
    pub tile_extents: Vec<i64>,
}
