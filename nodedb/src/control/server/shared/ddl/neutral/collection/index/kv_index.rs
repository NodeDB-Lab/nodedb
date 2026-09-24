// SPDX-License-Identifier: BUSL-1.1

//! The key-value engine side of a secondary index.
//!
//! A KV collection keeps its rows in the `KvEngine`, which maintains its own
//! per-field secondary indexes on every write. `CREATE INDEX` on a KV
//! collection therefore builds the index there, and `DROP INDEX` removes it
//! there. Both run through the autocommit write funnel, which appends the
//! `kv_register_index` / `kv_drop_index` WAL record. That record and the KV
//! checkpoint carry the index across a restart.
//!
//! The KV engine indexes one top-level field by equality. It has no UNIQUE
//! check, no COLLATE NOCASE fold, no partial predicate and no array or nested
//! path, so `CREATE INDEX` refuses those forms on a KV collection before any
//! catalog entry is written.

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;

use super::super::super::super::result::DdlError;
use super::commit::err;

/// The index options a `CREATE INDEX` statement asks for.
pub(super) struct KvIndexOptions<'a> {
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
}

/// The KV field a canonical index path names.
///
/// Refuses the options and paths the KV engine cannot index, with SQLSTATE
/// 0A000.
pub(super) fn kv_field(
    collection: &str,
    canonical_field: &str,
    options: &KvIndexOptions<'_>,
) -> Result<String, DdlError> {
    let refuse = |what: &str| {
        err(
            "0A000",
            format!(
                "{what} is not supported on key-value collection '{collection}': \
                 its index matches one top-level field by equality"
            ),
        )
    };
    if options.unique {
        return Err(refuse("a UNIQUE index"));
    }
    if options.case_insensitive {
        return Err(refuse("COLLATE NOCASE"));
    }
    if options.predicate.is_some() {
        return Err(refuse("a partial index (WHERE)"));
    }
    let field = canonical_field
        .strip_prefix("$.")
        .unwrap_or(canonical_field);
    if field.is_empty() || field.contains('.') || field.contains('[') || field.starts_with('$') {
        return Err(refuse(&format!("the index path '{canonical_field}'")));
    }
    Ok(field.to_string())
}

/// Build the index on the core that holds the collection's rows, backfilled
/// from every row it already holds.
pub(super) async fn register_kv_index(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    coll: &StoredCollection,
    field: &str,
) -> Result<(), DdlError> {
    // The engine keeps a field's schema position with the index for the
    // checkpoint. An undeclared field has none, and extraction is by name.
    let field_position = coll
        .fields
        .iter()
        .position(|(name, _)| name == field)
        .unwrap_or(0);
    let plan = PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: QualifiedCollection::new(database_id, &coll.name),
        field: field.to_string(),
        field_position,
        backfill: true,
    });
    dispatch_durable(state, tenant_id, database_id, &coll.name, plan, "build").await
}

/// Remove the index from the core that holds the collection's rows. A field
/// with no index is already in the state the drop asks for.
pub(crate) async fn drop_kv_index(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    field: &str,
) -> Result<(), DdlError> {
    let plan = PhysicalPlan::Kv(KvOp::DropIndex {
        collection: QualifiedCollection::new(database_id, collection),
        field: field.to_string(),
    });
    dispatch_durable(state, tenant_id, database_id, collection, plan, "drop").await
}

/// Dispatch a KV index plan through the autocommit write funnel, which
/// appends its WAL record, and fail on a refused reply.
async fn dispatch_durable(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    step: &str,
) -> Result<(), DdlError> {
    let response = crate::control::server::dispatch_utils::dispatch_autocommit_write(
        state,
        crate::control::server::dispatch_utils::AutocommitWrite {
            tenant_id,
            database_id,
            vshard_id: VShardId::from_collection_in_database(database_id, collection),
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
        },
    )
    .await
    .map_err(|e| {
        err(
            "XX000",
            format!("key-value index {step} on '{collection}': {e}"),
        )
    })?;

    if response.status == crate::bridge::envelope::Status::Error {
        let detail = match response.error_code.as_deref() {
            Some(code) => format!("{code:?}"),
            None => String::from_utf8_lossy(&response.payload).into_owned(),
        };
        return Err(err(
            "XX000",
            format!("key-value index {step} on '{collection}' was refused: {detail}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain() -> KvIndexOptions<'static> {
        KvIndexOptions {
            unique: false,
            case_insensitive: false,
            predicate: None,
        }
    }

    #[test]
    fn a_top_level_field_is_indexed_by_its_name() {
        assert_eq!(
            kv_field("sessions", "$.region", &plain()).expect("plain field"),
            "region"
        );
    }

    #[test]
    fn options_and_paths_the_engine_cannot_index_are_refused() {
        for path in ["$.a.b", "$.tags[]", "$"] {
            let error = kv_field("sessions", path, &plain()).expect_err(path);
            assert_eq!(error.sqlstate, "0A000", "{path}");
        }
        let unique = KvIndexOptions {
            unique: true,
            ..plain()
        };
        assert_eq!(
            kv_field("sessions", "$.region", &unique)
                .expect_err("unique")
                .sqlstate,
            "0A000"
        );
        let partial = KvIndexOptions {
            predicate: Some("region = 'eu'"),
            ..plain()
        };
        assert!(kv_field("sessions", "$.region", &partial).is_err());
    }
}
