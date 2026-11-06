// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SORTED INDEX` / `DROP SORTED INDEX`.
//!
//! Creation registers the index in the catalog index registry, which is what
//! binds the index name to the collection it was built over. Every later read
//! of the index — `RANK`, `TOPK`, `RANGE`, `SORTED_COUNT` — names only the
//! index, so that record is the sole thing that can resolve the collection
//! those reads must be authorized against.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::IndexKind;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::server::shared::session::ddl_buffer;
use crate::control::server::shared::session::ddl_effect::DeferredDdlEffect;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::super::super::index_registry::{
    IndexRegistration, propose_delete_index_record, propose_index_record,
};
use super::super::super::result::{DdlError, DdlResult};
use super::super::refuse_gate::RefusingReadGate;
use super::dispatch::{SortedIndexTarget, drop_in_engine, register_in_engine};
use super::gate::owning_collection;
use super::parse::{ddl_err, parse_sort_columns};
use super::tail::{IndexTail, parse_index_tail};

/// Building the index reads every row of the collection into the order-stat
/// tree, so the caller must be allowed to read it, and a read policy makes the
/// index underivable: it is shared by every reader, so one principal's
/// filtered view will answer other principals' `TOPK` from rows that were
/// never indexed.
const CREATE_WHAT: &str =
    "CREATE SORTED INDEX, which backfills the index from every row of the collection";

/// Handle `CREATE SORTED INDEX name ON collection (col DIR, ...) KEY key_col [WINDOW ...]`
pub async fn create_sorted_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // Extract index name.
    let rest = nodedb_types::strip_prefix_ascii_case_insensitive(sql, "CREATE SORTED INDEX ")
        .ok_or_else(|| ddl_err("42601", "expected CREATE SORTED INDEX"))?;
    let index_name = parse_ident_token(
        rest.split_whitespace()
            .next()
            .ok_or_else(|| ddl_err("42601", "missing index name"))?,
    )?;

    // Extract collection name after ON.
    let on_pos = find_ascii_case_insensitive(sql, " ON ")
        .ok_or_else(|| ddl_err("42601", "missing ON clause"))?;
    let after_on = sql[on_pos + 4..].trim();
    let collection = parse_ident_token(
        after_on
            .split_whitespace()
            .next()
            .ok_or_else(|| ddl_err("42601", "missing collection name after ON"))?,
    )?;

    // Extract sort columns from parentheses.
    let paren_start = sql
        .find('(')
        .ok_or_else(|| ddl_err("42601", "missing '(' for sort columns"))?;
    let paren_end = sql
        .find(')')
        .ok_or_else(|| ddl_err("42601", "missing ')' for sort columns"))?;
    let cols_str = &sql[paren_start + 1..paren_end];
    let sort_columns = parse_sort_columns(cols_str)?;

    // KEY column and optional WINDOW clause, from the original text so quoted
    // identifiers keep their case.
    let IndexTail { key_column, window } = parse_index_tail(&sql[paren_end + 1..], &sort_columns)?;

    let tenant_id = identity.tenant_id;
    if state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &collection)
        .map_err(|e| DdlError::from_error(&e))?
        .is_none()
    {
        return Err(ddl_err(
            "42P01",
            format!("collection '{collection}' not found"),
        ));
    }

    RefusingReadGate::open(state, identity, database_id, &collection, CREATE_WHAT)?;

    let fields: Vec<String> = sort_columns.iter().map(|(name, _)| name.clone()).collect();
    let plan = PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
        index_name: index_name.clone(),
        sort_columns,
        key_column,
        window_type: window.window_type,
        window_timestamp_column: window.timestamp_column,
        window_start_ms: window.start_ms,
        window_end_ms: window.end_ms,
    });

    // Inside an explicit transaction the records are buffered first and the
    // tree is built at COMMIT, so a ROLLBACK leaves no tree behind.
    let in_transaction = ddl_buffer::is_active();

    // Routed by the collection, not by the index name: the backfill this plan
    // performs reads the collection's rows out of the `KvEngine` of whichever
    // core executes it, and every later write that must keep the tree current
    // lands on the collection's own core. Registering anywhere else builds an
    // empty tree that no write ever updates.
    let response = if in_transaction {
        vec![DdlResult::Status {
            command: "CREATE SORTED INDEX".to_string(),
            rows_affected: None,
        }]
    } else {
        // The engine builds the tree before the catalog entry, so the catalog
        // chokepoint comes too late to refuse the build.
        super::super::audit_intent::record_action_intent(
            state,
            identity,
            Some(tenant_id),
            &format!("CREATE SORTED INDEX {index_name} ON {collection}"),
        )?;
        register_in_engine(
            state,
            &SortedIndexTarget {
                tenant_id,
                database_id,
                collection: &collection,
            },
            plan.clone(),
            "CREATE SORTED INDEX",
        )
        .await?
    };

    // Identity record: what resolves the index's owning collection on every
    // later read, what SHOW INDEXES lists, and what DROP INDEX resolves.
    propose_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: &index_name,
            kind: IndexKind::Sorted,
            collection: &collection,
            fields,
        },
    )
    .await?;

    // Ownership record backs authorization for a later DROP.
    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        IndexKind::Sorted.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
        &identity.username,
    )
    .await?;

    if in_transaction {
        let deferred = ddl_buffer::defer_effect(DeferredDdlEffect::SortedIndexRegister {
            tenant_id,
            database_id,
            collection: collection.clone(),
            plan,
        });
        if !deferred {
            return Err(DdlError::internal(
                "CREATE SORTED INDEX: the transaction buffer took no entry to defer the \
                 index build on",
            ));
        }
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created sorted index '{index_name}' on '{collection}'"),
    );

    Ok(response)
}

/// Handle `DROP SORTED INDEX name`
pub async fn drop_sorted_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let rest = nodedb_types::strip_prefix_ascii_case_insensitive(sql, "DROP SORTED INDEX ")
        .ok_or_else(|| ddl_err("42601", "expected DROP SORTED INDEX"))?;
    let index_name = parse_ident_token(
        rest.split_whitespace()
            .next()
            .ok_or_else(|| ddl_err("42601", "missing index name"))?,
    )?;

    let tenant_id = identity.tenant_id;
    // Resolved before the engine state goes: the registry row is what the
    // catalog cleanup below is keyed on, and it is unresolvable afterwards.
    // An unregistered name has no collection to authorize against, so it
    // fails closed with the same "not found" the read gates use rather than
    // dropping engine state ungated.
    let collection = owning_collection(state, identity, database_id, &index_name)?;

    // Ownership check against the row this index files under, mirroring the
    // generic `DROP INDEX` teardown's owner-or-admin gate
    // (`neutral/collection/index/drop.rs`): the caller must be the index's
    // recorded owner, a superuser, or hold TenantAdmin.
    let is_owner = state
        .permissions
        .get_owner(
            IndexKind::Sorted.owner_object_type(),
            database_id.as_u64(),
            tenant_id,
            &index_name,
        )
        .as_deref()
        == Some(&identity.username);

    if !is_owner
        && !identity.is_superuser
        && !identity.has_role(&crate::control::security::identity::Role::TenantAdmin)
    {
        return Err(ddl_err(
            "42501",
            "permission denied: must be index owner or admin",
        ));
    }

    // Autocommit drops the tree first, so a failed drop keeps the records a
    // retry resolves through. Inside an explicit transaction the records are
    // buffered first and the tree is dropped at COMMIT.
    let target = SortedIndexTarget {
        tenant_id,
        database_id,
        collection: &collection,
    };
    let in_transaction = ddl_buffer::is_active();
    if !in_transaction {
        super::super::audit_intent::record_action_intent(
            state,
            identity,
            Some(tenant_id),
            &format!("DROP SORTED INDEX {index_name} ON {collection}"),
        )?;
        drop_in_engine(state, &target, &index_name).await?;
    }

    propose_delete_index_record(state, database_id, tenant_id, &index_name, &collection).await?;
    crate::control::server::shared::ddl::owner::propose_delete_owner(
        state,
        IndexKind::Sorted.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
    )
    .await?;

    if in_transaction
        && !ddl_buffer::defer_effect(DeferredDdlEffect::SortedIndexDrop {
            tenant_id,
            database_id,
            collection: collection.clone(),
            index_name: index_name.clone(),
        })
    {
        return Err(DdlError::internal(
            "DROP SORTED INDEX: the transaction buffer took no entry to defer the index \
             drop on",
        ));
    }

    Ok(vec![DdlResult::Status {
        command: "DROP SORTED INDEX".to_string(),
        rows_affected: None,
    }])
}
