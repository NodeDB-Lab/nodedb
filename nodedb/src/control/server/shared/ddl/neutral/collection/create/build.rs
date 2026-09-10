// SPDX-License-Identifier: BUSL-1.1

//! Shared implementation behind `CREATE COLLECTION` and `CREATE TABLE`.
//!
//! Relocated verbatim from the pgwire `pgwire::ddl::collection::create::build`
//! module (now deleted). The two surface DDLs differ in only five places: the
//! error label ("collection" vs "table"), whether an empty column list is
//! allowed, the default `CollectionType` when no engine is named (schemaless
//! vs strict), the audit-log verb, and the response tag. Everything in
//! between — name validation, duplicate check, engine validation, schema
//! construction, vector-primary parsing, flag validation, `StoredCollection`
//! assembly, propose+apply, SERIAL sequence auto-creation, vector-field
//! auto-config — is identical, and is preserved verbatim here; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! [`build_and_persist`] is the single body; [`Variant`] supplies the five
//! differences declaratively. Name/flag validation lives in
//! [`build_flags`], vector-primary resolution in [`build_primary_engine`],
//! and post-create side effects in [`build_post_create`].

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::catalog::propose_and_apply;
use super::super::super::super::result::{DdlError, DdlResult};
use super::super::enforcement::{
    parse_and_validate_balanced_clause, resolve_custom_type_columns, validate_hash_chain_flags,
};
use super::engine_option::validate_engine_name;
use super::request::CreateCollectionRequest;

use super::build_flags::{err, resolve_crdt_flag, validate_crdt_signing_storage, validate_name};
use super::build_post_create::{create_serial_sequences, log_vector_fields};
use super::build_primary_engine::resolve_primary_engine;
use crate::control::server::shared::ddl::neutral::column_default::validate_column_defaults;

/// Per-surface configuration. The fields are the entire surface-level
/// difference between `CREATE COLLECTION` and `CREATE TABLE`.
pub struct Variant {
    /// Object-class label used in the duplicate-name / empty-columns
    /// error messages and in the audit log entry.
    /// `"collection"` for CREATE COLLECTION, `"table"` for CREATE TABLE.
    pub label: &'static str,
    /// Response tag returned on success.
    /// `"CREATE COLLECTION"` / `"CREATE TABLE"`.
    pub response_tag: &'static str,
    /// CREATE TABLE requires a column list by convention; CREATE
    /// COLLECTION accepts an empty one (schemaless documents).
    pub require_columns: bool,
    /// `default_strict` argument to `build_collection_type` when no
    /// engine is named in WITH: CREATE COLLECTION → schemaless,
    /// CREATE TABLE → strict.
    pub default_strict: bool,
}

/// Shared body. Validates the request, builds the
/// `StoredCollection`, replicates it through the metadata raft
/// group, and runs the post-create side effects (SERIAL sequence
/// auto-creation, vector-field logging, audit).
pub async fn build_and_persist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateCollectionRequest<'_>,
    database_id: DatabaseId,
    variant: &Variant,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateCollectionRequest {
        name,
        engine,
        columns,
        options,
        flags,
        balanced_raw,
    } = *req;

    validate_name(name, variant.label)?;
    if variant.require_columns && columns.is_empty() {
        return Err(err(
            "42601",
            "CREATE TABLE requires a column list; for schemaless collections use CREATE COLLECTION"
                .to_string(),
        ));
    }

    // Refuse a DEFAULT the server cannot evaluate here, not at the first
    // INSERT. It runs before any lifecycle guard or predecessor purge, so a
    // rejected declaration leaves the existing state untouched. A SERIAL
    // column carries no DEFAULT text yet; the `nextval` this build generates
    // for it names a registered function and clears the same gate.
    validate_column_defaults(columns)?;

    let tenant_id = identity.tenant_id;

    // Metadata Raft serializes clustered DDL. Without it, hold an exclusive
    // per-name lifecycle guard across validation, any predecessor reclaim,
    // catalog creation, and Data Plane registration.
    let mut local_lifecycle = if state.metadata_raft.get().is_none() {
        Some(
            state
                .quiesce
                .acquire_lifecycle(database_id.as_u64(), tenant_id.as_u64(), name)
                .await,
        )
    } else {
        None
    };

    // A materialized-view definition durably owns its same-name target even if
    // a crash occurred between definition and target registration.
    let catalog = state.credentials.catalog();
    if catalog
        .get_materialized_view(database_id.as_u64(), tenant_id.as_u64(), name)
        .map_err(|error| err("XX000", error.to_string()))?
        .is_some()
    {
        return Err(err(
            "42P07",
            format!("materialized view '{name}' already owns this collection name"),
        ));
    }

    // Check if the object already exists. A catalog-read fault must abort the
    // CREATE — proceeding as if no row exists could build a fresh collection
    // over a soft-deleted incarnation's still-present storage.
    let existing = catalog
        .get_collection(database_id, tenant_id.as_u64(), name)
        .map_err(|error| err("XX000", error.to_string()))?;
    if let Some(existing) = existing {
        if existing.is_active {
            return Err(err(
                "42P07",
                format!("{} '{name}' already exists", variant.label),
            ));
        }

        // Soft-deleted collection with the same name. A re-CREATE is an
        // explicit request for a FRESH collection, distinct from UNDROP
        // recovery — so the old catalog row and its Data Plane storage
        // keys must be gone before the new collection registers over the
        // reused `{db}:{tenant}:{name}:` storage prefix. Otherwise the
        // stale rows resurrect until the retention GC runs (days later).
        //
        // Hard-purge synchronously through the SAME path DROP ... PURGE
        // uses: remove the catalog row + reclaim every engine's storage
        // on the Data Plane, awaiting completion before we proceed. The
        // WAL tombstone boundary is the current `next_lsn`: every pre-drop
        // row sits below it and is shadowed on replay, while every row the
        // new collection writes sits at or above it and survives.
        let purge_lsn = state.wal.next_lsn().as_u64();
        // Fail closed: if the hard-purge could not remove the old
        // catalog row, ABORT the CREATE rather than build a new
        // collection over un-purged data (which would resurrect the
        // stale rows). Surface as an internal error to the client.
        let purge_result =
            crate::control::server::shared::ddl::neutral::collection::purge::hard_purge_collection(
                state,
                database_id.as_u64(),
                tenant_id.as_u64(),
                name,
                purge_lsn,
                local_lifecycle.is_some(),
            )
            .await;
        if let Err(failure) = purge_result {
            // Only disarm when a durable retry record owns the drain. Otherwise
            // let the guard release the in-memory hold so this same-name CREATE
            // can be retried against the durable inactive catalog row.
            if failure.retry_queued
                && let Some(guard) = local_lifecycle.take()
            {
                guard.disarm();
            }
            return Err(err("XX000", failure.error.to_string()));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let canonical_engine = validate_engine_name(engine, options)?;
    let bitemporal_flag = flags.iter().any(|f| f == "BITEMPORAL");

    // Expand SERIAL / BIGSERIAL first, so every later reader of the column
    // list sees the backing type plus the `nextval` DEFAULT naming the
    // sequence `create_serial_sequences` materializes below.
    let (expanded_columns, serial_fields) =
        crate::control::server::shared::ddl::schema_validation::parse_fields_clause_from_pairs(
            name, columns,
        );

    // Resolve user-defined type names to TEXT for schema building.
    // Original names are preserved in `fields` for drop-protection.
    let resolved_columns: Vec<(String, String)> = resolve_custom_type_columns(
        &expanded_columns,
        state,
        database_id.as_u64(),
        tenant_id.as_u64(),
    );

    let (collection_type, columnar_schema_columns) = nodedb_sql::ddl_ast::build_collection_type(
        canonical_engine,
        &resolved_columns,
        options,
        bitemporal_flag,
        variant.default_strict,
    )
    .map_err(|e| err("42601", e.to_string()))?;

    let mut fields = expanded_columns.clone();
    if fields.is_empty() && !columnar_schema_columns.is_empty() {
        fields = columnar_schema_columns;
    }

    let schema_json = match &collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            sonic_rs::to_string(schema).ok()
        }
        nodedb_types::CollectionType::KeyValue(config) => sonic_rs::to_string(config).ok(),
        _ => None,
    };

    let (primary, vector_primary) =
        resolve_primary_engine(options, &expanded_columns, &fields, &collection_type)?;

    let append_only = flags.iter().any(|f| f == "APPEND_ONLY");
    let hash_chain = flags.iter().any(|f| f == "HASH_CHAIN");
    let bitemporal = bitemporal_flag;
    let crdt_signing_required = flags.iter().any(|flag| flag == "SIGNED_DELTAS");
    validate_hash_chain_flags(hash_chain, append_only)
        .map_err(|e| err(e.sqlstate(), e.to_string()))?;

    let crdt = resolve_crdt_flag(options, &collection_type)?;
    validate_crdt_signing_storage(
        crdt_signing_required,
        crdt,
        state.wal.payloads_authenticated(),
    )?;
    // Checked only against an ENFORCED schema. Custom-typed columns are
    // physically TEXT, so the resolved list is the one to check.
    //
    // A schemaless collection is deliberately not checked: its field list is
    // advisory, a write may carry any field whether or not it appears there,
    // and the commit-time check reads whatever the row actually holds. Refusing
    // a BALANCED column that is merely absent from that list would reject
    // `CREATE COLLECTION x WITH BALANCED ON (...)` — a declaration with no
    // column list at all, which is the ordinary schemaless spelling.
    let balanced =
        parse_and_validate_balanced_clause(balanced_raw.unwrap_or(""), &resolved_columns)
            .map_err(|e| err(e.sqlstate(), e.to_string()))?;

    let partition_strategy =
        nodedb_types::PartitionStrategy::default_for_collection_type(&collection_type);

    // Extract the declared PRIMARY KEY column name (if any) from the raw
    // column list. Recorded on every engine so schemaless collections can
    // key their document id off it instead of the hardcoded `id` field;
    // harmless for strict/KV, which already track the PK on their schema.
    let declared_primary_key = expanded_columns.iter().find_map(|(col_name, type_str)| {
        let (_, is_pk, _, _) =
            nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full(type_str);
        is_pk.then(|| col_name.clone())
    });

    let coll = StoredCollection {
        tenant_id: tenant_id.as_u64(),
        name: name.to_string(),
        owner: identity.username.clone(),
        created_at: now,
        deactivated_at_ns: 0,
        descriptor_version: 0,
        constraint_version: 0,
        crdt_signing_required,
        modification_hlc: nodedb_types::Hlc::ZERO,
        fields,
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type,
        timeseries_config: schema_json,
        conflict_policy: None,
        is_active: true,
        append_only,
        hash_chain,
        balanced,
        last_chain_hash: None,
        period_lock: None,
        retention_period: None,
        legal_holds: Vec::new(),
        state_constraints: Vec::new(),
        transition_checks: Vec::new(),
        type_guards: Vec::new(),
        check_constraints: Vec::new(),
        materialized_sums: Vec::new(),
        lvc_enabled: false,
        bitemporal,
        crdt,
        permission_tree_def: None,
        indexes: Vec::new(),
        size_bytes_estimate: 0,
        primary,
        vector_primary,
        partition_strategy,
        database_id,
        cloned_from: None,
        clone_status: nodedb_types::CloneStatus::default(),
        has_implicit_edges: false,
        declared_primary_key,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    propose_and_apply(state, &entry)?;

    log_vector_fields(name, &coll.fields);
    create_serial_sequences(state, identity, database_id, name, &serial_fields, now)?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created {} '{name}'", variant.label),
    );

    Ok(vec![DdlResult::Status {
        command: variant.response_tag.to_string(),
        rows_affected: None,
    }])
}
