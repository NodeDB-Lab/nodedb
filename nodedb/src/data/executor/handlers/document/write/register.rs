// SPDX-License-Identifier: BUSL-1.1

//! `DocumentOp::Register` — bind a document collection's secondary-index /
//! storage-mode / enforcement configuration into this core's in-memory
//! `doc_configs`, and rehydrate any persisted CRDT conflict policy.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_register_document_collection`].
pub(in crate::data::executor) struct RegisterDocumentCollectionParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub indexes: &'a [nodedb_physical::physical_plan::RegisteredIndex],
    pub crdt_enabled: bool,
    pub storage_mode: &'a nodedb_physical::physical_plan::StorageMode,
    pub enforcement: &'a nodedb_physical::physical_plan::EnforcementOptions,
    pub bitemporal: bool,
    /// Durable CRDT conflict-resolution policy (JSON-serialized
    /// `CollectionPolicy`), persisted on the collection's catalog record.
    /// `Some` rehydrates this core's `PolicyRegistry` so the policy survives
    /// register/reboot instead of falling back to `CollectionPolicy::ephemeral()`.
    pub conflict_policy: Option<&'a str>,
    /// Declared columns + designated `TIME_KEY` when this is a timeseries
    /// collection; `None` for every other engine.
    pub timeseries: Option<&'a nodedb_physical::physical_plan::TimeseriesSchema>,
    /// Vector-primary access-path config when this is a
    /// `WITH (primary='vector')` collection; `None` for every other engine.
    /// The read path decodes this collection's sparse rows as `zerompk`
    /// TAGGED sidecars solely on the strength of this marker.
    pub vector_primary: Option<&'a nodedb_types::VectorPrimaryConfig>,
    /// Declared `VECTOR(n)` columns of a schemaless collection, as
    /// `(column, n)`. Empty for every other collection.
    pub vector_fields: &'a [(String, usize)],
    /// Declared numeric columns of a schemaless or KV collection. Empty for
    /// every other collection.
    pub declared_columns: &'a [nodedb_physical::physical_plan::DeclaredColumn],
    /// The collection's declared key column. `None` when rows key by the
    /// implicit `id` or `_rowid`.
    pub declared_key: Option<&'a str>,
    /// The collection's catalog incarnation, which its CRDT document's Loro
    /// peer id derives from.
    pub incarnation: nodedb_types::Hlc,
}

impl CoreLoop {
    /// Register a document collection's secondary index configuration.
    ///
    /// Stores the `CollectionConfig` in `self.doc_configs` so that subsequent
    /// `PointPut` and `DocumentBatchInsert` operations extract and write secondary
    /// index entries automatically.
    pub(in crate::data::executor) fn execute_register_document_collection(
        &mut self,
        task: &ExecutionTask,
        params: RegisterDocumentCollectionParams<'_>,
    ) -> Response {
        let RegisterDocumentCollectionParams {
            tid,
            collection,
            indexes,
            crdt_enabled,
            storage_mode,
            enforcement,
            bitemporal,
            conflict_policy,
            timeseries,
            vector_primary,
            vector_fields,
            declared_columns,
            declared_key,
            incarnation,
        } = params;
        let mode_label = match storage_mode {
            nodedb_physical::physical_plan::StorageMode::Schemaless => "document_schemaless",
            nodedb_physical::physical_plan::StorageMode::Strict { .. } => "document_strict",
        };
        debug!(
            core = self.core_id,
            %collection,
            index_count = indexes.len(),
            crdt_enabled,
            storage_mode = mode_label,
            append_only = enforcement.append_only,
            hash_chain = enforcement.hash_chain,
            balanced = enforcement.balanced.is_some(),
            "register document collection"
        );

        // Struct literal with every field named — never `..Default::default()`.
        // A `CollectionConfig` field left unassigned here would make the
        // attribute silently absent on every registered collection instead of
        // failing to compile.
        let config = crate::engine::document::store::CollectionConfig {
            name: collection.to_string(),
            index_paths: indexes
                .iter()
                .map(crate::engine::document::store::IndexPath::from_registered)
                .collect(),
            crdt_enabled,
            storage_mode: storage_mode.clone(),
            enforcement: enforcement.clone(),
            bitemporal,
            conflict_policy: conflict_policy.map(str::to_string),
            timeseries: timeseries.map(|ts| Box::new(ts.clone())),
            vector_primary: vector_primary.map(|vp| Box::new(vp.clone())),
            vector_fields: vector_fields.to_vec(),
            declared_columns: declared_columns.to_vec(),
            declared_key: declared_key.map(str::to_string),
            // A violation names the collection the user wrote, never the
            // database-qualified key the plan carries.
            type_guards:
                crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
                    nodedb_types::QualifiedCollection::from_stored(collection.to_string())
                        .collection_name(task.request.database_id),
                    &enforcement.type_guards,
                ),
            incarnation,
        };

        // Rehydrate the durable CRDT conflict-resolution policy (if any) into
        // this core's `PolicyRegistry`. Runs on every `Register` — live DDL
        // apply AND boot rehydration replay — so `ALTER COLLECTION ... SET ON
        // CONFLICT ...` survives a restart. A policy that does not apply
        // refuses the register before the collection config is installed.
        if let Some(policy_json) = conflict_policy {
            let applied = self
                .get_crdt_engine(task.request.database_id, crate::types::TenantId::new(tid))
                .and_then(|engine| engine.set_collection_policy(collection, policy_json));
            if let Err(e) = applied {
                warn!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "persisted conflict policy did not apply on register"
                );
                return self.response_error(task, ErrorCode::from(e));
            }
        }

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.insert(config_key, config);

        // The tenant's CRDT engine derives the collection's Loro peer id from
        // its incarnation. An engine created later binds it from
        // `doc_configs` (`get_crdt_engine`).
        if self
            .crdt_engines
            .contains_key(&(task.request.database_id, crate::types::TenantId::new(tid)))
        {
            let bound = self
                .get_crdt_engine(task.request.database_id, crate::types::TenantId::new(tid))
                .and_then(|engine| engine.bind_incarnation(collection, incarnation));
            if let Err(e) = bound {
                return self.response_error(task, ErrorCode::from(e));
            }
        }

        self.response_ok(task)
    }
}
