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

        // A violation names the collection the user wrote, never the
        // database-qualified key the plan carries.
        let user_collection = match nodedb_types::CollectionKey::from_qualified_str(
            task.request.database_id,
            collection,
        ) {
            Ok(key) => key.name(),
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };

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
            type_guards:
                crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
                    user_collection,
                    &enforcement.type_guards,
                ),
            incarnation,
        };

        // Rehydrate the durable CRDT conflict-resolution policy (if any) into
        // this core's `PolicyRegistry`. Runs on every `Register` — live DDL
        // apply AND boot rehydration replay — so `ALTER COLLECTION ... SET ON
        // CONFLICT ...` survives a restart. A policy that does not apply
        // refuses the register before the collection config is installed.
        let mut replaced_policy = None;
        if let Some(policy_json) = conflict_policy {
            let applied = self
                .get_crdt_engine(task.request.database_id, crate::types::TenantId::new(tid))
                .and_then(|engine| {
                    let previous = engine.registered_collection_policy(collection);
                    engine.set_collection_policy(collection, policy_json)?;
                    Ok(previous)
                });
            match applied {
                Ok(previous) => replaced_policy = Some(previous),
                Err(e) => {
                    warn!(
                        core = self.core_id,
                        %collection,
                        error = %e,
                        "persisted conflict policy did not apply on register"
                    );
                    return self.response_error(task, ErrorCode::from(e));
                }
            }
        }

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let last_value_cache = timeseries.map(|schema| schema.last_value_cache);
        let previous_config = self.doc_configs.insert(config_key.clone(), config);

        // `SET LAST_VALUE_CACHE` travels on the registration: turning it on
        // builds the cache from the stored rows, turning it off drops it. A
        // refused register leaves the config and the policy it found.
        if let Some(enabled) = last_value_cache
            && let Err(e) = self.sync_last_value_cache(&config_key, enabled)
        {
            warn!(
                core = self.core_id,
                %collection,
                error = %e,
                "last-value cache did not build on register"
            );
            self.undo_register(config_key, previous_config, replaced_policy);
            return self.response_error(task, ErrorCode::from(e));
        }

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
                self.undo_register(config_key, previous_config, replaced_policy);
                return self.response_error(task, ErrorCode::from(e));
            }
        }

        self.response_ok(task)
    }

    /// Put back what a refused register replaced. A `None` config removes the
    /// config the register installed. `replaced_policy` is `Some` when the
    /// register set a policy, and holds the policy it replaced, if any.
    fn undo_register(
        &mut self,
        key: (crate::types::DatabaseId, crate::types::TenantId, String),
        previous_config: Option<crate::engine::document::store::CollectionConfig>,
        replaced_policy: Option<Option<nodedb_crdt::policy::CollectionPolicy>>,
    ) {
        // The register reached the engine to set the policy, so it exists.
        if let Some(previous) = replaced_policy
            && let Some(engine) = self.crdt_engines.get_mut(&(key.0, key.1))
        {
            engine.restore_collection_policy(&key.2, previous);
        }
        match previous_config {
            Some(config) => {
                self.doc_configs.insert(key, config);
            }
            None => {
                self.doc_configs.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{EnforcementOptions, StorageMode, TimeseriesSchema};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::timeseries::{TimeseriesApplyMode, TimeseriesIngestExec};
    use crate::types::{DatabaseId, TenantId};

    const TID: u64 = 1;
    const COLLECTION: &str = "metrics";

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        )
    }

    fn register(core: &mut CoreLoop, last_value_cache: bool, hlc: u64) -> Status {
        register_with_policy(core, last_value_cache, hlc, None)
    }

    fn register_with_policy(
        core: &mut CoreLoop,
        last_value_cache: bool,
        hlc: u64,
        conflict_policy: Option<&str>,
    ) -> Status {
        let schema = TimeseriesSchema {
            time_key: "timestamp".into(),
            columns: Vec::new(),
            partition_interval: "1h".into(),
            last_value_cache,
        };
        let task = make_default_task();
        core.execute_register_document_collection(
            &task,
            RegisterDocumentCollectionParams {
                tid: TID,
                collection: COLLECTION,
                indexes: &[],
                crdt_enabled: false,
                storage_mode: &StorageMode::Schemaless,
                enforcement: &EnforcementOptions::default(),
                bitemporal: false,
                conflict_policy,
                timeseries: Some(&schema),
                vector_primary: None,
                vector_fields: &[],
                declared_columns: &[],
                declared_key: None,
                incarnation: nodedb_types::Hlc::new(hlc, 0),
            },
        )
        .status
    }

    /// One flushed partition, with its directory then removed: the registry
    /// lists it, so a last-value cache build does not read.
    fn unreadable_partition(core: &mut CoreLoop) {
        let task = make_default_task();
        let ingested = core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: TenantId::new(TID),
            collection: COLLECTION,
            payload: b"metrics,host=a value=1i 1000000000",
            format: "ilp",
            wal_lsn: Some(1),
            provenance: None,
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        });
        assert_eq!(ingested.status, Status::Ok, "{:?}", ingested.error_code);
        core.flush_ts_collection(TenantId::new(TID), DatabaseId::DEFAULT, COLLECTION, 0)
            .expect("flush");
        let ts_dir = core.ts_dir_of(&key());
        let dirs: Vec<String> = core
            .ts_registries
            .get(&key())
            .expect("registry")
            .current_partitions()
            .map(|entry| entry.dir_name.clone())
            .collect();
        assert!(!dirs.is_empty(), "the flush wrote a partition");
        for dir in dirs {
            std::fs::remove_dir_all(ts_dir.join(dir)).expect("remove partition");
        }
    }

    /// A register refused by its cache build leaves the prior config. The new
    /// config stays out, with its cache flag and incarnation.
    #[test]
    fn a_refused_register_restores_the_prior_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        assert_eq!(register(&mut core, false, 1), Status::Ok);
        unreadable_partition(&mut core);

        assert_ne!(register(&mut core, true, 2), Status::Ok);

        let config = core.doc_configs.get(&key()).expect("prior config");
        assert_eq!(config.incarnation, nodedb_types::Hlc::new(1, 0));
        let schema = config.timeseries.as_ref().expect("timeseries schema");
        assert!(!schema.last_value_cache);
    }

    fn policy_json(policy: nodedb_crdt::policy::CollectionPolicy) -> String {
        sonic_rs::to_string(&policy).expect("policy json")
    }

    fn registered_policy(core: &CoreLoop) -> Option<String> {
        core.crdt_engines
            .get(&(DatabaseId::DEFAULT, TenantId::new(TID)))
            .expect("the register made the engine")
            .registered_collection_policy(COLLECTION)
            .map(policy_json)
    }

    /// A refused register leaves the policy it found. Every refusal after the
    /// policy applies runs this one undo, a failed incarnation bind included.
    #[test]
    fn a_refused_register_restores_the_prior_policy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let prior = policy_json(nodedb_crdt::policy::CollectionPolicy::ephemeral());
        let refused = policy_json(nodedb_crdt::policy::CollectionPolicy::strict());
        assert_ne!(prior, refused);
        assert_eq!(
            register_with_policy(&mut core, false, 1, Some(&prior)),
            Status::Ok
        );
        unreadable_partition(&mut core);

        assert_ne!(
            register_with_policy(&mut core, true, 2, Some(&refused)),
            Status::Ok
        );

        assert_eq!(registered_policy(&core), Some(prior));
    }

    /// A refused register that found no policy leaves none.
    #[test]
    fn a_refused_register_removes_the_policy_it_set() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        assert_eq!(register(&mut core, false, 1), Status::Ok);
        unreadable_partition(&mut core);
        let refused = policy_json(nodedb_crdt::policy::CollectionPolicy::strict());

        assert_ne!(
            register_with_policy(&mut core, true, 2, Some(&refused)),
            Status::Ok
        );

        assert_eq!(registered_policy(&core), None);
    }

    /// A first register refused by its cache build installs no config.
    #[test]
    fn a_refused_first_register_installs_no_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        assert_eq!(register(&mut core, false, 1), Status::Ok);
        unreadable_partition(&mut core);
        core.doc_configs.remove(&key());

        assert_ne!(register(&mut core, true, 2), Status::Ok);

        assert!(!core.doc_configs.contains_key(&key()));
    }
}
