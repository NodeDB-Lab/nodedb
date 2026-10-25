// SPDX-License-Identifier: BUSL-1.1

//! `impl SqlCatalog for OriginCatalog` — relation/collection/array lookups.

use nodedb_cluster::{DescriptorId, DescriptorKind};
use nodedb_sql::{
    SqlCatalog, SqlCatalogError,
    types::{ArrayCatalogView, CollectionInfo},
};

use super::adapter::OriginCatalog;
use super::type_convert::convert_collection_type;

impl SqlCatalog for OriginCatalog {
    /// The built-in registry plus the user-defined CRSs of this catalog.
    fn crs_context(&self) -> nodedb_crs::CrsContext<'_> {
        crate::control::crs_context::catalog_crs_context(self.credentials.catalog())
    }

    /// Resolve any relation name to planner metadata. Catalog tables
    /// (pg_class, information_schema.*, _system.*, etc.) are returned
    /// directly from the static schema registry without a catalog round-trip.
    /// All other names fall through to `get_collection`.
    fn resolve_relation(
        &self,
        _database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        if let Some(info) =
            crate::control::server::pgwire::catalog::schema::catalog_collection_info(name)
        {
            return Ok(Some(info));
        }
        self.get_collection(_database_id, name)
    }

    fn get_collection(
        &self,
        _database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        // Read through the local `SystemCatalog` redb. On cluster
        // followers, the `MetadataCommitApplier` has already
        // written the replicated record here via
        // `CatalogEntry::apply_to`, so a single read path works
        // for both single-node and cluster modes.
        //
        // Use `self.database_id` (the session-bound database) rather than
        // the `_database_id` parameter, which is always `DatabaseId::DEFAULT`
        // from the nodedb-sql planner. This enforces per-database namespace
        // isolation at plan time: a query in `db_alpha` cannot resolve a
        // collection that lives in `db_beta`.
        let catalog = self.credentials.catalog();
        let Some(stored) = catalog
            .get_collection(self.database_id, self.tenant_id, name)
            .ok()
            .flatten()
        else {
            // Collections and continuous aggregates share one relation
            // namespace, so a name no collection holds can be an aggregate.
            return super::cagg_relation::continuous_aggregate_relation(
                catalog,
                self.database_id,
                self.tenant_id,
                name,
            );
        };
        if !stored.is_active {
            // Soft-deleted: surface a distinct error so the pgwire
            // handler renders the UNDROP hint instead of "unknown
            // table". Retention window uses the default config —
            // per-tenant override resolution is tracked as its own
            // checklist item.
            let retention = crate::config::server::RetentionSettings::default()
                .retention_window()
                .as_nanos() as u64;
            // `deactivated_at_ns == 0` means the drop time is unknown (a
            // pre-upgrade row the GC sweeper has not yet adopted a time
            // for — see `event::collection_gc::policy::PurgeDecision`,
            // which never purges such a row). Report `u64::MAX` rather
            // than `0 + retention`, which will falsely tell the caller
            // the row is already past its window.
            let retention_expires_at_ns = if stored.deactivated_at_ns == 0 {
                u64::MAX
            } else {
                stored.deactivated_at_ns.saturating_add(retention)
            };
            return Err(SqlCatalogError::CollectionDeactivated {
                name: name.to_string(),
                retention_expires_at_ns,
            });
        }

        // Record the observed descriptor version so the caller
        // can use the resulting set as a per-descriptor plan
        // cache key. The set is drained via
        // `take_recorded_versions` once planning finishes.
        //
        // Version 0 is the pre-B.1 sentinel; we record it as 1
        // so the cache's freshness check uses the same floor
        // that the drain gate uses. If the descriptor later
        // stamps its first real version 1, the cache stays
        // valid; if it bumps to 2+, the cache correctly
        // invalidates.
        let descriptor_id = DescriptorId::new(
            self.database_id.as_u64(),
            self.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        );
        let version = stored.descriptor_version.max(1);
        {
            let mut guard = self
                .recorded_versions
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            guard.record(descriptor_id.clone(), version);
        }

        // Drain observation: if a DDL is currently draining
        // this descriptor at the version we read, return
        // `RetryableSchemaChanged` so the pgwire handler's
        // retry loop re-plans. Without this check the planner
        // will compile a plan against a version that's about
        // to be retired, and only the post-plan lease acquisition
        // will notice — or (worse) it will succeed on first
        // holder because the drain finished right before the
        // refcount check. Catching it here keeps the common case
        // cheap; the acquisition sits inside the same retry unit
        // for the drain that starts after this read.
        //
        // Leases themselves are NOT acquired here.
        // The handler calls
        // `SharedState::acquire_plan_lease_scope` after
        // planning finishes (or after a cache hit returns a
        // pre-recorded version set), which increments
        // refcounts, performs a single raft acquire per
        // descriptor (on first-holder), and returns a
        // `QueryLeaseScope` the handler holds through execute.
        if let Some(drain) = &self.drain_tracker {
            let owners = drain.draining_owners(&descriptor_id, version);
            if !owners.is_empty() {
                return Err(SqlCatalogError::RetryableSchemaChanged {
                    descriptor: format!(
                        "collection {name}: {}",
                        crate::control::lease::drain_owner_list(&owners)
                    ),
                });
            }
        }

        let (engine, columns, primary_key) = convert_collection_type(&stored);
        let auto_tier = self.has_auto_tier(name);
        let indexes = stored
            .indexes
            .iter()
            .map(|i| nodedb_sql::types::IndexSpec {
                name: i.name.clone(),
                field: i.field.clone(),
                unique: i.unique,
                case_insensitive: i.case_insensitive,
                state: match i.state {
                    crate::control::security::catalog::IndexBuildState::Building => {
                        nodedb_sql::types::IndexState::Building
                    }
                    crate::control::security::catalog::IndexBuildState::Ready => {
                        nodedb_sql::types::IndexState::Ready
                    }
                },
                predicate: i.predicate.clone(),
            })
            .collect();

        Ok(Some(CollectionInfo {
            name: stored.name,
            engine,
            columns,
            primary_key,
            has_auto_tier: auto_tier,
            indexes,
            bitemporal: stored.bitemporal,
            primary: stored.primary,
            vector_primary: stored.vector_primary,
            partition_strategy: stored.partition_strategy,
            open_schema: CollectionInfo::open_schema_for(engine),
        }))
    }

    /// The time key the collection stores, as `CREATE COLLECTION` designated
    /// it. A later `ALTER ADD COLUMN` cannot change it.
    fn timeseries_time_key(
        &self,
        _database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<String>, SqlCatalogError> {
        let stored = self
            .credentials
            .catalog()
            .get_collection(self.database_id, self.tenant_id, name)
            .map_err(|e| SqlCatalogError::Unreadable {
                relation: name.to_string(),
                detail: e.to_string(),
            })?;
        Ok(stored.and_then(|stored| {
            if let nodedb_types::CollectionType::Columnar(
                nodedb_types::columnar::ColumnarProfile::Timeseries { time_key, .. },
            ) = stored.collection_type
            {
                Some(time_key)
            } else {
                None
            }
        }))
    }

    fn resolve_regclass(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        name: &str,
    ) -> Option<i64> {
        let name = nodedb_sql::catalog::normalize_regclass_name(name)?;

        // Check system/catalog relation OIDs first.
        if let Some(&oid) = crate::control::server::pgwire::catalog::oid::SYSTEM_REL_OIDS
            .iter()
            .find(|(relation, _)| *relation == name)
            .map(|(_, oid)| oid)
        {
            return Some(oid);
        }

        // A hash-derived OID is stable only after catalog existence has been
        // established. Fabricating one for an unknown name makes regclass
        // predicates silently compare against a relation that does not exist.
        let stored = self
            .credentials
            .catalog()
            .get_collection(self.database_id, self.tenant_id, &name)
            .ok()
            .flatten()
            .filter(|stored| stored.is_active)?;

        // A folded regclass literal is a schema dependency like a scan of
        // the relation. Record it so dropping or altering the target invalidates
        // a cached physical plan that embeds its OID.
        let descriptor_id = DescriptorId::new(
            self.database_id.as_u64(),
            self.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        );
        self.recorded_versions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .record(descriptor_id, stored.descriptor_version.max(1));

        Some(
            crate::control::server::pgwire::catalog::oid::stable_collection_oid(
                self.tenant_id,
                &name,
            ),
        )
    }

    /// Database and tenant come from the adapter's own scope, mirroring
    /// `resolve_regclass`; the trait's arguments are the caller's plan-time
    /// defaults and carry no session scope.
    fn sequence_nextval(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        name: &str,
    ) -> Result<i64, nodedb_sql::SqlError> {
        self.advance_sequence(name)
    }

    fn sequence_currval(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        name: &str,
    ) -> Result<i64, nodedb_sql::SqlError> {
        self.session_sequence_value(name)
    }

    fn sequence_setval(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _tenant_id: u64,
        name: &str,
        value: i64,
    ) -> Result<i64, nodedb_sql::SqlError> {
        self.position_sequence(name, value)
    }

    fn resolve_regtype(&self, name: &str) -> Option<i64> {
        crate::control::server::pgwire::catalog::tables::pg_type::type_oid_map()
            .get(name)
            .copied()
    }

    fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
        use nodedb_array::schema::{ArraySchema, DimType as EDT};
        use nodedb_array::types::domain::DomainBound;
        use nodedb_sql::types_array::{ArrayAttrAst, ArrayDimAst, ArrayDimType, ArrayDomainBound};

        let entry = {
            // A poisoned lock still holds a consistent mirror: every writer
            // replaces whole entries. Every other reader recovers it too.
            let cat = self
                .array_catalog
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cat.lookup_by_name_in_database(
                nodedb_types::TenantId::new(self.tenant_id),
                self.database_id,
                name,
            )?
        };
        // A plan built from this view (its georef, attrs, schema hash) is
        // cached against the array's version, so an ALTER, DROP, or
        // re-CREATE stops it matching.
        self.recorded_versions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .record_unleased(
                DescriptorId::new(
                    self.database_id.as_u64(),
                    self.tenant_id,
                    DescriptorKind::Array,
                    entry.name.clone(),
                ),
                entry.catalog_version(),
            );
        let schema: ArraySchema = zerompk::from_msgpack(&entry.schema_msgpack).ok()?;

        let dims = schema
            .dims
            .iter()
            .map(|d| ArrayDimAst {
                name: d.name.clone(),
                dtype: match d.dtype {
                    EDT::Int64 => ArrayDimType::Int64,
                    EDT::Float64 => ArrayDimType::Float64,
                    EDT::TimestampMs => ArrayDimType::TimestampMs,
                    EDT::String => ArrayDimType::String,
                },
                lo: bound_engine_to_ast(&d.domain.lo),
                hi: bound_engine_to_ast(&d.domain.hi),
            })
            .collect();

        let attrs = schema
            .attrs
            .iter()
            .map(|a| ArrayAttrAst {
                name: a.name.clone(),
                dtype: crate::control::planner::sql_plan_convert::array_convert::sql_attr_type(
                    a.dtype,
                ),
                nullable: a.nullable,
                meta: crate::control::array_catalog::attr_meta::meta_ast(&a.meta),
            })
            .collect();

        let tile_extents = schema.tile_extents.iter().map(|n| *n as i64).collect();

        // Closure-local helper.
        fn bound_engine_to_ast(b: &DomainBound) -> ArrayDomainBound {
            match b {
                DomainBound::Int64(v) => ArrayDomainBound::Int64(*v),
                DomainBound::Float64(v) => ArrayDomainBound::Float64(*v),
                DomainBound::TimestampMs(v) => ArrayDomainBound::TimestampMs(*v),
                DomainBound::String(v) => ArrayDomainBound::String(v.clone()),
            }
        }

        let georef = crate::control::array_catalog::georef::georef_ast(&schema);
        Some(ArrayCatalogView {
            name: schema.name,
            dims,
            attrs,
            tile_extents,
            georef,
        })
    }
}
