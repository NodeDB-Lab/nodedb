//! Implements `nodedb_sql::SqlCatalog` for Origin.
//!
//! The adapter acquires a descriptor lease at plan time. The
//! lease is what binds an in-flight query to the descriptor
//! version it was planned against: while the lease is held, no
//! DDL can bump the descriptor (drain blocks until the lease
//! releases or expires). This is the mechanism that closes the
//! planner-side race between "read descriptor" and "execute plan".
//!
//! Lease ownership is per-node, not per-query. Every call to
//! `get_collection` goes through `force-refresh the lease` via
//! the `lease::acquire_lease` fast path: if a valid lease
//! already exists, returns instantly with zero raft round-trips.
//! The first query on a cold collection pays one raft round-trip
//! to acquire; subsequent queries within the lease window read
//! from the in-memory cache. The renewal loop keeps held leases
//! alive indefinitely.
//!
//! **Drain interaction**: if the descriptor is being drained at
//! the version we read, `acquire_descriptor_lease` returns
//! `Err::Config { "drain in progress" }`. We translate that to
//! `SqlCatalogError::RetryableSchemaChanged`, which the pgwire
//! handler catches and retries the whole plan (up to the retry
//! budget). On any other lease-acquire failure we log and
//! proceed with the descriptor we read — lease acquisition is
//! best-effort; the planner's primary job is still to produce
//! a plan, and a transient lease glitch should not break user
//! queries.

use std::sync::{Arc, Mutex};

use nodedb_cluster::{DescriptorId, DescriptorKind};
use nodedb_sql::{
    SqlCatalog, SqlCatalogError,
    types::{ArrayCatalogView, CollectionInfo, ColumnInfo, EngineType, SqlDataType},
};

use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Adapter bridging the NodeDB catalog to the `SqlCatalog` trait.
///
/// The adapter reads descriptors from the local `SystemCatalog`
/// redb and records each observed descriptor into
/// `recorded_versions` for use as the plan-cache key. It does
/// NOT acquire leases itself — `SharedState::acquire_plan_lease_scope`
/// is called by the pgwire handler after planning finishes
/// (for both cache hits and fresh plans) so leases are held
/// through the execute phase via a refcounted
/// `QueryLeaseScope`.
pub struct OriginCatalog {
    credentials: Arc<CredentialStore>,
    tenant_id: u32,
    retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
    /// Array catalog handle. When `None`, `lookup_array` returns
    /// `None` for every name — used by sub-planners that don't own
    /// array state.
    array_catalog: Option<crate::control::array_catalog::ArrayCatalogHandle>,
    /// Optional reference to the host's drain tracker. When
    /// present, `get_collection` checks for an active drain
    /// on each descriptor it reads and returns
    /// `RetryableSchemaChanged` so the planner's retry loop
    /// re-plans. When absent (sub-planners that don't thread
    /// an `Arc<SharedState>`), drain is not observable at
    /// plan time — the outer query's scope is still protecting
    /// the lease.
    drain_tracker: Option<Arc<crate::control::lease::DescriptorDrainTracker>>,
    /// Descriptors read during planning, in stable order. Filled
    /// by `get_collection`, drained by the caller via
    /// `take_recorded_versions` once planning finishes. The
    /// resulting set becomes the cache key for the plan cache so
    /// DDL on unrelated descriptors does not invalidate cached
    /// plans.
    ///
    /// Wrapped in `Mutex` (not `RefCell`) because `SqlCatalog`
    /// is used through `&self` and the adapter must be `Sync`
    /// for axum / tokio handler bounds. Mutex overhead is
    /// negligible — `get_collection` is called only a handful
    /// of times per plan.
    recorded_versions: Mutex<DescriptorVersionSet>,
}

impl OriginCatalog {
    /// Construct an adapter that reads from the local redb
    /// catalog and records descriptor versions for the plan
    /// cache key. Lease acquisition happens in a separate,
    /// post-plan step — see
    /// `SharedState::acquire_plan_lease_scope`.
    /// Construct an adapter that reads from the local redb
    /// catalog WITHOUT drain observation. Used by internal
    /// sub-planners invoked inside a pgwire DDL handler
    /// whose outer query already holds leases through its
    /// `QueryLeaseScope`.
    pub fn new(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials,
            tenant_id,
            retention_policy_registry,
            drain_tracker: None,
            recorded_versions: Mutex::new(DescriptorVersionSet::new()),
            array_catalog: None,
        }
    }

    /// Construct an adapter with drain observation. Used by
    /// the top-level pgwire dispatch so every user-initiated
    /// query's plan sees `RetryableSchemaChanged` when any
    /// descriptor it reads is being drained by an in-flight
    /// DDL; the pgwire handler's retry loop then re-plans.
    pub fn new_with_lease(
        shared: &Arc<SharedState>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials: Arc::clone(&shared.credentials),
            tenant_id,
            retention_policy_registry,
            drain_tracker: Some(Arc::clone(&shared.lease_drain)),
            recorded_versions: Mutex::new(DescriptorVersionSet::new()),
            array_catalog: Some(shared.array_catalog.clone()),
        }
    }

    /// Drain the recorded descriptor-version set and return it.
    /// Callers capture this after planning finishes and use it
    /// as the plan cache key + freshness witness.
    pub fn take_recorded_versions(&self) -> DescriptorVersionSet {
        let mut guard = self
            .recorded_versions
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    fn has_auto_tier(&self, collection: &str) -> bool {
        let registry = match &self.retention_policy_registry {
            Some(r) => r,
            None => return false,
        };
        registry
            .get(self.tenant_id, collection)
            .is_some_and(|p| p.auto_tier)
    }
}

impl SqlCatalog for OriginCatalog {
    fn get_collection(
        &self,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        // Read through the local `SystemCatalog` redb. On cluster
        // followers, the `MetadataCommitApplier` has already
        // written the replicated record here via
        // `CatalogEntry::apply_to`, so a single read path works
        // for both single-node and cluster modes.
        let catalog_ref = self.credentials.catalog();
        let Some(catalog) = catalog_ref.as_ref() else {
            return Ok(None);
        };
        let Some(stored) = catalog.get_collection(self.tenant_id, name).ok().flatten() else {
            return Ok(None);
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
            let retention_expires_at_ns = stored.modification_hlc.wall_ns.saturating_add(retention);
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
        // this descriptor at the version we just read, return
        // `RetryableSchemaChanged` so the pgwire handler's
        // retry loop re-plans. Without this check the planner
        // would compile a plan against a version that's about
        // to be retired, and the post-plan lease acquisition
        // would either hit a "drain in progress" error (which
        // is too late to retry) or (worse) succeed on first
        // holder because the drain finished just before the
        // refcount check.
        //
        // Leases themselves are NOT acquired here anymore.
        // The handler calls
        // `SharedState::acquire_plan_lease_scope` after
        // planning finishes (or after a cache hit returns a
        // pre-recorded version set), which increments
        // refcounts, performs a single raft acquire per
        // descriptor (on first-holder), and returns a
        // `QueryLeaseScope` the handler holds through execute.
        if let Some(drain) = &self.drain_tracker {
            let now_wall_ns = crate::control::lease::wall_now_ns();
            if drain.is_draining(&descriptor_id, version, now_wall_ns) {
                return Err(SqlCatalogError::RetryableSchemaChanged {
                    descriptor: format!("collection {name}"),
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
        }))
    }

    fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
        use nodedb_array::schema::{ArraySchema, AttrType as EAT, DimType as EDT};
        use nodedb_array::types::domain::DomainBound;
        use nodedb_sql::types_array::{
            ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound,
        };

        let handle = self.array_catalog.as_ref()?;
        let entry = {
            let cat = handle.read().ok()?;
            cat.lookup_by_name(name)?
        };
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
                dtype: match a.dtype {
                    EAT::Int64 => ArrayAttrType::Int64,
                    EAT::Float64 => ArrayAttrType::Float64,
                    EAT::String => ArrayAttrType::String,
                    EAT::Bytes => ArrayAttrType::Bytes,
                },
                nullable: a.nullable,
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

        Some(ArrayCatalogView {
            name: schema.name,
            dims,
            attrs,
            tile_extents,
        })
    }
}

/// Convert a StoredCollection to engine type, columns, and primary key.
fn convert_collection_type(
    stored: &crate::control::security::catalog::StoredCollection,
) -> (EngineType, Vec<ColumnInfo>, Option<String>) {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let columns = schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                })
                .collect();
            let pk = schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());
            (EngineType::DocumentStrict, columns, pk)
        }

        CollectionType::Document(DocumentMode::Schemaless) => {
            let mut columns = vec![ColumnInfo {
                name: "id".into(),
                data_type: SqlDataType::String,
                nullable: false,
                is_primary_key: true,
                default: None,
            }];
            // Add tracked fields from catalog.
            for (name, type_str) in &stored.fields {
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                });
            }
            (EngineType::DocumentSchemaless, columns, Some("id".into()))
        }

        CollectionType::KeyValue(config) => {
            let columns = config
                .schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                })
                .collect();
            let pk = config
                .schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone())
                .or_else(|| Some("key".into()));
            (EngineType::KeyValue, columns, pk)
        }

        CollectionType::Columnar(profile) => {
            let engine = if profile.is_timeseries() {
                EngineType::Timeseries
            } else if profile.is_spatial() {
                EngineType::Spatial
            } else {
                EngineType::Columnar
            };
            let mut columns = Vec::new();
            if !profile.is_timeseries() {
                columns.push(ColumnInfo {
                    name: "id".into(),
                    data_type: SqlDataType::String,
                    nullable: false,
                    is_primary_key: true,
                    default: Some("UUID_V7".into()),
                });
            }
            for (name, type_str) in &stored.fields {
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                });
            }
            let pk = if profile.is_timeseries() {
                None
            } else {
                Some("id".into())
            };
            (engine, columns, pk)
        }
    }
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes | ColumnType::Geometry | ColumnType::Json => SqlDataType::Bytes,
        ColumnType::Timestamp | ColumnType::SystemTimestamp => SqlDataType::Timestamp,
        ColumnType::Decimal | ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex => {
            SqlDataType::String
        }
        ColumnType::Duration => SqlDataType::Int64,
        ColumnType::Array | ColumnType::Set | ColumnType::Range | ColumnType::Record => {
            SqlDataType::Bytes
        }
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
    }
}

fn parse_type_str(s: &str) -> SqlDataType {
    match s.to_uppercase().as_str() {
        "INT" | "INTEGER" | "INT4" | "INT8" | "BIGINT" => SqlDataType::Int64,
        "FLOAT" | "FLOAT4" | "FLOAT8" | "FLOAT64" | "DOUBLE" | "REAL" => SqlDataType::Float64,
        "BOOL" | "BOOLEAN" => SqlDataType::Bool,
        "BYTES" | "BYTEA" | "BLOB" => SqlDataType::Bytes,
        "TIMESTAMP" | "TIMESTAMPTZ" => SqlDataType::Timestamp,
        _ => SqlDataType::String,
    }
}
