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

use std::sync::{Arc, Weak};

use nodedb_cluster::{DescriptorId, DescriptorKind};
use nodedb_sql::{
    SqlCatalog, SqlCatalogError,
    types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType},
};

use crate::control::lease::DEFAULT_LEASE_DURATION;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Adapter bridging the NodeDB catalog to the `SqlCatalog` trait.
///
/// The optional `shared` field holds a `Weak<SharedState>` so
/// long-lived `QueryContext`s do not pin the global
/// `SharedState` alive past process shutdown. When present, the
/// adapter calls `acquire_descriptor_lease` on every
/// `get_collection` call to bind the plan to the descriptor
/// version it reads.
///
/// When absent, the adapter behaves as a pure redb reader.
/// Sub-planners invoked from inside a pgwire DDL handler that
/// already holds leases use the no-lease constructor so we
/// don't double-acquire.
pub struct OriginCatalog {
    credentials: Arc<CredentialStore>,
    shared: Option<Weak<SharedState>>,
    tenant_id: u32,
    retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
}

impl OriginCatalog {
    /// Construct an adapter without lease integration. Used by
    /// internal sub-planners that run inside a pgwire handler
    /// which already leased the outer query's descriptors.
    pub fn new(
        credentials: Arc<CredentialStore>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials,
            shared: None,
            tenant_id,
            retention_policy_registry,
        }
    }

    /// Construct an adapter with descriptor lease integration.
    /// Used by the top-level pgwire dispatch (sql_exec,
    /// prepared parser) so every user-initiated query plan
    /// acquires leases on the collections it touches.
    pub fn new_with_lease(
        shared: &Arc<SharedState>,
        tenant_id: u32,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials: Arc::clone(&shared.credentials),
            shared: Some(Arc::downgrade(shared)),
            tenant_id,
            retention_policy_registry,
        }
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
            return Ok(None);
        }

        // Lease acquisition (only if constructed via
        // `new_with_lease`). Fast path (lease already valid at
        // this version or higher) returns instantly. Slow path
        // proposes a `DescriptorLeaseGrant` through the metadata
        // raft group.
        //
        // Version 0 is the sentinel for "legacy record, version
        // unknown" — we lease at 1 in that case so the drain gate
        // can still reject acquires at v1 and trigger re-planning.
        if let Some(shared_weak) = &self.shared
            && let Some(shared) = shared_weak.upgrade()
        {
            let descriptor_id = DescriptorId::new(
                self.tenant_id,
                DescriptorKind::Collection,
                stored.name.clone(),
            );
            let version = stored.descriptor_version.max(1);
            match shared.acquire_descriptor_lease(descriptor_id, version, DEFAULT_LEASE_DURATION) {
                Ok(_) => {}
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("drain in progress") {
                        return Err(SqlCatalogError::RetryableSchemaChanged {
                            descriptor: format!("collection {name}"),
                        });
                    }
                    // Non-drain failure: log at warn and proceed
                    // with the descriptor we read. Treating the
                    // planner as best-effort-leased on non-drain
                    // errors means a transient lease failure
                    // (e.g. a brief leader election during which
                    // `force_refresh_lease` returns NotLeader
                    // without a hint) doesn't break user queries.
                    tracing::warn!(
                        error = %msg,
                        descriptor = %name,
                        version,
                        "OriginCatalog: lease acquire failed, proceeding without lease"
                    );
                }
            }
        }

        let (engine, columns, primary_key) = convert_collection_type(&stored);
        let auto_tier = self.has_auto_tier(name);

        Ok(Some(CollectionInfo {
            name: stored.name,
            engine,
            columns,
            primary_key,
            has_auto_tier: auto_tier,
        }))
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
        ColumnType::Timestamp => SqlDataType::Timestamp,
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
