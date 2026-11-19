// SPDX-License-Identifier: BUSL-1.1

//! Dispatch `DocumentOp::Register` to this node's Data Plane
//! after a collection has been committed.
//!
//! [`dispatch_register_from_stored`] runs from the metadata applier's
//! post-apply hook after a `CatalogEntry::PutCollection` commits. It
//! derives index paths from `coll.fields`, then
//! [`dispatch_register_from_stored_inner`] builds the config and
//! dispatches it to the Data Plane.
//!
//! The neutral `continuous_agg` / `materialized_view` families, the
//! `catalog_entry::post_apply` hook, and the pgwire `alter` handlers all
//! depend on this protocol-neutral home.

use crate::bootstrap::constraint_reconcile::CollectionSource;
use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;
use crate::types::TraceId;

/// Applier-side entry point: dispatch `DocumentOp::Register` using
/// a fully-populated [`StoredCollection`]. Called from the
/// production `MetadataCommitApplier` after it materializes a
/// replicated `CatalogEntry::PutCollection` into local
/// `SystemCatalog` redb, so every follower's Data Plane knows
/// about the collection before the first cross-node INSERT
/// arrives.
///
/// Returns an error if any Data Plane core fails to acknowledge the
/// registration — the post-apply hook must not bump the applied-index
/// watcher until every core carries the new schema.
pub async fn dispatch_register_from_stored(
    state: &SharedState,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let tenant_id = crate::types::TenantId::new(coll.tenant_id);
    let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
    extend_with_catalog_indexes(&mut indexes, coll);
    dispatch_register_from_stored_inner(state, tenant_id, coll, indexes).await
}

/// Register `coll` on this node's Data Plane after a handler proposed its
/// `PutCollection` with result `outcome`, when the proposal left that to the
/// handler.
///
/// - A durable outcome ran the entry's awaited post-apply on this node. That
///   post-apply registered the collection on every core, so this call
///   registers nothing.
/// - A buffered outcome ran no post-apply. The open transaction's later
///   statements read the new shape, so this call registers it. ROLLBACK
///   registers the stored shape again.
pub async fn register_proposed_collection(
    state: &SharedState,
    outcome: crate::control::propose_outcome::ProposeOutcome,
    coll: &StoredCollection,
) -> crate::Result<()> {
    if outcome.is_durable() {
        return Ok(());
    }
    dispatch_register_from_stored(state, coll).await
}

/// Re-register every collection that DRIVES a materialized-sum binding `coll`
/// declares, so those sources learn they now drive one.
///
/// A binding is stored on the TARGET collection, but the Data-Plane config that
/// decides whether a write folds it is derived for the SOURCE:
/// [`super::enforcement::find_materialized_sum_bindings`] scans every collection for bindings that
/// name the collection BEING REGISTERED as their source. Registering the target
/// alone therefore propagates nothing — a target carries no binding it is the
/// target OF — and a source registered before the binding existed keeps a config
/// asserting it drives nothing. Every write into it then folds nothing at all
/// and the stored total silently stays where it was.
///
/// Only the co-resident path reads that config, which is why this can go
/// unnoticed: a cross-shard binding is settled on the Control Plane at plan time
/// from the catalog and travels on its own `ApplyBalanceDelta` task, which
/// consults no Data-Plane config.
///
/// Call only AFTER the catalog carries `coll`, so the scan sees the binding
/// being propagated. A source that is not in the catalog is skipped: its own
/// registration derives the binding when it lands.
pub async fn dispatch_register_for_sum_sources(
    state: &SharedState,
    coll: &StoredCollection,
) -> crate::Result<()> {
    let mut registered: Vec<&str> = Vec::new();
    for def in &coll.materialized_sums {
        let source = def.source_collection.as_str();
        // The target's own registration already covers it, and two bindings
        // fanning out of one source share a single re-registration.
        if source == coll.name || registered.contains(&source) {
            continue;
        }
        registered.push(source);
        let stored =
            state
                .credentials
                .catalog()
                .get_collection(coll.database_id, coll.tenant_id, source)?;
        if let Some(stored) = stored {
            dispatch_register_from_stored(state, &stored).await?;
        }
    }
    Ok(())
}

/// Per-field auto-derived indexes (schemaless default: each declared field
/// becomes a non-unique `$.field` index). Always `Ready` — these exist
/// from the moment the collection is created.
///
/// `pub(crate)` so the boot-time `doc_configs` seed loader
/// ([`crate::bootstrap::data_plane::load_doc_config_registry`]) can derive
/// the same index set as the live-DDL path without drift.
pub(crate) fn derive_auto_indexes<'a>(
    field_names: impl IntoIterator<Item = &'a str>,
) -> Vec<nodedb_physical::physical_plan::RegisteredIndex> {
    field_names
        .into_iter()
        .map(|n| nodedb_physical::physical_plan::RegisteredIndex {
            name: n.to_string(),
            path: format!("$.{n}"),
            unique: false,
            case_insensitive: false,
            state: nodedb_physical::physical_plan::RegisteredIndexState::Ready,
            predicate: None,
        })
        .collect()
}

/// Append explicit `CREATE INDEX` entries from the catalog. When an
/// explicit catalog index shares a path with an auto-derived one, the
/// catalog entry supersedes the auto-derived one: UNIQUE/COLLATE
/// modifiers have to take effect.
pub(crate) fn extend_with_catalog_indexes(
    out: &mut Vec<nodedb_physical::physical_plan::RegisteredIndex>,
    coll: &StoredCollection,
) {
    for idx in &coll.indexes {
        let state = match idx.state {
            crate::control::security::catalog::IndexBuildState::Building => {
                nodedb_physical::physical_plan::RegisteredIndexState::Building
            }
            crate::control::security::catalog::IndexBuildState::Ready => {
                nodedb_physical::physical_plan::RegisteredIndexState::Ready
            }
        };
        let spec = nodedb_physical::physical_plan::RegisteredIndex {
            name: idx.name.clone(),
            path: idx.field.clone(),
            unique: idx.unique,
            case_insensitive: idx.case_insensitive,
            state,
            predicate: idx.predicate.clone(),
        };
        if let Some(existing) = out.iter_mut().find(|e| e.path == spec.path) {
            *existing = spec;
        } else {
            out.push(spec);
        }
    }
}

/// Extract the declared timeseries shape — column list plus designated
/// `TIME_KEY` — from a stored collection, or `None` for every other engine.
///
/// The time key is read from the persisted `ColumnarProfile::Timeseries`
/// rather than re-derived from the column list, so the name the Data Plane
/// uses is always the one DDL resolved and the catalog recorded.
fn build_timeseries_schema(
    coll: &StoredCollection,
) -> Option<Box<nodedb_physical::physical_plan::TimeseriesSchema>> {
    let nodedb_types::CollectionType::Columnar(nodedb_types::ColumnarProfile::Timeseries {
        time_key,
        interval,
    }) = &coll.collection_type
    else {
        return None;
    };
    Some(Box::new(nodedb_physical::physical_plan::TimeseriesSchema {
        time_key: time_key.clone(),
        columns: coll.fields.clone(),
        // The collection type's interval is the one source of the partition
        // interval: CREATE and ALTER both write it.
        partition_interval: interval.clone(),
        last_value_cache: coll.lvc_enabled,
    }))
}

/// Build the `CollectionConfig` a `DocumentOp::Register` will install in
/// `doc_configs`, straight from the durable catalog — storage mode,
/// enforcement options, generated columns, and secondary indexes.
/// `text_fields` is the full-text field set the caller read from the
/// collection's index records: one collection's records on the live path,
/// a tenant's records grouped once on the boot path.
///
/// Shared by two callers that must never drift apart:
/// - [`dispatch_register_from_stored_inner`] (live DDL / applier path):
///   derives the same fields to build the `DocumentOp::Register` plan
///   broadcast to Data Plane cores.
/// - [`crate::bootstrap::data_plane::load_doc_config_registry`] (boot
///   path): seeds every core's `doc_configs` synchronously, before WAL
///   redo replay runs, so strict collections re-encode Binary Tuple
///   instead of falling through to the raw-MessagePack fallback.
pub(crate) fn build_doc_config_from_stored<S: CollectionSource + ?Sized>(
    catalog: &S,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    indexes: &[nodedb_physical::physical_plan::RegisteredIndex],
    text_fields: nodedb_physical::physical_plan::TextFieldSet,
) -> crate::Result<crate::engine::document::store::CollectionConfig> {
    let name = crate::control::planner::sql_plan_convert::convert::db_qualified(
        coll.database_id,
        &coll.name,
    );

    // The wire form holds the storage mode, the enforcement options, the
    // indexes, the declared key, the vector-primary config, the declared
    // columns and the vector fields. Its digest is the one a config pin of
    // this collection carries.
    let wire = super::doc_config_wire_from_stored(catalog, tenant_id, coll, indexes);

    // Written as a struct literal with every field named — and deliberately
    // NOT `..Default::default()`. A field added to `CollectionConfig` that is
    // never derived from the catalog is invisible at runtime: the collection
    // behaves as though the attribute was never declared. Naming every
    // field turns that into a compile error here, in the one function both the
    // live-DDL path and the boot seed go through.
    // A violation names the collection the user wrote, never the qualified key.
    let type_guards = crate::data::executor::enforcement::typeguard::CompiledTypeGuard::compile_all(
        &coll.name,
        &wire.enforcement.type_guards,
    );
    let digest = wire.digest()?;
    let nodedb_physical::physical_plan::DocConfigWire {
        crdt_enabled,
        enforcement,
        storage_mode,
        declared_key,
        vector_primary,
        declared_columns,
        vector_fields,
        ..
    } = wire;
    Ok(crate::engine::document::store::CollectionConfig {
        name,
        type_guards,
        index_paths: indexes
            .iter()
            .map(crate::engine::document::store::IndexPath::from_registered)
            .collect(),
        crdt_enabled,
        storage_mode,
        enforcement,
        bitemporal: coll.bitemporal,
        conflict_policy: coll.conflict_policy.clone(),
        timeseries: build_timeseries_schema(coll),
        vector_primary: vector_primary.map(Box::new),
        vector_fields,
        declared_columns,
        declared_key,
        text_fields,
        incarnation: coll.incarnation,
        digest,
    })
}

async fn dispatch_register_from_stored_inner(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    indexes: Vec<nodedb_physical::physical_plan::RegisteredIndex>,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    let text_fields = super::text_fields::text_field_set(catalog, coll)?;
    let config = build_doc_config_from_stored(catalog, tenant_id, coll, &indexes, text_fields)?;

    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::Register {
            collection: nodedb_types::QualifiedCollection::from_stored(
                coll.database_id,
                config.name.clone(),
            )?,
            indexes,
            crdt_enabled: config.crdt_enabled,
            storage_mode: config.storage_mode,
            enforcement: Box::new(config.enforcement),
            bitemporal: config.bitemporal,
            conflict_policy: config.conflict_policy.clone(),
            timeseries: config.timeseries.clone(),
            vector_primary: config.vector_primary.clone(),
            vector_fields: config.vector_fields.clone(),
            declared_columns: config.declared_columns.clone(),
            declared_key: config.declared_key.clone(),
            text_fields: config.text_fields.clone(),
            incarnation: config.incarnation,
            digest: config.digest,
        },
    );

    crate::control::server::broadcast::broadcast_register_to_all_cores(
        state,
        tenant_id,
        coll.database_id,
        plan,
        TraceId::ZERO,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::TextFieldSet;
    use nodedb_types::DatabaseId;
    use nodedb_types::{PrimaryEngine, VectorPrimaryConfig};

    /// A catalog with no other collections — enough for the builder, which
    /// only consults it to discover materialized-sum bindings.
    struct EmptyCatalog;

    impl CollectionSource for EmptyCatalog {
        fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
            Ok(Vec::new())
        }
        fn collections_in(&self, _database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
            Ok(Vec::new())
        }
        fn collections_for_tenant(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
        ) -> crate::Result<Vec<StoredCollection>> {
            Ok(Vec::new())
        }
        fn index_records_for_tenant(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
        ) -> crate::Result<Vec<crate::control::security::catalog::StoredIndexRecord>> {
            Ok(Vec::new())
        }
        fn index_records_for_collection(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
            _collection: &str,
        ) -> crate::Result<Vec<crate::control::security::catalog::StoredIndexRecord>> {
            Ok(Vec::new())
        }
    }

    fn vector_primary_collection() -> StoredCollection {
        let mut coll = StoredCollection::new(1, "vec_primary", "owner");
        coll.primary = PrimaryEngine::Vector;
        coll.vector_primary = Some(VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 3,
            ..VectorPrimaryConfig::default()
        });
        coll
    }

    /// The vector-primary marker must reach `CollectionConfig`.
    ///
    /// This is the ONE builder both the live-DDL register broadcast and the
    /// boot-time `doc_configs` seed go through, so a marker that survives here
    /// survives a restart too. A marker that existed only after a live CREATE
    /// will make a vector-primary collection readable until the first restart
    /// and unreadable after it — the same defect one layer down.
    #[test]
    fn a_vector_primary_collection_carries_its_marker_into_the_doc_config() {
        let coll = vector_primary_collection();
        let config = build_doc_config_from_stored(
            &EmptyCatalog,
            crate::types::TenantId::new(coll.tenant_id),
            &coll,
            &[],
            TextFieldSet::default(),
        )
        .expect("build doc config");
        let vp = config
            .vector_primary
            .as_ref()
            .expect("a primary='vector' collection must register its vector-primary config");
        assert_eq!(vp.vector_field, "vec");
        assert_eq!(vp.dim, 3);
    }

    /// Every other engine must leave the marker unset, or every collection's
    /// rows will be decoded as tagged sidecars.
    #[test]
    fn a_plain_document_collection_carries_no_vector_primary_marker() {
        let coll = StoredCollection::new(1, "plain_docs", "owner");
        let config = build_doc_config_from_stored(
            &EmptyCatalog,
            crate::types::TenantId::new(coll.tenant_id),
            &coll,
            &[],
            TextFieldSet::default(),
        )
        .expect("build doc config");
        assert!(config.vector_primary.is_none());
    }

    /// A declared key reaches the Data Plane config. The implicit `id` key
    /// names no declared key.
    #[test]
    fn a_declared_key_reaches_the_doc_config() {
        let mut coll = StoredCollection::new(1, "items", "owner");
        coll.declared_primary_key = Some("sku".to_string());
        let config = build_doc_config_from_stored(
            &EmptyCatalog,
            crate::types::TenantId::new(coll.tenant_id),
            &coll,
            &[],
            TextFieldSet::default(),
        )
        .expect("build doc config");
        assert_eq!(config.declared_key.as_deref(), Some("sku"));

        let plain = StoredCollection::new(1, "docs", "owner");
        let config = build_doc_config_from_stored(
            &EmptyCatalog,
            crate::types::TenantId::new(plain.tenant_id),
            &plain,
            &[],
            TextFieldSet::default(),
        )
        .expect("build doc config");
        assert_eq!(config.declared_key, None);
    }

    /// A catalog that holds a fixed set of collections for one tenant.
    struct FixedCatalog(Vec<StoredCollection>);

    impl CollectionSource for FixedCatalog {
        fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
            Ok(Vec::new())
        }
        fn collections_in(&self, _database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
            Ok(self.0.clone())
        }
        fn collections_for_tenant(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
        ) -> crate::Result<Vec<StoredCollection>> {
            Ok(self.0.clone())
        }
        fn index_records_for_tenant(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
        ) -> crate::Result<Vec<crate::control::security::catalog::StoredIndexRecord>> {
            Ok(Vec::new())
        }
        fn index_records_for_collection(
            &self,
            _database_id: DatabaseId,
            _tenant_id: u64,
            _collection: &str,
        ) -> crate::Result<Vec<crate::control::security::catalog::StoredIndexRecord>> {
            Ok(Vec::new())
        }
    }

    fn built_config<S: CollectionSource + ?Sized>(
        catalog: &S,
        coll: &StoredCollection,
    ) -> crate::engine::document::store::CollectionConfig {
        let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
        extend_with_catalog_indexes(&mut indexes, coll);
        build_doc_config_from_stored(
            catalog,
            crate::types::TenantId::new(coll.tenant_id),
            coll,
            &indexes,
            TextFieldSet::default(),
        )
        .expect("build doc config")
    }

    /// The live Register and the boot seed both call the one builder, so
    /// one collection gives one digest, and it is the wire form's digest.
    #[test]
    fn the_builder_gives_one_digest_for_one_collection() {
        let coll = vector_primary_collection();
        let first = built_config(&EmptyCatalog, &coll);
        let second = built_config(&EmptyCatalog, &coll);
        assert_eq!(first.digest, second.digest);
        let wire = nodedb_physical::physical_plan::DocConfigWire {
            bitemporal: first.bitemporal,
            crdt_enabled: first.crdt_enabled,
            enforcement: first.enforcement.clone(),
            storage_mode: first.storage_mode.clone(),
            index_paths: Vec::new(),
            declared_key: first.declared_key.clone(),
            vector_primary: first.vector_primary.as_deref().cloned(),
            declared_columns: first.declared_columns.clone(),
            vector_fields: first.vector_fields.clone(),
        };
        assert_eq!(first.digest, wire.digest().expect("digest"));
    }

    /// A sum binding lives on the target. It changes the source's
    /// `materialized_sum_sources`, so the source's digest changes.
    #[test]
    fn a_sum_binding_on_another_collection_changes_the_source_digest() {
        let source = StoredCollection::new(1, "entries", "owner");
        let mut target = StoredCollection::new(1, "accounts", "owner");
        let before = built_config(&FixedCatalog(vec![source.clone(), target.clone()]), &source);

        target
            .materialized_sums
            .push(crate::control::security::catalog::MaterializedSumDef {
                target_collection: target.name.clone(),
                target_column: "balance".into(),
                source_collection: crate::control::planner::sql_plan_convert::convert::db_qualified(
                    source.database_id,
                    &source.name,
                ),
                join_column: "account_id".into(),
                value_expr: nodedb_query::expr::SqlExpr::Column("amount".into()),
            });
        let after = built_config(&FixedCatalog(vec![source.clone(), target]), &source);

        assert!(before.enforcement.materialized_sum_sources.is_empty());
        assert_eq!(after.enforcement.materialized_sum_sources.len(), 1);
        assert_ne!(before.digest, after.digest);
    }
}
