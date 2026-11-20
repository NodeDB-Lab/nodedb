// SPDX-License-Identifier: BUSL-1.1

//! The proposer's cache of config pins, so a commit derives no document
//! config from the catalog while the catalog is unchanged. It also holds the
//! declared columns each KV write resolves, so a write reads no catalog row.
//!
//! A document config pin derives from collection rows alone: the collection's
//! own row, and the materialized-sum bindings other rows declare with it as
//! their source. The catalog's relations generation moves on every committed
//! write of a collection row, so the cache holds its pins at one generation
//! and drops them all once the generation moves.
//!
//! The CRS pin derives from the catalog's user-defined CRS set. The catalog
//! swaps in a new set on every change, and the cache holds the set it
//! pinned, so the set is unchanged exactly while the catalog still shares it.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_crs::CustomCrsSet;
use nodedb_physical::physical_plan::{
    ConfigPin, ConfigPinKey, CrsPin, DeclaredColumn, KvDeclaredColumns, RegisteredIndex,
};
use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection, TenantId};

use crate::bootstrap::constraint_reconcile::CollectionSource;
use crate::control::security::catalog::{StoredCollection, StoredIndexRecord, SystemCatalog};
use crate::control::server::shared::ddl::neutral::collection::register::{
    derive_auto_indexes, extend_with_catalog_indexes,
};
use crate::control::server::shared::ddl::neutral::collection::{
    declared_columns, doc_config_wire_from_stored,
};

/// The pins the cache holds.
#[derive(Default)]
struct Cached {
    /// The relations generation `configs` was derived at.
    generation: u64,
    /// `None` for a key with no active collection, which has no config.
    configs: HashMap<ConfigPinKey, Option<Arc<ConfigPin>>>,
    /// The CRS set the pin was derived from, and the pin.
    crs: Option<(Arc<CustomCrsSet>, Arc<CrsPin>)>,
    /// The declared columns a KV write re-types under, at `generation`, by
    /// database and tenant, then by qualified collection name.
    kv_declared: HashMap<(DatabaseId, TenantId), HashMap<String, KvDeclaredColumns>>,
}

/// Config pins of committed catalog state, shared by every session.
#[derive(Default)]
pub struct ConfigPinCache {
    cached: RwLock<Cached>,
}

impl ConfigPinCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The pin of the config `key` names, as `catalog` commits it. `None`
    /// when no active collection holds the key.
    pub fn config_pin(
        &self,
        catalog: &SystemCatalog,
        key: &ConfigPinKey,
    ) -> crate::Result<Option<Arc<ConfigPin>>> {
        // Read before the derive: a write during it moves the generation
        // past the one kept, so the next lookup derives again.
        let generation = catalog.relations_generation();
        {
            let cached = self.cached.read().unwrap_or_else(|p| p.into_inner());
            if cached.generation == generation
                && let Some(pin) = cached.configs.get(key)
            {
                return Ok(pin.clone());
            }
        }
        let pin = derive_config_pin(catalog, key)?.map(Arc::new);
        let mut cached = self.cached.write().unwrap_or_else(|p| p.into_inner());
        cached.advance_to(generation);
        if cached.generation == generation {
            cached.configs.insert(key.clone(), pin.clone());
        }
        Ok(pin)
    }

    /// The declared columns a KV write of `tenant_id` to `collection`
    /// re-types under, as `catalog` commits it. The database is the one
    /// `collection` names. Empty when no active collection holds the name
    /// or it declares none. A hit allocates nothing.
    pub fn kv_declared_columns(
        &self,
        catalog: &SystemCatalog,
        tenant_id: TenantId,
        collection: &QualifiedCollection,
    ) -> crate::Result<KvDeclaredColumns> {
        let database_id = QualifiedCollection::database_of_stored(collection.as_str())?;
        let scope = (database_id, tenant_id);
        // Read before the derive, as in `config_pin`.
        let generation = catalog.relations_generation();
        let hit = {
            let cached = self.cached.read().unwrap_or_else(|p| p.into_inner());
            (cached.generation == generation)
                .then(|| {
                    cached
                        .kv_declared
                        .get(&scope)
                        .and_then(|named| named.get(collection.as_str()))
                        .cloned()
                })
                .flatten()
        };
        if let Some(columns) = hit {
            return Ok(columns);
        }
        let columns = KvDeclaredColumns::resolved(derive_declared_columns(
            catalog,
            database_id,
            tenant_id,
            collection.as_str(),
        )?);
        let mut cached = self.cached.write().unwrap_or_else(|p| p.into_inner());
        cached.advance_to(generation);
        if cached.generation == generation {
            cached
                .kv_declared
                .entry(scope)
                .or_default()
                .insert(collection.as_str().to_owned(), columns.clone());
        }
        Ok(columns)
    }

    /// The pin of the catalog's user-defined CRS set.
    pub fn crs_pin(&self, catalog: &SystemCatalog) -> crate::Result<Arc<CrsPin>> {
        let set = catalog.custom_crs_set();
        {
            let cached = self.cached.read().unwrap_or_else(|p| p.into_inner());
            if let Some((pinned, pin)) = &cached.crs
                && Arc::ptr_eq(pinned, &set)
            {
                return Ok(Arc::clone(pin));
            }
        }
        let pin = Arc::new(crs_pin_of(&set)?);
        let mut cached = self.cached.write().unwrap_or_else(|p| p.into_inner());
        cached.crs = Some((set, Arc::clone(&pin)));
        Ok(pin)
    }
}

impl Cached {
    /// Drop every entry derived below `generation`, and hold `generation`.
    fn advance_to(&mut self, generation: u64) {
        if self.generation < generation {
            self.configs.clear();
            self.kv_declared.clear();
            self.generation = generation;
        }
    }
}

/// The declared columns of the collection `qualified` names in
/// `database_id`, derived from the committed catalog. Empty when no active
/// collection holds the name. A name not qualified for `database_id` is a
/// typed error, never an empty set.
fn derive_declared_columns(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: TenantId,
    qualified: &str,
) -> crate::Result<Vec<DeclaredColumn>> {
    let name = CollectionKey::from_qualified_str(database_id, qualified)?;
    let coll = catalog.get_committed_collection(database_id, tenant_id.as_u64(), name.name())?;
    Ok(coll
        .filter(|coll| coll.is_active)
        .map_or_else(Vec::new, |coll| declared_columns(&coll)))
}

/// The active committed collection `key` names, if any.
fn active_collection(
    catalog: &SystemCatalog,
    key: &ConfigPinKey,
) -> crate::Result<Option<StoredCollection>> {
    // A name that is not qualified for its database names no collection
    // there.
    let Ok(name) = CollectionKey::from_qualified_str(key.database_id, &key.collection) else {
        return Ok(None);
    };
    let coll =
        catalog.get_committed_collection(key.database_id, key.tenant_id.as_u64(), name.name())?;
    Ok(coll.filter(|coll| coll.is_active))
}

/// The pin of `set`: its MessagePack bytes and their XXH3-64 digest. The
/// set holds its entries sorted by SRID and no map, so one set gives one
/// byte string.
pub fn crs_pin_of(set: &CustomCrsSet) -> crate::Result<CrsPin> {
    let bytes = zerompk::to_msgpack_vec(set).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("custom CRS set pin encode: {e}"),
    })?;
    Ok(CrsPin {
        digest: nodedb_physical::physical_plan::pin_digest(&bytes),
        bytes: bytes.into(),
    })
}

/// The pin of `key`'s config, derived from the committed catalog.
fn derive_config_pin(
    catalog: &SystemCatalog,
    key: &ConfigPinKey,
) -> crate::Result<Option<ConfigPin>> {
    let Some(coll) = active_collection(catalog, key)? else {
        return Ok(None);
    };
    let wire = doc_config_wire_from_stored(
        &Committed(catalog),
        key.tenant_id,
        &coll,
        &stored_indexes(&coll),
    );
    let (wire, digest) = wire.pin_parts()?;
    Ok(Some(ConfigPin {
        key: key.clone(),
        digest,
        wire: wire.into(),
    }))
}

/// The indexes a `Register` of `coll` carries.
fn stored_indexes(coll: &StoredCollection) -> Vec<RegisteredIndex> {
    let mut indexes = derive_auto_indexes(coll.fields.iter().map(|(n, _)| n.as_str()));
    extend_with_catalog_indexes(&mut indexes, coll);
    indexes
}

/// The catalog's committed state, with no transaction's buffered DDL.
struct Committed<'a>(&'a SystemCatalog);

impl CollectionSource for Committed<'_> {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
        Ok(self
            .0
            .list_databases()?
            .into_iter()
            .map(|db| db.id)
            .collect())
    }
    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
        self.0.load_all_collections(database_id)
    }
    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        self.0
            .load_committed_collections_for_tenant(database_id, tenant_id)
    }
    fn index_records_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        self.0
            .list_committed_index_records(database_id.as_u64(), tenant_id)
    }
    fn index_records_for_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        self.0.list_committed_index_records_for_collection(
            database_id.as_u64(),
            tenant_id,
            collection,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::MaterializedSumDef;
    use crate::control::server::shared::ddl::neutral::collection::register::build_doc_config_from_stored;
    use nodedb_physical::physical_plan::TextFieldSet;
    use nodedb_types::TenantId;

    fn key(name: &str) -> ConfigPinKey {
        ConfigPinKey {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: name.into(),
        }
    }

    fn pin(cache: &ConfigPinCache, catalog: &SystemCatalog, name: &str) -> Arc<ConfigPin> {
        cache
            .config_pin(catalog, &key(name))
            .expect("pin")
            .expect("an active collection has a pin")
    }

    /// A pin's digest is the digest the core stores for the same
    /// collection, and its wire bytes hash to it.
    #[test]
    fn a_pin_digest_equals_the_core_config_digest() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let mut coll = StoredCollection::stamped_for_test(1, "orders", "owner");
        coll.fields = vec![("sku".into(), "STRING".into())];
        coll.append_only = true;
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("put");
        let config = build_doc_config_from_stored(
            &catalog,
            TenantId::new(1),
            &coll,
            &stored_indexes(&coll),
            TextFieldSet::default(),
        )
        .expect("config");
        let pinned = pin(&ConfigPinCache::new(), &catalog, "orders");
        assert_ne!(config.digest, 0);
        assert_eq!(pinned.digest, config.digest);
        assert_eq!(
            pinned.digest,
            nodedb_physical::physical_plan::pin_digest(&pinned.wire)
        );
    }

    /// The cache answers the pin it holds while the catalog is unchanged,
    /// and a fresh pin after a DDL that changes the config: an ALTER of the
    /// collection, or a sum binding added on another collection.
    #[test]
    fn a_ddl_that_changes_the_config_gives_a_fresh_pin() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let cache = ConfigPinCache::new();
        let mut source = StoredCollection::stamped_for_test(1, "entries", "owner");
        let mut target = StoredCollection::stamped_for_test(1, "accounts", "owner");
        catalog
            .put_collection(DatabaseId::DEFAULT, &source)
            .expect("put");
        catalog
            .put_collection(DatabaseId::DEFAULT, &target)
            .expect("put");

        let first = pin(&cache, &catalog, "entries");
        assert!(Arc::ptr_eq(&first, &pin(&cache, &catalog, "entries")));

        target.materialized_sums.push(MaterializedSumDef {
            target_collection: target.name.clone(),
            target_column: "balance".into(),
            source_collection: source.name.clone(),
            join_column: "account_id".into(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".into()),
        });
        catalog
            .put_collection(DatabaseId::DEFAULT, &target)
            .expect("put");
        let bound = pin(&cache, &catalog, "entries");
        assert_ne!(bound.digest, first.digest);

        source.append_only = true;
        catalog
            .put_collection(DatabaseId::DEFAULT, &source)
            .expect("put");
        let altered = pin(&cache, &catalog, "entries");
        assert_ne!(altered.digest, bound.digest);
    }

    #[test]
    fn a_missing_or_dropped_collection_has_no_pin() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let cache = ConfigPinCache::new();
        assert!(
            cache
                .config_pin(&catalog, &key("ghost"))
                .expect("lookup")
                .is_none()
        );
        let mut coll = StoredCollection::stamped_for_test(1, "gone", "owner");
        coll.is_active = false;
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("put");
        assert!(
            cache
                .config_pin(&catalog, &key("gone"))
                .expect("lookup")
                .is_none()
        );
    }

    /// A KV collection's declared columns come from its typed schema, the
    /// key column left out. A DDL that changes the schema gives fresh
    /// columns. A missing collection declares none.
    #[test]
    fn kv_declared_columns_follow_the_committed_schema() {
        use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let cache = ConfigPinCache::new();
        let schema = |value: ColumnType| {
            StrictSchema::new(vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::nullable("v", value),
            ])
            .expect("schema")
        };
        let mut coll = StoredCollection::stamped_for_test(1, "counters", "owner");
        coll.collection_type = nodedb_types::CollectionType::kv(schema(ColumnType::String));
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("put");
        let columns = |name: &str| {
            cache
                .kv_declared_columns(
                    &catalog,
                    TenantId::new(1),
                    &QualifiedCollection::new(DatabaseId::DEFAULT, name),
                )
                .expect("lookup")
        };
        assert!(
            columns("counters")
                .columns("counters")
                .expect("resolved")
                .is_empty()
        );

        coll.collection_type = nodedb_types::CollectionType::kv(schema(ColumnType::Int64));
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("put");
        let resolved = columns("counters");
        let declared = resolved.columns("counters").expect("resolved");
        assert_eq!(declared.len(), 1);
        assert_eq!(declared[0].name, "v");

        assert!(
            columns("ghost")
                .columns("ghost")
                .expect("resolved")
                .is_empty()
        );

        // The lookup takes its database from the collection name. The same
        // name in another database is another collection, with no columns.
        let other = QualifiedCollection::new(DatabaseId::new(5), "counters");
        assert!(
            cache
                .kv_declared_columns(&catalog, TenantId::new(1), &other)
                .expect("lookup")
                .columns(other.as_str())
                .expect("resolved")
                .is_empty()
        );
    }

    /// The CRS pin holds while the catalog shares its set, and changes once
    /// a CRS is defined.
    #[test]
    fn a_crs_definition_gives_a_fresh_crs_pin() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let cache = ConfigPinCache::new();
        let empty = cache.crs_pin(&catalog).expect("pin");
        assert!(Arc::ptr_eq(&empty, &cache.crs_pin(&catalog).expect("pin")));
        let srid = nodedb_crs::Srid::new(910_020).expect("srid");
        let def =
            nodedb_crs::parse_proj_string(srid, "user tm", "+proj=tmerc +lon_0=101 +ellps=GRS80")
                .expect("def");
        catalog.define_custom_crs(&def).expect("define");
        let defined = cache.crs_pin(&catalog).expect("pin");
        assert_ne!(defined.digest, empty.digest);
        assert_eq!(
            *defined,
            crs_pin_of(&catalog.custom_crs_set()).expect("pin")
        );
    }
}
