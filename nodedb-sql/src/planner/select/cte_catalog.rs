// SPDX-License-Identifier: Apache-2.0

//! Catalog wrapper that resolves CTE and derived-alias names as relations.

use nodedb_types::DatabaseId;

use crate::error::Result;
use crate::resolver::derived::open_subquery_relation;
use crate::types::{ArrayCatalogView, CollectionInfo, SqlCatalog, SqlCatalogError};

/// Catalog wrapper that answers for a synthesized relation before delegating
/// to the stored catalog.
pub(crate) struct CteCatalog<'a> {
    pub(crate) inner: &'a dyn SqlCatalog,
    /// Each synthesized relation name paired with the shape its body exposes.
    pub(crate) relations: Vec<(String, CollectionInfo)>,
}

impl<'a> CteCatalog<'a> {
    /// A catalog exposing one relation whose shape is not inferable.
    ///
    /// The recursive arm of a `WITH RECURSIVE` names the working table while
    /// planning its own body, so that arm's shape is not known yet.
    pub(crate) fn open(inner: &'a dyn SqlCatalog, name: &str) -> Self {
        Self {
            inner,
            relations: vec![(name.to_string(), open_subquery_relation(name))],
        }
    }
}

impl CteCatalog<'_> {
    fn relation(&self, name: &str) -> Option<CollectionInfo> {
        self.relations
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, info)| info.clone())
    }
}

/// A synthesized relation shadows a stored one of the same name. Every
/// other lookup reaches the stored catalog unchanged, so a query planned
/// against this wrapper still sees catalog tables, arrays, types and
/// sequences.
impl SqlCatalog for CteCatalog<'_> {
    fn get_collection(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        match self.relation(name) {
            Some(info) => Ok(Some(info)),
            None => self.inner.get_collection(database_id, name),
        }
    }

    fn resolve_relation(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        match self.relation(name) {
            Some(info) => Ok(Some(info)),
            None => self.inner.resolve_relation(database_id, name),
        }
    }

    /// A synthesized relation is never a timeseries collection.
    fn timeseries_time_key(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<String>, SqlCatalogError> {
        match self.relation(name) {
            Some(_) => Ok(None),
            None => self.inner.timeseries_time_key(database_id, name),
        }
    }

    fn crs_context(&self) -> nodedb_crs::CrsContext<'_> {
        self.inner.crs_context()
    }

    fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
        self.inner.lookup_array(name)
    }

    fn array_exists(&self, name: &str) -> bool {
        self.inner.array_exists(name)
    }

    fn resolve_regclass(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> Option<i64> {
        self.inner.resolve_regclass(database_id, tenant_id, name)
    }

    fn resolve_regtype(&self, name: &str) -> Option<i64> {
        self.inner.resolve_regtype(name)
    }

    fn sequence_nextval(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> Result<i64> {
        self.inner.sequence_nextval(database_id, tenant_id, name)
    }

    fn sequence_currval(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> Result<i64> {
        self.inner.sequence_currval(database_id, tenant_id, name)
    }

    fn sequence_setval(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
        value: i64,
    ) -> Result<i64> {
        self.inner
            .sequence_setval(database_id, tenant_id, name, value)
    }
}
