// SPDX-License-Identifier: BUSL-1.1

//! The stored row a fenced entry names, and the incarnation of that row.

use nodedb_types::Hlc;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;

/// `(descriptor_version, modification_hlc)` of one stored row. The clock is
/// the fence key: a recreate restarts the version at 1, so an older
/// incarnation can hold a higher version. Families without a descriptor
/// version carry `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Incarnation {
    pub descriptor_version: u64,
    pub hlc: Hlc,
}

impl Incarnation {
    /// Carried by a delete proposed against an absent row.
    pub const UNSTAMPED: Self = Self {
        descriptor_version: 0,
        hlc: Hlc::ZERO,
    };

    fn versioned(descriptor_version: u64, hlc: Hlc) -> Self {
        Self {
            descriptor_version,
            hlc,
        }
    }

    fn unversioned(hlc: Hlc) -> Self {
        Self {
            descriptor_version: 0,
            hlc,
        }
    }
}

/// Identity of one stored row: `(database_id, tenant_id, name)`, plus the
/// field name for vector index parameters and the stream name for consumer
/// groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKey<'a> {
    Collection(u64, u64, &'a str),
    Sequence(u64, u64, &'a str),
    Trigger(u64, u64, &'a str),
    Function(u64, u64, &'a str),
    Procedure(u64, u64, &'a str),
    MaterializedView(u64, u64, &'a str),
    ContinuousAggregate(u64, u64, &'a str),
    SynonymGroup(u64, u64, &'a str),
    Topic(u64, u64, &'a str),
    VectorIndexParams(u64, u64, &'a str, &'a str),
    ChangeStream(u64, u64, &'a str),
    /// `(database_id, tenant_id, stream_name, group_name)`.
    ConsumerGroup(u64, u64, &'a str, &'a str),
    Array(u64, u64, &'a str),
}

impl<'a> RowKey<'a> {
    /// The descriptor name reported in a fence anomaly.
    pub fn name(&self) -> &'a str {
        match *self {
            Self::Collection(_, _, name)
            | Self::Sequence(_, _, name)
            | Self::Trigger(_, _, name)
            | Self::Function(_, _, name)
            | Self::Procedure(_, _, name)
            | Self::MaterializedView(_, _, name)
            | Self::ContinuousAggregate(_, _, name)
            | Self::SynonymGroup(_, _, name)
            | Self::Topic(_, _, name)
            | Self::ChangeStream(_, _, name)
            | Self::ConsumerGroup(_, _, _, name)
            | Self::Array(_, _, name) => name,
            Self::VectorIndexParams(_, _, collection, _) => collection,
        }
    }

    /// Read the committed row this key names.
    pub fn read(&self, catalog: &SystemCatalog) -> crate::Result<Option<Incarnation>> {
        Ok(match *self {
            Self::Collection(db, tenant, name) => catalog
                .get_committed_collection(DatabaseId::new(db), tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::Sequence(db, tenant, name) => catalog
                .get_sequence(db, tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::Trigger(db, tenant, name) => catalog
                .get_committed_trigger_in_database(DatabaseId::new(db), tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::Function(db, tenant, name) => catalog
                .get_committed_function_in_database(DatabaseId::new(db), tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::Procedure(db, tenant, name) => catalog
                .get_committed_procedure_in_database(DatabaseId::new(db), tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::MaterializedView(db, tenant, name) => catalog
                .get_committed_materialized_view(db, tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::ContinuousAggregate(db, tenant, name) => catalog
                .get_continuous_aggregate(db, tenant, name)?
                .map(|row| Incarnation::versioned(row.descriptor_version, row.modification_hlc)),
            Self::SynonymGroup(db, tenant, name) => catalog
                .get_synonym_group(db, tenant, name)?
                .map(|row| Incarnation::unversioned(row.modification_hlc)),
            Self::Topic(db, tenant, name) => catalog
                .get_committed_ep_topic(DatabaseId::new(db), tenant, name)?
                .map(|row| Incarnation::unversioned(row.modification_hlc)),
            Self::VectorIndexParams(db, tenant, collection, field) => catalog
                .get_committed_vector_index_params(db, tenant, collection, field)?
                .map(|row| Incarnation::unversioned(row.modification_hlc)),
            Self::ChangeStream(db, tenant, name) => catalog
                .get_change_stream(DatabaseId::new(db), tenant, name)?
                .map(|row| Incarnation::unversioned(row.modification_hlc)),
            Self::ConsumerGroup(db, tenant, stream, group) => catalog
                .get_consumer_group(DatabaseId::new(db), tenant, stream, group)?
                .map(|row| Incarnation::unversioned(row.modification_hlc)),
            Self::Array(db, tenant, name) => catalog
                .get_array_in_database(
                    nodedb_types::TenantId::new(tenant),
                    DatabaseId::new(db),
                    name,
                )?
                .map(|row| Incarnation::unversioned(row.modification_hlc)),
        })
    }
}

/// The row a fenced delete removes. `None` for every other entry.
pub fn delete_key(entry: &CatalogEntry) -> Option<RowKey<'_>> {
    Some(match entry {
        CatalogEntry::PurgeCollection {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Collection(*database_id, *tenant_id, name),
        CatalogEntry::DeleteSequence {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Sequence(*database_id, *tenant_id, name),
        CatalogEntry::DeleteTrigger {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Trigger(database_id.as_u64(), *tenant_id, name),
        CatalogEntry::DeleteFunction {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Function(database_id.as_u64(), *tenant_id, name),
        CatalogEntry::DeleteProcedure {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Procedure(database_id.as_u64(), *tenant_id, name),
        CatalogEntry::DeleteMaterializedView {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::MaterializedView(*database_id, *tenant_id, name),
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::ContinuousAggregate(*database_id, *tenant_id, name),
        CatalogEntry::DeleteSynonymGroup {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::SynonymGroup(*database_id, *tenant_id, name),
        CatalogEntry::DeleteTopicWithConsumerGroups {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Topic(*database_id, *tenant_id, name),
        CatalogEntry::DeleteVectorIndexParams {
            database_id,
            tenant_id,
            collection,
            field_name,
            ..
        } => RowKey::VectorIndexParams(*database_id, *tenant_id, collection, field_name),
        CatalogEntry::DeleteChangeStream {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::ChangeStream(*database_id, *tenant_id, name),
        CatalogEntry::DeleteConsumerGroup {
            database_id,
            tenant_id,
            stream_name,
            name,
            ..
        } => RowKey::ConsumerGroup(*database_id, *tenant_id, stream_name, name),
        CatalogEntry::DeleteArray {
            database_id,
            tenant_id,
            name,
            ..
        } => RowKey::Array(*database_id, *tenant_id, name),
        _ => return None,
    })
}

/// The incarnation a fenced delete targets. `None` for every other entry.
pub fn carried_target(entry: &CatalogEntry) -> Option<Incarnation> {
    match entry {
        CatalogEntry::PurgeCollection {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteSequence {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteTrigger {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteFunction {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteProcedure {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteMaterializedView {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteContinuousAggregate {
            target_descriptor_version,
            target_hlc,
            ..
        } => Some(Incarnation::versioned(
            *target_descriptor_version,
            *target_hlc,
        )),
        CatalogEntry::DeleteSynonymGroup { target_hlc, .. }
        | CatalogEntry::DeleteTopicWithConsumerGroups { target_hlc, .. }
        | CatalogEntry::DeleteVectorIndexParams { target_hlc, .. }
        | CatalogEntry::DeleteChangeStream { target_hlc, .. }
        | CatalogEntry::DeleteConsumerGroup { target_hlc, .. }
        | CatalogEntry::DeleteArray { target_hlc, .. } => {
            Some(Incarnation::unversioned(*target_hlc))
        }
        _ => None,
    }
}

/// Replace the incarnation a fenced delete targets. Other entries pass through.
pub fn with_target(mut entry: CatalogEntry, target: Incarnation) -> CatalogEntry {
    match &mut entry {
        CatalogEntry::PurgeCollection {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteSequence {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteTrigger {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteFunction {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteProcedure {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteMaterializedView {
            target_descriptor_version,
            target_hlc,
            ..
        }
        | CatalogEntry::DeleteContinuousAggregate {
            target_descriptor_version,
            target_hlc,
            ..
        } => {
            *target_descriptor_version = target.descriptor_version;
            *target_hlc = target.hlc;
        }
        CatalogEntry::DeleteSynonymGroup { target_hlc, .. }
        | CatalogEntry::DeleteTopicWithConsumerGroups { target_hlc, .. }
        | CatalogEntry::DeleteVectorIndexParams { target_hlc, .. }
        | CatalogEntry::DeleteChangeStream { target_hlc, .. }
        | CatalogEntry::DeleteConsumerGroup { target_hlc, .. }
        | CatalogEntry::DeleteArray { target_hlc, .. } => {
            *target_hlc = target.hlc;
        }
        _ => {}
    }
    entry
}

/// The row a put, create, or soft delete leaves behind, and its incarnation.
pub fn written_row(entry: &CatalogEntry) -> Option<(RowKey<'_>, Incarnation)> {
    Some(match entry {
        CatalogEntry::PutCollection(row) | CatalogEntry::PutCollectionIfAbsent(row) => (
            RowKey::Collection(row.database_id.as_u64(), row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            descriptor_version,
            modification_hlc,
        } => (
            RowKey::Collection(*database_id, *tenant_id, name),
            Incarnation::versioned(*descriptor_version, *modification_hlc),
        ),
        CatalogEntry::PutSequence(row) => (
            RowKey::Sequence(row.database_id, row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::PutTrigger(row) => (
            RowKey::Trigger(row.database_id.as_u64(), row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::PutFunction(row) => (
            RowKey::Function(row.database_id.as_u64(), row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::PutProcedure(row) => (
            RowKey::Procedure(row.database_id.as_u64(), row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::PutMaterializedView(row) => (
            RowKey::MaterializedView(row.database_id, row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::PutContinuousAggregate(row) => (
            RowKey::ContinuousAggregate(row.database_id, row.tenant_id, &row.name),
            Incarnation::versioned(row.descriptor_version, row.modification_hlc),
        ),
        CatalogEntry::PutSynonymGroup(row) => (
            RowKey::SynonymGroup(row.database_id, row.tenant_id, &row.name),
            Incarnation::unversioned(row.modification_hlc),
        ),
        CatalogEntry::CreateTopicIfAbsent(row) => (
            RowKey::Topic(row.database_id.as_u64(), row.tenant_id, &row.name),
            Incarnation::unversioned(row.modification_hlc),
        ),
        CatalogEntry::PutVectorIndexParams(row) => (
            RowKey::VectorIndexParams(
                row.database_id,
                row.tenant_id,
                &row.collection,
                &row.field_name,
            ),
            Incarnation::unversioned(row.modification_hlc),
        ),
        CatalogEntry::PutChangeStream(row) => (
            RowKey::ChangeStream(row.database_id.as_u64(), row.tenant_id, &row.name),
            Incarnation::unversioned(row.modification_hlc),
        ),
        CatalogEntry::PutConsumerGroupIfAbsent(row) => (
            RowKey::ConsumerGroup(
                row.database_id.as_u64(),
                row.tenant_id,
                &row.stream_name,
                &row.name,
            ),
            Incarnation::unversioned(row.modification_hlc),
        ),
        CatalogEntry::PutArray(row) => (
            RowKey::Array(
                row.array_id.database_id.as_u64(),
                row.array_id.tenant_id.as_u64(),
                &row.name,
            ),
            Incarnation::unversioned(row.modification_hlc),
        ),
        _ => return None,
    })
}
