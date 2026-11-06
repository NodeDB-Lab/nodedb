// SPDX-License-Identifier: BUSL-1.1

//! The databases a [`CatalogEntry`] changes objects in, for the mirror gate.
//!
//! A mirror follows its source's objects, so object DDL on it is refused.
//! Three kinds of entry stay out of that scope:
//!
//! - Cluster objects: users, roles, keys, tenants, CRSs, credentials and
//!   permission grants. A mirror's readers are this cluster's principals, and
//!   no source object names them.
//! - The database's own lifecycle: the descriptor, its grants and its quotas.
//!   PROMOTE and DROP DATABASE are these.
//! - Reader state: consumer groups, their committed offsets and column
//!   statistics. These change no stored row and no schema.

use crate::types::DatabaseId;

use super::entry::CatalogEntry;

/// The database a `db_qualified` collection name carries. A bare name belongs
/// to the default database, which is never a mirror.
fn qualified_database(collection: &str) -> Option<DatabaseId> {
    let (head, _) = collection.split_once('/')?;
    head.parse::<u64>().ok().map(DatabaseId::new)
}

fn raw(database_id: u64) -> [Option<DatabaseId>; 2] {
    [Some(DatabaseId::new(database_id)), None]
}

fn one(database_id: DatabaseId) -> [Option<DatabaseId>; 2] {
    [Some(database_id), None]
}

const UNSCOPED: [Option<DatabaseId>; 2] = [None, None];

impl CatalogEntry {
    /// The databases this entry changes objects in. A MOVE TENANT cutover
    /// names two.
    pub fn mirror_scope(&self) -> [Option<DatabaseId>; 2] {
        match self {
            Self::PutCollection(stored) | Self::PutCollectionIfAbsent(stored) => {
                one(stored.database_id)
            }
            Self::PutSequence(stored) => raw(stored.database_id),
            Self::PutSequenceState(stored) => raw(stored.database_id),
            Self::PutTrigger(stored) => one(stored.database_id),
            Self::PutFunction(stored) => one(stored.database_id),
            Self::PutProcedure(stored) => one(stored.database_id),
            Self::PutSchedule(stored) => raw(stored.database_id),
            Self::PutSynonymGroup(stored) => raw(stored.database_id),
            Self::PutCustomType(stored) => raw(stored.database_id),
            Self::PutChangeStream(stored) => one(stored.database_id),
            Self::PutMaterializedView(stored) => raw(stored.database_id),
            Self::PutStreamingMaterializedView(stored) => one(stored.database_id),
            Self::PutContinuousAggregate(stored) => raw(stored.database_id),
            Self::PutIndexRecord(stored) => raw(stored.database_id),
            Self::PutOwner(stored) => raw(stored.database_id),
            Self::PutRetentionPolicy(stored) => raw(stored.database_id),
            Self::PutAlertRule(stored) => raw(stored.database_id),
            Self::CreateTopicIfAbsent(stored) => one(stored.database_id),
            Self::PutCheckpoint(stored) => raw(stored.database_id),
            Self::PutVectorModel(stored) => raw(stored.database_id),
            Self::PutVectorIndexParams(stored) => raw(stored.database_id),
            Self::PutRlsPolicy(stored) => raw(stored.database_id),
            Self::PutRedactionPolicy(stored) => [qualified_database(&stored.collection), None],
            Self::DeleteRlsPolicy { collection, .. }
            | Self::DeleteRedactionPolicy { collection, .. } => {
                [qualified_database(collection), None]
            }
            Self::PutCloneSourceDrain(row) => one(row.clone_database_id()),
            Self::PutArray(stored) => one(stored.array_id.database_id),

            Self::DeactivateCollection { database_id, .. }
            | Self::PurgeCollection { database_id, .. }
            | Self::DeleteSequence { database_id, .. }
            | Self::DeleteSynonymGroup { database_id, .. }
            | Self::DeleteCustomType { database_id, .. }
            | Self::DeleteChangeStream { database_id, .. }
            | Self::DeleteMaterializedView { database_id, .. }
            | Self::DeleteStreamingMaterializedView { database_id, .. }
            | Self::DeleteContinuousAggregate { database_id, .. }
            | Self::DeleteIndexRecord { database_id, .. }
            | Self::DeleteOwner { database_id, .. }
            | Self::RecordWalTombstone { database_id, .. }
            | Self::DeleteRetentionPolicy { database_id, .. }
            | Self::DeleteAlertRule { database_id, .. }
            | Self::DeleteTopicWithConsumerGroups { database_id, .. }
            | Self::DeleteCheckpoint { database_id, .. }
            | Self::CompactHistory { database_id, .. }
            | Self::DeleteVectorModel { database_id, .. }
            | Self::DeleteVectorIndexParams { database_id, .. }
            | Self::PutCloneCopyup { database_id, .. }
            | Self::PutCloneTombstone { database_id, .. }
            | Self::PutKvCloneTombstone { database_id, .. }
            | Self::DeleteArray { database_id, .. } => raw(*database_id),
            Self::DeleteCloneSourceDrain { clone_database, .. } => raw(*clone_database),
            Self::DeleteTrigger { database_id, .. }
            | Self::DeleteFunction { database_id, .. }
            | Self::DeleteProcedure { database_id, .. }
            | Self::DeleteSchedule { database_id, .. } => one(*database_id),

            Self::MoveTenantCutover {
                source_db_id,
                target_db_id,
                ..
            } => [
                Some(DatabaseId::new(*source_db_id)),
                Some(DatabaseId::new(*target_db_id)),
            ],

            // Cluster objects.
            Self::PutUser(_)
            | Self::DropUser { .. }
            | Self::PutRole(_)
            | Self::DeleteRole { .. }
            | Self::PutApiKey(_)
            | Self::RevokeApiKey { .. }
            | Self::PutAuthUser(_)
            | Self::PutTenant(_)
            | Self::PutTenantWithAdmin { .. }
            | Self::DeleteTenant { .. }
            | Self::PutPermission(_)
            | Self::DeletePermission { .. }
            | Self::PutOidcProvider(_)
            | Self::DeleteOidcProvider { .. }
            | Self::PutScopeGrant(_)
            | Self::DeleteScopeGrant { .. }
            | Self::PutScopeQuota(_)
            | Self::DeleteScopeQuota { .. }
            | Self::PutBackupScheduleMark(_)
            | Self::PutSpatialRefSys(_)
            | Self::DropSpatialRefSys { .. }
            | Self::PutCredential(_)
            | Self::DropCredential { .. } => UNSCOPED,

            // The database's own lifecycle. CLONE DATABASE refuses a mirror
            // source on its own path, and its target is a new database.
            Self::PutDatabase(_)
            | Self::DeleteDatabase { .. }
            | Self::PutDatabaseGrant { .. }
            | Self::DeleteDatabaseGrant { .. }
            | Self::CloneDatabase { .. }
            | Self::PutDatabaseQuota { .. }
            | Self::DeleteDatabaseQuota { .. }
            | Self::PutTenantQuota { .. }
            | Self::DeleteTenantQuota { .. } => UNSCOPED,

            // How far the mirror followed its source: the applier's own row.
            Self::PutMirrorCutState { .. } => UNSCOPED,

            // The mirror's own status: the applier's, the monitor's or PROMOTE's.
            Self::StepMirrorStatus { .. } => UNSCOPED,

            // A link's hold on this cluster's logs: the source's own row.
            Self::PutMirrorPin(_) | Self::DeleteMirrorPin { .. } => UNSCOPED,

            // Reader state.
            Self::PutConsumerGroupIfAbsent(_)
            | Self::DeleteConsumerGroup { .. }
            | Self::CommitConsumerOffsets(_)
            | Self::PutColumnStats(_) => UNSCOPED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_qualified_name_carries_its_database() {
        assert_eq!(
            qualified_database("1024/users"),
            Some(DatabaseId::new(1024))
        );
    }

    #[test]
    fn a_bare_name_names_no_mirror() {
        assert_eq!(qualified_database("users"), None);
    }

    #[test]
    fn a_collection_drop_is_scoped_to_its_database() {
        let entry = CatalogEntry::DeactivateCollection {
            database_id: 7,
            tenant_id: 1,
            name: "c".into(),
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        assert_eq!(entry.mirror_scope(), [Some(DatabaseId::new(7)), None]);
    }

    #[test]
    fn a_policy_drop_is_scoped_by_its_qualified_collection() {
        let entry = CatalogEntry::DeleteRlsPolicy {
            tenant_id: 1,
            collection: "7/users".into(),
            name: "p".into(),
        };
        assert_eq!(entry.mirror_scope(), [Some(DatabaseId::new(7)), None]);
    }

    #[test]
    fn a_tenant_move_names_both_databases() {
        let entry = CatalogEntry::MoveTenantCutover {
            tenant_id: 1,
            source_db_id: 7,
            target_db_id: 8,
            collections: Vec::new(),
        };
        assert_eq!(
            entry.mirror_scope(),
            [Some(DatabaseId::new(7)), Some(DatabaseId::new(8))]
        );
    }

    #[test]
    fn the_database_lifecycle_is_unscoped() {
        assert_eq!(
            CatalogEntry::DeleteDatabase { db_id: 7 }.mirror_scope(),
            UNSCOPED
        );
        assert_eq!(
            CatalogEntry::PutDatabaseGrant {
                db_id: 7,
                user_id: 1,
                privilege: "read".into(),
            }
            .mirror_scope(),
            UNSCOPED
        );
    }

    #[test]
    fn a_permission_grant_is_unscoped() {
        let entry = CatalogEntry::DeletePermission {
            target: "collection:1:users".into(),
            grantee: "alice".into(),
            permission: "read".into(),
        };
        assert_eq!(entry.mirror_scope(), UNSCOPED);
    }
}
