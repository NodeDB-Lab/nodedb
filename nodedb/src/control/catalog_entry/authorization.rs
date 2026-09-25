// SPDX-License-Identifier: BUSL-1.1

//! Which catalog entries change authorization state.
//!
//! An entry that changes who may do what, or what a statement may see, is
//! acknowledged only after it binds every node (see the authorization
//! lease). The match is exhaustive, so a new entry kind is classified when
//! it is added.

use super::entry::CatalogEntry;

impl CatalogEntry {
    /// Whether applying this entry can change what a statement is allowed to
    /// read or write.
    pub fn bears_authorization(&self) -> bool {
        match self {
            // A collection carries its owner and its permission tree. Dropping
            // or purging it removes the owner and the grants on it.
            Self::PutCollection(_)
            | Self::PutCollectionIfAbsent(_)
            | Self::DeactivateCollection { .. }
            | Self::PurgeCollection { .. } => true,
            Self::PutUser(_)
            | Self::DropUser { .. }
            | Self::PutRole(_)
            | Self::DeleteRole { .. }
            | Self::PutApiKey(_)
            | Self::RevokeApiKey { .. }
            | Self::PutAuthUser(_)
            | Self::PutOidcProvider(_)
            | Self::DeleteOidcProvider { .. } => true,
            Self::PutTenant(_) | Self::PutTenantWithAdmin { .. } | Self::DeleteTenant { .. } => {
                true
            }
            Self::PutRlsPolicy(_)
            | Self::DeleteRlsPolicy { .. }
            | Self::PutRedactionPolicy(_)
            | Self::DeleteRedactionPolicy { .. } => true,
            Self::PutPermission(_)
            | Self::DeletePermission { .. }
            | Self::PutScopeGrant(_)
            | Self::DeleteScopeGrant { .. }
            | Self::PutOwner(_)
            | Self::DeleteOwner { .. } => true,
            Self::PutDatabase(_)
            | Self::DeleteDatabase { .. }
            | Self::PutDatabaseGrant { .. }
            | Self::DeleteDatabaseGrant { .. }
            | Self::CloneDatabase { .. } => true,
            Self::PutSequence(_)
            | Self::DeleteSequence { .. }
            | Self::PutSequenceState(_)
            | Self::PutTrigger(_)
            | Self::DeleteTrigger { .. }
            | Self::PutFunction(_)
            | Self::DeleteFunction { .. }
            | Self::PutProcedure(_)
            | Self::DeleteProcedure { .. }
            | Self::PutSchedule(_)
            | Self::DeleteSchedule { .. }
            | Self::PutChangeStream(_)
            | Self::DeleteChangeStream { .. }
            | Self::PutMaterializedView(_)
            | Self::DeleteMaterializedView { .. }
            | Self::PutStreamingMaterializedView(_)
            | Self::DeleteStreamingMaterializedView { .. }
            | Self::PutContinuousAggregate(_)
            | Self::DeleteContinuousAggregate { .. }
            | Self::PutIndexRecord(_)
            | Self::DeleteIndexRecord { .. }
            | Self::PutSynonymGroup(_)
            | Self::DeleteSynonymGroup { .. }
            | Self::PutCustomType(_)
            | Self::DeleteCustomType { .. }
            | Self::RecordWalTombstone { .. }
            | Self::MoveTenantCutover { .. }
            | Self::PutDatabaseQuota { .. }
            | Self::DeleteDatabaseQuota { .. }
            | Self::PutTenantQuota { .. }
            | Self::DeleteTenantQuota { .. }
            | Self::PutScopeQuota(_)
            | Self::DeleteScopeQuota { .. }
            | Self::PutRetentionPolicy(_)
            | Self::DeleteRetentionPolicy { .. }
            | Self::PutAlertRule(_)
            | Self::DeleteAlertRule { .. }
            | Self::CreateTopicIfAbsent(_)
            | Self::DeleteTopicWithConsumerGroups { .. }
            | Self::PutConsumerGroupIfAbsent(_)
            | Self::DeleteConsumerGroup { .. }
            | Self::MigrateConsumerGroupStream { .. }
            | Self::PutCheckpoint(_)
            | Self::DeleteCheckpoint { .. }
            | Self::CompactHistory { .. }
            | Self::PutVectorModel(_)
            | Self::DeleteVectorModel { .. }
            | Self::PutVectorIndexParams(_)
            | Self::PutColumnStats(_)
            | Self::DeleteVectorIndexParams { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;

    #[test]
    fn a_collection_bears_authorization_and_a_sequence_does_not() {
        assert!(
            CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "a", "b")))
                .bears_authorization()
        );
        assert!(
            !CatalogEntry::DeleteSequence {
                database_id: 0,
                tenant_id: 1,
                name: "c".into(),
            }
            .bears_authorization()
        );
    }
}
