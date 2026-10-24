// SPDX-License-Identifier: BUSL-1.1

//! Stable `kind()` label for every [`CatalogEntry`] variant.
//!
//! Kept out of `entry.rs` so the enum definition stays a pure type
//! declaration: the label table grows one line per variant and
//! pushes the definition file past its size budget when kept inline.

use super::entry::CatalogEntry;

impl CatalogEntry {
    /// Short, human-readable descriptor of this entry — used in
    /// trace / metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PutCollection(_) => "put_collection",
            Self::PutCollectionIfAbsent(_) => "put_collection_if_absent",
            Self::DeactivateCollection { .. } => "deactivate_collection",
            Self::PurgeCollection { .. } => "purge_collection",
            Self::PutSequence(_) => "put_sequence",
            Self::DeleteSequence { .. } => "delete_sequence",
            Self::PutSequenceState(_) => "put_sequence_state",
            Self::PutTrigger(_) => "put_trigger",
            Self::DeleteTrigger { .. } => "delete_trigger",
            Self::PutFunction(_) => "put_function",
            Self::DeleteFunction { .. } => "delete_function",
            Self::PutProcedure(_) => "put_procedure",
            Self::DeleteProcedure { .. } => "delete_procedure",
            Self::PutSchedule(_) => "put_schedule",
            Self::DeleteSchedule { .. } => "delete_schedule",
            Self::PutChangeStream(_) => "put_change_stream",
            Self::DeleteChangeStream { .. } => "delete_change_stream",
            Self::PutUser(_) => "put_user",
            Self::DropUser { .. } => "drop_user",
            Self::PutRole(_) => "put_role",
            Self::DeleteRole { .. } => "delete_role",
            Self::PutApiKey(_) => "put_api_key",
            Self::RevokeApiKey { .. } => "revoke_api_key",
            Self::PutAuthUser(_) => "put_auth_user",
            Self::PutMaterializedView(_) => "put_materialized_view",
            Self::DeleteMaterializedView { .. } => "delete_materialized_view",
            Self::PutStreamingMaterializedView(_) => "put_streaming_materialized_view",
            Self::DeleteStreamingMaterializedView { .. } => "delete_streaming_materialized_view",
            Self::PutContinuousAggregate(_) => "put_continuous_aggregate",
            Self::DeleteContinuousAggregate { .. } => "delete_continuous_aggregate",
            Self::PutTenant(_) => "put_tenant",
            Self::PutTenantWithAdmin { .. } => "put_tenant_with_admin",
            Self::DeleteTenant { .. } => "delete_tenant",
            Self::PutRlsPolicy(_) => "put_rls_policy",
            Self::DeleteRlsPolicy { .. } => "delete_rls_policy",
            Self::PutRedactionPolicy(_) => "put_redaction_policy",
            Self::DeleteRedactionPolicy { .. } => "delete_redaction_policy",
            Self::PutPermission(_) => "put_permission",
            Self::DeletePermission { .. } => "delete_permission",
            Self::PutScopeGrant(_) => "put_scope_grant",
            Self::DeleteScopeGrant { .. } => "delete_scope_grant",
            Self::PutIndexRecord(_) => "put_index_record",
            Self::DeleteIndexRecord { .. } => "delete_index_record",
            Self::PutOwner(_) => "put_owner",
            Self::DeleteOwner { .. } => "delete_owner",
            Self::PutDatabase(_) => "put_database",
            Self::DeleteDatabase { .. } => "delete_database",
            Self::PutDatabaseGrant { .. } => "put_database_grant",
            Self::DeleteDatabaseGrant { .. } => "delete_database_grant",
            Self::PutSynonymGroup(_) => "put_synonym_group",
            Self::DeleteSynonymGroup { .. } => "delete_synonym_group",
            Self::PutCustomType(_) => "put_custom_type",
            Self::DeleteCustomType { .. } => "delete_custom_type",
            Self::PutOidcProvider(_) => "put_oidc_provider",
            Self::DeleteOidcProvider { .. } => "delete_oidc_provider",
            Self::RecordWalTombstone { .. } => "record_wal_tombstone",
            Self::MoveTenantCutover { .. } => "move_tenant_cutover",
            Self::CloneDatabase { .. } => "clone_database",
            Self::PutDatabaseQuota { .. } => "put_database_quota",
            Self::DeleteDatabaseQuota { .. } => "delete_database_quota",
            Self::PutTenantQuota { .. } => "put_tenant_quota",
            Self::DeleteTenantQuota { .. } => "delete_tenant_quota",
            Self::PutScopeQuota(_) => "put_scope_quota",
            Self::DeleteScopeQuota { .. } => "delete_scope_quota",
            Self::PutRetentionPolicy(_) => "put_retention_policy",
            Self::DeleteRetentionPolicy { .. } => "delete_retention_policy",
            Self::PutAlertRule(_) => "put_alert_rule",
            Self::DeleteAlertRule { .. } => "delete_alert_rule",
            Self::CreateTopicIfAbsent(_) => "create_topic_if_absent",
            Self::DeleteTopicWithConsumerGroups { .. } => "delete_topic_with_consumer_groups",
            Self::PutConsumerGroupIfAbsent(_) => "put_consumer_group_if_absent",
            Self::DeleteConsumerGroup { .. } => "delete_consumer_group",
            Self::MigrateConsumerGroupStream { .. } => "migrate_consumer_group_stream",
            Self::PutCheckpoint(_) => "put_checkpoint",
            Self::DeleteCheckpoint { .. } => "delete_checkpoint",
            Self::CompactHistory { .. } => "compact_history",
            Self::PutVectorModel(_) => "put_vector_model",
            Self::DeleteVectorModel { .. } => "delete_vector_model",
            Self::PutVectorIndexParams(_) => "put_vector_index_params",
            Self::PutColumnStats(_) => "put_column_stats",
            Self::PutCloneCopyup { .. } => "put_clone_copyup",
            Self::PutCloneTombstone { .. } => "put_clone_tombstone",
            Self::PutKvCloneTombstone { .. } => "put_kv_clone_tombstone",
            Self::PutCloneSourceDrain(_) => "put_clone_source_drain",
            Self::DeleteCloneSourceDrain { .. } => "delete_clone_source_drain",
            Self::PutArray(_) => "put_array",
            Self::DeleteArray { .. } => "delete_array",
            Self::DeleteVectorIndexParams { .. } => "delete_vector_index_params",
            Self::CommitConsumerOffsets(_) => "commit_consumer_offsets",
            Self::PutBackupScheduleMark(_) => "put_backup_schedule_mark",
            Self::PutSpatialRefSys(_) => "put_spatial_ref_sys",
            Self::DropSpatialRefSys { .. } => "drop_spatial_ref_sys",
            Self::PutCredential(_) => "put_credential",
            Self::DropCredential { .. } => "drop_credential",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::catalog::oidc_providers::StoredOidcProvider;
    use crate::control::security::catalog::sequence_types::StoredSequence;
    use crate::control::security::catalog::{StoredCollection, StoredScopeGrant};

    #[test]
    fn kind_label_is_stable() {
        assert_eq!(
            CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "a", "b"))).kind(),
            "put_collection"
        );
        assert_eq!(
            CatalogEntry::DeactivateCollection {
                database_id: 0,
                tenant_id: 1,
                name: "a".into(),
                descriptor_version: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            }
            .kind(),
            "deactivate_collection"
        );
        assert_eq!(
            CatalogEntry::PutSequence(Box::new(StoredSequence::new(0, 1, "c".into(), "b".into())))
                .kind(),
            "put_sequence"
        );
        assert_eq!(
            CatalogEntry::DeleteSequence {
                database_id: 0,
                tenant_id: 1,
                name: "c".into(),
                target_descriptor_version: 0,
                target_hlc: nodedb_types::Hlc::ZERO,
            }
            .kind(),
            "delete_sequence"
        );
        let provider = StoredOidcProvider {
            provider_name: "test".into(),
            issuer: "https://example.com".into(),
            jwks_uri: "https://example.com/.well-known/jwks.json".into(),
            audience: None,
            tenant_id: None,
            claim_mapping: vec![],
            created_at_lsn: 0,
        };
        assert_eq!(
            CatalogEntry::PutOidcProvider(Box::new(provider)).kind(),
            "put_oidc_provider"
        );
        assert_eq!(
            CatalogEntry::DeleteOidcProvider {
                name: "test".into()
            }
            .kind(),
            "delete_oidc_provider"
        );
        let scope_grant = StoredScopeGrant {
            scope_name: "pro:all".into(),
            grantee_type: "org".into(),
            grantee_id: "acme".into(),
            granted_by: "admin".into(),
            granted_at: 1_000,
            expires_at: 0,
            grace_period_secs: 0,
            on_expire_action: String::new(),
            conditions_json: String::new(),
        };
        assert_eq!(
            CatalogEntry::PutScopeGrant(Box::new(scope_grant)).kind(),
            "put_scope_grant"
        );
        assert_eq!(
            CatalogEntry::DeleteScopeGrant {
                scope_name: "pro:all".into(),
                grantee_type: "org".into(),
                grantee_id: "acme".into(),
            }
            .kind(),
            "delete_scope_grant"
        );
    }
}
