// SPDX-License-Identifier: BUSL-1.1

pub mod alert;
pub mod arrays;
pub mod audit;
pub mod auth_types;
pub mod auth_users;
pub mod backup_schedule_marks;
pub mod blacklist;
pub mod bootstrap_tables;
pub mod calvin_applied;
pub mod calvin_barrier_logs;
pub mod calvin_base;
pub mod change_streams;
pub mod checkpoint;
pub mod checkpoints;
pub mod clone_catalog;
pub mod clone_source_drains;
pub mod collection;
pub mod collection_constraints;
pub mod collection_descriptor_convert;
pub mod collection_incarnation;
pub mod collections;
pub mod column_stats;
pub mod constraint_translate;
pub mod consumer_groups;
pub mod continuous_aggregate;
pub mod continuous_aggregates;
pub mod crdt_compaction_points;
pub mod custom_type_oid_hwm;
pub mod custom_types;
pub mod cut_floors;
pub mod database;
pub mod database_grants;
pub mod database_quotas;
pub mod database_types;
pub mod dependencies;
pub mod event_defs_index;
pub mod function_types;
pub mod functions;
pub mod index_record;
pub mod index_registry;
pub mod l2_cleanup_queue;
pub mod lockout;
pub mod materialized_view;
pub mod materialized_views;
pub mod metadata;
pub mod metadata_host;
pub mod mirror;
pub mod move_tenant_journal;
pub mod move_tenant_journal_types;
pub mod oidc_providers;
pub mod orgs;
pub mod owner_rewrite;
pub mod ownership_fallback;
pub mod pending_history_compaction;
pub mod pending_leave_cleanup;
pub mod pending_reclaim;
pub mod procedure_types;
pub mod procedures;
pub mod read_only;
pub mod redaction;
pub mod redo_snapshot_owed;
pub mod replicated_image;
pub mod replicated_image_merge;
pub mod restore_points;
pub mod retention_policy;
pub mod rls;
pub mod schedules;
pub mod scope_quotas;
pub mod scopes;
pub mod security;
pub mod sequence_types;
pub mod sequences;
pub mod streaming_mvs;
pub mod surrogate_hwm;
pub mod surrogate_pk;
pub mod sync_producer;
pub mod synonym_groups;
pub mod system_catalog;
pub mod tables;
pub mod tenant_group_marks;
pub mod tenant_id_hwm;
pub mod tenant_quotas;
pub mod topic_lookup;
pub mod topic_messages;
pub mod topic_publish_marks;
pub mod topics;
pub mod trigger_types;
pub mod triggers;
pub mod types;
pub mod users;
pub mod vector_index_params;
pub mod vector_model;
pub mod wal_tombstones;

pub use auth_types::{
    StoredApiKey, StoredAuditEntry, StoredAuthUser, StoredBlacklistEntry, StoredOwner,
    StoredPermission, StoredRole, StoredTenant, StoredUser,
};
pub use collection_constraints::{
    BalancedConstraintDef, CheckConstraintDef, EventDefinition, FieldDefinition, LegalHold,
    MaterializedSumDef, PeriodLockDef, StateTransitionDef, TransitionCheckDef, TransitionRule,
};
pub use collections::merge_inferred_fields;
pub use constraint_translate::collection_constraints;
pub use crdt_compaction_points::StoredCompactionPoint;
pub use custom_type_oid_hwm::USER_TYPE_OID_BASE;
pub use custom_types::{CompositeField, CustomTypeDef, StoredCustomType, UNASSIGNED_OID};
pub use database_grants::DatabaseGrant;
pub use database_quotas::GlobalQuotaCeiling;
pub use database_types::{DatabaseDescriptor, DatabaseStatus, ParentCloneRef};
pub use function_types::{
    FunctionLanguage, FunctionParam, FunctionSecurity, FunctionVolatility, StoredFunction,
};
pub use index_record::{IndexKind, StoredIndexRecord};
pub use l2_cleanup_queue::StoredL2CleanupEntry;
pub use lockout::StoredLockoutRecord;
pub use metadata_host::ddl::StoredPendingDdl;
pub use metadata_host::drains::StoredDrain;
pub use move_tenant_journal_types::{MovePhase, MoveTenantJournalEntry};
pub use oidc_providers::{StoredClaimMappingRule, StoredOidcProvider};
pub use orgs::{StoredOrg, StoredOrgMember};
pub use pending_history_compaction::StoredPendingHistoryCompaction;
pub use pending_reclaim::StoredPendingReclaim;
pub use procedure_types::StoredProcedure;
pub use read_only::{ReadOnlyOpenError, ReadOnlySystemCatalog};
pub use redaction::StoredRedactionPolicy;
pub use rls::StoredRlsPolicy;
pub use scopes::{StoredScope, StoredScopeGrant};
pub use sequence_types::{SequenceState, StoredSequence};
pub use synonym_groups::StoredSynonymGroup;
pub use system_catalog::SystemCatalog;
pub use trigger_types::StoredTrigger;
pub use types::{
    IndexBuildState, StoredCollection, StoredContinuousAggregate, StoredIndex,
    StoredMaterializedView, catalog_err, owner_key,
};
