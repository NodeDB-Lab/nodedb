// SPDX-License-Identifier: BUSL-1.1

//! CollectionSchema receive handler.
//!
//! When a sync peer announces a [`CollectionSchemaSyncMsg`], the receiving
//! cluster materializes the collection into its system catalog (create-only,
//! via `PutCollectionIfAbsent` — never clobbering an existing collection).
//! The Data-Plane engine register happens in the shared post-apply path on
//! **every** node that applies the Raft entry, exactly as it does for a
//! local `CREATE COLLECTION`. This handler is therefore symmetric with the
//! pgwire CREATE handler: it only `stored_from_descriptor` → propose
//! `PutCollectionIfAbsent`, and never dispatches the register itself.

use std::sync::Arc;

use tracing::warn;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::audit::{
    ArcAuditEmitter, AuditEmitContext, AuditEmitter, AuditEvent,
};
use crate::control::security::identity::Permission;
use crate::control::server::shared::authorization::authorize_database_permission;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::wire::{CollectionSchemaSyncMsg, SyncFrame};
use super::state::SyncSession;

impl SyncSession {
    /// Materialize a peer-announced collection descriptor into the local
    /// catalog. Returns `None`: this is a fire-and-forget announce with no
    /// ack frame (mirrors `ShapeUnsubscribe`).
    pub async fn handle_collection_schema(
        &mut self,
        msg: &CollectionSchemaSyncMsg,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        let Some(shared) = shared else {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                "CollectionSchema received without SharedState (permissive/test path); dropping"
            );
            return None;
        };

        if !self.authenticated {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                "CollectionSchema received before authentication; dropping"
            );
            return None;
        }

        let Some(identity) = self.identity.as_ref() else {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                "CollectionSchema received without authenticated identity; dropping"
            );
            return None;
        };

        // The descriptor is untrusted peer input. Bind both namespace axes to
        // the identity rather than the legacy session tenant field.
        if msg.descriptor.tenant_id != identity.tenant_id.as_u64() {
            audit_denial(
                shared,
                identity,
                Some(msg.descriptor.database_id),
                "CollectionSchema tenant mismatch",
            );
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                descriptor_tenant = msg.descriptor.tenant_id,
                identity_tenant = identity.tenant_id.as_u64(),
                "CollectionSchema tenant mismatch; refusing to materialize"
            );
            return None;
        }

        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        if msg.descriptor.database_id != database_id {
            audit_denial(
                shared,
                identity,
                Some(msg.descriptor.database_id),
                "CollectionSchema database mismatch",
            );
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                descriptor_database = msg.descriptor.database_id.as_u64(),
                identity_database = database_id.as_u64(),
                "CollectionSchema database mismatch; refusing to materialize"
            );
            return None;
        }

        let audit = ArcAuditEmitter(Arc::clone(&shared.audit));
        if let Err(error) = authorize_database_permission(
            identity,
            database_id,
            Permission::Create,
            shared.credentials.catalog(),
            &audit,
        ) {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                database = database_id.as_u64(),
                error = ?error,
                "CollectionSchema create authorization denied; refusing to materialize"
            );
            return None;
        }

        // Owner is the receiving peer's authenticated principal — the same
        // identity a local CREATE records as owner.
        let owner = &identity.username;

        let stored =
            crate::control::security::catalog::collection_descriptor_convert::stored_from_descriptor(
                &msg.descriptor,
                owner,
            );

        let action = format!(
            "SYNC COLLECTION SCHEMA {} (session {})",
            msg.descriptor.name, self.session_id
        );
        let entry = CatalogEntry::PutCollectionIfAbsent(Box::new(stored));
        let proposed = crate::control::server::shared::session::audit_context::with_action_context(
            identity,
            action,
            crate::control::metadata_proposer::propose_catalog_entry_async(shared, &entry),
        )
        .await;
        if let Err(e) = proposed {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                error = %e,
                "CollectionSchema: failed to propose PutCollectionIfAbsent; \
                 collection not materialized"
            );
        }
        None
    }
}

fn audit_denial(
    shared: &SharedState,
    identity: &crate::control::security::identity::AuthenticatedIdentity,
    database_id: Option<DatabaseId>,
    detail: &str,
) {
    ArcAuditEmitter(Arc::clone(&shared.audit)).emit(
        AuditEvent::PermissionDenied,
        &identity.username,
        detail,
        AuditEmitContext {
            tenant_id: Some(identity.tenant_id),
            database_id,
            auth_user_id: &identity.user_id.to_string(),
            auth_user_name: &identity.username,
        },
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::collection::CollectionType;
    use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
    use nodedb_types::hlc::Hlc;
    use nodedb_types::sync::wire::{CollectionDescriptor, CollectionSchemaSyncMsg};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;
    use crate::wal::WalManager;

    fn shared_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let tempdir = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&tempdir.path().join("collection-schema.wal"))
                .expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("construct shared state");
        crate::bootstrap::state_wiring::install_gateway(&shared).expect("install gateway");
        (shared, tempdir)
    }

    fn message(tenant_id: u64, database_id: DatabaseId, name: &str) -> CollectionSchemaSyncMsg {
        CollectionSchemaSyncMsg {
            descriptor: CollectionDescriptor {
                tenant_id,
                database_id,
                name: name.into(),
                collection_type: CollectionType::document(),
                bitemporal: false,
                crdt: true,
                fields: Vec::new(),
                primary: PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: PartitionStrategy::CollectionHomed,
                declared_primary_key: None,
                descriptor_version: 1,
            },
            creation_hlc: Hlc::ZERO,
        }
    }

    fn authenticated_session(identity: AuthenticatedIdentity) -> SyncSession {
        let mut session = SyncSession::new("collection-schema-test".into());
        session.authenticated = true;
        session.tenant_id = Some(identity.tenant_id);
        session.username = Some(identity.username.clone());
        session.identity = Some(identity);
        session
    }

    fn identity(
        database_id: DatabaseId,
        roles: Vec<Role>,
        default_database: Option<DatabaseId>,
    ) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "alice",
            TenantId::new(1),
            AuthMethod::ApiKey,
            roles,
            default_database,
            DatabaseSet::Some(smallvec::smallvec![database_id]),
        )
    }

    fn collection_is_absent(shared: &SharedState, database_id: DatabaseId, name: &str) -> bool {
        shared
            .credentials
            .catalog()
            .get_collection(database_id, 1, name)
            .expect("catalog lookup")
            .is_none()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unauthenticated_schema_is_dropped_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = SyncSession::new("unauthenticated".into());
        let msg = message(1, DatabaseId::DEFAULT, "unauthenticated_schema");

        assert!(
            session
                .handle_collection_schema(&msg, Some(&shared))
                .await
                .is_none()
        );
        assert!(collection_is_absent(
            &shared,
            DatabaseId::DEFAULT,
            "unauthenticated_schema"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tenant_mismatched_schema_is_dropped_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = authenticated_session(identity(
            DatabaseId::DEFAULT,
            vec![Role::DatabaseEditor(DatabaseId::DEFAULT)],
            None,
        ));
        let msg = message(2, DatabaseId::DEFAULT, "tenant_mismatch_schema");

        assert!(
            session
                .handle_collection_schema(&msg, Some(&shared))
                .await
                .is_none()
        );
        assert!(collection_is_absent(
            &shared,
            DatabaseId::DEFAULT,
            "tenant_mismatch_schema"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn database_mismatched_schema_is_dropped_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = authenticated_session(identity(
            DatabaseId::DEFAULT,
            vec![Role::DatabaseEditor(DatabaseId::DEFAULT)],
            None,
        ));
        let msg = message(1, DatabaseId::new(9), "database_mismatch_schema");

        assert!(
            session
                .handle_collection_schema(&msg, Some(&shared))
                .await
                .is_none()
        );
        assert!(collection_is_absent(
            &shared,
            DatabaseId::new(9),
            "database_mismatch_schema"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ungranted_create_is_dropped_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = authenticated_session(identity(DatabaseId::DEFAULT, Vec::new(), None));
        let msg = message(1, DatabaseId::DEFAULT, "ungranted_schema");

        assert!(
            session
                .handle_collection_schema(&msg, Some(&shared))
                .await
                .is_none()
        );
        assert!(collection_is_absent(
            &shared,
            DatabaseId::DEFAULT,
            "ungranted_schema"
        ));
        assert_eq!(
            shared
                .audit
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .query_by_event(&AuditEvent::PermissionDenied)
                .len(),
            1
        );
    }

    /// An authorized descriptor commits through the metadata group, so the
    /// test runs on a one-node cluster.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_identity_database_with_create_permission_materializes_schema() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let shared = Arc::clone(&cluster.state);
        let selected_database = DatabaseId::new(9);
        let mut session = authenticated_session(identity(
            selected_database,
            vec![Role::DatabaseEditor(selected_database)],
            Some(selected_database),
        ));
        let msg = message(1, selected_database, "selected_database_schema");

        assert!(
            session
                .handle_collection_schema(&msg, Some(&shared))
                .await
                .is_none()
        );
        assert!(
            !collection_is_absent(&shared, selected_database, "selected_database_schema"),
            "the authorized selected-database descriptor must be materialized"
        );
        cluster.shutdown().await;
    }
}
