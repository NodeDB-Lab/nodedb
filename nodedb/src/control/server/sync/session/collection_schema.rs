// SPDX-License-Identifier: BUSL-1.1

//! CollectionSchema receive handler.
//!
//! When a sync peer announces a [`CollectionSchemaSyncMsg`], the receiving
//! cluster materializes the collection into its system catalog. The write is
//! create-only, via `PutCollectionIfAbsent`, so it never clobbers an existing
//! collection. The Data-Plane engine register runs in the shared post-apply
//! path on every node that applies the Raft entry, as for a local
//! `CREATE COLLECTION`. This handler never dispatches the register itself.
//!
//! Every announce gets an answer. A held collection answers a
//! `CollectionSchema` frame with Origin's descriptor and its incarnation.
//! The client states that incarnation on every delta it pushes. Every other
//! outcome answers a typed `CollectionSchemaReject` frame.
//!
//! An active collection needs only read permission on it, as a shape
//! subscribe does. Create permission applies only when the announce creates.

use std::sync::Arc;

use tracing::warn;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::audit::{
    ArcAuditEmitter, AuditEmitContext, AuditEmitter, AuditEvent,
};
use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::Permission;
use crate::control::server::shared::authorization::{
    AuthorizationError, authorize_collection, authorize_database_permission,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::refusal::{is_retryable_dispatch_error, is_unknown_commit_outcome};
use super::super::wire::{
    CollectionDescriptor, CollectionSchemaRefusal, CollectionSchemaRejectMsg,
    CollectionSchemaSyncMsg, SyncFrame, SyncMessageType,
};
use super::state::SyncSession;

/// An announce Origin cannot answer with a held collection.
struct AnnounceRefused {
    refusal: CollectionSchemaRefusal,
    detail: String,
}

impl AnnounceRefused {
    fn new(refusal: CollectionSchemaRefusal, detail: impl Into<String>) -> Self {
        Self {
            refusal,
            detail: detail.into(),
        }
    }

    /// The refusal for a failed propose or catalog read. A create-only
    /// announce is idempotent, so an unknown commit outcome is retryable.
    fn from_error(context: &str, error: &crate::Error) -> Self {
        let refusal = if is_retryable_dispatch_error(error) || is_unknown_commit_outcome(error) {
            CollectionSchemaRefusal::Unavailable
        } else {
            CollectionSchemaRefusal::Failed
        };
        Self::new(refusal, format!("{context}: {error}"))
    }
}

impl SyncSession {
    /// Materialize a peer-announced collection descriptor into the local
    /// catalog, then answer with the collection Origin holds.
    ///
    /// The answer is a `CollectionSchema` frame on success. It carries
    /// Origin's descriptor and incarnation, which can differ from the
    /// announce. A refused announce answers a `CollectionSchemaReject` frame.
    pub async fn handle_collection_schema(
        &mut self,
        msg: &CollectionSchemaSyncMsg,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        match self.materialize_announced(msg, shared).await {
            Ok(held) => SyncFrame::try_encode(SyncMessageType::CollectionSchema, &held),
            Err(refused) => {
                warn!(
                    session = %self.session_id,
                    collection = %msg.descriptor.name,
                    refusal = ?refused.refusal,
                    detail = %refused.detail,
                    "CollectionSchema announce refused"
                );
                let reject = CollectionSchemaRejectMsg {
                    name: msg.descriptor.name.clone(),
                    refusal: refused.refusal,
                    detail: refused.detail,
                };
                SyncFrame::try_encode(SyncMessageType::CollectionSchemaReject, &reject)
            }
        }
    }

    /// Run the announce's checks, then answer the collection the catalog holds.
    ///
    /// An active collection answers to a session with read permission on it.
    /// Any other name needs create permission and a create-only put first.
    async fn materialize_announced(
        &self,
        msg: &CollectionSchemaSyncMsg,
        shared: Option<&Arc<SharedState>>,
    ) -> Result<CollectionSchemaSyncMsg, AnnounceRefused> {
        let Some(shared) = shared else {
            return Err(AnnounceRefused::new(
                CollectionSchemaRefusal::NotFound,
                "this sync listener has no catalog",
            ));
        };

        if !self.authenticated {
            return Err(AnnounceRefused::new(
                CollectionSchemaRefusal::Unauthenticated,
                "announce received before authentication",
            ));
        }

        let Some(identity) = self.identity.as_ref() else {
            return Err(AnnounceRefused::new(
                CollectionSchemaRefusal::Unauthenticated,
                "announce received without an authenticated identity",
            ));
        };

        // The descriptor is untrusted peer input. Bind both namespace axes to
        // the identity rather than the legacy session tenant field.
        let tenant_id = identity.tenant_id.as_u64();
        if msg.descriptor.tenant_id != tenant_id {
            audit_denial(
                shared,
                identity,
                Some(msg.descriptor.database_id),
                "CollectionSchema tenant mismatch",
            );
            return Err(AnnounceRefused::new(
                CollectionSchemaRefusal::NotVisible,
                format!(
                    "descriptor tenant {} is not the session tenant {tenant_id}",
                    msg.descriptor.tenant_id
                ),
            ));
        }

        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        if msg.descriptor.database_id != database_id {
            audit_denial(
                shared,
                identity,
                Some(msg.descriptor.database_id),
                "CollectionSchema database mismatch",
            );
            return Err(AnnounceRefused::new(
                CollectionSchemaRefusal::NotVisible,
                format!(
                    "descriptor database {} is not the session database {}",
                    msg.descriptor.database_id.as_u64(),
                    database_id.as_u64()
                ),
            ));
        }

        let name = msg.descriptor.name.as_str();
        let audit = ArcAuditEmitter(Arc::clone(&shared.audit));
        let existing = shared
            .credentials
            .catalog()
            .get_collection(database_id, tenant_id, name)
            .map_err(|error| AnnounceRefused::from_error("read collection", &error))?;

        // An active collection answers under the shape-subscribe read check.
        // That check already gates the descriptor and incarnation a shape
        // snapshot announces, so this answer discloses nothing more.
        if let Some(collection) = existing.filter(|collection| collection.is_active) {
            authorize_collection(
                identity,
                database_id,
                name,
                Permission::Read,
                &shared.permissions,
                &shared.roles,
                &audit,
            )
            .map_err(|error| self.permission_denied(name, database_id, Permission::Read, &error))?;
            return Ok(held_answer(&collection));
        }

        // An absent or dropped name makes the announce a create.
        authorize_database_permission(
            identity,
            database_id,
            Permission::Create,
            shared.credentials.catalog(),
            &audit,
        )
        .map_err(|error| self.permission_denied(name, database_id, Permission::Create, &error))?;

        // Owner is the receiving peer's authenticated principal, the same
        // identity a local CREATE records as owner.
        let stored =
            crate::control::security::catalog::collection_descriptor_convert::stored_from_descriptor(
                &msg.descriptor,
                &identity.username,
            );

        let action = format!(
            "SYNC COLLECTION SCHEMA {} (session {})",
            msg.descriptor.name, self.session_id
        );
        let entry = CatalogEntry::PutCollectionIfAbsent(Box::new(stored));
        crate::control::server::shared::session::audit_context::with_action_context(
            identity,
            action,
            crate::control::metadata_proposer::propose_catalog_entry_async(shared, &entry),
        )
        .await
        .map_err(|error| AnnounceRefused::from_error("propose PutCollectionIfAbsent", &error))?;

        // The propose returns after this node applied the entry. The catalog
        // row is either the announced collection or one that existed before.
        let held = shared
            .credentials
            .catalog()
            .get_collection(database_id, tenant_id, name)
            .map_err(|error| AnnounceRefused::from_error("read collection", &error))?;
        match held {
            Some(collection) if collection.is_active => Ok(held_answer(&collection)),
            // A dropped collection keeps its row until purge. The create-only
            // put leaves that row in place, so Origin holds no live collection.
            Some(_) | None => Err(AnnounceRefused::new(
                CollectionSchemaRefusal::NotFound,
                format!("Origin holds no active collection named '{name}'"),
            )),
        }
    }

    /// The refusal for a denied read or create check. The detail is the same
    /// for both checks, so it never reveals whether the collection exists.
    fn permission_denied(
        &self,
        name: &str,
        database_id: DatabaseId,
        permission: Permission,
        error: &AuthorizationError,
    ) -> AnnounceRefused {
        warn!(
            session = %self.session_id,
            collection = name,
            database = database_id.as_u64(),
            permission = ?permission,
            error = ?error,
            "CollectionSchema authorization denied"
        );
        AnnounceRefused::new(
            CollectionSchemaRefusal::PermissionDenied,
            format!(
                "permission denied for collection '{name}' on database {}",
                database_id.as_u64()
            ),
        )
    }
}

/// The answer for a collection Origin holds: its descriptor and incarnation.
fn held_answer(collection: &StoredCollection) -> CollectionSchemaSyncMsg {
    CollectionSchemaSyncMsg {
        descriptor: CollectionDescriptor::from(collection),
        creation_hlc: collection.incarnation,
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
        identity_as(7, "alice", database_id, roles, default_database)
    }

    /// A principal other than the owner of the collections `identity` creates.
    fn identity_as(
        user_id: u64,
        username: &str,
        database_id: DatabaseId,
        roles: Vec<Role>,
        default_database: Option<DatabaseId>,
    ) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            username,
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

    /// The incarnation the catalog holds for an active collection.
    fn catalog_incarnation(shared: &SharedState, database_id: DatabaseId, name: &str) -> Hlc {
        let stored = shared
            .credentials
            .catalog()
            .get_collection(database_id, 1, name)
            .expect("catalog lookup")
            .expect("collection present");
        assert!(stored.is_active, "collection '{name}' must be active");
        stored.incarnation
    }

    /// The typed refusal an announce answered.
    fn refusal_of(reply: Option<SyncFrame>) -> CollectionSchemaRejectMsg {
        let reply = reply.expect("every announce is answered");
        assert_eq!(reply.msg_type, SyncMessageType::CollectionSchemaReject);
        reply
            .decode_body()
            .expect("decode CollectionSchemaRejectMsg")
    }

    /// The held collection an announce answered.
    fn held_of(reply: Option<SyncFrame>) -> CollectionSchemaSyncMsg {
        let reply = reply.expect("every announce is answered");
        assert_eq!(reply.msg_type, SyncMessageType::CollectionSchema);
        reply.decode_body().expect("decode CollectionSchemaSyncMsg")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_without_a_catalog_answers_not_found() {
        let mut session = SyncSession::new("no-catalog".into());
        let msg = message(1, DatabaseId::DEFAULT, "no_catalog_schema");

        let reject = refusal_of(session.handle_collection_schema(&msg, None).await);
        assert_eq!(reject.refusal, CollectionSchemaRefusal::NotFound);
        assert_eq!(reject.name, "no_catalog_schema");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unauthenticated_schema_is_refused_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = SyncSession::new("unauthenticated".into());
        let msg = message(1, DatabaseId::DEFAULT, "unauthenticated_schema");

        let reject = refusal_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(reject.refusal, CollectionSchemaRefusal::Unauthenticated);
        assert_eq!(reject.name, "unauthenticated_schema");
        assert!(collection_is_absent(
            &shared,
            DatabaseId::DEFAULT,
            "unauthenticated_schema"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tenant_mismatched_schema_is_refused_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = authenticated_session(identity(
            DatabaseId::DEFAULT,
            vec![Role::DatabaseEditor(DatabaseId::DEFAULT)],
            None,
        ));
        let msg = message(2, DatabaseId::DEFAULT, "tenant_mismatch_schema");

        let reject = refusal_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(reject.refusal, CollectionSchemaRefusal::NotVisible);
        assert!(collection_is_absent(
            &shared,
            DatabaseId::DEFAULT,
            "tenant_mismatch_schema"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn database_mismatched_schema_is_refused_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = authenticated_session(identity(
            DatabaseId::DEFAULT,
            vec![Role::DatabaseEditor(DatabaseId::DEFAULT)],
            None,
        ));
        let msg = message(1, DatabaseId::new(9), "database_mismatch_schema");

        let reject = refusal_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(reject.refusal, CollectionSchemaRefusal::NotVisible);
        assert!(collection_is_absent(
            &shared,
            DatabaseId::new(9),
            "database_mismatch_schema"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ungranted_create_is_refused_without_catalog_mutation() {
        let (shared, _tempdir) = shared_state();
        let mut session = authenticated_session(identity(DatabaseId::DEFAULT, Vec::new(), None));
        let msg = message(1, DatabaseId::DEFAULT, "ungranted_schema");

        let reject = refusal_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(reject.refusal, CollectionSchemaRefusal::PermissionDenied);
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

        let held = held_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert!(
            !collection_is_absent(&shared, selected_database, "selected_database_schema"),
            "the authorized selected-database descriptor must be materialized"
        );
        assert_eq!(held.descriptor.name, "selected_database_schema");
        assert_eq!(held.descriptor.database_id, selected_database);
        assert_ne!(
            held.creation_hlc,
            Hlc::ZERO,
            "a committed collection has an incarnation"
        );
        assert_eq!(
            held.creation_hlc,
            catalog_incarnation(&shared, selected_database, "selected_database_schema"),
            "the answer carries the incarnation the catalog holds"
        );

        // A second announce changes nothing and answers the same incarnation.
        let again = held_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(again.creation_hlc, held.creation_hlc);
        cluster.shutdown().await;
    }

    /// A drop leaves no live collection, so the announce answers `NotFound`.
    /// After the purge, the announce creates a new incarnation and answers it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recreated_collection_answers_its_new_incarnation() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let shared = Arc::clone(&cluster.state);
        let database = DatabaseId::new(9);
        let name = "recreated_schema";
        let mut session = authenticated_session(identity(
            database,
            vec![Role::DatabaseEditor(database)],
            Some(database),
        ));
        let msg = message(1, database, name);

        let first = held_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_ne!(first.creation_hlc, Hlc::ZERO);

        let drop = CatalogEntry::DeactivateCollection {
            database_id: database.as_u64(),
            tenant_id: 1,
            name: name.into(),
            descriptor_version: 0,
            modification_hlc: Hlc::ZERO,
        };
        crate::control::metadata_proposer::propose_catalog_entry_async(&shared, &drop)
            .await
            .expect("drop the collection");

        let dropped = refusal_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(dropped.refusal, CollectionSchemaRefusal::NotFound);
        assert_eq!(dropped.name, name);

        let purge = CatalogEntry::PurgeCollection {
            database_id: database.as_u64(),
            tenant_id: 1,
            name: name.into(),
            target_descriptor_version: 0,
            target_hlc: Hlc::ZERO,
        };
        crate::control::metadata_proposer::propose_catalog_entry_async(&shared, &purge)
            .await
            .expect("purge the dropped collection");
        assert!(collection_is_absent(&shared, database, name));

        let recreated = held_of(session.handle_collection_schema(&msg, Some(&shared)).await);
        assert_ne!(
            recreated.creation_hlc, first.creation_hlc,
            "a drop and a same-name create start a new incarnation"
        );
        assert!(recreated.creation_hlc > first.creation_hlc);
        assert_eq!(
            recreated.creation_hlc,
            catalog_incarnation(&shared, database, name)
        );
        cluster.shutdown().await;
    }

    /// The count of `PermissionDenied` audit events.
    fn permission_denied_audits(shared: &SharedState) -> usize {
        shared
            .audit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .query_by_event(&AuditEvent::PermissionDenied)
            .len()
    }

    /// An editor session creates `name` and answers its incarnation.
    async fn create_as_editor(shared: &Arc<SharedState>, database: DatabaseId, name: &str) -> Hlc {
        let mut editor = authenticated_session(identity(
            database,
            vec![Role::DatabaseEditor(database)],
            Some(database),
        ));
        let msg = message(1, database, name);
        let held = held_of(editor.handle_collection_schema(&msg, Some(shared)).await);
        assert_ne!(held.creation_hlc, Hlc::ZERO);
        held.creation_hlc
    }

    /// A session that can read but not create learns the existing collection's
    /// incarnation, so its deltas can state it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_only_session_answers_existing_collection_incarnation() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let shared = Arc::clone(&cluster.state);
        let database = DatabaseId::new(9);
        let name = "read_only_existing_schema";
        let created = create_as_editor(&shared, database, name).await;

        let mut reader = authenticated_session(identity_as(
            8,
            "reader",
            database,
            vec![Role::DatabaseReader(database)],
            Some(database),
        ));
        let msg = message(1, database, name);
        let held = held_of(reader.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(held.descriptor.name, name);
        assert_eq!(held.descriptor.database_id, database);
        assert_eq!(held.creation_hlc, created);
        assert_eq!(
            held.creation_hlc,
            catalog_incarnation(&shared, database, name)
        );
        assert_eq!(permission_denied_audits(&shared), 0);
        cluster.shutdown().await;
    }

    /// A read-only session cannot create, so an absent name is refused.
    #[tokio::test(flavor = "current_thread")]
    async fn read_only_session_announcing_absent_collection_is_denied() {
        let (shared, _tempdir) = shared_state();
        let mut reader = authenticated_session(identity_as(
            8,
            "reader",
            DatabaseId::DEFAULT,
            vec![Role::DatabaseReader(DatabaseId::DEFAULT)],
            None,
        ));
        let msg = message(1, DatabaseId::DEFAULT, "read_only_absent_schema");

        let reject = refusal_of(reader.handle_collection_schema(&msg, Some(&shared)).await);
        assert_eq!(reject.refusal, CollectionSchemaRefusal::PermissionDenied);
        assert_eq!(reject.name, "read_only_absent_schema");
        assert!(collection_is_absent(
            &shared,
            DatabaseId::DEFAULT,
            "read_only_absent_schema"
        ));
        assert_eq!(permission_denied_audits(&shared), 1);
    }

    /// A session without read permission learns nothing about an existing
    /// collection. Its refusal matches the refusal for an absent name.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn session_without_sync_permission_is_denied_existing_collection() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let shared = Arc::clone(&cluster.state);
        let database = DatabaseId::new(9);
        let name = "unsynced_existing_schema";
        let created = create_as_editor(&shared, database, name).await;

        let mut outsider = authenticated_session(identity_as(
            9,
            "outsider",
            database,
            Vec::new(),
            Some(database),
        ));
        let existing = refusal_of(
            outsider
                .handle_collection_schema(&message(1, database, name), Some(&shared))
                .await,
        );
        assert_eq!(existing.refusal, CollectionSchemaRefusal::PermissionDenied);
        assert_eq!(existing.name, name);
        assert_eq!(permission_denied_audits(&shared), 1);
        assert_eq!(catalog_incarnation(&shared, database, name), created);

        let absent_name = "unsynced_absent_schema";
        let absent = refusal_of(
            outsider
                .handle_collection_schema(&message(1, database, absent_name), Some(&shared))
                .await,
        );
        assert_eq!(absent.refusal, CollectionSchemaRefusal::PermissionDenied);
        assert_eq!(
            existing.detail.replace(name, absent_name),
            absent.detail,
            "the refusal must not reveal whether the collection exists"
        );
        assert!(collection_is_absent(&shared, database, absent_name));
        assert_eq!(permission_denied_audits(&shared), 2);
        cluster.shutdown().await;
    }
}
