// SPDX-License-Identifier: BUSL-1.1

//! Push-time authorization of the deltas one session receives.
//!
//! The delivery registry routes a delta to a session by its subscriptions
//! alone. Nothing leaves the server until this gate decides it, against the
//! session's principal as it stands at the drain. The gate resolves the
//! principal once per drain and each collection's checks once per drain, so
//! the per-row cost is the row evaluation alone.

use std::collections::HashMap;
use std::sync::Arc;

use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, Role};
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;
use crate::event::crdt_sync::subscriptions::SharedSubscriptions;
use crate::event::crdt_sync::types::{DeltaAudience, DeltaOp, OutboundDelta};
use crate::types::DatabaseId;

use super::collection::{CollectionGate, GateInputs};

/// What the session sends for one delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum PushDecision {
    /// Send the delta's payload, redacted in place, as an upsert.
    Upsert,
    /// Send a delete for the delta's document: the row left what the session
    /// can see, or the session saw it before it was deleted.
    Delete,
    /// Send nothing.
    Drop,
    /// Send nothing: a redaction rule covers the row and it does not
    /// rewrite.
    DropUnredactable,
}

/// The session's principal and subscriptions, resolved for one drain.
pub(in super::super) struct PushGate {
    identity: AuthenticatedIdentity,
    database_id: DatabaseId,
    auth: AuthContext,
    subscriptions: Arc<SharedSubscriptions>,
    collections: HashMap<String, CollectionGate>,
    /// Read grants on arrays and presence collections, cached per drain.
    grants: HashMap<String, bool>,
}

impl PushGate {
    /// Resolve the gate for one drain of `session`.
    ///
    /// Returns `Ok(None)` when the session has no established identity: there
    /// is no principal to authorize, and the caller delivers nothing. An error
    /// means the principal's request scope did not resolve, or the
    /// principal's address is blacklisted or its account is not active. The
    /// caller delivers nothing and ends the session.
    ///
    /// The scope is resolved against the session's real remote address, as
    /// the shape snapshot's is, so `$auth.*` values an RLS policy names
    /// resolve the same for the push as for the snapshot. The blacklist and
    /// account-status gate is the one subscribe, resync and inbound deltas
    /// pass.
    pub(in super::super) fn for_session(
        shared: &SharedState,
        session: &SyncSession,
    ) -> crate::Result<Option<Self>> {
        let Some(identity) = session.identity.as_ref() else {
            return Ok(None);
        };
        let database_id = session.database_id();
        let scope = ClientRequestScope::for_database(
            identity,
            shared.auth_stores(),
            database_id,
            &session.device_metadata.remote_addr,
        )?;
        crate::control::server::session_auth::check_blacklist_and_status(shared, &scope)?;
        let auth = scope.scope().auth().clone();
        // An unregistered session holds no subscription, so it is denied
        // every row.
        let subscriptions = shared
            .crdt_sync_delivery
            .subscriptions(&session.session_id)
            .unwrap_or_default();
        Ok(Some(Self {
            identity: identity.clone(),
            database_id,
            auth,
            subscriptions,
            collections: HashMap::new(),
            grants: HashMap::new(),
        }))
    }

    /// Decide what the session sends for `delta`. An [`PushDecision::Upsert`]
    /// leaves the redacted post-image in `delta.payload`.
    pub(in super::super) fn decide(
        &mut self,
        shared: &SharedState,
        delta: &mut OutboundDelta,
    ) -> PushDecision {
        // The registry routes by the registration's scope. The principal's
        // own scope is checked again here, so a delta never crosses a tenant
        // or database boundary even if the two disagree.
        if delta.tenant_id != self.identity.tenant_id.as_u64()
            || delta.database_id != self.database_id
        {
            return PushDecision::Drop;
        }
        match delta.audience {
            DeltaAudience::TenantAdmins => self.decide_admin_notice(delta.op),
            DeltaAudience::Subscribers => {
                if !self.collections.contains_key(&delta.collection) {
                    let inputs = GateInputs {
                        shared,
                        identity: &self.identity,
                        database_id: self.database_id,
                        auth: &self.auth,
                        subscriptions: &self.subscriptions,
                    };
                    let gate = CollectionGate::resolve(&inputs, &delta.collection);
                    self.collections.insert(delta.collection.clone(), gate);
                }
                let Some(gate) = self.collections.get(&delta.collection) else {
                    return PushDecision::Drop;
                };
                let crs = crate::control::crs_context::crs_context_for(shared);
                gate.decide(delta, &shared.redaction, &crs)
            }
        }
    }

    /// Whether the principal holds a read grant on `name`, a collection or an
    /// array, as of this drain. Array frames and shape presence channels pass
    /// this before they leave.
    pub(in super::super) fn read_granted(&mut self, shared: &SharedState, name: &str) -> bool {
        if let Some(granted) = self.grants.get(name) {
            return *granted;
        }
        let granted = authorize_collection(
            &self.identity,
            self.database_id,
            name,
            Permission::Read,
            &shared.permissions,
            &shared.roles,
            &NoopAuditEmitter,
        )
        .is_ok();
        self.grants.insert(name.to_string(), granted);
        granted
    }

    /// Whether the session's registration still holds `collection`: by a
    /// shape, or as a collection its client stores. A purge notice goes out
    /// only while it does.
    pub(in super::super) fn holds(&self, collection: &str) -> bool {
        self.subscriptions.read().holds(collection)
    }

    /// Whether `tenant_id` and `database_id` are the principal's own scope.
    pub(in super::super) fn in_scope(&self, tenant_id: u64, database_id: DatabaseId) -> bool {
        tenant_id == self.identity.tenant_id.as_u64() && database_id == self.database_id
    }

    /// Whether the principal is a superuser or tenant admin: the privilege
    /// the DDL that writes alert, retention and schedule definitions
    /// requires. Those definition notices reach only such a principal.
    pub(in super::super) fn is_tenant_admin(&self) -> bool {
        self.identity.is_superuser || self.identity.has_role(&Role::TenantAdmin)
    }

    /// A definition notice reaches a superuser or tenant admin only.
    fn decide_admin_notice(&self, op: DeltaOp) -> PushDecision {
        if !self.is_tenant_admin() {
            return PushDecision::Drop;
        }
        match op {
            DeltaOp::Upsert => PushDecision::Upsert,
            DeltaOp::Delete => PushDecision::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crs::CrsContext;
    use nodedb_types::filter::MetadataFilter;

    use crate::bridge::scan_filter::ScanFilter;
    use crate::control::security::identity::AuthMethod;
    use crate::control::security::redaction::RedactionStore;
    use crate::types::TenantId;

    use super::super::super::row_redaction::RowPushRedaction;
    use super::*;

    fn row(owner: &str, status: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({"owner": owner, "status": status}))
            .expect("encode row image")
    }

    fn delta(op: DeltaOp, payload: Vec<u8>, prior: Option<Vec<u8>>) -> OutboundDelta {
        OutboundDelta {
            database_id: DatabaseId::DEFAULT,
            collection: "orders".into(),
            document_id: "o1".into(),
            payload,
            prior_payload: prior,
            audience: DeltaAudience::Subscribers,
            op,
            lsn: 1,
            tenant_id: 1,
            peer_id: 0,
            sequence: 1,
        }
    }

    /// The filter an `owner = '<owner>'` read policy injects.
    fn owner_is(owner: &str) -> Vec<ScanFilter> {
        vec![ScanFilter {
            field: "owner".into(),
            op: crate::bridge::scan_filter::FilterOp::Eq,
            value: nodedb_types::Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        }]
    }

    fn status_eq(status: &str) -> MetadataFilter {
        MetadataFilter::Eq {
            field: "status".into(),
            value: nodedb_types::Value::String(status.into()),
        }
    }

    fn status_is(status: &str) -> Option<Arc<MetadataFilter>> {
        Some(Arc::new(status_eq(status)))
    }

    fn gate(
        readable: bool,
        filters: Option<Vec<ScanFilter>>,
        shapes: Vec<Option<Arc<MetadataFilter>>>,
    ) -> CollectionGate {
        CollectionGate::from_parts(
            readable,
            filters,
            shapes,
            RowPushRedaction::new(TenantId::new(1), Vec::new(), DatabaseId::DEFAULT, "orders"),
        )
    }

    fn decide(gate: &CollectionGate, mut delta: OutboundDelta) -> PushDecision {
        gate.decide(
            &mut delta,
            &RedactionStore::new(),
            &CrsContext::builtin_only(),
        )
    }

    #[test]
    fn unreadable_collection_sends_nothing() {
        let gate = gate(false, None, vec![None]);
        let upsert = delta(DeltaOp::Upsert, row("alice", "open"), None);
        let delete = delta(DeltaOp::Delete, Vec::new(), Some(row("alice", "open")));
        assert_eq!(decide(&gate, upsert), PushDecision::Drop);
        assert_eq!(decide(&gate, delete), PushDecision::Drop);
    }

    #[test]
    fn collection_without_a_shape_sends_nothing() {
        let gate = gate(true, None, Vec::new());
        let upsert = delta(DeltaOp::Upsert, row("alice", "open"), None);
        assert_eq!(decide(&gate, upsert), PushDecision::Drop);
    }

    #[test]
    fn unrestricted_shape_sends_every_upsert_and_delete() {
        let gate = gate(true, None, vec![None]);
        let upsert = delta(DeltaOp::Upsert, row("alice", "open"), None);
        assert_eq!(decide(&gate, upsert), PushDecision::Upsert);
        // Every row is visible, so a delete goes out without a pre-image.
        let delete = delta(DeltaOp::Delete, Vec::new(), None);
        assert_eq!(decide(&gate, delete), PushDecision::Delete);
    }

    #[test]
    fn rls_hidden_row_is_not_pushed() {
        let gate = gate(true, Some(owner_is("alice")), vec![None]);
        let hidden = delta(DeltaOp::Upsert, row("bob", "open"), None);
        let visible = delta(DeltaOp::Upsert, row("alice", "open"), None);
        assert_eq!(decide(&gate, hidden), PushDecision::Drop);
        assert_eq!(decide(&gate, visible), PushDecision::Upsert);
    }

    #[test]
    fn row_leaving_rls_sends_a_move_out_delete() {
        let gate = gate(true, Some(owner_is("alice")), vec![None]);
        let moved = delta(
            DeltaOp::Upsert,
            row("bob", "open"),
            Some(row("alice", "open")),
        );
        assert_eq!(decide(&gate, moved), PushDecision::Delete);
    }

    #[test]
    fn row_leaving_the_shape_sends_a_move_out_delete() {
        let gate = gate(true, None, vec![status_is("open")]);
        let moved = delta(
            DeltaOp::Upsert,
            row("alice", "closed"),
            Some(row("alice", "open")),
        );
        let entered = delta(
            DeltaOp::Upsert,
            row("alice", "open"),
            Some(row("alice", "closed")),
        );
        assert_eq!(decide(&gate, moved), PushDecision::Delete);
        assert_eq!(decide(&gate, entered), PushDecision::Upsert);
    }

    #[test]
    fn row_never_visible_sends_nothing() {
        let gate = gate(true, None, vec![status_is("open")]);
        let never = delta(
            DeltaOp::Upsert,
            row("alice", "closed"),
            Some(row("alice", "closed")),
        );
        let inserted_hidden = delta(DeltaOp::Upsert, row("alice", "closed"), None);
        let deleted_hidden = delta(DeltaOp::Delete, Vec::new(), Some(row("alice", "closed")));
        assert_eq!(decide(&gate, never), PushDecision::Drop);
        assert_eq!(decide(&gate, inserted_hidden), PushDecision::Drop);
        assert_eq!(decide(&gate, deleted_hidden), PushDecision::Drop);
    }

    /// A conditional view with no pre-image cannot prove the session saw the
    /// row, so neither a delete nor a move-out goes out.
    #[test]
    fn missing_pre_image_under_a_condition_sends_nothing() {
        let gate = gate(true, Some(owner_is("alice")), vec![None]);
        let delete = delta(DeltaOp::Delete, Vec::new(), None);
        let moved = delta(DeltaOp::Upsert, row("bob", "open"), None);
        assert_eq!(decide(&gate, delete), PushDecision::Drop);
        assert_eq!(decide(&gate, moved), PushDecision::Drop);
    }

    /// An image that does not decode is not visible, even to a negated
    /// predicate an empty stand-in row satisfies.
    #[test]
    fn undecodable_image_is_never_visible() {
        let not_closed = Some(Arc::new(MetadataFilter::Not(Box::new(status_eq("closed")))));
        let gate = gate(true, None, vec![not_closed]);
        let garbage = vec![0xc1, 0x00, 0xff];
        let upsert = delta(DeltaOp::Upsert, garbage.clone(), None);
        let delete = delta(DeltaOp::Delete, Vec::new(), Some(garbage));
        assert_eq!(decide(&gate, upsert), PushDecision::Drop);
        assert_eq!(decide(&gate, delete), PushDecision::Drop);
        // A decodable row the negation admits is pushed.
        let open = delta(DeltaOp::Upsert, row("alice", "open"), None);
        assert_eq!(decide(&gate, open), PushDecision::Upsert);
    }

    #[test]
    fn deleted_visible_row_sends_a_delete() {
        let gate = gate(true, Some(owner_is("alice")), vec![status_is("open")]);
        let delete = delta(DeltaOp::Delete, Vec::new(), Some(row("alice", "open")));
        assert_eq!(decide(&gate, delete), PushDecision::Delete);
    }

    fn push_gate(roles: Vec<Role>) -> PushGate {
        let identity = AuthenticatedIdentity::new_regular(
            7,
            "device",
            TenantId::new(1),
            AuthMethod::ApiKey,
            roles,
            None,
            AuthenticatedIdentity::default_database_set(false),
        );
        let auth = AuthContext::from_identity(&identity, "sync-test".into());
        PushGate {
            identity,
            database_id: DatabaseId::DEFAULT,
            auth,
            subscriptions: Arc::new(SharedSubscriptions::default()),
            collections: HashMap::new(),
            grants: HashMap::new(),
        }
    }

    #[test]
    fn admin_notice_reaches_tenant_admins_only() {
        let admin = push_gate(vec![Role::TenantAdmin]);
        let reader = push_gate(vec![Role::ReadWrite]);
        assert_eq!(
            admin.decide_admin_notice(DeltaOp::Upsert),
            PushDecision::Upsert
        );
        assert_eq!(
            admin.decide_admin_notice(DeltaOp::Delete),
            PushDecision::Delete
        );
        assert_eq!(
            reader.decide_admin_notice(DeltaOp::Upsert),
            PushDecision::Drop
        );
    }

    fn gate_state() -> (Arc<SharedState>, tempfile::TempDir) {
        use crate::bridge::dispatch::Dispatcher;

        let dir = tempfile::tempdir().expect("create push gate test directory");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("push-gate.wal"))
                .expect("open push gate test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_risk_config(
            dispatcher,
            wal,
            crate::control::security::risk::RiskConfig::default(),
        )
        .expect("construct push gate state");
        (state, dir)
    }

    fn authenticated_session(roles: Vec<Role>) -> SyncSession {
        let identity = AuthenticatedIdentity::new_regular(
            78,
            "reader",
            TenantId::new(1),
            AuthMethod::ApiKey,
            roles,
            None,
            AuthenticatedIdentity::default_database_set(false),
        );
        let mut session = SyncSession::new("s-gate".into());
        session.tenant_id = Some(identity.tenant_id);
        session.identity = Some(identity);
        session.authenticated = true;
        session
            .tenant_id
            .get_or_insert(crate::types::TenantId::new(1));
        session
    }

    /// An array push needs a read grant on the array. A definition frame
    /// reaches any principal of its own tenant and database, read grant or
    /// not, and no other scope.
    #[test]
    fn array_and_definition_pushes_follow_their_rules() {
        let (state, _dir) = gate_state();

        let reader = authenticated_session(vec![Role::ReadWrite]);
        let mut gate = PushGate::for_session(&state, &reader)
            .expect("the principal passes admission")
            .expect("the session has an identity");
        assert!(gate.read_granted(&state, "prices"));
        assert!(!gate.is_tenant_admin());
        assert!(gate.in_scope(1, DatabaseId::DEFAULT));

        let no_read = authenticated_session(vec![Role::Custom("no_grants".into())]);
        let mut gate = PushGate::for_session(&state, &no_read)
            .expect("the principal passes admission")
            .expect("the session has an identity");
        assert!(!gate.read_granted(&state, "prices"));
        assert!(gate.in_scope(1, DatabaseId::DEFAULT));
        assert!(!gate.in_scope(2, DatabaseId::DEFAULT));
        assert!(!gate.in_scope(1, DatabaseId::new(7)));

        let admin = authenticated_session(vec![Role::TenantAdmin]);
        let gate = PushGate::for_session(&state, &admin)
            .expect("the principal passes admission")
            .expect("the session has an identity");
        assert!(gate.is_tenant_admin());
    }

    /// End to end in a non-default database: a write event names its
    /// collection qualified, the packager routes it by the bare name the
    /// shape names, and the gate masks the row with the policy stored under
    /// the qualified name.
    #[tokio::test]
    async fn non_default_database_row_is_routed_and_masked() {
        use crate::control::security::identity::DatabaseSet;
        use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};
        use crate::event::crdt_sync::SubscribedShape;
        use crate::event::crdt_sync::types::DeliveryConfig;
        use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
        use crate::types::{Lsn, VShardId};

        let (state, _dir) = gate_state();

        let db = DatabaseId::new(7);
        state.redaction.create_policy(RedactionPolicy {
            name: "users_support_email".into(),
            tenant_id: 1,
            collection: "7/users".into(),
            display_collection: "users".into(),
            for_role: "support".into(),
            rules: vec![RedactionRule {
                field: "email".into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });

        let identity = AuthenticatedIdentity::new_regular(
            77,
            "device",
            TenantId::new(1),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite, Role::Custom("support".into())],
            Some(db),
            DatabaseSet::Some(smallvec::smallvec![db]),
        );
        let mut session = SyncSession::new("s-db7".into());
        session.tenant_id = Some(identity.tenant_id);
        session.identity = Some(identity);
        session.authenticated = true;
        session
            .tenant_id
            .get_or_insert(crate::types::TenantId::new(1));

        let (mut rx, _crx) =
            state
                .crdt_sync_delivery
                .register("s-db7".into(), 1, 1, db, &DeliveryConfig::default());
        let shape = SubscribedShape::new("users".into(), Vec::new()).expect("empty predicate");
        state
            .crdt_sync_delivery
            .subscribe_shape("s-db7", "sh-users", shape);

        let row = nodedb_types::json_to_msgpack(&serde_json::json!({"email": "a@b.c"}))
            .expect("encode row");
        let event = WriteEvent {
            sequence: 1,
            collection: Arc::from("7/users"),
            op: WriteOp::Insert,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("u-1")),
            lsn: Lsn::new(10),
            record: None,
            database_id: db,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(row.as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: Some(crate::event::test_utils::test_commit_hlc()),
            image_fault: None,
        };
        assert!(state.delta_packager.package_and_enqueue(
            &event,
            None,
            None,
            &state.crdt_sync_delivery
        ));

        let mut delta = rx
            .try_recv()
            .expect("the subscribed session receives the delta");
        assert_eq!(delta.collection, "users");
        let mut gate = PushGate::for_session(&state, &session)
            .expect("the principal passes admission")
            .expect("the session has an identity");
        assert_eq!(gate.decide(&state, &mut delta), PushDecision::Upsert);
        let pushed = nodedb_types::json_from_msgpack(&delta.payload).expect("decode pushed row");
        assert_eq!(pushed["email"], "***");
    }
}
