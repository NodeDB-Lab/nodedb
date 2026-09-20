// SPDX-License-Identifier: BUSL-1.1

//! Catalog / registry accessor methods on [`TestClusterNode`].

use crate::cluster_harness::node::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Number of active collections visible on this node (read through
    /// the local `SystemCatalog` redb — populated by the
    /// `MetadataCommitApplier` on every node via
    /// `CatalogEntry::apply_to`).
    pub fn cached_collection_count(&self) -> usize {
        let catalog = self.shared.credentials.catalog();
        // `load_collections_for_tenant` filters out `is_active = false`
        // records, so a deactivated collection drops out of the count.
        catalog
            .load_collections_for_tenant(nodedb_types::DatabaseId::DEFAULT, 1)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Number of sequences of the default database visible in this node's
    /// in-memory `sequence_registry`. After the applier spawns its
    /// post-apply side effect for a `PutSequence`, the registry
    /// sees the new record on every node.
    pub fn sequence_count(&self, tenant_id: u64) -> usize {
        self.shared
            .sequence_registry
            .list(nodedb_types::DatabaseId::DEFAULT.as_u64(), tenant_id)
            .len()
    }

    /// Check whether the default database holds a sequence with the given
    /// name in this node's in-memory registry.
    pub fn has_sequence(&self, tenant_id: u64, name: &str) -> bool {
        self.shared.sequence_registry.exists(
            nodedb_types::DatabaseId::DEFAULT.as_u64(),
            tenant_id,
            name,
        )
    }

    /// Read the current counter of a default-database sequence from this
    /// node's in-memory registry, if present.
    pub fn sequence_current_value(&self, tenant_id: u64, name: &str) -> Option<i64> {
        self.shared
            .sequence_registry
            .list(nodedb_types::DatabaseId::DEFAULT.as_u64(), tenant_id)
            .into_iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, current, _)| current)
    }

    /// The value the next `nextval` call on this sequence returns:
    /// `current_value + increment`. Holds whether or not the sequence has
    /// been called yet — a restart stores `value - increment` with
    /// `called = false` precisely so this formula gives `value` right
    /// after `RESTART WITH value`, matching Postgres semantics.
    pub fn sequence_next_value(&self, tenant_id: u64, name: &str) -> Option<i64> {
        let db = nodedb_types::DatabaseId::DEFAULT.as_u64();
        let current = self
            .shared
            .sequence_registry
            .list(db, tenant_id)
            .into_iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, current, _)| current)?;
        let def = self.shared.sequence_registry.get_def(db, tenant_id, name)?;
        Some(current + def.increment)
    }

    /// Check whether a trigger with the given name exists in this
    /// node's in-memory trigger registry.
    pub fn has_trigger(&self, tenant_id: u64, name: &str) -> bool {
        self.shared
            .trigger_registry
            .list_for_tenant(nodedb_types::DatabaseId::DEFAULT, tenant_id)
            .iter()
            .any(|t| t.name == name)
    }

    /// Read a function record from this node's local `SystemCatalog`
    /// redb (which the applier writes through on every node).
    pub fn has_function(&self, tenant_id: u64, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .get_function(tenant_id, name)
            .ok()
            .flatten()
            .is_some()
    }

    /// Read a procedure record from this node's local `SystemCatalog`.
    pub fn has_procedure(&self, tenant_id: u64, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .get_procedure(tenant_id, name)
            .ok()
            .flatten()
            .is_some()
    }

    /// Check whether a scheduled job with the given name exists in
    /// this node's in-memory `schedule_registry`.
    pub fn has_schedule(&self, tenant_id: u64, name: &str) -> bool {
        self.shared
            .schedule_registry
            .get(nodedb_types::DatabaseId::DEFAULT, tenant_id, name)
            .is_some()
    }

    /// Check whether a change stream with the given name exists in
    /// this node's in-memory `stream_registry`.
    pub fn has_change_stream(
        &self,
        database_id: nodedb_types::DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> bool {
        self.shared
            .stream_registry
            .get(database_id, tenant_id, name)
            .is_some()
    }

    /// Check whether a user exists and is active in this node's
    /// in-memory `credentials` cache (which the applier writes via
    /// `install_replicated_user`).
    pub fn has_active_user(&self, username: &str) -> bool {
        self.shared.credentials.get_user(username).is_some()
    }

    /// Check whether a permission grant exists in this node's
    /// in-memory `PermissionStore`. `permission` is the lowercase
    /// canonical name (`read|write|create|drop|alter|admin|monitor|execute`).
    pub fn has_grant(&self, target: &str, grantee: &str, permission: &str) -> bool {
        let Some(perm) = nodedb::control::security::permission::parse_permission(permission) else {
            return false;
        };
        self.shared
            .permissions
            .grants_on(target)
            .iter()
            .any(|g| g.grantee == grantee && g.permission == perm)
    }

    /// Read the recorded owner of an object on this node. Owner rows
    /// are keyed by database, so `database_id` selects the row.
    pub fn owner_of(
        &self,
        object_type: &str,
        database_id: u64,
        tenant_id: u64,
        object_name: &str,
    ) -> Option<String> {
        self.shared.permissions.get_owner(
            object_type,
            database_id,
            nodedb_types::TenantId::new(tenant_id),
            object_name,
        )
    }

    /// Check whether a tenant identity exists in this node's local
    /// `SystemCatalog` redb (written by the `PutTenant` applier).
    pub fn has_tenant(&self, tenant_id: u64) -> bool {
        let catalog = self.shared.credentials.catalog();
        match catalog.load_all_tenants() {
            Ok(list) => list.iter().any(|t| t.tenant_id == tenant_id),
            Err(_) => false,
        }
    }

    /// Check whether an RLS policy exists in this node's local
    /// `SystemCatalog` redb (written by the `PutRlsPolicy` applier).
    pub fn has_rls_policy(&self, tenant_id: u64, collection: &str, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .get_rls_policy(tenant_id, collection, name)
            .ok()
            .flatten()
            .is_some()
    }

    /// Check whether a materialized view exists in this node's local
    /// `SystemCatalog` redb (written by the applier on every node).
    pub fn has_materialized_view(&self, tenant_id: u64, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .get_materialized_view(nodedb_types::DatabaseId::DEFAULT.as_u64(), tenant_id, name)
            .ok()
            .flatten()
            .is_some()
    }

    /// Check whether a custom role exists in this node's in-memory
    /// `roles` cache.
    pub fn has_role(&self, name: &str) -> bool {
        self.shared.roles.get_role(name).is_some()
    }

    /// Check whether an API key exists and is active in this node's
    /// in-memory `api_keys` cache.
    pub fn has_active_api_key(&self, key_id: &str) -> bool {
        self.shared
            .api_keys
            .get_key(key_id)
            .map(|k| !k.is_revoked)
            .unwrap_or(false)
    }

    /// Check whether a given user's role set contains a specific
    /// role. Used to assert `ALTER USER ... SET ROLE` replication.
    pub fn user_has_role(&self, username: &str, role: &str) -> bool {
        self.shared
            .credentials
            .get_user(username)
            .map(|u| u.roles.iter().any(|r| r.to_string() == role))
            .unwrap_or(false)
    }

    /// Read the `(descriptor_version, modification_hlc)` stamp of a
    /// collection on this node's local `SystemCatalog`. The applier
    /// is the only writer, so this is what every other node should
    /// agree on after the apply has propagated.
    pub fn collection_descriptor(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .get_collection(nodedb_types::DatabaseId::DEFAULT, tenant_id, name)
            .ok()
            .flatten()
            .map(|coll| (coll.descriptor_version, coll.modification_hlc))
    }

    /// Same as [`collection_descriptor`] for stored functions.
    pub fn function_descriptor(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .get_function(tenant_id, name)
            .ok()
            .flatten()
            .map(|f| (f.descriptor_version, f.modification_hlc))
    }

    /// Same as [`collection_descriptor`] for stored procedures.
    pub fn procedure_descriptor(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .get_procedure(tenant_id, name)
            .ok()
            .flatten()
            .map(|p| (p.descriptor_version, p.modification_hlc))
    }
}
