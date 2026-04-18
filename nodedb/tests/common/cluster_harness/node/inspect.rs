//! Local-state inspector methods on [`TestClusterNode`].
//!
//! These read-only accessors let integration tests assert that an
//! applier ran on every node (catalog caches, trigger/schedule/stream
//! registries, lease/drain maps, etc.) without reaching through
//! private fields. Grouped here to keep `lifecycle.rs` focused on
//! spawn/shutdown.

use super::lifecycle::TestClusterNode;

impl TestClusterNode {
    /// Number of nodes currently visible in this node's topology view.
    pub fn topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| t.read().unwrap_or_else(|p| p.into_inner()).node_count())
            .unwrap_or(0)
    }

    /// Observed metadata-group leader id from this node's local Raft
    /// state, or `0` if no leader is known yet (election in progress).
    /// Polled by the cluster harness `spawn_three()` to gate test
    /// execution on a stable leader — otherwise tests racing the first
    /// election see `not leader (leader hint: None)` errors when CPU
    /// pressure delays the initial heartbeats past topology convergence.
    pub fn metadata_group_leader(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .find(|g| g.group_id == nodedb_cluster::METADATA_GROUP_ID)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Number of active collections visible on this node (read through
    /// the local `SystemCatalog` redb — populated by the
    /// `MetadataCommitApplier` on every node via
    /// `CatalogEntry::apply_to`).
    pub fn cached_collection_count(&self) -> usize {
        let Some(catalog) = self.shared.credentials.catalog() else {
            return 0;
        };
        // `load_collections_for_tenant` filters out `is_active = false`
        // records, so a deactivated collection drops out of the count.
        catalog
            .load_collections_for_tenant(1)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Number of sequences visible in this node's in-memory
    /// `sequence_registry`. After the applier spawns its
    /// post-apply side effect for a `PutSequence`, the registry
    /// should see the new record on every node.
    pub fn sequence_count(&self, tenant_id: u32) -> usize {
        self.shared.sequence_registry.list(tenant_id).len()
    }

    /// Check whether a sequence with the given name exists in this
    /// node's in-memory registry.
    pub fn has_sequence(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.sequence_registry.exists(tenant_id, name)
    }

    /// Read the current counter of a sequence from this node's
    /// in-memory registry, if present.
    pub fn sequence_current_value(&self, tenant_id: u32, name: &str) -> Option<i64> {
        self.shared
            .sequence_registry
            .list(tenant_id)
            .into_iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, current, _)| current)
    }

    /// Check whether a trigger with the given name exists in this
    /// node's in-memory trigger registry.
    pub fn has_trigger(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .trigger_registry
            .list_for_tenant(tenant_id)
            .iter()
            .any(|t| t.name == name)
    }

    /// Read a function record from this node's local `SystemCatalog`
    /// redb (which the applier writes through on every node).
    pub fn has_function(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_function(tenant_id, name).ok().flatten())
            .is_some()
    }

    /// Read a procedure record from this node's local `SystemCatalog`.
    pub fn has_procedure(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_procedure(tenant_id, name).ok().flatten())
            .is_some()
    }

    /// Check whether a scheduled job with the given name exists in
    /// this node's in-memory `schedule_registry`.
    pub fn has_schedule(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.schedule_registry.get(tenant_id, name).is_some()
    }

    /// Check whether a change stream with the given name exists in
    /// this node's in-memory `stream_registry`.
    pub fn has_change_stream(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.stream_registry.get(tenant_id, name).is_some()
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

    /// Read the recorded owner of an object on this node.
    pub fn owner_of(&self, object_type: &str, tenant_id: u32, object_name: &str) -> Option<String> {
        self.shared.permissions.get_owner(
            object_type,
            nodedb_types::TenantId::new(tenant_id),
            object_name,
        )
    }

    /// Check whether a tenant identity exists in this node's local
    /// `SystemCatalog` redb (written by the `PutTenant` applier).
    pub fn has_tenant(&self, tenant_id: u32) -> bool {
        let Some(catalog) = self.shared.credentials.catalog() else {
            return false;
        };
        match catalog.load_all_tenants() {
            Ok(list) => list.iter().any(|t| t.tenant_id == tenant_id),
            Err(_) => false,
        }
    }

    /// Check whether an RLS policy exists in this node's local
    /// `SystemCatalog` redb (written by the `PutRlsPolicy` applier).
    pub fn has_rls_policy(&self, tenant_id: u32, collection: &str, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_rls_policy(tenant_id, collection, name).ok().flatten())
            .is_some()
    }

    /// Check whether a materialized view exists in this node's local
    /// `SystemCatalog` redb (written by the applier on every node).
    pub fn has_materialized_view(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_materialized_view(tenant_id, name).ok().flatten())
            .is_some()
    }

    /// Whether this node's `lease_drain` tracker currently holds
    /// an ACTIVE drain entry (not expired) for the given
    /// `(descriptor_id, min_version)`. Used by the drain tests
    /// to assert replicated drain state.
    pub fn has_drain_for(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
        min_version: u64,
    ) -> bool {
        let now_wall_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        self.shared
            .lease_drain
            .is_draining(descriptor_id, min_version, now_wall_ns)
    }

    /// Total number of leases (across all descriptors and node_ids)
    /// in this node's `MetadataCache.leases` map. Includes expired
    /// records — for filtered counts use [`active_lease_count`].
    pub fn lease_count(&self) -> usize {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.leases.len()
    }

    /// Number of leases whose `expires_at` is strictly greater
    /// than this node's current HLC peek.
    pub fn active_lease_count(&self) -> usize {
        let now = self.shared.hlc_clock.peek();
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.leases.values().filter(|l| l.expires_at > now).count()
    }

    /// Whether this node's `MetadataCache` holds a non-expired
    /// lease at the given version (or higher) on
    /// `(kind, tenant_id, name)` granted to `holder_node_id`.
    pub fn has_lease(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u32,
        name: &str,
        holder_node_id: u64,
        min_version: u64,
    ) -> bool {
        let now = self.shared.hlc_clock.peek();
        let id = nodedb_cluster::DescriptorId::new(tenant_id, kind, name);
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .get(&(id, holder_node_id))
            .map(|l| l.expires_at > now && l.version >= min_version)
            .unwrap_or(false)
    }

    /// Snapshot of every lease on this node for the given descriptor.
    pub fn leases_for_descriptor(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u32,
        name: &str,
    ) -> Vec<nodedb_cluster::DescriptorLease> {
        let id = nodedb_cluster::DescriptorId::new(tenant_id, kind, name);
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .iter()
            .filter(|((did, _), _)| did == &id)
            .map(|(_, l)| l.clone())
            .collect()
    }

    /// Direct accessor for the `applied_index` watermark — used by
    /// the lease tests to assert that the fast-path acquire did NOT
    /// advance raft.
    pub fn metadata_applied_index(&self) -> u64 {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.applied_index
    }

    /// Acquire a lease on this node via the SharedState facade.
    /// Called directly from the test's tokio runtime worker so the
    /// `block_in_place` inside `acquire_descriptor_lease` lands on
    /// a real runtime thread (which is what `block_in_place`
    /// requires — it cannot be called from a `spawn_blocking`
    /// worker).
    pub async fn acquire_lease(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u32,
        name: &str,
        version: u64,
        duration: std::time::Duration,
    ) -> Result<nodedb_cluster::DescriptorLease, String> {
        let id = nodedb_cluster::DescriptorId::new(tenant_id, kind, name.to_string());
        self.shared
            .acquire_descriptor_lease(id, version, duration)
            .map_err(|e| format!("acquire failed: {e}"))
    }

    /// Release a batch of leases on this node via the SharedState facade.
    pub async fn release_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> Result<(), String> {
        self.shared
            .release_descriptor_leases(descriptor_ids)
            .map_err(|e| format!("release failed: {e}"))
    }

    /// Read the `(descriptor_version, modification_hlc)` stamp of a
    /// collection on this node's local `SystemCatalog`. The applier
    /// is the only writer, so this is what every other node should
    /// agree on after the apply has propagated.
    pub fn collection_descriptor(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_collection(tenant_id, name).ok().flatten())
            .map(|coll| (coll.descriptor_version, coll.modification_hlc))
    }

    /// Same as [`collection_descriptor`] for stored functions.
    pub fn function_descriptor(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_function(tenant_id, name).ok().flatten())
            .map(|f| (f.descriptor_version, f.modification_hlc))
    }

    /// Same as [`collection_descriptor`] for stored procedures.
    pub fn procedure_descriptor(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_procedure(tenant_id, name).ok().flatten())
            .map(|p| (p.descriptor_version, p.modification_hlc))
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

    /// Force the routing table on this node to point `group_id` at `fake_leader`,
    /// creating a stale route.
    ///
    /// When the gateway on this node next dispatches to `group_id`, it will send
    /// the request to `fake_leader` instead of the real leader. The remote node
    /// (which is NOT the leader for that group) will return `TypedClusterError::NotLeader`,
    /// causing `retry_not_leader` to update the routing table and retry against
    /// the real leader. This is the canonical way to exercise the NotLeader retry
    /// path in tests without needing a real leadership change (which is slow and
    /// flaky).
    pub fn force_stale_route_for_test(&self, group_id: u64, fake_leader: u64) {
        if let Some(ref routing) = self.shared.cluster_routing {
            let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
            table.set_leader(group_id, fake_leader);
        }
    }

    /// Read the current `not_leader_retry_count` from this node's shared gateway.
    ///
    /// Returns 0 if the gateway has not been constructed yet (shouldn't happen
    /// in tests since the harness wires the gateway during spawn).
    pub fn not_leader_retry_count(&self) -> u64 {
        self.shared
            .gateway
            .as_ref()
            .map(|gw| gw.not_leader_retry_count())
            .unwrap_or(0)
    }
}
