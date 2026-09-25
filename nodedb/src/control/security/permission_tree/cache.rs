// SPDX-License-Identifier: BUSL-1.1

//! In-memory permission cache: parent hierarchy + grant lookups.
//!
//! Loaded from the governed collections and permission tables by a reload,
//! and kept current by the Event Plane's permission step. `progress` records
//! how far the cache reflects each core's writes. Lives entirely in the
//! Control Plane (Send + Sync).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::{debug, info};

use super::sources::SourceIndex;
use super::sync_state::ApplyProgress;
use super::types::{PermissionGrant, PermissionTreeDef};

/// Per-tenant permission state: resource hierarchy + permission grants.
#[derive(Debug, Default)]
struct TenantPermissions {
    /// Resource hierarchy: `child_id → parent_id`.
    /// Walk this map upward to find ancestors.
    parent_map: HashMap<String, String>,

    /// Permission grants: `(resource_id, grantee) → (level, inherited)`.
    /// Only explicit (non-inherited) grants are stored; inherited access is
    /// resolved dynamically by walking the parent chain.
    grants: HashMap<(String, String), (String, bool)>,

    /// Reverse children map: `parent_id → set of child_ids`.
    /// Used for cache invalidation: when a grant changes, evict all descendants.
    children_map: HashMap<String, HashSet<String>>,

    /// Monotonic version, bumped on every grant or edge mutation for this
    /// tenant. A cached physical plan stamps this at build time so a plan
    /// cache can detect a revoked grant instead of replaying a frozen filter.
    version: u64,
}

/// Central permission cache shared across all sessions.
///
/// Thread-safe: wrapped in `Arc<tokio::sync::RwLock<_>>` by SharedState.
#[derive(Debug)]
pub struct PermissionCache {
    /// Per-tenant permission state.
    tenants: HashMap<u64, TenantPermissions>,

    /// Per-collection permission tree definitions.
    /// Key: `(tenant_id, collection_name)`.
    tree_defs: HashMap<(u64, String), PermissionTreeDef>,

    /// How far the cache reflects each core's writes.
    progress: ApplyProgress,

    /// The source collections of `tree_defs`, readable without this cache's
    /// lock. Rebuilt on every tree-definition change.
    sources: Arc<SourceIndex>,
}

impl Default for PermissionCache {
    fn default() -> Self {
        Self::new()
    }
}

/// One collection a reload scans, and what its rows carry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TreeSource {
    pub tenant_id: u64,
    pub collection: String,
    pub kind: TreeSourceKind,
}

/// What a source collection's rows carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TreeSourceKind {
    /// A governed collection: each row's `id` and `parent_id` form an edge.
    Hierarchy,
    /// A permission table: each row is a grant.
    Grants,
}

impl PermissionCache {
    /// An empty cache. It needs a reload before planning may use it.
    pub fn new() -> Self {
        Self {
            tenants: HashMap::new(),
            tree_defs: HashMap::new(),
            progress: ApplyProgress::new(),
            sources: Arc::new(SourceIndex::default()),
        }
    }

    /// The lock-free index of this cache's source collections.
    pub fn sources(&self) -> Arc<SourceIndex> {
        Arc::clone(&self.sources)
    }

    /// How far the cache reflects each core's writes.
    pub fn progress(&self) -> &ApplyProgress {
        &self.progress
    }

    /// Mutable access to the apply progress, for the permission step and a
    /// reload.
    pub fn progress_mut(&mut self) -> &mut ApplyProgress {
        &mut self.progress
    }

    /// Every collection a reload scans, deduplicated.
    pub fn tree_sources(&self) -> Vec<TreeSource> {
        let mut sources: HashSet<TreeSource> = HashSet::new();
        for ((tenant_id, collection), def) in &self.tree_defs {
            sources.insert(TreeSource {
                tenant_id: *tenant_id,
                collection: collection.clone(),
                kind: TreeSourceKind::Hierarchy,
            });
            sources.insert(TreeSource {
                tenant_id: *tenant_id,
                collection: def.permission_table.clone(),
                kind: TreeSourceKind::Grants,
            });
        }
        sources.into_iter().collect()
    }

    /// Replace a tenant's hierarchy and grants with a reload's result, and
    /// bump its version so a cached plan built from the old state goes stale.
    pub fn replace_tenant_state(
        &mut self,
        tenant_id: u64,
        edges: &[(String, String)],
        grants: &[PermissionGrant],
    ) {
        let version = self.tenant_version(tenant_id);
        self.tenants.insert(
            tenant_id,
            TenantPermissions {
                version,
                ..TenantPermissions::default()
            },
        );
        self.load_edges(tenant_id, edges);
        self.load_grants(tenant_id, grants);
        self.bump_tenant_version(tenant_id);
    }

    /// Register a permission tree definition for a collection. Registering
    /// the definition already held changes nothing, so the DDL node and the
    /// metadata applier can both apply one change.
    pub fn register_tree_def(&mut self, tenant_id: u64, collection: &str, def: PermissionTreeDef) {
        if self.get_tree_def(tenant_id, collection) == Some(&def) {
            return;
        }
        info!(
            tenant_id,
            collection,
            levels = ?def.levels,
            "permission_tree: registered"
        );
        self.tree_defs
            .insert((tenant_id, collection.to_owned()), def);
        self.sources.rebuild(&self.tree_defs);
        // The new sources may already hold rows no reload has read.
        self.progress.mark_reload_needed();
        self.bump_tenant_version(tenant_id);
    }

    /// Remove a permission tree definition for a collection. Removing an
    /// absent definition changes nothing.
    pub fn unregister_tree_def(&mut self, tenant_id: u64, collection: &str) {
        if self
            .tree_defs
            .remove(&(tenant_id, collection.to_owned()))
            .is_none()
        {
            return;
        }
        self.sources.rebuild(&self.tree_defs);
        self.progress.mark_reload_needed();
        self.bump_tenant_version(tenant_id);
        info!(tenant_id, collection, "permission_tree: unregistered");
    }

    /// Get the permission tree definition for a collection (if any).
    pub fn get_tree_def(&self, tenant_id: u64, collection: &str) -> Option<&PermissionTreeDef> {
        self.tree_defs.get(&(tenant_id, collection.to_owned()))
    }

    /// Load a parent→child edge into the hierarchy.
    pub fn put_edge(&mut self, tenant_id: u64, child_id: &str, parent_id: &str) {
        let tenant = self.tenants.entry(tenant_id).or_default();
        tenant
            .parent_map
            .insert(child_id.to_owned(), parent_id.to_owned());
        tenant
            .children_map
            .entry(parent_id.to_owned())
            .or_default()
            .insert(child_id.to_owned());
    }

    /// Remove a parent→child edge from the hierarchy.
    pub fn remove_edge(&mut self, tenant_id: u64, child_id: &str) {
        let Some(tenant) = self.tenants.get_mut(&tenant_id) else {
            return;
        };
        if let Some(old_parent) = tenant.parent_map.remove(child_id)
            && let Some(children) = tenant.children_map.get_mut(&old_parent)
        {
            children.remove(child_id);
            if children.is_empty() {
                tenant.children_map.remove(&old_parent);
            }
        }
    }

    /// Load a permission grant into the cache.
    pub fn put_grant(&mut self, tenant_id: u64, grant: &PermissionGrant) {
        let tenant = self.tenants.entry(tenant_id).or_default();
        tenant.grants.insert(
            (grant.resource_id.clone(), grant.grantee.clone()),
            (grant.level.clone(), grant.inherited),
        );
    }

    /// Remove a permission grant from the cache.
    pub fn remove_grant(&mut self, tenant_id: u64, resource_id: &str, grantee: &str) {
        if let Some(tenant) = self.tenants.get_mut(&tenant_id) {
            tenant
                .grants
                .remove(&(resource_id.to_owned(), grantee.to_owned()));
        }
    }

    /// Look up the explicit grant for a (resource, grantee) pair.
    /// Returns `None` if no grant exists at this exact resource.
    pub fn get_grant(
        &self,
        tenant_id: u64,
        resource_id: &str,
        grantee: &str,
    ) -> Option<(&str, bool)> {
        self.tenants
            .get(&tenant_id)?
            .grants
            .get(&(resource_id.to_owned(), grantee.to_owned()))
            .map(|(level, inherited)| (level.as_str(), *inherited))
    }

    /// Get the parent of a resource. Returns `None` if root.
    pub fn get_parent(&self, tenant_id: u64, resource_id: &str) -> Option<&str> {
        self.tenants
            .get(&tenant_id)?
            .parent_map
            .get(resource_id)
            .map(|s| s.as_str())
    }

    /// Get all children of a resource (direct, not recursive).
    pub fn get_children(&self, tenant_id: u64, resource_id: &str) -> Vec<&str> {
        self.tenants
            .get(&tenant_id)
            .and_then(|t| t.children_map.get(resource_id))
            .map(|children| children.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get all resource IDs for a tenant (for iteration during accessible_resources).
    pub fn all_resource_ids(&self, tenant_id: u64) -> Vec<&str> {
        self.tenants
            .get(&tenant_id)
            .map(|t| {
                // Collect from all sources: edges (parent_map), reverse edges
                // (children_map), AND grant keys (for root resources with
                // direct grants but no parent/child edges).
                let mut ids: HashSet<&str> = HashSet::new();
                for k in t.parent_map.keys() {
                    ids.insert(k.as_str());
                }
                for k in t.parent_map.values() {
                    ids.insert(k.as_str());
                }
                for k in t.children_map.keys() {
                    ids.insert(k.as_str());
                }
                for (resource_id, _) in t.grants.keys() {
                    ids.insert(resource_id.as_str());
                }
                ids.into_iter().collect()
            })
            .unwrap_or_default()
    }

    /// Get all grantees that have explicit grants for a given resource.
    pub fn grantees_for_resource(&self, tenant_id: u64, resource_id: &str) -> Vec<&str> {
        self.tenants
            .get(&tenant_id)
            .map(|t| {
                t.grants
                    .keys()
                    .filter(|(rid, _)| rid == resource_id)
                    .map(|(_, grantee)| grantee.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Bulk load edges from a list of (child_id, parent_id) pairs.
    pub fn load_edges(&mut self, tenant_id: u64, edges: &[(String, String)]) {
        for (child, parent) in edges {
            self.put_edge(tenant_id, child, parent);
        }
        debug!(
            tenant_id,
            edges = edges.len(),
            "permission_tree: loaded edges"
        );
    }

    /// Bulk load grants.
    pub fn load_grants(&mut self, tenant_id: u64, grants: &[PermissionGrant]) {
        for grant in grants {
            self.put_grant(tenant_id, grant);
        }
        debug!(
            tenant_id,
            grants = grants.len(),
            "permission_tree: loaded grants"
        );
    }

    /// Number of resources tracked for a tenant.
    pub fn resource_count(&self, tenant_id: u64) -> usize {
        self.tenants
            .get(&tenant_id)
            .map(|t| t.parent_map.len())
            .unwrap_or(0)
    }

    /// Number of grants tracked for a tenant.
    pub fn grant_count(&self, tenant_id: u64) -> usize {
        self.tenants
            .get(&tenant_id)
            .map(|t| t.grants.len())
            .unwrap_or(0)
    }

    /// Check if any permission tree definitions are registered.
    pub fn has_tree_defs(&self) -> bool {
        !self.tree_defs.is_empty()
    }

    /// Check if any permission tree definition is registered for this tenant.
    ///
    /// Used by plan-time enforcement for operations that name no collection:
    /// they cannot be shown to avoid a governed collection, so the question
    /// widens to the whole tenant.
    pub fn has_tree_defs_for_tenant(&self, tenant_id: u64) -> bool {
        self.tree_defs.keys().any(|(tid, _)| *tid == tenant_id)
    }

    /// Check if any tree def for this tenant uses `collection` as its permission table.
    pub fn tree_defs_using_permission_table(&self, tenant_id: u64, collection: &str) -> bool {
        self.tree_defs
            .iter()
            .any(|((tid, _), def)| *tid == tenant_id && def.permission_table == collection)
    }

    /// Check if any tree def for this tenant uses `collection` as its resource graph source.
    /// The graph index name references a graph on a collection — we check by matching
    /// the collection name itself (the resource hierarchy collection).
    pub fn tree_defs_using_graph(&self, tenant_id: u64, collection: &str) -> bool {
        self.tree_defs
            .iter()
            .any(|((tid, coll), _)| *tid == tenant_id && coll == collection)
    }

    /// Bump and return the per-tenant permission version. Called from every
    /// mutator in `invalidation.rs`, in the same critical section as the
    /// state change, so a stamped plan cache entry goes stale on the spot.
    pub fn bump_tenant_version(&mut self, tenant_id: u64) -> u64 {
        let tenant = self.tenants.entry(tenant_id).or_default();
        tenant.version += 1;
        tenant.version
    }

    /// Current permission version for a tenant. `0` if the tenant has never
    /// been mutated.
    pub fn tenant_version(&self, tenant_id: u64) -> u64 {
        self.tenants.get(&tenant_id).map(|t| t.version).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_hierarchy() {
        let mut cache = PermissionCache::new();
        // workspace → folder → doc
        cache.put_edge(1, "folder-1", "workspace-1");
        cache.put_edge(1, "doc-1", "folder-1");

        assert_eq!(cache.get_parent(1, "doc-1"), Some("folder-1"));
        assert_eq!(cache.get_parent(1, "folder-1"), Some("workspace-1"));
        assert_eq!(cache.get_parent(1, "workspace-1"), None); // Root.

        let children = cache.get_children(1, "workspace-1");
        assert_eq!(children, vec!["folder-1"]);
    }

    #[test]
    fn grant_lookup() {
        let mut cache = PermissionCache::new();
        cache.put_grant(
            1,
            &PermissionGrant {
                resource_id: "folder-1".into(),
                grantee: "user-42".into(),
                level: "editor".into(),
                inherited: false,
            },
        );

        let (level, inherited) = cache.get_grant(1, "folder-1", "user-42").unwrap();
        assert_eq!(level, "editor");
        assert!(!inherited);

        assert!(cache.get_grant(1, "folder-1", "user-99").is_none());
    }

    #[test]
    fn remove_edge() {
        let mut cache = PermissionCache::new();
        cache.put_edge(1, "doc-1", "folder-1");
        assert_eq!(cache.get_parent(1, "doc-1"), Some("folder-1"));

        cache.remove_edge(1, "doc-1");
        assert_eq!(cache.get_parent(1, "doc-1"), None);
        assert!(cache.get_children(1, "folder-1").is_empty());
    }

    #[test]
    fn remove_grant() {
        let mut cache = PermissionCache::new();
        cache.put_grant(
            1,
            &PermissionGrant {
                resource_id: "doc-1".into(),
                grantee: "user-1".into(),
                level: "viewer".into(),
                inherited: false,
            },
        );
        assert!(cache.get_grant(1, "doc-1", "user-1").is_some());
        cache.remove_grant(1, "doc-1", "user-1");
        assert!(cache.get_grant(1, "doc-1", "user-1").is_none());
    }

    #[test]
    fn tenant_isolation() {
        let mut cache = PermissionCache::new();
        cache.put_edge(1, "doc-1", "folder-1");
        cache.put_edge(2, "doc-1", "folder-2");

        assert_eq!(cache.get_parent(1, "doc-1"), Some("folder-1"));
        assert_eq!(cache.get_parent(2, "doc-1"), Some("folder-2"));
    }

    #[test]
    fn tenant_version_bumps_and_is_isolated_per_tenant() {
        let mut cache = PermissionCache::new();
        assert_eq!(cache.tenant_version(1), 0);
        assert_eq!(cache.bump_tenant_version(1), 1);
        assert_eq!(cache.bump_tenant_version(1), 2);
        assert_eq!(cache.tenant_version(1), 2);
        // A different tenant's bumps do not affect this one's version.
        assert_eq!(cache.bump_tenant_version(2), 1);
        assert_eq!(cache.tenant_version(1), 2);
    }

    #[test]
    fn tree_def_registration() {
        let mut cache = PermissionCache::new();
        let def = PermissionTreeDef {
            resource_column: "id".into(),
            graph_index: "tree".into(),
            permission_table: "perms".into(),
            levels: vec!["none".into(), "viewer".into(), "editor".into()],
            read_level: "viewer".into(),
            write_level: "editor".into(),
            delete_level: "editor".into(),
        };
        cache.register_tree_def(1, "documents", def.clone());
        assert!(cache.get_tree_def(1, "documents").is_some());
        assert!(cache.get_tree_def(1, "other").is_none());
        assert!(cache.get_tree_def(2, "documents").is_none());

        // The DDL node and the metadata applier both apply one change.
        let version = cache.tenant_version(1);
        cache.register_tree_def(1, "documents", def);
        assert_eq!(cache.tenant_version(1), version);

        cache.unregister_tree_def(1, "documents");
        assert!(cache.get_tree_def(1, "documents").is_none());
        let version = cache.tenant_version(1);
        cache.unregister_tree_def(1, "documents");
        assert_eq!(cache.tenant_version(1), version);
    }

    #[test]
    fn a_reload_replaces_the_tenant_state_and_bumps_its_version() {
        let mut cache = PermissionCache::new();
        cache.put_edge(1, "doc-1", "folder-1");
        cache.put_grant(
            1,
            &PermissionGrant {
                resource_id: "doc-1".into(),
                grantee: "user-1".into(),
                level: "viewer".into(),
                inherited: false,
            },
        );
        let before = cache.bump_tenant_version(1);

        cache.replace_tenant_state(1, &[("doc-2".into(), "folder-2".into())], &[]);

        assert_eq!(cache.get_parent(1, "doc-1"), None);
        assert_eq!(cache.get_parent(1, "doc-2"), Some("folder-2"));
        assert!(cache.get_grant(1, "doc-1", "user-1").is_none());
        assert!(cache.tenant_version(1) > before);
    }

    #[test]
    fn tree_sources_name_the_governed_collection_and_the_permission_table() {
        let mut cache = PermissionCache::new();
        let def: PermissionTreeDef = sonic_rs::from_str(
            r#"{"resource_column":"id","graph_index":"tree","permission_table":"grants"}"#,
        )
        .expect("tree def");
        cache.register_tree_def(1, "docs", def);
        let mut sources = cache.tree_sources();
        sources.sort_by(|a, b| a.collection.cmp(&b.collection));
        assert_eq!(
            sources,
            vec![
                TreeSource {
                    tenant_id: 1,
                    collection: "docs".into(),
                    kind: TreeSourceKind::Hierarchy,
                },
                TreeSource {
                    tenant_id: 1,
                    collection: "grants".into(),
                    kind: TreeSourceKind::Grants,
                },
            ]
        );
        assert!(cache.progress().needs_reload_for(&[]));
    }
}
