// SPDX-License-Identifier: BUSL-1.1

//! Custom role management with inheritance.
//!
//! Built-in roles (Superuser, TenantAdmin, ReadWrite, ReadOnly, Monitor) are
//! defined in `identity.rs`. This module manages user-defined custom roles
//! with optional single-parent inheritance.

use parking_lot::RwLock;
use std::collections::HashMap;

use crate::types::TenantId;

use super::catalog::{StoredRole, SystemCatalog};
use super::identity::Role;

/// Maximum allowed depth of a custom role inheritance chain (self + ancestors).
///
/// A chain of depth 8 means the role itself plus up to 7 ancestors. Any
/// attempt to create a role or assign a parent that would produce a chain
/// longer than this is rejected at catalog-write time with
/// [`crate::Error::RoleInheritanceDepthExceeded`].
pub const MAX_ROLE_INHERITANCE_DEPTH: usize = 8;

/// In-memory custom role record.
#[derive(Debug, Clone)]
pub struct CustomRole {
    pub name: String,
    pub tenant_id: TenantId,
    /// Parent role for inheritance. None = standalone.
    pub parent: Option<String>,
    pub created_at: u64,
}

/// Custom role store with in-memory cache and redb persistence.
pub struct RoleStore {
    roles: RwLock<HashMap<String, CustomRole>>,
}

impl Default for RoleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RoleStore {
    pub fn new() -> Self {
        Self {
            roles: RwLock::new(HashMap::new()),
        }
    }

    pub fn load_from(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let stored = catalog.load_all_roles()?;
        let mut roles = self.roles.write();
        for s in stored {
            let role = CustomRole {
                name: s.name.clone(),
                tenant_id: TenantId::new(s.tenant_id),
                parent: if s.parent.is_empty() {
                    None
                } else {
                    Some(s.parent)
                },
                created_at: s.created_at,
            };
            roles.insert(role.name.clone(), role);
        }
        if !roles.is_empty() {
            tracing::info!(count = roles.len(), "loaded custom roles from catalog");
        }
        Ok(())
    }

    /// Clear the in-memory role map and re-run `load_from`.
    /// Used by the catalog recovery sanity checker to repair
    /// a divergent registry. Callers keep their existing
    /// `&RoleStore` reference.
    pub(crate) fn clear_and_reload(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        {
            let mut roles = self.roles.write();
            roles.clear();
        }
        self.load_from(catalog)
    }

    // ── Cluster replication hooks ──────────────────────────────
    //
    // Symmetric partners to `CredentialStore::install_replicated_user`:
    // the production metadata applier calls these on every node
    // after writing / removing a `StoredRole` via
    // `CatalogEntry::PutRole` / `DeleteRole`.

    /// Install a replicated `StoredRole` into the in-memory cache.
    /// Never touches the catalog — the applier handles redb
    /// separately via `SystemCatalog::put_role`. Upsert semantics.
    pub fn install_replicated_role(&self, stored: &StoredRole) {
        let custom = CustomRole {
            name: stored.name.clone(),
            tenant_id: TenantId::new(stored.tenant_id),
            parent: if stored.parent.is_empty() {
                None
            } else {
                Some(stored.parent.clone())
            },
            created_at: stored.created_at,
        };
        let mut roles = self.roles.write();
        roles.insert(stored.name.clone(), custom);
    }

    /// Remove a replicated role from the in-memory cache.
    pub fn install_replicated_drop_role(&self, name: &str) {
        let mut roles = self.roles.write();
        roles.remove(name);
    }

    /// Build a `StoredRole` ready for replication via
    /// `CatalogEntry::PutRole`, without writing to redb or the
    /// in-memory cache. Performs the same validation
    /// `create_role` does (built-in name rejection, duplicate
    /// check, parent existence).
    pub fn prepare_role(
        &self,
        name: &str,
        tenant_id: TenantId,
        parent: Option<&str>,
    ) -> crate::Result<StoredRole> {
        prepare_role_against(name, tenant_id, parent, &self.roles.read())
    }

    /// Create a custom role. Returns error if it already exists or would create a cycle.
    pub fn create_role(
        &self,
        name: &str,
        tenant_id: TenantId,
        parent: Option<&str>,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        // Reject built-in role names.
        if is_builtin(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("'{name}' is a built-in role and cannot be created"),
            });
        }

        let mut roles = self.roles.write();

        if roles.contains_key(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("role '{name}' already exists"),
            });
        }

        // Validate parent exists (built-in or custom) and enforce depth/cycle rules.
        if let Some(parent_name) = parent {
            validate_parent(name, parent_name, &roles)?;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let role = CustomRole {
            name: name.to_string(),
            tenant_id,
            parent: parent.map(|s| s.to_string()),
            created_at: now,
        };

        if let Some(catalog) = catalog {
            catalog.put_role(&StoredRole {
                name: name.to_string(),
                tenant_id: tenant_id.as_u64(),
                parent: parent.unwrap_or("").to_string(),
                created_at: now,
            })?;
        }

        roles.insert(name.to_string(), role);
        Ok(())
    }

    /// Drop a custom role.
    pub fn drop_role(&self, name: &str, catalog: Option<&SystemCatalog>) -> crate::Result<bool> {
        if is_builtin(name) {
            return Err(crate::Error::BadRequest {
                detail: format!("cannot drop built-in role '{name}'"),
            });
        }

        let mut roles = self.roles.write();

        // Check no other role inherits from this one.
        let has_children = roles.values().any(|r| r.parent.as_deref() == Some(name));
        if has_children {
            return Err(crate::Error::BadRequest {
                detail: format!("cannot drop role '{name}': other roles inherit from it"),
            });
        }

        if roles.remove(name).is_some() {
            if let Some(catalog) = catalog {
                catalog.delete_role(name)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Resolve the full permission chain for a role, following inheritance.
    ///
    /// Returns a list of role names from the given role up through its
    /// ancestors, capped at `MAX_ROLE_INHERITANCE_DEPTH` entries (self +
    /// ancestors). The catalog enforces no cycles and no chains deeper than
    /// `MAX_ROLE_INHERITANCE_DEPTH` at write time, so this walk is O(depth)
    /// with no HashSet needed. If the stored chain somehow violates the
    /// invariant (e.g. data written before the cap was introduced), the walk
    /// returns an error rather than truncating silently.
    pub fn resolve_inheritance(&self, role: &Role) -> crate::Result<Vec<Role>> {
        let mut chain = vec![role.clone()];

        if let Role::Custom(name) = role {
            let roles = self.roles.read();

            let mut current = name.as_str();

            loop {
                if chain.len() > MAX_ROLE_INHERITANCE_DEPTH {
                    return Err(crate::Error::RoleInheritanceDepthExceeded {
                        depth: chain.len(),
                        limit: MAX_ROLE_INHERITANCE_DEPTH,
                    });
                }
                // Built-in roles have no further parents.
                if is_builtin(current) {
                    break;
                }
                match roles.get(current) {
                    Some(custom) => match &custom.parent {
                        Some(parent_name) => {
                            // Role::from_str is infallible (Err = Infallible);
                            // matching on the uninhabited error proves it at
                            // compile time without an unwrap.
                            let parent_role: Role = match parent_name.parse() {
                                Ok(r) => r,
                                Err(e) => match e {},
                            };
                            chain.push(parent_role);
                            current = parent_name.as_str();
                        }
                        None => break,
                    },
                    None => break,
                }
            }
        }

        Ok(chain)
    }

    /// Check that adopting `parent` as `role_name`'s inheritance parent would
    /// not create a cycle or exceed [`MAX_ROLE_INHERITANCE_DEPTH`].
    ///
    /// Used by `ALTER ROLE ... SET INHERIT` and the role-to-role form of
    /// `GRANT` so that re-parenting an existing role enforces the same
    /// chain invariant `create_role` enforces at creation. The parent's
    /// existence is the caller's responsibility.
    pub fn check_inheritance_cycle(&self, role_name: &str, parent: &str) -> crate::Result<()> {
        let roles = self.roles.read();
        check_inheritance_chain(role_name, parent, &roles)
    }

    /// Look up a custom role by name. Returns None if not found.
    pub fn get_role(&self, name: &str) -> Option<CustomRole> {
        let roles = self.roles.read();
        roles.get(name).cloned()
    }

    /// List all custom roles.
    pub fn list_roles(&self) -> Vec<CustomRole> {
        let roles = self.roles.read();
        roles.values().cloned().collect()
    }
}

/// Build a `StoredRole` ready for replication via `CatalogEntry::PutRole`,
/// validated against `roles`: the custom roles the creating statement sees.
/// Inside a transaction those include the roles it created earlier. Rejects a
/// built-in name, a duplicate, an undefined parent, and a parent that would
/// close a cycle or exceed [`MAX_ROLE_INHERITANCE_DEPTH`].
pub fn prepare_role_against(
    name: &str,
    tenant_id: TenantId,
    parent: Option<&str>,
    roles: &HashMap<String, CustomRole>,
) -> crate::Result<StoredRole> {
    if is_builtin(name) {
        return Err(crate::Error::BadRequest {
            detail: format!("'{name}' is a built-in role and cannot be created"),
        });
    }
    if roles.contains_key(name) {
        return Err(crate::Error::BadRequest {
            detail: format!("role '{name}' already exists"),
        });
    }
    if let Some(parent_name) = parent {
        validate_parent(name, parent_name, roles)?;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(StoredRole {
        name: name.to_string(),
        tenant_id: tenant_id.as_u64(),
        parent: parent.unwrap_or("").to_string(),
        created_at: now,
    })
}

/// [`RoleStore::check_inheritance_cycle`] against `roles`, the custom roles
/// the altering statement sees.
pub fn check_inheritance_cycle_against(
    role_name: &str,
    parent: &str,
    roles: &HashMap<String, CustomRole>,
) -> crate::Result<()> {
    check_inheritance_chain(role_name, parent, roles)
}

/// Walk the inheritance chain starting from `start_name` upward through the
/// given `roles` map. Returns the chain length (number of hops including
/// `start_name` itself). If the chain is a cycle or exceeds
/// `MAX_ROLE_INHERITANCE_DEPTH`, returns the corresponding error.
///
/// This is the single authoritative write-time check — both `create_role` and
/// `prepare_role` call it so the guarantee holds for every catalog mutation path.
fn check_inheritance_chain(
    proposed_child: &str,
    proposed_parent: &str,
    roles: &HashMap<String, CustomRole>,
) -> crate::Result<()> {
    // Walk upward from the proposed parent. If we encounter `proposed_child`
    // we have a cycle. If we exceed MAX_ROLE_INHERITANCE_DEPTH hops we stop.
    let mut current = proposed_parent;
    // depth counts the total chain: proposed_child (1) + proposed_parent (2) + ancestors.
    let mut depth: usize = 2;

    loop {
        if current == proposed_child {
            return Err(crate::Error::RoleInheritanceCycle {
                child: proposed_child.to_string(),
                parent: proposed_parent.to_string(),
            });
        }
        if depth > MAX_ROLE_INHERITANCE_DEPTH {
            return Err(crate::Error::RoleInheritanceDepthExceeded {
                depth,
                limit: MAX_ROLE_INHERITANCE_DEPTH,
            });
        }
        // Built-in roles have no further parents; chain ends here.
        if is_builtin(current) {
            break;
        }
        match roles.get(current) {
            Some(role) => match &role.parent {
                Some(parent_name) => {
                    current = parent_name.as_str();
                    depth += 1;
                }
                None => break,
            },
            None => break,
        }
    }
    Ok(())
}

/// Validate that `parent_name` refers to an existing role (built-in or
/// custom) and that adopting it as `child_name`'s parent does not create a
/// cycle or exceed `MAX_ROLE_INHERITANCE_DEPTH`. Shared by `prepare_role`
/// and `create_role` so both catalog mutation paths enforce the same rules.
fn validate_parent(
    child_name: &str,
    parent_name: &str,
    roles: &HashMap<String, CustomRole>,
) -> crate::Result<()> {
    if !is_builtin(parent_name) && !roles.contains_key(parent_name) {
        return Err(crate::Error::BadRequest {
            detail: format!("parent role '{parent_name}' does not exist"),
        });
    }
    check_inheritance_chain(child_name, parent_name, roles)
}

/// Every name the role parser maps to a built-in role, so a custom role can
/// never shadow one: `cluster_admin` and the `database_*:{id}` forms as well
/// as the five tenant roles.
fn is_builtin(name: &str) -> bool {
    super::role_assignment::is_builtin_role_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_custom_role() {
        let store = RoleStore::new();
        store
            .create_role("analyst", TenantId::new(1), None, None)
            .unwrap();
        assert!(store.get_role("analyst").is_some());
    }

    #[test]
    fn role_cache_remains_usable_after_panic_while_write_locked() {
        let store = RoleStore::new();
        store
            .create_role("analyst", TenantId::new(1), None, None)
            .expect("create role");

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = store.roles.write();
            panic!("simulated interrupted role cache update");
        }));
        assert!(outcome.is_err());
        assert!(store.get_role("analyst").is_some());
        assert!(store.drop_role("analyst", None).expect("drop role"));
        assert!(store.get_role("analyst").is_none());
    }

    #[test]
    fn create_with_builtin_parent() {
        let store = RoleStore::new();
        store
            .create_role("senior_analyst", TenantId::new(1), Some("readonly"), None)
            .unwrap();
        let role = store.get_role("senior_analyst").unwrap();
        assert_eq!(role.parent.as_deref(), Some("readonly"));
    }

    #[test]
    fn create_with_custom_parent() {
        let store = RoleStore::new();
        store
            .create_role("base", TenantId::new(1), None, None)
            .unwrap();
        store
            .create_role("child", TenantId::new(1), Some("base"), None)
            .unwrap();
        assert!(store.get_role("child").is_some());
    }

    #[test]
    fn reject_builtin_name() {
        let store = RoleStore::new();
        assert!(
            store
                .create_role("superuser", TenantId::new(1), None, None)
                .is_err()
        );
    }

    #[test]
    fn reject_duplicate() {
        let store = RoleStore::new();
        store
            .create_role("analyst", TenantId::new(1), None, None)
            .unwrap();
        assert!(
            store
                .create_role("analyst", TenantId::new(1), None, None)
                .is_err()
        );
    }

    #[test]
    fn reject_nonexistent_parent() {
        let store = RoleStore::new();
        assert!(
            store
                .create_role("child", TenantId::new(1), Some("nonexistent"), None)
                .is_err()
        );
    }

    #[test]
    fn drop_role() {
        let store = RoleStore::new();
        store
            .create_role("temp", TenantId::new(1), None, None)
            .unwrap();
        assert!(store.drop_role("temp", None).unwrap());
        assert!(store.get_role("temp").is_none());
    }

    #[test]
    fn drop_builtin_rejected() {
        let store = RoleStore::new();
        assert!(store.drop_role("readonly", None).is_err());
    }

    #[test]
    fn drop_with_children_rejected() {
        let store = RoleStore::new();
        store
            .create_role("parent", TenantId::new(1), None, None)
            .unwrap();
        store
            .create_role("child", TenantId::new(1), Some("parent"), None)
            .unwrap();
        assert!(store.drop_role("parent", None).is_err());
    }

    #[test]
    fn resolve_inheritance_chain() {
        let store = RoleStore::new();
        store
            .create_role("base", TenantId::new(1), Some("readonly"), None)
            .unwrap();
        store
            .create_role("mid", TenantId::new(1), Some("base"), None)
            .unwrap();
        store
            .create_role("leaf", TenantId::new(1), Some("mid"), None)
            .unwrap();

        let chain = store
            .resolve_inheritance(&Role::Custom("leaf".into()))
            .unwrap();
        assert_eq!(chain.len(), 4); // leaf → mid → base → readonly
    }

    #[test]
    fn resolve_builtin_no_chain() {
        let store = RoleStore::new();
        let chain = store.resolve_inheritance(&Role::ReadOnly).unwrap();
        assert_eq!(chain.len(), 1);
    }

    /// A chain of 8 custom roles (each inheriting from the previous) must
    /// succeed at grant time and resolve cleanly.
    #[test]
    fn role_inheritance_depth_8_succeeds() {
        let store = RoleStore::new();
        // Create roles r1 … r8 where r1 has no parent, r2→r1, …, r8→r7.
        store
            .create_role("r1", TenantId::new(1), None, None)
            .unwrap();
        for i in 2..=8usize {
            let name = format!("r{i}");
            let parent = format!("r{}", i - 1);
            store
                .create_role(&name, TenantId::new(1), Some(&parent), None)
                .expect("depth-8 chain should be accepted");
        }
        // resolve_inheritance for r8 should return 8 entries.
        let chain = store
            .resolve_inheritance(&Role::Custom("r8".into()))
            .unwrap();
        assert_eq!(chain.len(), 8);
    }

    /// Adding a 9th ancestor must be rejected with `RoleInheritanceDepthExceeded`.
    #[test]
    fn role_inheritance_depth_9_rejected() {
        let store = RoleStore::new();
        store
            .create_role("r1", TenantId::new(1), None, None)
            .unwrap();
        for i in 2..=8usize {
            let name = format!("r{i}");
            let parent = format!("r{}", i - 1);
            store
                .create_role(&name, TenantId::new(1), Some(&parent), None)
                .unwrap();
        }
        // r9 → r8 would create a 9-node chain — must be rejected.
        let err = store
            .create_role("r9", TenantId::new(1), Some("r8"), None)
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::RoleInheritanceDepthExceeded { .. }),
            "expected RoleInheritanceDepthExceeded, got: {err:?}"
        );
    }

    /// A→B→C, then attempting to grant C a parent of A must return
    /// `RoleInheritanceCycle`.
    #[test]
    fn role_inheritance_cycle_rejected() {
        let store = RoleStore::new();
        store
            .create_role("a", TenantId::new(1), None, None)
            .unwrap();
        store
            .create_role("b", TenantId::new(1), Some("a"), None)
            .unwrap();
        store
            .create_role("c", TenantId::new(1), Some("b"), None)
            .unwrap();
        // Now try to create a new role "d" with parent "c" that would make a→b→c→d and
        // then try to make "a" a child of "c" (a cycle): create_role("a2", parent=c)
        // won't cycle, but creating any role with parent chain looping back is what we test.
        // Simulate by directly trying to insert a role whose parent chain leads back to itself.
        // The simplest test: try to create role "loop" with parent "c" AND
        // separately verify we can't create a role whose ancestor is itself.
        // Direct cycle: create role "x" with parent "x" is caught by existence check first.
        // Real test: A→B→C exists. Now drop C and re-create with parent A (not a cycle).
        // The canonical test: A inherits B, B inherits C. Try to make C's parent = A.
        // We can't mutate existing parents in this API, so test via prepare_role:
        // a has no parent, b→a, c→b. Making a role "d" with parent "c" is fine.
        // Cycle test: simulate A→B→C and try making A's chain go through C by
        // adding role "cycle_root" with parent="c" where "cycle_root" is also
        // somewhere above c — but since parents are immutable after creation,
        // we use check_inheritance_chain directly.
        //
        // Real observable cycle via public API: create roles in reverse to force a
        // cycle attempt at write time.
        let store2 = RoleStore::new();
        store2
            .create_role("roleA", TenantId::new(1), None, None)
            .unwrap();
        store2
            .create_role("roleB", TenantId::new(1), Some("roleA"), None)
            .unwrap();
        store2
            .create_role("roleC", TenantId::new(1), Some("roleB"), None)
            .unwrap();
        // Now try to create "roleA_child" with parent "roleC" — no cycle.
        store2
            .create_role("roleA_child", TenantId::new(1), Some("roleC"), None)
            .unwrap();
        // Cycle: call check_inheritance_chain directly for roleA → roleC (roleA is ancestor).
        let roles_guard = store2.roles.read();
        let err = check_inheritance_chain("roleA", "roleC", &roles_guard).unwrap_err();
        drop(roles_guard);
        assert!(
            matches!(err, crate::Error::RoleInheritanceCycle { .. }),
            "expected RoleInheritanceCycle, got: {err:?}"
        );
    }

    /// `resolve_inheritance` must return at most `MAX_ROLE_INHERITANCE_DEPTH`
    /// entries and never spin infinitely.
    #[test]
    fn resolve_inheritance_bounded() {
        let store = RoleStore::new();
        // Build a valid chain of exactly MAX_ROLE_INHERITANCE_DEPTH roles.
        store
            .create_role("root", TenantId::new(1), None, None)
            .unwrap();
        let mut prev = "root".to_string();
        for i in 1..MAX_ROLE_INHERITANCE_DEPTH {
            let name = format!("node{i}");
            store
                .create_role(&name, TenantId::new(1), Some(&prev), None)
                .unwrap();
            prev = name;
        }
        let chain = store.resolve_inheritance(&Role::Custom(prev)).unwrap();
        assert!(
            chain.len() <= MAX_ROLE_INHERITANCE_DEPTH,
            "chain length {} exceeds MAX_ROLE_INHERITANCE_DEPTH {}",
            chain.len(),
            MAX_ROLE_INHERITANCE_DEPTH
        );
    }
}
