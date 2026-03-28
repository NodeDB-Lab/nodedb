//! Scope definition store: in-memory cache + redb persistence.
//!
//! A scope is a named bundle of permissions:
//! ```sql
//! DEFINE SCOPE 'profile:read' AS READ ON user_profiles, READ ON user_settings
//! DEFINE SCOPE 'customer' AS INCLUDE 'profile:read', INCLUDE 'orders:write'
//! ```

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::info;

use crate::control::security::catalog::{StoredScope, SystemCatalog};
use crate::control::security::time::now_secs;

/// In-memory scope definition.
#[derive(Debug, Clone)]
pub struct ScopeDefinition {
    pub name: String,
    /// Direct grants: `[("read", "user_profiles"), ("write", "orders")]`.
    pub grants: Vec<(String, String)>,
    /// Included sub-scopes for composition.
    pub includes: Vec<String>,
    pub created_by: String,
    pub created_at: u64,
}

impl ScopeDefinition {
    fn from_stored(s: &StoredScope) -> Self {
        Self {
            name: s.name.clone(),
            grants: s.grants.clone(),
            includes: s.includes.clone(),
            created_by: s.created_by.clone(),
            created_at: s.created_at,
        }
    }

    fn to_stored(&self) -> StoredScope {
        StoredScope {
            name: self.name.clone(),
            grants: self.grants.clone(),
            includes: self.includes.clone(),
            created_by: self.created_by.clone(),
            created_at: self.created_at,
        }
    }
}

/// Thread-safe scope definition store.
pub struct ScopeStore {
    scopes: RwLock<HashMap<String, ScopeDefinition>>,
    catalog: Option<SystemCatalog>,
}

impl ScopeStore {
    pub fn new() -> Self {
        Self {
            scopes: RwLock::new(HashMap::new()),
            catalog: None,
        }
    }

    pub fn open(catalog: SystemCatalog) -> crate::Result<Self> {
        let stored = catalog.load_all_scopes()?;
        let mut scopes = HashMap::with_capacity(stored.len());
        for s in &stored {
            scopes.insert(s.name.clone(), ScopeDefinition::from_stored(s));
        }
        if !scopes.is_empty() {
            info!(count = scopes.len(), "scopes loaded from catalog");
        }
        Ok(Self {
            scopes: RwLock::new(scopes),
            catalog: Some(catalog),
        })
    }

    /// Define or replace a scope.
    pub fn define(
        &self,
        name: &str,
        grants: Vec<(String, String)>,
        includes: Vec<String>,
        created_by: &str,
    ) -> crate::Result<()> {
        // Validate included scopes exist.
        {
            let scopes = self.scopes.read().unwrap_or_else(|p| p.into_inner());
            for inc in &includes {
                if !scopes.contains_key(inc) {
                    return Err(crate::Error::BadRequest {
                        detail: format!("included scope '{inc}' does not exist"),
                    });
                }
            }
        }

        let def = ScopeDefinition {
            name: name.into(),
            grants,
            includes,
            created_by: created_by.into(),
            created_at: now_secs(),
        };

        if let Some(ref catalog) = self.catalog {
            catalog.put_scope(&def.to_stored())?;
        }

        let mut scopes = self.scopes.write().unwrap_or_else(|p| p.into_inner());
        scopes.insert(name.into(), def);
        info!(scope = %name, "scope defined");
        Ok(())
    }

    /// Drop a scope.
    pub fn drop_scope(&self, name: &str) -> crate::Result<bool> {
        if let Some(ref catalog) = self.catalog {
            catalog.delete_scope(name)?;
        }
        let mut scopes = self.scopes.write().unwrap_or_else(|p| p.into_inner());
        Ok(scopes.remove(name).is_some())
    }

    /// Get a scope definition.
    pub fn get(&self, name: &str) -> Option<ScopeDefinition> {
        let scopes = self.scopes.read().unwrap_or_else(|p| p.into_inner());
        scopes.get(name).cloned()
    }

    /// Resolve a scope to its full set of `(permission, collection)` grants.
    ///
    /// Recursively expands `INCLUDE` references. Detects cycles.
    pub fn resolve(&self, name: &str) -> Vec<(String, String)> {
        let scopes = self.scopes.read().unwrap_or_else(|p| p.into_inner());
        let mut resolved = Vec::new();
        let mut visited = std::collections::HashSet::new();
        Self::resolve_recursive(&scopes, name, &mut resolved, &mut visited);
        resolved
    }

    fn resolve_recursive(
        scopes: &HashMap<String, ScopeDefinition>,
        name: &str,
        out: &mut Vec<(String, String)>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if !visited.insert(name.to_string()) {
            return; // Cycle detected — skip.
        }
        if let Some(def) = scopes.get(name) {
            out.extend(def.grants.iter().cloned());
            for inc in &def.includes {
                Self::resolve_recursive(scopes, inc, out, visited);
            }
        }
    }

    /// Check if a scope grants a specific `(permission, collection)` pair.
    pub fn scope_grants(&self, scope_name: &str, permission: &str, collection: &str) -> bool {
        let resolved = self.resolve(scope_name);
        resolved
            .iter()
            .any(|(p, c)| p == permission && c == collection)
    }

    /// List all defined scopes.
    pub fn list(&self) -> Vec<ScopeDefinition> {
        let scopes = self.scopes.read().unwrap_or_else(|p| p.into_inner());
        scopes.values().cloned().collect()
    }

    pub fn count(&self) -> usize {
        self.scopes.read().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn catalog(&self) -> Option<&SystemCatalog> {
        self.catalog.as_ref()
    }
}

impl Default for ScopeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn define_and_get() {
        let store = ScopeStore::new();
        store
            .define(
                "profile:read",
                vec![
                    ("read".into(), "user_profiles".into()),
                    ("read".into(), "user_settings".into()),
                ],
                vec![],
                "admin",
            )
            .unwrap();

        let scope = store.get("profile:read").unwrap();
        assert_eq!(scope.grants.len(), 2);
    }

    #[test]
    fn scope_composition() {
        let store = ScopeStore::new();
        store
            .define(
                "profile:read",
                vec![("read".into(), "profiles".into())],
                vec![],
                "admin",
            )
            .unwrap();
        store
            .define(
                "orders:write",
                vec![("write".into(), "orders".into())],
                vec![],
                "admin",
            )
            .unwrap();
        store
            .define(
                "customer",
                vec![],
                vec!["profile:read".into(), "orders:write".into()],
                "admin",
            )
            .unwrap();

        let resolved = store.resolve("customer");
        assert_eq!(resolved.len(), 2);
        assert!(resolved.contains(&("read".into(), "profiles".into())));
        assert!(resolved.contains(&("write".into(), "orders".into())));
    }

    #[test]
    fn cycle_detection() {
        let store = ScopeStore::new();
        // Create a → b → a cycle (manually bypass validation for test).
        {
            let mut scopes = store.scopes.write().unwrap();
            scopes.insert(
                "a".into(),
                ScopeDefinition {
                    name: "a".into(),
                    grants: vec![("read".into(), "t1".into())],
                    includes: vec!["b".into()],
                    created_by: "test".into(),
                    created_at: 0,
                },
            );
            scopes.insert(
                "b".into(),
                ScopeDefinition {
                    name: "b".into(),
                    grants: vec![("write".into(), "t2".into())],
                    includes: vec!["a".into()],
                    created_by: "test".into(),
                    created_at: 0,
                },
            );
        }
        // Should not infinite loop.
        let resolved = store.resolve("a");
        assert_eq!(resolved.len(), 2);
    }

    #[test]
    fn scope_grants_check() {
        let store = ScopeStore::new();
        store
            .define(
                "profile:read",
                vec![("read".into(), "profiles".into())],
                vec![],
                "admin",
            )
            .unwrap();

        assert!(store.scope_grants("profile:read", "read", "profiles"));
        assert!(!store.scope_grants("profile:read", "write", "profiles"));
    }

    #[test]
    fn include_validation() {
        let store = ScopeStore::new();
        let result = store.define("bad", vec![], vec!["nonexistent".into()], "admin");
        assert!(result.is_err());
    }
}
