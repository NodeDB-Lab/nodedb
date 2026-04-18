//! Object dependency tracking for the system catalog.
//!
//! Stores edges: source (function/trigger/procedure) → targets (functions, collections).
//! Used to block DROP when dependents exist.

use super::types::{DEPENDENCIES, SystemCatalog, catalog_err};

/// A single dependency edge: the source object references the target.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack, PartialEq, Eq)]
pub struct Dependency {
    /// Type of referenced object: "function", "collection".
    pub target_type: String,
    /// Name of referenced object.
    pub target_name: String,
}

/// All dependencies for a source object.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DependencyList {
    pub deps: Vec<Dependency>,
}

impl SystemCatalog {
    /// Store the dependency list for a source object.
    ///
    /// Key format: `"{source_type}:{tenant_id}:{source_name}"`.
    /// Overwrites any previous dependency list.
    pub fn put_dependencies(
        &self,
        source_type: &str,
        tenant_id: u32,
        source_name: &str,
        deps: &[Dependency],
    ) -> crate::Result<()> {
        let key = dep_key(source_type, tenant_id, source_name);
        let list = DependencyList {
            deps: deps.to_vec(),
        };
        let bytes = zerompk::to_msgpack_vec(&list).map_err(|e| catalog_err("serialize deps", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("open dependencies", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert deps", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete the dependency list for a source object.
    pub fn delete_dependencies(
        &self,
        source_type: &str,
        tenant_id: u32,
        source_name: &str,
    ) -> crate::Result<()> {
        let key = dep_key(source_type, tenant_id, source_name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("open dependencies", e))?;
            let _ = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove deps", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Find all source objects that depend on a given target.
    ///
    /// Scans all dependency lists for the tenant and returns source names
    /// that reference `(target_type, target_name)`.
    pub fn find_dependents(
        &self,
        tenant_id: u32,
        target_type: &str,
        target_name: &str,
    ) -> crate::Result<Vec<(String, String)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(DEPENDENCIES)
            .map_err(|e| catalog_err("open dependencies", e))?;

        let mut dependents = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range deps", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read dep", e))?;
            let key_str = key.value();

            // Only check entries for this tenant.
            // Key format: "{source_type}:{tenant_id}:{source_name}"
            let parts: Vec<&str> = key_str.splitn(3, ':').collect();
            if parts.len() < 3 {
                continue;
            }
            let entry_tid: u32 = match parts[1].parse() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if entry_tid != tenant_id {
                continue;
            }

            let list: DependencyList = match zerompk::from_msgpack(value.value()) {
                Ok(l) => l,
                Err(_) => continue,
            };

            for dep in &list.deps {
                if dep.target_type == target_type && dep.target_name == target_name {
                    dependents.push((parts[0].to_string(), parts[2].to_string()));
                    break;
                }
            }
        }
        Ok(dependents)
    }
}

fn dep_key(source_type: &str, tenant_id: u32, source_name: &str) -> String {
    format!("{source_type}:{tenant_id}:{source_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn store_and_find_dependents() {
        let catalog = make_catalog();

        // Function "f" depends on collection "users".
        catalog
            .put_dependencies(
                "function",
                1,
                "f",
                &[Dependency {
                    target_type: "collection".into(),
                    target_name: "users".into(),
                }],
            )
            .unwrap();

        let deps = catalog.find_dependents(1, "collection", "users").unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], ("function".into(), "f".into()));
    }

    #[test]
    fn no_dependents() {
        let catalog = make_catalog();
        let deps = catalog.find_dependents(1, "collection", "orders").unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn tenant_isolation() {
        let catalog = make_catalog();
        catalog
            .put_dependencies(
                "function",
                1,
                "f",
                &[Dependency {
                    target_type: "collection".into(),
                    target_name: "users".into(),
                }],
            )
            .unwrap();

        // Tenant 2 should see no dependents.
        let deps = catalog.find_dependents(2, "collection", "users").unwrap();
        assert!(deps.is_empty());
    }

    #[test]
    fn delete_dependencies() {
        let catalog = make_catalog();
        catalog
            .put_dependencies(
                "function",
                1,
                "f",
                &[Dependency {
                    target_type: "collection".into(),
                    target_name: "users".into(),
                }],
            )
            .unwrap();
        catalog.delete_dependencies("function", 1, "f").unwrap();

        let deps = catalog.find_dependents(1, "collection", "users").unwrap();
        assert!(deps.is_empty());
    }
}
