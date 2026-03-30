//! User-defined function metadata operations for the system catalog.

use super::function_types::StoredFunction;
use super::types::{FUNCTIONS, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Store a user-defined function record.
    ///
    /// Key format: `"{tenant_id}:{function_name}"`.
    /// Overwrites any existing definition with the same key (for CREATE OR REPLACE).
    pub fn put_function(&self, func: &StoredFunction) -> crate::Result<()> {
        let key = function_key(func.tenant_id, &func.name);
        let bytes = rmp_serde::to_vec(func).map_err(|e| catalog_err("serialize function", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(FUNCTIONS)
                .map_err(|e| catalog_err("open functions", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert function", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a single function by tenant_id + name.
    pub fn get_function(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> crate::Result<Option<StoredFunction>> {
        let key = function_key(tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(FUNCTIONS)
            .map_err(|e| catalog_err("open functions", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let func: StoredFunction = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser function", e))?;
                Ok(Some(func))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get function", e)),
        }
    }

    /// Delete a function by tenant_id + name.
    ///
    /// Returns `true` if the function existed and was removed.
    pub fn delete_function(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = function_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(FUNCTIONS)
                .map_err(|e| catalog_err("open functions", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove function", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all user-defined functions for a tenant.
    pub fn load_functions_for_tenant(&self, tenant_id: u32) -> crate::Result<Vec<StoredFunction>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(FUNCTIONS)
            .map_err(|e| catalog_err("open functions", e))?;
        let mut funcs = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range functions", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read function", e))?;
            if key.value().starts_with(&prefix) {
                let func: StoredFunction = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser function", e))?;
                funcs.push(func);
            }
        }
        Ok(funcs)
    }
}

/// Build the redb key for a function: `"{tenant_id}:{name}"`.
fn function_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::function_types::{FunctionParam, FunctionVolatility};

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample_function(tenant_id: u32, name: &str) -> StoredFunction {
        StoredFunction {
            tenant_id,
            name: name.to_string(),
            parameters: vec![FunctionParam {
                name: "email".into(),
                data_type: "TEXT".into(),
            }],
            return_type: "TEXT".into(),
            body_sql: "SELECT LOWER(TRIM(email))".into(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Immutable,
            security: crate::control::security::catalog::FunctionSecurity::default(),
            owner: "admin".into(),
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_get() {
        let catalog = make_catalog();
        let func = sample_function(1, "normalize_email");
        catalog.put_function(&func).unwrap();

        let loaded = catalog.get_function(1, "normalize_email").unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.name, "normalize_email");
        assert_eq!(loaded.parameters.len(), 1);
        assert_eq!(loaded.return_type, "TEXT");
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let catalog = make_catalog();
        assert!(catalog.get_function(1, "nope").unwrap().is_none());
    }

    #[test]
    fn delete_function() {
        let catalog = make_catalog();
        catalog.put_function(&sample_function(1, "f")).unwrap();
        assert!(catalog.delete_function(1, "f").unwrap());
        assert!(!catalog.delete_function(1, "f").unwrap());
        assert!(catalog.get_function(1, "f").unwrap().is_none());
    }

    #[test]
    fn load_for_tenant_filters() {
        let catalog = make_catalog();
        catalog.put_function(&sample_function(1, "a")).unwrap();
        catalog.put_function(&sample_function(1, "b")).unwrap();
        catalog.put_function(&sample_function(2, "c")).unwrap();

        let t1 = catalog.load_functions_for_tenant(1).unwrap();
        assert_eq!(t1.len(), 2);
        let t2 = catalog.load_functions_for_tenant(2).unwrap();
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].name, "c");
    }

    #[test]
    fn replace_overwrites() {
        let catalog = make_catalog();
        catalog.put_function(&sample_function(1, "f")).unwrap();

        let mut updated = sample_function(1, "f");
        updated.body_sql = "SELECT UPPER(email)".into();
        catalog.put_function(&updated).unwrap();

        let loaded = catalog.get_function(1, "f").unwrap().unwrap();
        assert_eq!(loaded.body_sql, "SELECT UPPER(email)");
    }
}
