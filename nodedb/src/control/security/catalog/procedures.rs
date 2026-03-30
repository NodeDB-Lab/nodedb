//! Stored procedure metadata operations for the system catalog.

use super::procedure_types::StoredProcedure;
use super::types::{PROCEDURES, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Store a procedure definition.
    pub fn put_procedure(&self, proc: &StoredProcedure) -> crate::Result<()> {
        let key = procedure_key(proc.tenant_id, &proc.name);
        let bytes = rmp_serde::to_vec(proc).map_err(|e| catalog_err("serialize procedure", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(PROCEDURES)
                .map_err(|e| catalog_err("open procedures", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert procedure", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a single procedure by tenant_id + name.
    pub fn get_procedure(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> crate::Result<Option<StoredProcedure>> {
        let key = procedure_key(tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(PROCEDURES)
            .map_err(|e| catalog_err("open procedures", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let p: StoredProcedure = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser procedure", e))?;
                Ok(Some(p))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get procedure", e)),
        }
    }

    /// Delete a procedure. Returns true if it existed.
    pub fn delete_procedure(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = procedure_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(PROCEDURES)
                .map_err(|e| catalog_err("open procedures", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove procedure", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all procedures for a tenant.
    pub fn load_procedures_for_tenant(
        &self,
        tenant_id: u32,
    ) -> crate::Result<Vec<StoredProcedure>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(PROCEDURES)
            .map_err(|e| catalog_err("open procedures", e))?;
        let mut procs = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range procedures", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read procedure", e))?;
            if key.value().starts_with(&prefix) {
                let p: StoredProcedure = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser procedure", e))?;
                procs.push(p);
            }
        }
        Ok(procs)
    }
}

fn procedure_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::procedure_types::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample(tenant_id: u32, name: &str) -> StoredProcedure {
        StoredProcedure {
            tenant_id,
            name: name.into(),
            parameters: vec![ProcedureParam {
                name: "cutoff".into(),
                data_type: "INT".into(),
                direction: ParamDirection::In,
            }],
            body_sql: "BEGIN DELETE FROM orders WHERE age > cutoff; END".into(),
            max_iterations: 1_000_000,
            timeout_secs: 60,
            owner: "admin".into(),
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_get() {
        let catalog = make_catalog();
        catalog.put_procedure(&sample(1, "archive")).unwrap();
        let loaded = catalog.get_procedure(1, "archive").unwrap().unwrap();
        assert_eq!(loaded.name, "archive");
        assert_eq!(loaded.parameters.len(), 1);
    }

    #[test]
    fn delete_procedure() {
        let catalog = make_catalog();
        catalog.put_procedure(&sample(1, "p")).unwrap();
        assert!(catalog.delete_procedure(1, "p").unwrap());
        assert!(!catalog.delete_procedure(1, "p").unwrap());
    }

    #[test]
    fn load_for_tenant() {
        let catalog = make_catalog();
        catalog.put_procedure(&sample(1, "a")).unwrap();
        catalog.put_procedure(&sample(1, "b")).unwrap();
        catalog.put_procedure(&sample(2, "c")).unwrap();
        assert_eq!(catalog.load_procedures_for_tenant(1).unwrap().len(), 2);
        assert_eq!(catalog.load_procedures_for_tenant(2).unwrap().len(), 1);
    }
}
