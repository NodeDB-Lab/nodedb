// SPDX-License-Identifier: BUSL-1.1

//! User-defined function metadata operations for the system catalog.

use std::collections::HashMap;

use nodedb_types::id::DatabaseId;
use redb::{ReadableDatabase, ReadableTable};

use super::dependencies::{DependencyList, dep_key};
use super::function_types::StoredFunction;
use super::types::{DEPENDENCIES, FUNCTIONS, SystemCatalog, WASM_MODULES, catalog_err};

impl SystemCatalog {
    /// Store a function under its tenant/database key.
    pub fn put_function(&self, func: &StoredFunction) -> crate::Result<()> {
        let key = function_key(func.tenant_id, func.database_id, &func.name);
        // Module bytes are transport payload, not function metadata. Keeping
        // them out of this row avoids duplicating content-addressed blobs.
        let mut persisted = func.clone();
        persisted.wasm_module = None;
        let bytes = zerompk::to_msgpack_vec(&persisted)
            .map_err(|e| catalog_err("serialize function", e))?;
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
        replace_function_dependencies(&write_txn, func)?;
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Atomically persist a function row, replace its dependency row, install
    /// its optional WASM module, and remove the replaced module only when no
    /// other function references it.
    ///
    /// Callers must validate the module before this method is entered. Module
    /// payloads are intentionally omitted from the persisted function row.
    pub fn put_function_with_wasm_module(
        &self,
        func: &StoredFunction,
        module: Option<&[u8]>,
    ) -> crate::Result<()> {
        let key = function_key(func.tenant_id, func.database_id, &func.name);
        let mut persisted = func.clone();
        persisted.wasm_module = None;
        let bytes = zerompk::to_msgpack_vec(&persisted)
            .map_err(|e| catalog_err("serialize function", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("function WASM write txn", e))?;

        let old_hash = {
            let mut functions = write_txn
                .open_table(FUNCTIONS)
                .map_err(|e| catalog_err("open functions", e))?;
            let previous: Option<StoredFunction> = functions
                .get(key.as_str())
                .map_err(|e| catalog_err("get function", e))?
                .map(|value| zerompk::from_msgpack(value.value()))
                .transpose()
                .map_err(|e| catalog_err("deser function", e))?;
            functions
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert function", e))?;

            let old_hash = previous.and_then(|function| function.wasm_hash);
            if let Some(old_hash) = old_hash.as_deref()
                && func.wasm_hash.as_deref() != Some(old_hash)
            {
                let mut still_referenced = false;
                for entry in functions
                    .range(..)
                    .map_err(|e| catalog_err("scan functions", e))?
                {
                    let (_, value) = entry.map_err(|e| catalog_err("read function", e))?;
                    let function: StoredFunction = zerompk::from_msgpack(value.value())
                        .map_err(|e| catalog_err("deser function", e))?;
                    if function.wasm_hash.as_deref() == Some(old_hash) {
                        still_referenced = true;
                        break;
                    }
                }
                (!still_referenced).then(|| old_hash.to_string())
            } else {
                None
            }
        };

        replace_function_dependencies(&write_txn, func)?;

        {
            let mut modules = write_txn
                .open_table(WASM_MODULES)
                .map_err(|e| catalog_err("open WASM modules", e))?;
            if let Some(module) = module {
                let hash = func
                    .wasm_hash
                    .as_deref()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "WASM module is missing its content hash".into(),
                    })?;
                let module_key = format!("wasm_module:{hash}");
                modules
                    .insert(module_key.as_str(), module)
                    .map_err(|e| catalog_err("install WASM module", e))?;
            }
            if let Some(old_hash) = old_hash {
                let module_key = format!("wasm_module:{old_hash}");
                modules
                    .remove(module_key.as_str())
                    .map_err(|e| catalog_err("remove replaced WASM module", e))?;
            }
        }

        #[cfg(test)]
        if self
            .faults
            .fail_next_function_wasm_write
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::Error::Storage {
                engine: "catalog".into(),
                detail: "injected function/WASM transaction failure".into(),
            });
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit function WASM write", e))
    }

    /// Get a function scoped to the default database.
    pub fn get_function(
        &self,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredFunction>> {
        self.get_function_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }

    /// Get a function in an exact database scope, with the calling
    /// connection's buffered transactional DDL merged in — a `CREATE
    /// FUNCTION` this same transaction has buffered but not yet committed
    /// resolves here too.
    pub fn get_function_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredFunction>> {
        let committed = self.get_committed_function_in_database(database_id, tenant_id, name)?;
        Ok(crate::control::catalog_overlay::resolve_function(
            database_id,
            tenant_id,
            name,
            committed,
        ))
    }

    /// Committed-only read, bypassing the transaction DDL overlay. The
    /// descriptor stamper reads through this — see
    /// `get_committed_collection` for why.
    pub fn get_committed_function_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredFunction>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(FUNCTIONS)
            .map_err(|e| catalog_err("open functions", e))?;
        let key = function_key(tenant_id, database_id, name);
        table
            .get(key.as_str())
            .map_err(|e| catalog_err("get function", e))?
            .map(|value| zerompk::from_msgpack(value.value()))
            .transpose()
            .map_err(|e| catalog_err("deser function", e))
    }

    /// Delete a function scoped to the default database.
    pub fn delete_function(&self, tenant_id: u64, name: &str) -> crate::Result<bool> {
        self.delete_function_in_database(DatabaseId::DEFAULT, tenant_id, name)
    }

    /// Delete only the selected database's function.
    pub fn delete_function_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        self.delete_function_with_unreferenced_wasm(database_id, tenant_id, name)
    }

    /// Atomically delete a function row and dependency row, and remove the
    /// removed WASM blob only when no remaining function references its hash.
    pub fn delete_function_with_unreferenced_wasm(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("function/WASM delete txn", e))?;
        let (existed, removed_hash) = {
            let mut functions = write_txn
                .open_table(FUNCTIONS)
                .map_err(|e| catalog_err("open functions", e))?;
            let key = function_key(tenant_id, database_id, name);
            let removed = functions
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove function", e))?;
            let existed = removed.is_some();
            let removed_function: Option<StoredFunction> = removed
                .as_ref()
                .map(|value| zerompk::from_msgpack(value.value()))
                .transpose()
                .map_err(|e| catalog_err("deserialize removed function", e))?;
            drop(removed);

            let removed_hash = removed_function.and_then(|function| function.wasm_hash);
            let unreferenced_hash = match removed_hash {
                Some(hash) => {
                    let mut still_referenced = false;
                    for row in functions
                        .range(..)
                        .map_err(|e| catalog_err("scan functions", e))?
                    {
                        let (_, value) = row.map_err(|e| catalog_err("read function", e))?;
                        let function: StoredFunction = zerompk::from_msgpack(value.value())
                            .map_err(|e| catalog_err("deserialize function", e))?;
                        if function.wasm_hash.as_deref() == Some(hash.as_str()) {
                            still_referenced = true;
                            break;
                        }
                    }
                    (!still_referenced).then_some(hash)
                }
                None => None,
            };
            (existed, unreferenced_hash)
        };
        {
            let mut dependencies = write_txn
                .open_table(DEPENDENCIES)
                .map_err(|e| catalog_err("open dependencies", e))?;
            dependencies
                .remove(dep_key(database_id, "function", tenant_id, name).as_str())
                .map_err(|e| catalog_err("remove function dependencies", e))?;
        }
        if let Some(hash) = removed_hash {
            let mut modules = write_txn
                .open_table(WASM_MODULES)
                .map_err(|e| catalog_err("open WASM modules", e))?;
            let module_key = format!("wasm_module:{hash}");
            modules
                .remove(module_key.as_str())
                .map_err(|e| catalog_err("remove unreferenced WASM module", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit function/WASM delete", e))?;
        Ok(existed)
    }

    pub fn load_all_functions(&self) -> crate::Result<Vec<StoredFunction>> {
        self.load_functions_matching(|_| true)
    }

    pub fn load_functions_for_tenant(&self, tenant_id: u64) -> crate::Result<Vec<StoredFunction>> {
        self.load_functions_matching(|f| f.tenant_id == tenant_id)
    }

    pub fn load_functions_in_database(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredFunction>> {
        self.load_functions_matching(|f| f.database_id == database_id && f.tenant_id == tenant_id)
    }

    fn load_functions_matching(
        &self,
        include: impl Fn(&StoredFunction) -> bool,
    ) -> crate::Result<Vec<StoredFunction>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(FUNCTIONS)
            .map_err(|e| catalog_err("open functions", e))?;
        let mut dedup = HashMap::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range functions", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read function", e))?;
            let function: StoredFunction = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deser function", e))?;
            if include(&function) {
                let scope = (
                    function.tenant_id,
                    function.database_id,
                    function.name.clone(),
                );
                dedup.insert(scope, function);
            }
        }
        Ok(dedup.into_values().collect())
    }
}

/// Replace the single dependency row for a replicated function as part of
/// its function catalog transaction. An empty list is represented by no row.
fn replace_function_dependencies(
    write_txn: &redb::WriteTransaction,
    func: &StoredFunction,
) -> crate::Result<()> {
    let key = dep_key(func.database_id, "function", func.tenant_id, &func.name);
    let mut dependencies = write_txn
        .open_table(DEPENDENCIES)
        .map_err(|e| catalog_err("open dependencies", e))?;
    if func.dependencies.is_empty() {
        dependencies
            .remove(key.as_str())
            .map_err(|e| catalog_err("remove function dependencies", e))?;
    } else {
        let bytes = zerompk::to_msgpack_vec(&DependencyList {
            deps: func.dependencies.clone(),
        })
        .map_err(|e| catalog_err("serialize function dependencies", e))?;
        dependencies
            .insert(key.as_str(), bytes.as_slice())
            .map_err(|e| catalog_err("insert function dependencies", e))?;
    }
    Ok(())
}

fn function_key(tenant_id: u64, database_id: DatabaseId, name: &str) -> String {
    format!("v2:{tenant_id}:{}:{name}", database_id.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::dependencies::Dependency;
    use crate::control::security::catalog::function_types::{
        FunctionLanguage, FunctionParam, FunctionSecurity, FunctionVolatility,
    };

    fn catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn function(database_id: DatabaseId, body_sql: &str) -> StoredFunction {
        StoredFunction {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            parameters: vec![FunctionParam {
                name: "x".into(),
                data_type: "INT".into(),
            }],
            return_type: "INT".into(),
            body_sql: body_sql.into(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Immutable,
            security: FunctionSecurity::Invoker,
            language: FunctionLanguage::Sql,
            wasm_hash: None,
            wasm_module: None,
            dependencies: vec![],
            wasm_fuel: 1_000_000,
            wasm_memory: 16 * 1024 * 1024,
            owner: "admin".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: Default::default(),
        }
    }

    #[test]
    fn functions_are_isolated_by_database() {
        let catalog = catalog();
        let db1 = DatabaseId::new(1);
        let db2 = DatabaseId::new(2);
        catalog.put_function(&function(db1, "SELECT 1")).unwrap();
        catalog.put_function(&function(db2, "SELECT 2")).unwrap();

        assert_eq!(
            catalog
                .get_function_in_database(db1, 1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "SELECT 1"
        );
        assert_eq!(
            catalog
                .get_function_in_database(db2, 1, "same_name")
                .unwrap()
                .unwrap()
                .body_sql,
            "SELECT 2"
        );
        assert_eq!(catalog.load_functions_in_database(db1, 1).unwrap().len(), 1);
        assert_eq!(catalog.load_functions_in_database(db2, 1).unwrap().len(), 1);
        assert!(
            catalog
                .delete_function_in_database(db1, 1, "same_name")
                .unwrap()
        );
        assert!(
            catalog
                .get_function_in_database(db1, 1, "same_name")
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_function_in_database(db2, 1, "same_name")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn function_dependencies_are_replaced_and_deleted_atomically_with_metadata() {
        let catalog = catalog();
        let mut stored = function(DatabaseId::DEFAULT, "SELECT 1");
        stored.dependencies = vec![Dependency {
            target_type: "function".into(),
            target_name: "first_target".into(),
        }];
        catalog.put_function(&stored).unwrap();
        assert_eq!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "function", "first_target")
                .unwrap(),
            vec![("function".into(), "same_name".into())]
        );

        stored.dependencies = vec![];
        catalog.put_function(&stored).unwrap();
        assert!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "function", "first_target")
                .unwrap()
                .is_empty()
        );

        stored.dependencies = vec![Dependency {
            target_type: "function".into(),
            target_name: "second_target".into(),
        }];
        catalog.put_function(&stored).unwrap();
        catalog
            .delete_function_with_unreferenced_wasm(DatabaseId::DEFAULT, 1, "same_name")
            .unwrap();
        assert!(
            catalog
                .find_dependents(DatabaseId::DEFAULT, 1, "function", "second_target")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn delete_removes_the_unreferenced_wasm_module() {
        use crate::control::planner::wasm::store::{load_wasm_binary, store_wasm_binary};

        let catalog = catalog();
        let hash = store_wasm_binary(
            &catalog,
            b"\0asmv2",
            crate::control::planner::wasm::WasmConfig::default().max_binary_size,
        )
        .unwrap();
        let mut stored = function(DatabaseId::DEFAULT, "");
        stored.language = FunctionLanguage::Wasm;
        stored.wasm_hash = Some(hash.clone());
        catalog.put_function(&stored).unwrap();

        catalog
            .delete_function_with_unreferenced_wasm(DatabaseId::DEFAULT, 1, "same_name")
            .unwrap();
        assert!(load_wasm_binary(&catalog, &hash).is_err());
    }
}
