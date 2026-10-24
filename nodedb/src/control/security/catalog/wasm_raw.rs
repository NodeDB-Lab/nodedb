// SPDX-License-Identifier: BUSL-1.1

//! Raw byte storage in the `WASM_MODULES` table, keyed by string.

use redb::{ReadableDatabase, ReadableTable};

use super::types::{SystemCatalog, WASM_MODULES, catalog_err};

impl SystemCatalog {
    /// Execute a write transaction on the WASM_MODULES table.
    fn wasm_write<F, T>(&self, op: &str, f: F) -> crate::Result<T>
    where
        F: FnOnce(&mut redb::Table<&str, &[u8]>) -> crate::Result<T>,
    {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err(&format!("{op} txn"), e))?;
        let result = {
            let mut table = txn
                .open_table(WASM_MODULES)
                .map_err(|e| catalog_err(&format!("{op} open"), e))?;
            f(&mut table)?
        };
        txn.commit()
            .map_err(|e| catalog_err(&format!("{op} commit"), e))?;
        Ok(result)
    }

    /// Store raw bytes under a string key in the WASM_MODULES table.
    pub fn put_raw(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("put_raw key", e))?;
        self.wasm_write("put_raw", |table| {
            table
                .insert(key_str, value)
                .map_err(|e| catalog_err("put_raw insert", e))?;
            Ok(())
        })
    }

    /// Load raw bytes by string key from the WASM_MODULES table.
    pub fn get_raw(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("get_raw key", e))?;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("get_raw txn", e))?;
        let table = txn
            .open_table(WASM_MODULES)
            .map_err(|e| catalog_err("get_raw open", e))?;
        match table
            .get(key_str)
            .map_err(|e| catalog_err("get_raw get", e))?
        {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Delete raw bytes by string key from the WASM_MODULES table.
    pub fn delete_raw(&self, key: &[u8]) -> crate::Result<()> {
        let key_str = std::str::from_utf8(key).map_err(|e| catalog_err("delete_raw key", e))?;
        self.wasm_write("delete_raw", |table| {
            table
                .remove(key_str)
                .map_err(|e| catalog_err("delete_raw remove", e))?;
            Ok(())
        })
    }
}
