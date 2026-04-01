//! Trigger metadata operations for the system catalog.

use super::trigger_types::StoredTrigger;
use super::types::{SystemCatalog, TRIGGERS, catalog_err};

impl SystemCatalog {
    /// Store a trigger definition.
    ///
    /// Key format: `"{tenant_id}:{trigger_name}"`.
    pub fn put_trigger(&self, trigger: &StoredTrigger) -> crate::Result<()> {
        let key = trigger_key(trigger.tenant_id, &trigger.name);
        let bytes = rmp_serde::to_vec(trigger).map_err(|e| catalog_err("serialize trigger", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(TRIGGERS)
                .map_err(|e| catalog_err("open triggers", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert trigger", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a single trigger by tenant_id + name.
    pub fn get_trigger(&self, tenant_id: u32, name: &str) -> crate::Result<Option<StoredTrigger>> {
        let key = trigger_key(tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TRIGGERS)
            .map_err(|e| catalog_err("open triggers", e))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let t: StoredTrigger = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser trigger", e))?;
                Ok(Some(t))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(catalog_err("get trigger", e)),
        }
    }

    /// Delete a trigger by tenant_id + name. Returns true if it existed.
    pub fn delete_trigger(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = trigger_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(TRIGGERS)
                .map_err(|e| catalog_err("open triggers", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove trigger", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all triggers for a tenant.
    pub fn load_triggers_for_tenant(&self, tenant_id: u32) -> crate::Result<Vec<StoredTrigger>> {
        let prefix = format!("{tenant_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TRIGGERS)
            .map_err(|e| catalog_err("open triggers", e))?;
        let mut triggers = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range triggers", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read trigger", e))?;
            if key.value().starts_with(&prefix) {
                let t: StoredTrigger = rmp_serde::from_slice(value.value())
                    .map_err(|e| catalog_err("deser trigger", e))?;
                triggers.push(t);
            }
        }
        Ok(triggers)
    }

    /// Load all triggers across all tenants.
    pub fn load_all_triggers(&self) -> crate::Result<Vec<StoredTrigger>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TRIGGERS)
            .map_err(|e| catalog_err("open triggers", e))?;
        let mut triggers = Vec::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range triggers", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read trigger", e))?;
            let t: StoredTrigger = rmp_serde::from_slice(value.value())
                .map_err(|e| catalog_err("deser trigger", e))?;
            triggers.push(t);
        }
        Ok(triggers)
    }
}

fn trigger_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::trigger_types::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    fn sample_trigger(tenant_id: u32, name: &str, collection: &str) -> StoredTrigger {
        StoredTrigger {
            tenant_id,
            name: name.to_string(),
            collection: collection.to_string(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN INSERT INTO audit (id) VALUES (NEW.id); END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::default(),
            security: TriggerSecurity::default(),
            batch_mode: TriggerBatchMode::default(),
            owner: "admin".into(),
            created_at: 1000,
        }
    }

    #[test]
    fn put_and_get() {
        let catalog = make_catalog();
        let t = sample_trigger(1, "audit_insert", "orders");
        catalog.put_trigger(&t).unwrap();
        let loaded = catalog.get_trigger(1, "audit_insert").unwrap().unwrap();
        assert_eq!(loaded.name, "audit_insert");
        assert_eq!(loaded.collection, "orders");
    }

    #[test]
    fn delete_trigger() {
        let catalog = make_catalog();
        catalog.put_trigger(&sample_trigger(1, "t", "c")).unwrap();
        assert!(catalog.delete_trigger(1, "t").unwrap());
        assert!(!catalog.delete_trigger(1, "t").unwrap());
    }

    #[test]
    fn load_for_tenant() {
        let catalog = make_catalog();
        catalog.put_trigger(&sample_trigger(1, "a", "c1")).unwrap();
        catalog.put_trigger(&sample_trigger(1, "b", "c2")).unwrap();
        catalog.put_trigger(&sample_trigger(2, "c", "c3")).unwrap();
        assert_eq!(catalog.load_triggers_for_tenant(1).unwrap().len(), 2);
        assert_eq!(catalog.load_triggers_for_tenant(2).unwrap().len(), 1);
    }
}
