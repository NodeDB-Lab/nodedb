//! Lite catalog for function, trigger, and procedure definitions.
//!
//! Stores definitions in redb `Namespace::Meta` using typed keys:
//! - `function:{name}` → serialized StoredFunction
//! - `trigger:{name}` → serialized StoredTrigger
//! - `procedure:{name}` → serialized StoredProcedure
//!
//! Definitions are synced from Origin via CRDT (definitions only — execution
//! is local). Triggers fire on Lite-originated writes only, not on CRDT sync
//! merges (origin-only rule).

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::NodeDbLite;
use crate::storage::engine::StorageEngine;

/// A stored function definition (mirrors Origin's StoredFunction).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiteStoredFunction {
    pub name: String,
    pub parameters: Vec<LiteFunctionParam>,
    pub return_type: String,
    pub body_sql: String,
    pub owner: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiteFunctionParam {
    pub name: String,
    pub data_type: String,
}

/// A stored trigger definition (mirrors Origin's StoredTrigger).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiteStoredTrigger {
    pub name: String,
    pub collection: String,
    pub timing: String,
    pub events: Vec<String>,
    pub granularity: String,
    pub when_condition: Option<String>,
    pub body_sql: String,
    pub priority: i32,
    pub enabled: bool,
    pub execution_mode: String,
    pub owner: String,
    pub created_at: u64,
}

/// A stored procedure definition (mirrors Origin's StoredProcedure).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiteStoredProcedure {
    pub name: String,
    pub parameters: Vec<LiteProcedureParam>,
    pub body_sql: String,
    pub max_iterations: u64,
    pub timeout_secs: u64,
    pub owner: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiteProcedureParam {
    pub name: String,
    pub data_type: String,
    pub direction: String,
}

// ── Generic CRUD helpers ────────────────────────────────────────────────

impl<S: StorageEngine> NodeDbLite<S> {
    /// Store a definition in the local catalog under `{prefix}:{name}`.
    async fn put_definition(
        &self,
        prefix: &str,
        name: &str,
        def: &impl serde::Serialize,
    ) -> NodeDbResult<()> {
        let key = format!("{prefix}:{name}");
        let bytes = sonic_rs::to_vec(def).map_err(|e| NodeDbError::storage(e.to_string()))?;
        self.storage
            .put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
            .await?;
        Ok(())
    }

    /// Get a definition by key prefix and name.
    async fn get_definition<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
        name: &str,
    ) -> NodeDbResult<Option<T>> {
        let key = format!("{prefix}:{name}");
        let bytes = self
            .storage
            .get(nodedb_types::Namespace::Meta, key.as_bytes())
            .await?;
        match bytes {
            Some(b) => {
                let def: T =
                    sonic_rs::from_slice(&b).map_err(|e| NodeDbError::storage(e.to_string()))?;
                Ok(Some(def))
            }
            None => Ok(None),
        }
    }

    /// List all definitions with a given key prefix.
    async fn list_definitions<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> NodeDbResult<Vec<T>> {
        let scan_prefix = format!("{prefix}:");
        let pairs = self
            .storage
            .scan_prefix(nodedb_types::Namespace::Meta, scan_prefix.as_bytes())
            .await?;
        let mut defs = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            if let Ok(d) = sonic_rs::from_slice::<T>(&value) {
                defs.push(d);
            }
        }
        Ok(defs)
    }

    /// Delete a definition by key prefix and name.
    async fn delete_definition(&self, prefix: &str, name: &str) -> NodeDbResult<()> {
        let key = format!("{prefix}:{name}");
        self.storage
            .delete(nodedb_types::Namespace::Meta, key.as_bytes())
            .await?;
        Ok(())
    }
}

// ── Type-specific convenience methods ───────────────────────────────────

impl<S: StorageEngine> NodeDbLite<S> {
    /// Store a function definition in the local catalog.
    pub async fn put_function(&self, func: &LiteStoredFunction) -> NodeDbResult<()> {
        self.put_definition("function", &func.name, func).await
    }

    /// Get a function definition by name.
    pub async fn get_function(&self, name: &str) -> NodeDbResult<Option<LiteStoredFunction>> {
        self.get_definition("function", name).await
    }

    /// List all function definitions.
    pub async fn list_functions(&self) -> NodeDbResult<Vec<LiteStoredFunction>> {
        self.list_definitions("function").await
    }

    /// Delete a function definition.
    pub async fn delete_function(&self, name: &str) -> NodeDbResult<()> {
        self.delete_definition("function", name).await
    }

    /// Store a trigger definition in the local catalog.
    pub async fn put_trigger(&self, trigger: &LiteStoredTrigger) -> NodeDbResult<()> {
        self.put_definition("trigger", &trigger.name, trigger).await
    }

    /// Get a trigger definition by name.
    pub async fn get_trigger(&self, name: &str) -> NodeDbResult<Option<LiteStoredTrigger>> {
        self.get_definition("trigger", name).await
    }

    /// List all trigger definitions.
    pub async fn list_triggers(&self) -> NodeDbResult<Vec<LiteStoredTrigger>> {
        self.list_definitions("trigger").await
    }

    /// List triggers for a specific collection.
    pub async fn list_triggers_for_collection(
        &self,
        collection: &str,
    ) -> NodeDbResult<Vec<LiteStoredTrigger>> {
        let all = self.list_triggers().await?;
        Ok(all
            .into_iter()
            .filter(|t| t.collection == collection)
            .collect())
    }

    /// Delete a trigger definition.
    pub async fn delete_trigger(&self, name: &str) -> NodeDbResult<()> {
        self.delete_definition("trigger", name).await
    }

    /// Store a procedure definition in the local catalog.
    pub async fn put_procedure(&self, proc: &LiteStoredProcedure) -> NodeDbResult<()> {
        self.put_definition("procedure", &proc.name, proc).await
    }

    /// Get a procedure definition by name.
    pub async fn get_procedure(&self, name: &str) -> NodeDbResult<Option<LiteStoredProcedure>> {
        self.get_definition("procedure", name).await
    }

    /// List all procedure definitions.
    pub async fn list_procedures(&self) -> NodeDbResult<Vec<LiteStoredProcedure>> {
        self.list_definitions("procedure").await
    }

    /// Delete a procedure definition.
    pub async fn delete_procedure(&self, name: &str) -> NodeDbResult<()> {
        self.delete_definition("procedure", name).await
    }
}
