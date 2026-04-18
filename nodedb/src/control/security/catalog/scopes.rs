//! Scope catalog operations (redb persistence).

use super::types::{SCOPE_GRANTS, SCOPES, SystemCatalog, catalog_err};

/// Serializable scope definition for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredScope {
    /// Scope name (e.g., "profile:read", "orders:write").
    pub name: String,
    /// What this scope grants: `[("read", "user_profiles"), ("read", "user_settings")]`.
    pub grants: Vec<(String, String)>,
    /// Included sub-scopes (composition): `["profile:read", "settings:read"]`.
    #[msgpack(default)]
    pub includes: Vec<String>,
    pub created_by: String,
    pub created_at: u64,
}

/// Serializable scope grant for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredScopeGrant {
    pub scope_name: String,
    /// Grantee type: "user", "role", "org", "team".
    pub grantee_type: String,
    /// Grantee identifier (user_id, role_name, org_id, team_id).
    pub grantee_id: String,
    pub granted_by: String,
    pub granted_at: u64,
    /// Unix timestamp when this grant expires. 0 = no expiry.
    #[msgpack(default)]
    pub expires_at: u64,
    /// Grace period in seconds before hard cutoff after expiry.
    #[msgpack(default)]
    pub grace_period_secs: u64,
    /// Action on expiry: "revoke_all", "grant:<scope_name>", or empty (just expire).
    #[msgpack(default)]
    pub on_expire_action: String,
}

impl SystemCatalog {
    pub fn put_scope(&self, scope: &StoredScope) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(scope).map_err(|e| catalog_err("serialize scope", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("scope write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SCOPES)
                .map_err(|e| catalog_err("open scopes", e))?;
            table
                .insert(scope.name.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert scope", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("scope commit", e))?;
        Ok(())
    }

    pub fn get_scope(&self, name: &str) -> crate::Result<Option<StoredScope>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("scope read txn", e))?;
        let table = read_txn
            .open_table(SCOPES)
            .map_err(|e| catalog_err("open scopes", e))?;
        match table.get(name).map_err(|e| catalog_err("get scope", e))? {
            Some(val) => {
                let scope: StoredScope = zerompk::from_msgpack(val.value())
                    .map_err(|e| catalog_err("deserialize scope", e))?;
                Ok(Some(scope))
            }
            None => Ok(None),
        }
    }

    pub fn delete_scope(&self, name: &str) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("scope write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(SCOPES)
                .map_err(|e| catalog_err("open scopes", e))?;
            table
                .remove(name)
                .map_err(|e| catalog_err("remove scope", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("scope commit", e))?;
        Ok(removed)
    }

    pub fn load_all_scopes(&self) -> crate::Result<Vec<StoredScope>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("scope read txn", e))?;
        let table = read_txn
            .open_table(SCOPES)
            .map_err(|e| catalog_err("open scopes", e))?;
        let mut scopes = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range scopes", e))?
        {
            let (_, val) = item.map_err(|e| catalog_err("read scope", e))?;
            if let Ok(s) = zerompk::from_msgpack::<StoredScope>(val.value()) {
                scopes.push(s);
            }
        }
        Ok(scopes)
    }

    // ── Scope Grants ────────────────────────────────────────────────

    fn scope_grant_key(scope_name: &str, grantee_type: &str, grantee_id: &str) -> String {
        format!("{scope_name}:{grantee_type}:{grantee_id}")
    }

    pub fn put_scope_grant(&self, grant: &StoredScopeGrant) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(grant).map_err(|e| catalog_err("serialize scope grant", e))?;
        let key = Self::scope_grant_key(&grant.scope_name, &grant.grantee_type, &grant.grantee_id);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("grant write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SCOPE_GRANTS)
                .map_err(|e| catalog_err("open scope_grants", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert scope grant", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("grant commit", e))?;
        Ok(())
    }

    pub fn delete_scope_grant(
        &self,
        scope_name: &str,
        grantee_type: &str,
        grantee_id: &str,
    ) -> crate::Result<bool> {
        let key = Self::scope_grant_key(scope_name, grantee_type, grantee_id);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("grant write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(SCOPE_GRANTS)
                .map_err(|e| catalog_err("open scope_grants", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove scope grant", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("grant commit", e))?;
        Ok(removed)
    }

    pub fn load_all_scope_grants(&self) -> crate::Result<Vec<StoredScopeGrant>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("grant read txn", e))?;
        let table = read_txn
            .open_table(SCOPE_GRANTS)
            .map_err(|e| catalog_err("open scope_grants", e))?;
        let mut grants = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range grants", e))?
        {
            let (_, val) = item.map_err(|e| catalog_err("read grant", e))?;
            if let Ok(g) = zerompk::from_msgpack::<StoredScopeGrant>(val.value()) {
                grants.push(g);
            }
        }
        Ok(grants)
    }

    /// Load scope grants for a specific grantee (user, org, etc.).
    pub fn load_scope_grants_for(
        &self,
        grantee_type: &str,
        grantee_id: &str,
    ) -> crate::Result<Vec<StoredScopeGrant>> {
        let all = self.load_all_scope_grants()?;
        Ok(all
            .into_iter()
            .filter(|g| g.grantee_type == grantee_type && g.grantee_id == grantee_id)
            .collect())
    }
}
