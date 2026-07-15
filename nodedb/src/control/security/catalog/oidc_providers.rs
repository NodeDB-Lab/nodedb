// SPDX-License-Identifier: BUSL-1.1

//! OIDC provider catalog ops for `_system.oidc_providers`.
//!
//! Keyed by `provider_name` (string). Stores issuer, JWKS URI, audience, and
//! claim-mapping rules used by the OIDC verify path.

use redb::ReadableTable;

use super::tables::OIDC_PROVIDERS;
use super::types::{SystemCatalog, catalog_err};

/// A claim-mapping rule that maps a JWT claim value to NodeDB databases and roles.
#[derive(
    Debug,
    Clone,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    serde::Serialize,
    serde::Deserialize,
)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredClaimMappingRule {
    /// Name of the JWT claim to inspect (e.g. `"org_id"`, `"groups"`).
    pub claim_name: String,
    /// Expected claim value. Use `"*"` to match any non-empty value.
    pub claim_value: String,
    /// Default database ID for sessions matched by this rule. `None` = no override.
    #[msgpack(default)]
    pub default_database: Option<u64>,
    /// Additional database IDs accessible to sessions matched by this rule.
    #[msgpack(default)]
    pub add_databases: Vec<u64>,
    /// Additional role names granted to sessions matched by this rule.
    #[msgpack(default)]
    pub add_roles: Vec<String>,
}

/// Persisted OIDC provider record in `_system.oidc_providers`.
#[derive(
    Debug,
    Clone,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    serde::Serialize,
    serde::Deserialize,
)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredOidcProvider {
    /// Human-readable name; also the catalog key.
    pub provider_name: String,
    /// Expected `iss` claim value. Must be non-empty.
    pub issuer: String,
    /// JWKS endpoint URI. Fetched by `JwksRegistry`.
    pub jwks_uri: String,
    /// Expected `aud` claim value. `None` = skip audience validation.
    #[msgpack(default)]
    pub audience: Option<String>,
    /// Tenant bound to this provider. `None` preserves compatibility with
    /// records written before tenant binding was introduced.
    #[msgpack(default)]
    #[serde(default)]
    pub tenant_id: Option<u64>,
    /// Ordered claim-mapping rules evaluated at login to derive databases and roles.
    #[msgpack(default)]
    pub claim_mapping: Vec<StoredClaimMappingRule>,
    /// WAL LSN at which this provider was created or last altered.
    pub created_at_lsn: u64,
}

impl SystemCatalog {
    // ── oidc_providers ────────────────────────────────────────────────────

    /// Insert or overwrite an OIDC provider record.
    pub fn put_oidc_provider(&self, provider: &StoredOidcProvider) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(provider)
            .map_err(|e| catalog_err("serialize StoredOidcProvider", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("oidc_providers write txn", e))?;
        {
            let mut table = txn
                .open_table(OIDC_PROVIDERS)
                .map_err(|e| catalog_err("open oidc_providers", e))?;
            table
                .insert(provider.provider_name.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert oidc_providers", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("oidc_providers commit", e))
    }

    /// Retrieve an OIDC provider record by name.
    pub fn get_oidc_provider(&self, name: &str) -> crate::Result<Option<StoredOidcProvider>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("oidc_providers read txn", e))?;
        let table = txn
            .open_table(OIDC_PROVIDERS)
            .map_err(|e| catalog_err("open oidc_providers", e))?;
        match table
            .get(name)
            .map_err(|e| catalog_err("get oidc_providers", e))?
        {
            Some(v) => {
                let p: StoredOidcProvider = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deser StoredOidcProvider", e))?;
                Ok(Some(p))
            }
            None => Ok(None),
        }
    }

    /// List all OIDC providers.
    pub fn list_oidc_providers(&self) -> crate::Result<Vec<StoredOidcProvider>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("oidc_providers read txn", e))?;
        let table = txn
            .open_table(OIDC_PROVIDERS)
            .map_err(|e| catalog_err("open oidc_providers", e))?;
        let mut out = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iter oidc_providers", e))?
        {
            let (_, v) = entry.map_err(|e| catalog_err("iter item oidc_providers", e))?;
            let p: StoredOidcProvider = zerompk::from_msgpack(v.value())
                .map_err(|e| catalog_err("deser StoredOidcProvider", e))?;
            out.push(p);
        }
        Ok(out)
    }

    /// Delete an OIDC provider record by name.
    pub fn delete_oidc_provider(&self, name: &str) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("oidc_providers write txn", e))?;
        {
            let mut table = txn
                .open_table(OIDC_PROVIDERS)
                .map_err(|e| catalog_err("open oidc_providers", e))?;
            table
                .remove(name)
                .map_err(|e| catalog_err("remove oidc_providers", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("oidc_providers commit", e))
    }
}
