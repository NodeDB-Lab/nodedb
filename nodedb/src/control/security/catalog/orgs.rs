//! Organization catalog operations (redb persistence).

use super::types::{ORG_MEMBERS, ORGS, SystemCatalog, catalog_err};

/// Serializable organization record for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredOrg {
    pub org_id: String,
    pub name: String,
    pub tenant_id: u32,
    /// Organization status: active, suspended, banned.
    #[msgpack(default = "default_active")]
    pub status: String,
    pub created_at: u64,
    #[msgpack(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

/// Serializable org membership record for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredOrgMember {
    pub auth_user_id: String,
    pub org_id: String,
    /// Role within the org (e.g., "owner", "admin", "member").
    pub role: String,
    pub joined_at: u64,
}

fn default_active() -> String {
    "active".into()
}

impl SystemCatalog {
    pub fn put_org(&self, org: &StoredOrg) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(org).map_err(|e| catalog_err("serialize org", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("org write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ORGS)
                .map_err(|e| catalog_err("open orgs", e))?;
            table
                .insert(org.org_id.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert org", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("org commit", e))?;
        Ok(())
    }

    pub fn get_org(&self, org_id: &str) -> crate::Result<Option<StoredOrg>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("org read txn", e))?;
        let table = read_txn
            .open_table(ORGS)
            .map_err(|e| catalog_err("open orgs", e))?;
        match table.get(org_id).map_err(|e| catalog_err("get org", e))? {
            Some(val) => {
                let org: StoredOrg = zerompk::from_msgpack(val.value())
                    .map_err(|e| catalog_err("deserialize org", e))?;
                Ok(Some(org))
            }
            None => Ok(None),
        }
    }

    pub fn delete_org(&self, org_id: &str) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("org write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(ORGS)
                .map_err(|e| catalog_err("open orgs", e))?;
            table
                .remove(org_id)
                .map_err(|e| catalog_err("remove org", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("org commit", e))?;
        Ok(removed)
    }

    pub fn load_all_orgs(&self) -> crate::Result<Vec<StoredOrg>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("org read txn", e))?;
        let table = read_txn
            .open_table(ORGS)
            .map_err(|e| catalog_err("open orgs", e))?;
        let mut orgs = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range orgs", e))?
        {
            let (_, val) = item.map_err(|e| catalog_err("read org", e))?;
            if let Ok(org) = zerompk::from_msgpack::<StoredOrg>(val.value()) {
                orgs.push(org);
            }
        }
        Ok(orgs)
    }

    // ── Org Members ─────────────────────────────────────────────────

    fn member_key(org_id: &str, user_id: &str) -> String {
        format!("{org_id}:{user_id}")
    }

    pub fn put_org_member(&self, member: &StoredOrgMember) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(member).map_err(|e| catalog_err("serialize org member", e))?;
        let key = Self::member_key(&member.org_id, &member.auth_user_id);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("member write txn", e))?;
        {
            let mut table = write_txn
                .open_table(ORG_MEMBERS)
                .map_err(|e| catalog_err("open org_members", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert member", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("member commit", e))?;
        Ok(())
    }

    pub fn delete_org_member(&self, org_id: &str, user_id: &str) -> crate::Result<bool> {
        let key = Self::member_key(org_id, user_id);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("member write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(ORG_MEMBERS)
                .map_err(|e| catalog_err("open org_members", e))?;
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove member", e))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| catalog_err("member commit", e))?;
        Ok(removed)
    }

    pub fn load_members_for_org(&self, org_id: &str) -> crate::Result<Vec<StoredOrgMember>> {
        let prefix = format!("{org_id}:");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("member read txn", e))?;
        let table = read_txn
            .open_table(ORG_MEMBERS)
            .map_err(|e| catalog_err("open org_members", e))?;
        let mut members = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range members", e))?
        {
            let (key, val) = item.map_err(|e| catalog_err("read member", e))?;
            if key.value().starts_with(&prefix)
                && let Ok(m) = zerompk::from_msgpack::<StoredOrgMember>(val.value())
            {
                members.push(m);
            }
        }
        Ok(members)
    }

    pub fn load_orgs_for_user(&self, user_id: &str) -> crate::Result<Vec<StoredOrgMember>> {
        let suffix = format!(":{user_id}");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("member read txn", e))?;
        let table = read_txn
            .open_table(ORG_MEMBERS)
            .map_err(|e| catalog_err("open org_members", e))?;
        let mut memberships = Vec::new();
        for item in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range members", e))?
        {
            let (key, val) = item.map_err(|e| catalog_err("read member", e))?;
            if key.value().ends_with(&suffix)
                && let Ok(m) = zerompk::from_msgpack::<StoredOrgMember>(val.value())
            {
                memberships.push(m);
            }
        }
        Ok(memberships)
    }
}
