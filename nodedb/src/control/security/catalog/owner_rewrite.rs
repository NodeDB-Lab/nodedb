// SPDX-License-Identifier: BUSL-1.1

//! Catalog-local owner migration shared by DDL fallback and startup repair.

use super::auth_types::{StoredOwner, object_type};
use super::{SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Rewrite both the primary record's in-band owner and its `StoredOwner`
    /// row. Unknown kinds and missing primaries fail closed. Index ownership is
    /// represented solely by `StoredOwner` and therefore has no primary row.
    pub fn rewrite_object_owner(
        &self,
        kind: &str,
        tenant_id: u64,
        name: &str,
        new_owner: &str,
    ) -> crate::Result<()> {
        match kind {
            object_type::COLLECTION => {
                let mut matched = false;
                for mut stored in self
                    .load_all_collections_across_databases()?
                    .into_iter()
                    .filter(|stored| stored.tenant_id == tenant_id && stored.name == name)
                {
                    matched = true;
                    stored.owner = new_owner.to_string();
                    self.put_collection(stored.database_id, &stored)?;
                }
                if !matched {
                    return Err(missing(kind, tenant_id, name));
                }
            }
            object_type::FUNCTION => {
                let mut stored = self
                    .get_function(tenant_id, name)?
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_function(&stored)?;
            }
            object_type::PROCEDURE => {
                let mut stored = self
                    .get_procedure(tenant_id, name)?
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_procedure(&stored)?;
            }
            object_type::TRIGGER => {
                let mut stored = self
                    .get_trigger(tenant_id, name)?
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_trigger(&stored)?;
            }
            object_type::MATERIALIZED_VIEW => {
                let mut stored = self
                    .get_materialized_view(tenant_id, name)?
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_materialized_view(&stored)?;
            }
            object_type::SEQUENCE => {
                let mut stored = self
                    .get_sequence(tenant_id, name)?
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_sequence(&stored)?;
            }
            object_type::SCHEDULE => {
                let mut stored = self
                    .load_all_schedules()?
                    .into_iter()
                    .find(|stored| stored.tenant_id == tenant_id && stored.name == name)
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_schedule(&stored)?;
            }
            object_type::CHANGE_STREAM => {
                let mut stored = self
                    .get_change_stream(tenant_id, name)?
                    .ok_or_else(|| missing(kind, tenant_id, name))?;
                stored.owner = new_owner.to_string();
                self.put_change_stream(&stored)?;
            }
            object_type::CONTINUOUS_AGGREGATE => {
                let mut matched = false;
                for mut stored in self
                    .load_all_continuous_aggregates()?
                    .into_iter()
                    .filter(|stored| stored.tenant_id == tenant_id && stored.name == name)
                {
                    matched = true;
                    stored.owner = new_owner.to_string();
                    self.put_continuous_aggregate(&stored)?;
                }
                if !matched {
                    return Err(missing(kind, tenant_id, name));
                }
            }
            object_type::INDEX => {}
            unknown => {
                return Err(catalog_err(
                    "rewrite object owner",
                    format!("unknown owner kind '{unknown}'"),
                ));
            }
        }

        self.put_owner(&StoredOwner {
            object_type: kind.to_string(),
            object_name: name.to_string(),
            tenant_id,
            owner_username: new_owner.to_string(),
        })
    }
}

fn missing(kind: &str, tenant_id: u64, name: &str) -> crate::Error {
    catalog_err(
        "rewrite object owner",
        format!("{kind} '{tenant_id}:{name}' has no primary record"),
    )
}
