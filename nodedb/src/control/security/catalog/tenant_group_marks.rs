// SPDX-License-Identifier: BUSL-1.1

//! Persistent per-group tenant write marks backing
//! `_system.tenant_group_marks`.
//!
//! For each data group this node replicates, the newest commit HLC of any
//! write of each tenant the group applied. The apply loop writes a group's
//! marks before it saves the applied floor that covers them, so every
//! committed entry is either covered by a persisted mark or above the floor,
//! where Raft delivers it again after a restart and the loop derives its mark
//! again. A Calvin commit writes its mark before its install is acknowledged.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

/// Table: `(group_id, tenant_id)` -> `(commit_hlc, site_code, collection)`.
pub(super) const TENANT_GROUP_MARKS: TableDefinition<(u64, u64), (u64, u8, &str)> =
    TableDefinition::new("_system.tenant_group_marks");

/// One persisted mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredGroupMark {
    pub group_id: u64,
    pub tenant_id: u64,
    /// HLC wall time, in nanoseconds, of the newest write.
    pub hlc: u64,
    /// Which apply path recorded the write.
    pub site: u8,
    /// The collection the write named, empty when it named none.
    pub collection: String,
}

impl SystemCatalog {
    /// Every persisted mark.
    pub fn load_tenant_group_marks(&self) -> crate::Result<Vec<StoredGroupMark>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_tenant_group_marks read txn", e))?;
        let table = read_txn
            .open_table(TENANT_GROUP_MARKS)
            .map_err(|e| catalog_err("open tenant_group_marks", e))?;
        let mut marks = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iterate tenant_group_marks", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read tenant_group_mark", e))?;
            let (group_id, tenant_id) = key.value();
            let (hlc, site, collection) = value.value();
            marks.push(StoredGroupMark {
                group_id,
                tenant_id,
                hlc,
                site,
                collection: collection.to_owned(),
            });
        }
        Ok(marks)
    }

    /// Raise every mark in `marks` in one transaction. A persisted mark at or
    /// above the new one stays.
    pub fn raise_tenant_group_marks(&self, marks: &[StoredGroupMark]) -> crate::Result<()> {
        if marks.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("raise_tenant_group_marks txn", e))?;
        {
            let mut table = write_txn
                .open_table(TENANT_GROUP_MARKS)
                .map_err(|e| catalog_err("open tenant_group_marks", e))?;
            for mark in marks {
                let key = (mark.group_id, mark.tenant_id);
                let current = table
                    .get(key)
                    .map_err(|e| catalog_err("get tenant_group_mark", e))?
                    .map(|guard| guard.value().0);
                if current.is_some_and(|hlc| hlc >= mark.hlc) {
                    continue;
                }
                table
                    .insert(key, (mark.hlc, mark.site, mark.collection.as_str()))
                    .map_err(|e| catalog_err("insert tenant_group_mark", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit tenant_group_marks", e))
    }
}
