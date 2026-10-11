// SPDX-License-Identifier: BUSL-1.1

//! Per-group tenant write marks: for each data group this node replicates,
//! the newest commit HLC of each tenant's writes the group applied.
//!
//! The marks are replicated state: every replica of a group applies the same
//! entries with the same commit stamps, so every replica derives the same
//! marks. They are durable: the apply loop persists a group's marks before
//! the applied floor that covers them, and Raft delivers every entry above
//! the floor again after a restart. RESTORE's staleness guard reads them for
//! every group that holds the tenant's data, so its answer does not depend
//! on which node's memory saw the write.
//!
//! Each `(group, tenant)` keeps two marks: the newest write a RESTORE
//! re-issued, with that restore's id, and the newest of every other write.
//! A retry of a restore then tells its own writes apart, and no restore write
//! hides a newer user write.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::tenant_group_marks::{RESTORE_SITE_CODE, StoredGroupMark};

/// One group mark as a group snapshot carries it:
/// `(tenant_id, commit_hlc, site_code, collection, restore_id)`.
pub type GroupMarkEntry = (u64, u64, u8, String, u64);

/// The apply path that recorded a mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkSite {
    /// A committed data-group entry, a committed Calvin slice's install
    /// included.
    ReplicatedApply,
    /// A write a RESTORE re-issued.
    Restore,
}

impl MarkSite {
    /// The name a refused restore reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReplicatedApply => "replicated apply",
            Self::Restore => "restore re-issue",
        }
    }

    /// The code the persisted and wire forms carry.
    pub fn code(self) -> u8 {
        match self {
            Self::ReplicatedApply => 0,
            Self::Restore => RESTORE_SITE_CODE,
        }
    }

    /// The site a persisted or wire code names.
    pub fn from_code(code: u8) -> Self {
        match code {
            RESTORE_SITE_CODE => Self::Restore,
            _ => Self::ReplicatedApply,
        }
    }
}

/// One group's newest write of one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMark {
    /// HLC wall time, in nanoseconds, of the write's commit.
    pub hlc: u64,
    pub site: MarkSite,
    /// The collection the write named, when it named one.
    pub collection: Option<String>,
    /// The restore that re-issued the write, `0` for any other write.
    pub restore_id: u64,
}

/// `(group_id, tenant_id, is_restore_mark)`.
type MarkKey = (u64, u64, bool);

#[derive(Debug, Default)]
struct MarkState {
    marks: HashMap<MarkKey, GroupMark>,
    /// Marks raised since the last persist.
    dirty: BTreeSet<MarkKey>,
}

/// This node's per-group tenant write marks.
#[derive(Debug, Default)]
pub struct TenantMarks {
    state: Mutex<MarkState>,
    /// Serializes persists, so two never write the same dirty marks at once.
    persist_lock: Mutex<()>,
}

impl TenantMarks {
    /// The marks persisted in `catalog`.
    pub fn load(catalog: &SystemCatalog) -> crate::Result<Self> {
        let marks = Self::default();
        {
            let mut state = marks.state.lock().unwrap_or_else(|p| p.into_inner());
            for stored in catalog.load_tenant_group_marks()? {
                let key = (stored.group_id, stored.tenant_id, stored.restore_id != 0);
                state.marks.insert(
                    key,
                    GroupMark {
                        hlc: stored.hlc,
                        site: MarkSite::from_code(stored.site),
                        collection: (!stored.collection.is_empty()).then_some(stored.collection),
                        restore_id: stored.restore_id,
                    },
                );
            }
        }
        Ok(marks)
    }

    /// Raise `tenant_id`'s mark in `group_id` to `hlc`. A mark at or above it
    /// stays.
    pub fn raise(
        &self,
        group_id: u64,
        tenant_id: u64,
        hlc: u64,
        site: MarkSite,
        collection: Option<&str>,
    ) {
        self.raise_mark(group_id, tenant_id, hlc, site, collection, 0);
    }

    /// Raise the mark of the writes RESTORE `restore_id` re-issued. `0` is a
    /// re-issue that is no RESTORE: its writes raise the user mark.
    pub fn raise_restore(
        &self,
        group_id: u64,
        tenant_id: u64,
        hlc: u64,
        collection: Option<&str>,
        restore_id: u64,
    ) {
        let site = if restore_id == 0 {
            MarkSite::ReplicatedApply
        } else {
            MarkSite::Restore
        };
        self.raise_mark(group_id, tenant_id, hlc, site, collection, restore_id);
    }

    fn raise_mark(
        &self,
        group_id: u64,
        tenant_id: u64,
        hlc: u64,
        site: MarkSite,
        collection: Option<&str>,
        restore_id: u64,
    ) {
        if hlc == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let key = (group_id, tenant_id, restore_id != 0);
        if state.marks.get(&key).is_some_and(|mark| mark.hlc >= hlc) {
            return;
        }
        state.marks.insert(
            key,
            GroupMark {
                hlc,
                site,
                collection: collection.map(str::to_owned),
                restore_id,
            },
        );
        state.dirty.insert(key);
    }

    /// Persist every mark raised since the last persist. On an error the
    /// marks stay pending, so the next persist writes them.
    pub fn persist(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let _persisting = self.persist_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.persist_pending(catalog)
    }

    /// Write every dirty mark. The caller holds `persist_lock`.
    fn persist_pending(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let pending: Vec<(MarkKey, StoredGroupMark)> = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state
                .dirty
                .iter()
                .filter_map(|key| {
                    state.marks.get(key).map(|mark| {
                        (
                            *key,
                            StoredGroupMark {
                                group_id: key.0,
                                tenant_id: key.1,
                                hlc: mark.hlc,
                                site: mark.site.code(),
                                collection: mark.collection.clone().unwrap_or_default(),
                                restore_id: mark.restore_id,
                            },
                        )
                    })
                })
                .collect()
        };
        if pending.is_empty() {
            return Ok(());
        }
        let stored: Vec<StoredGroupMark> = pending.iter().map(|(_, m)| m.clone()).collect();
        catalog.raise_tenant_group_marks(&stored)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        for (key, mark) in &pending {
            // A raise after the snapshot above stays pending.
            if state.marks.get(key).is_some_and(|m| m.hlc == mark.hlc) {
                state.dirty.remove(key);
            }
        }
        Ok(())
    }

    /// Every tenant's marks in `group_id`, user and restore, in the form a
    /// group snapshot carries.
    pub fn group_entries(&self, group_id: u64) -> Vec<GroupMarkEntry> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut entries: Vec<GroupMarkEntry> = state
            .marks
            .iter()
            .filter(|((group, _, _), _)| *group == group_id)
            .map(|((_, tenant_id, _), mark)| {
                (
                    *tenant_id,
                    mark.hlc,
                    mark.site.code(),
                    mark.collection.clone().unwrap_or_default(),
                    mark.restore_id,
                )
            })
            .collect();
        entries.sort_unstable();
        entries
    }

    /// Raise `group_id`'s marks from the entries of a group snapshot.
    pub fn raise_group_entries(&self, group_id: u64, entries: &[GroupMarkEntry]) {
        for (tenant_id, hlc, site, collection, restore_id) in entries {
            self.raise_mark(
                group_id,
                *tenant_id,
                *hlc,
                MarkSite::from_code(*site),
                (!collection.is_empty()).then_some(collection.as_str()),
                *restore_id,
            );
        }
    }

    /// `tenant_id`'s user mark in `group_id`, if the group applied any write
    /// of it no RESTORE re-issued.
    pub fn get(&self, group_id: u64, tenant_id: u64) -> Option<GroupMark> {
        self.get_key((group_id, tenant_id, false))
    }

    /// Both of `tenant_id`'s marks in `group_id`: the user mark, then the
    /// mark of the newest restore write.
    pub fn get_all(&self, group_id: u64, tenant_id: u64) -> Vec<GroupMark> {
        [false, true]
            .into_iter()
            .filter_map(|restore| self.get_key((group_id, tenant_id, restore)))
            .collect()
    }

    fn get_key(&self, key: MarkKey) -> Option<GroupMark> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .marks
            .get(&key)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mark_only_rises() {
        let marks = TenantMarks::default();
        marks.raise(1, 7, 100, MarkSite::ReplicatedApply, Some("docs"));
        marks.raise(1, 7, 50, MarkSite::ReplicatedApply, None);
        let mark = marks.get(1, 7).expect("mark");
        assert_eq!(mark.hlc, 100);
        assert_eq!(mark.collection.as_deref(), Some("docs"));
        assert!(marks.get(2, 7).is_none(), "a mark binds only its group");
    }

    /// A restore write keeps its own mark: it never hides an older user write
    /// newer than a backup, and the user mark never carries a restore id.
    #[test]
    fn a_restore_write_never_hides_a_user_write() {
        let marks = TenantMarks::default();
        marks.raise(1, 7, 100, MarkSite::ReplicatedApply, Some("docs"));
        marks.raise_restore(1, 7, 300, Some("docs"), 42);
        assert_eq!(marks.get(1, 7).map(|m| m.hlc), Some(100));
        let all = marks.get_all(1, 7);
        assert_eq!(all.len(), 2);
        assert_eq!(all[1].restore_id, 42);
        assert_eq!(all[1].site, MarkSite::Restore);

        marks.raise_restore(1, 7, 400, None, 0);
        assert_eq!(
            marks.get(1, 7).map(|m| m.hlc),
            Some(400),
            "a re-issue with no restore id raises the user mark"
        );
    }

    #[test]
    fn group_entries_carry_restore_marks_across_a_snapshot() {
        let source = TenantMarks::default();
        source.raise(4, 7, 100, MarkSite::ReplicatedApply, None);
        source.raise_restore(4, 7, 200, Some("c"), 9);
        let entries = source.group_entries(4);
        assert_eq!(entries.len(), 2);

        let follower = TenantMarks::default();
        follower.raise_group_entries(4, &entries);
        assert_eq!(follower.get_all(4, 7), source.get_all(4, 7));
    }

    #[test]
    fn persisted_marks_survive_a_reload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let marks = TenantMarks::default();
        marks.raise(3, 1, 900, MarkSite::ReplicatedApply, Some("orders"));
        marks.raise_restore(4, 1, 950, Some("orders"), 5);
        marks.persist(&catalog).expect("persist");

        let reloaded = TenantMarks::load(&catalog).expect("reload");
        assert_eq!(
            reloaded.get(3, 1),
            Some(GroupMark {
                hlc: 900,
                site: MarkSite::ReplicatedApply,
                collection: Some("orders".to_owned()),
                restore_id: 0,
            })
        );
        assert_eq!(
            reloaded.get_all(4, 1),
            vec![GroupMark {
                hlc: 950,
                site: MarkSite::Restore,
                collection: Some("orders".to_owned()),
                restore_id: 5,
            }]
        );
    }
}
