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
//! A server with no Raft groups records its writes under
//! [`LOCAL_MARK_GROUP`]. Each such write's mark is durable before its WAL
//! record is minted (see [`TenantMarks::stamp_local_write`]).

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::tenant_group_marks::StoredGroupMark;

use super::local_write_stamps::{LocalWriteStamp, LocalWriteStamps};

/// The pseudo-group a server with no Raft groups records its writes under.
/// No Raft group carries this id.
pub const LOCAL_MARK_GROUP: u64 = u64::MAX;

/// The apply path that recorded a mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkSite {
    /// A committed data-group entry.
    ReplicatedApply,
    /// A committed Calvin transaction's install.
    CalvinFlush,
    /// A write on a server with no Raft groups.
    LocalWrite,
}

impl MarkSite {
    /// The name a refused restore reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReplicatedApply => "replicated apply",
            Self::CalvinFlush => "calvin flush",
            Self::LocalWrite => "local write",
        }
    }

    /// The code the persisted and wire forms carry.
    pub fn code(self) -> u8 {
        match self {
            Self::ReplicatedApply => 0,
            Self::CalvinFlush => 1,
            Self::LocalWrite => 2,
        }
    }

    /// The site a persisted or wire code names.
    pub fn from_code(code: u8) -> Self {
        match code {
            1 => Self::CalvinFlush,
            2 => Self::LocalWrite,
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
}

#[derive(Debug, Default)]
struct MarkState {
    marks: HashMap<(u64, u64), GroupMark>,
    /// Marks raised since the last persist.
    dirty: BTreeSet<(u64, u64)>,
    /// The highest commit HLC of each mark the catalog holds.
    persisted: HashMap<(u64, u64), u64>,
}

/// This node's per-group tenant write marks.
#[derive(Debug, Default)]
pub struct TenantMarks {
    state: Mutex<MarkState>,
    /// Held across each persist, so a caller that finds its mark persisted
    /// knows the catalog commit that wrote it finished.
    persist_lock: Mutex<()>,
    /// The local writes minting their records now.
    local_stamps: LocalWriteStamps,
}

impl TenantMarks {
    /// The marks persisted in `catalog`.
    pub fn load(catalog: &SystemCatalog) -> crate::Result<Self> {
        let marks = Self::default();
        {
            let mut state = marks.state.lock().unwrap_or_else(|p| p.into_inner());
            for stored in catalog.load_tenant_group_marks()? {
                state
                    .persisted
                    .insert((stored.group_id, stored.tenant_id), stored.hlc);
                state.marks.insert(
                    (stored.group_id, stored.tenant_id),
                    GroupMark {
                        hlc: stored.hlc,
                        site: MarkSite::from_code(stored.site),
                        collection: (!stored.collection.is_empty()).then_some(stored.collection),
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
        if hlc == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let key = (group_id, tenant_id);
        if state.marks.get(&key).is_some_and(|mark| mark.hlc >= hlc) {
            return;
        }
        state.marks.insert(
            key,
            GroupMark {
                hlc,
                site,
                collection: collection.map(str::to_owned),
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

    /// Persist until the catalog holds `tenant_id`'s mark in `group_id` at or
    /// above `hlc`. A persist that already wrote it, or one running now, covers
    /// it: concurrent callers share one catalog commit.
    pub fn persist_through(
        &self,
        catalog: &SystemCatalog,
        group_id: u64,
        tenant_id: u64,
        hlc: u64,
    ) -> crate::Result<()> {
        let _persisting = self.persist_lock.lock().unwrap_or_else(|p| p.into_inner());
        let covered = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .persisted
            .get(&(group_id, tenant_id))
            .is_some_and(|persisted| *persisted >= hlc);
        if covered {
            return Ok(());
        }
        self.persist_pending(catalog)
    }

    /// Stamp a user write on a server with no Raft groups, and make its mark
    /// durable before the write mints its WAL record.
    ///
    /// The stamp is the write's commit HLC. It stays open until the returned
    /// guard drops, which the caller does once the record is minted. A backup
    /// cut waits for every open stamp at or below its watermark (see
    /// [`Self::await_local_stamps_minted`]). So a write stamped below the
    /// watermark is in the backup, and one stamped above it has a mark above
    /// it. A crash after the mint leaves the mark in the catalog, whether or
    /// not the record reached disk.
    pub fn stamp_local_write(
        &self,
        clock: &nodedb_types::HlcClock,
        catalog: &SystemCatalog,
        tenant_id: u64,
        collection: Option<&str>,
    ) -> crate::Result<LocalWriteStamp<'_>> {
        let stamp = self.local_stamps.stamp(clock);
        self.record_local_write(catalog, tenant_id, stamp.hlc(), collection)?;
        Ok(stamp)
    }

    /// Raise `tenant_id`'s mark under [`LOCAL_MARK_GROUP`] to `hlc` and
    /// persist it. A mark the catalog already holds at or above `hlc` costs
    /// no catalog commit.
    pub fn record_local_write(
        &self,
        catalog: &SystemCatalog,
        tenant_id: u64,
        hlc: u64,
        collection: Option<&str>,
    ) -> crate::Result<()> {
        self.raise(
            LOCAL_MARK_GROUP,
            tenant_id,
            hlc,
            MarkSite::LocalWrite,
            collection,
        );
        self.persist_through(catalog, LOCAL_MARK_GROUP, tenant_id, hlc)
    }

    /// Wait until every local write stamped at or below `watermark` minted
    /// its record. `false` when `deadline` passes first.
    pub async fn await_local_stamps_minted(
        &self,
        watermark: u64,
        deadline: tokio::time::Instant,
    ) -> bool {
        self.local_stamps
            .await_minted_through(watermark, deadline)
            .await
    }

    /// Write every dirty mark. The caller holds `persist_lock`.
    fn persist_pending(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let pending: Vec<StoredGroupMark> = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state
                .dirty
                .iter()
                .filter_map(|key| {
                    state.marks.get(key).map(|mark| StoredGroupMark {
                        group_id: key.0,
                        tenant_id: key.1,
                        hlc: mark.hlc,
                        site: mark.site.code(),
                        collection: mark.collection.clone().unwrap_or_default(),
                    })
                })
                .collect()
        };
        if pending.is_empty() {
            return Ok(());
        }
        catalog.raise_tenant_group_marks(&pending)?;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        for mark in &pending {
            let key = (mark.group_id, mark.tenant_id);
            // A raise after the snapshot above stays pending.
            if state.marks.get(&key).is_some_and(|m| m.hlc == mark.hlc) {
                state.dirty.remove(&key);
            }
            let persisted = state.persisted.entry(key).or_insert(0);
            *persisted = (*persisted).max(mark.hlc);
        }
        Ok(())
    }

    /// Every tenant's mark in `group_id`, in the form a group snapshot carries:
    /// `(tenant_id, commit_hlc, site_code, collection)`.
    pub fn group_entries(&self, group_id: u64) -> Vec<(u64, u64, u8, String)> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut entries: Vec<(u64, u64, u8, String)> = state
            .marks
            .iter()
            .filter(|((group, _), _)| *group == group_id)
            .map(|((_, tenant_id), mark)| {
                (
                    *tenant_id,
                    mark.hlc,
                    mark.site.code(),
                    mark.collection.clone().unwrap_or_default(),
                )
            })
            .collect();
        entries.sort_unstable();
        entries
    }

    /// Raise `group_id`'s marks from the entries of a group snapshot.
    pub fn raise_group_entries(&self, group_id: u64, entries: &[(u64, u64, u8, String)]) {
        for (tenant_id, hlc, site, collection) in entries {
            self.raise(
                group_id,
                *tenant_id,
                *hlc,
                MarkSite::from_code(*site),
                (!collection.is_empty()).then_some(collection.as_str()),
            );
        }
    }

    /// `tenant_id`'s mark in `group_id`, if the group applied any write of it.
    pub fn get(&self, group_id: u64, tenant_id: u64) -> Option<GroupMark> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .marks
            .get(&(group_id, tenant_id))
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
        marks.raise(1, 7, 50, MarkSite::CalvinFlush, None);
        let mark = marks.get(1, 7).expect("mark");
        assert_eq!(mark.hlc, 100);
        assert_eq!(mark.collection.as_deref(), Some("docs"));
        assert!(marks.get(2, 7).is_none(), "a mark binds only its group");
    }

    #[test]
    fn a_local_write_mark_is_durable_once_stamped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let clock = nodedb_types::HlcClock::new();
        let marks = TenantMarks::default();
        let hlc = {
            let stamp = marks
                .stamp_local_write(&clock, &catalog, 4, Some("orders"))
                .expect("stamp");
            stamp.hlc()
        };
        let reloaded = TenantMarks::load(&catalog).expect("reload");
        assert_eq!(
            reloaded.get(LOCAL_MARK_GROUP, 4),
            Some(GroupMark {
                hlc,
                site: MarkSite::LocalWrite,
                collection: Some("orders".to_owned()),
            })
        );
    }

    #[test]
    fn persisted_marks_survive_a_reload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let marks = TenantMarks::default();
        marks.raise(3, 1, 900, MarkSite::CalvinFlush, Some("orders"));
        marks.persist(&catalog).expect("persist");

        let reloaded = TenantMarks::load(&catalog).expect("reload");
        assert_eq!(
            reloaded.get(3, 1),
            Some(GroupMark {
                hlc: 900,
                site: MarkSite::CalvinFlush,
                collection: Some("orders".to_owned()),
            })
        );
    }
}
