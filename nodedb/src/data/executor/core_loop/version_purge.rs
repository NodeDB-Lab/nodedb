// SPDX-License-Identifier: BUSL-1.1

//! A core's history purge state: the cleanup horizon it last received, the
//! pass in progress, and the commit snapshot window every replica checks.
//!
//! The Control Plane computes the horizon (`control::version_cleanup`). It is
//! at or below every open snapshot, and at least the commit snapshot window
//! below a cut every replica of every hosted group closed. A commit whose
//! snapshot trails its own ordinal by more than the window is refused on
//! every replica alike (`CoreLoop::snapshot_within_window`). So every commit
//! a replica still applies has a snapshot at or above any horizon it purged
//! at, and a purge changes no verdict: it only removes versions at or below
//! the horizon that no read at or above it reaches.

use std::collections::VecDeque;

use nodedb_physical::physical_plan::{MirrorHistoryFloor, RetainedHistory};
use nodedb_types::{DatabaseId, QualifiedCollection};

use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::types::TenantId;

/// One store the pass purges: one engine of one collection, or a whole
/// engine whose purge reads every collection in one walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) enum PurgeUnit {
    /// The versioned document and index tables of one collection.
    Documents {
        database_id: u64,
        tenant: u64,
        collection: String,
    },
    /// Every KV table. KV keeps no audit window.
    Kv,
    /// One plain columnar or spatial collection's row versions.
    Columnar((DatabaseId, TenantId, String)),
    /// One timeseries collection's partitions a TRUNCATE ended.
    Timeseries((DatabaseId, TenantId, String)),
    /// One vector index's node versions, multi-vector members included.
    Vectors((DatabaseId, TenantId, String)),
    /// One sparse-vector index's posting versions.
    SparseVectors((DatabaseId, TenantId, String, String)),
    /// One R-tree's ended entries.
    Spatial(SpatialIndexKey),
    /// Every full-text index's history, each at its collection's horizon.
    Text,
    /// One edge collection's versions and TRUNCATE cuts.
    Edges {
        database_id: u64,
        tenant: u64,
        collection: String,
    },
    /// Every node-label version, each at its tenant's horizon.
    NodeLabels,
}

/// One purge pass at one horizon.
#[derive(Debug)]
pub(in crate::data::executor) struct PurgePass {
    pub horizon: i64,
    pub retained: Vec<RetainedHistory>,
    pub mirror_floors: Vec<MirrorHistoryFloor>,
    pub pending: VecDeque<PurgeUnit>,
}

impl PurgePass {
    /// The horizon the stored collection `stored` of `(database_id, tenant)`
    /// purges at. A retained collection purges at its own horizon, `None`
    /// to keep its history. `stored` can carry a `:field` suffix, as a
    /// vector index key does. A read-only mirror's floor limits either.
    pub(in crate::data::executor) fn horizon_of(
        &self,
        database_id: u64,
        tenant: u64,
        stored: &str,
    ) -> Option<i64> {
        let horizon = match self
            .retained
            .iter()
            .find(|r| names(r, database_id, tenant, stored))
        {
            Some(retained) => retained.horizon?,
            None => self.horizon,
        };
        Some(under_floor(&self.mirror_floors, database_id, horizon))
    }

    /// The horizon a tenant-wide store of `(database_id, tenant)` purges at:
    /// the lowest of the pass horizon, a read-only mirror's floor, and every
    /// retained collection's, and `None` when any retained collection keeps its
    /// whole history.
    pub(in crate::data::executor) fn tenant_horizon(
        &self,
        database_id: u64,
        tenant: u64,
    ) -> Option<i64> {
        self.retained
            .iter()
            .filter(|r| r.database_id == database_id && r.tenant_id == tenant)
            .try_fold(
                under_floor(&self.mirror_floors, database_id, self.horizon),
                |lowest, r| r.horizon.map(|h| lowest.min(h)),
            )
    }
}

/// `horizon`, lowered to `database_id`'s floor when it is a read-only mirror.
pub(in crate::data::executor) fn under_floor(
    floors: &[MirrorHistoryFloor],
    database_id: u64,
    horizon: i64,
) -> i64 {
    floors
        .iter()
        .find(|floor| floor.database_id == database_id)
        .map_or(horizon, |floor| horizon.min(floor.floor))
}

/// Whether `stored`, a Data Plane collection name with an optional
/// `:field` suffix, names the retained collection `r`.
fn names(r: &RetainedHistory, database_id: u64, tenant: u64, stored: &str) -> bool {
    if r.database_id != database_id || r.tenant_id != tenant {
        return false;
    }
    let qualified = QualifiedCollection::new(DatabaseId::new(database_id), &r.collection);
    [r.collection.as_str(), qualified.as_str()]
        .into_iter()
        .any(|name| {
            stored == name
                || stored
                    .strip_prefix(name)
                    .is_some_and(|rest| rest.starts_with(':'))
        })
}

/// The purge state of one core.
pub(in crate::data::executor) struct VersionPurgeState {
    /// The longest a commit's snapshot can trail its own ordinal, in
    /// nanoseconds. Every node runs one value.
    pub commit_window_ns: i64,
    /// The newest cleanup horizon this core received, `0` before the first.
    /// Every purge this core decides stays at or below it. A logged audit purge
    /// carries a cutoff its decider held below a horizon of its own.
    pub safe_horizon: i64,
    /// Each read-only mirror's limit on `safe_horizon`, as the newest request
    /// named it.
    pub mirror_floors: Vec<MirrorHistoryFloor>,
    /// The pass in progress, `None` before the first request.
    pub pass: Option<PurgePass>,
    /// Each plain columnar collection's charged history bytes and the
    /// governor reservation that holds them
    /// (`CoreLoop::recharge_columnar_history`).
    pub columnar_history: std::collections::HashMap<
        super::columnar_history_charge::ColumnarKey,
        (usize, nodedb_mem::ReservationToken),
    >,
    /// Each vector, sparse-vector, and R-tree index's charged resident bytes
    /// and the reservation that holds them (`CoreLoop::recharge_index`).
    pub index_charges: std::collections::HashMap<
        super::index_charge::IndexChargeKey,
        (usize, nodedb_mem::ReservationToken),
    >,
    /// Vector indexes a history purge left removable nodes in. The next
    /// maintenance pass compacts each whatever its tombstone ratio.
    pub vector_compaction_due:
        std::collections::HashSet<(crate::types::DatabaseId, TenantId, String)>,
}

impl VersionPurgeState {
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            commit_window_ns: window_ns(
                nodedb_types::config::tuning::NetworkTuning::default().snapshot_max_hold(),
            ),
            safe_horizon: 0,
            mirror_floors: Vec::new(),
            pass: None,
            columnar_history: std::collections::HashMap::new(),
            index_charges: std::collections::HashMap::new(),
            vector_compaction_due: std::collections::HashSet::new(),
        }
    }
}

impl super::CoreLoop {
    /// Whether a commit at `ordinal` with snapshot `snapshot` sits within
    /// the commit snapshot window. Every replica of the commit computes the
    /// same answer: both ordinals travel with the commit, and every node
    /// runs one window. A commit with no ordinal of its own is within it.
    pub(in crate::data::executor) fn snapshot_within_window(
        &self,
        snapshot: i64,
        ordinal: Option<i64>,
    ) -> bool {
        let window = self.maintenance.version_purge.commit_window_ns;
        ordinal.is_none_or(|ordinal| snapshot >= ordinal.saturating_sub(window))
    }
}

/// `window` in nanoseconds, saturating.
pub(in crate::data::executor) fn window_ns(window: std::time::Duration) -> i64 {
    i64::try_from(window.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retained(collection: &str, horizon: Option<i64>) -> RetainedHistory {
        RetainedHistory {
            database_id: 3,
            tenant_id: 1,
            collection: collection.into(),
            horizon,
        }
    }

    fn pass(retained: Vec<RetainedHistory>) -> PurgePass {
        PurgePass {
            horizon: 100,
            retained,
            mirror_floors: Vec::new(),
            pending: VecDeque::new(),
        }
    }

    /// A mirror's floor limits its plain and retained stores, and no other
    /// database's. A collection that keeps its whole history still does.
    #[test]
    fn a_mirror_floor_limits_only_its_own_database() {
        let mut pass = pass(vec![retained("audit", Some(40)), retained("keep", None)]);
        pass.mirror_floors = vec![MirrorHistoryFloor {
            database_id: 3,
            floor: 30,
        }];
        assert_eq!(pass.horizon_of(3, 1, "plain"), Some(30));
        assert_eq!(pass.horizon_of(3, 1, "audit"), Some(30));
        assert_eq!(pass.horizon_of(3, 1, "keep"), None);
        assert_eq!(pass.horizon_of(4, 1, "plain"), Some(100));
        assert_eq!(pass.tenant_horizon(4, 1), Some(100));
        pass.retained.pop();
        assert_eq!(pass.tenant_horizon(3, 1), Some(30));
    }

    #[test]
    fn a_retained_collection_purges_at_its_own_horizon() {
        let pass = pass(vec![retained("audit", Some(40)), retained("keep", None)]);
        assert_eq!(pass.horizon_of(3, 1, "plain"), Some(100));
        assert_eq!(pass.horizon_of(3, 1, "3/audit"), Some(40));
        assert_eq!(pass.horizon_of(3, 1, "audit"), Some(40));
        assert_eq!(
            pass.horizon_of(3, 1, "3/audit:emb"),
            Some(40),
            "a vector key"
        );
        assert_eq!(pass.horizon_of(3, 1, "3/auditx"), Some(100));
        assert_eq!(
            pass.horizon_of(3, 2, "3/audit"),
            Some(100),
            "another tenant"
        );
        assert_eq!(pass.horizon_of(3, 1, "3/keep"), None);
    }

    #[test]
    fn a_tenant_horizon_is_the_lowest_of_its_collections() {
        assert_eq!(pass(vec![]).tenant_horizon(3, 1), Some(100));
        assert_eq!(
            pass(vec![retained("audit", Some(40))]).tenant_horizon(3, 1),
            Some(40)
        );
        assert_eq!(
            pass(vec![retained("audit", Some(40)), retained("keep", None)]).tenant_horizon(3, 1),
            None
        );
    }
}
