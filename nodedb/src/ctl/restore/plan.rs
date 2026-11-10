// SPDX-License-Identifier: BUSL-1.1

//! Restore planning: resolve the target to an LSN, pick the base, and check
//! the archive holds every WAL record that brings the base to the target.
//!
//! Planning fetches and checks every segment it reads, and writes nothing.
//! It keeps only per-segment metadata, so memory stays bounded by one
//! segment image.

use std::collections::{BTreeSet, HashMap};

use nodedb_wal::crypto::KeyRing;

use super::archive::Archive;
use super::args::RestoreTarget;
use super::env::RestoreEnv;
use super::error::RestoreError;
use super::life::{Base, Life};
use super::node_metadata::{MetadataStep, NodeMetadata, plan_node_metadata};
use super::segment::{SegmentScan, cut_segment, scan_segment};
use super::time_coverage::resolve_time;
use super::timeline::target_ns;
use crate::storage::snapshot::SnapshotCatalog;
use crate::storage::snapshot_restore::{CoverageStep, WalCoverage};
use crate::wal::GroupMembership;

/// One archived segment the restore writes into `data_dir/wal/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedSegment {
    /// Position in the archive listing the plan was made from.
    pub index: usize,
    pub first_lsn: u64,
    pub size: u64,
    /// The checksum planning verified. The write pass refuses any other.
    pub crc32c: u32,
}

/// What the cut does to the last planned segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutSummary {
    pub first_lsn: u64,
    pub kept_bytes: u64,
    pub dropped_records: u64,
}

#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub node_id: u64,
    pub incarnation: String,
    pub base: Base,
    pub target_lsn: u64,
    /// The requested instant of a time target, microseconds since the epoch.
    pub requested_us: Option<u64>,
    /// Commit time of the batch holding the target, when an anchor covers it.
    pub target_commit_ns: Option<u64>,
    /// Segments replay reads, in LSN order. Empty when the base alone
    /// reaches the target.
    pub segments: Vec<PlannedSegment>,
    pub cut: Option<CutSummary>,
    /// Records at or below the target whose write-abort marker lies above it.
    pub refused: Vec<u64>,
    /// Highest LSN the write-abort scan read, through the end of the archive.
    pub aborts_scanned_through: Option<u64>,
    /// A surrogate reservation the cut appends after its markers.
    pub surrogate_hwm: Option<u32>,
    /// The metadata log a node restore replays after the base. `None` for a
    /// base with no cluster catalogs, and for a cluster restore, which
    /// replays the metadata log by its own plan.
    pub metadata: Option<NodeMetadata>,
}

impl RestorePlan {
    /// Bytes of archived WAL the restore fetches.
    pub fn wal_bytes(&self) -> u64 {
        self.segments.iter().map(|seg| seg.size).sum()
    }

    /// First LSN replay needs: the one above the base's lowest core floor.
    pub fn replay_start(&self) -> u64 {
        self.base.meta.begin_lsn.as_u64().saturating_add(1)
    }
}

/// Fetches, checks and scans archived segments, keeping each scan.
pub(super) struct Scanner<'a, 'b> {
    pub(super) archive: &'a Archive<'b>,
    ring: KeyRing,
    scans: HashMap<usize, (u32, SegmentScan)>,
}

impl<'a, 'b> Scanner<'a, 'b> {
    pub(super) fn new(
        archive: &'a Archive<'b>,
        key: &nodedb_wal::crypto::WalEncryptionKey,
    ) -> Self {
        Self {
            archive,
            ring: KeyRing::new(key.clone()),
            scans: HashMap::new(),
        }
    }
}

impl Scanner<'_, '_> {
    /// Checksum and scan of the segment at `index`, kept for later calls.
    pub(super) async fn scan(&mut self, index: usize) -> Result<(u32, SegmentScan), RestoreError> {
        if let Some(hit) = self.scans.get(&index) {
            return Ok(hit.clone());
        }
        let scanned = self.fetch_and_scan(index).await?;
        self.scans.insert(index, scanned.clone());
        Ok(scanned)
    }

    /// Scan of the segment at `index`, fetched again unless held. Nothing is
    /// kept: the write-abort pass reads to the end of the archive.
    async fn scan_once(&self, index: usize) -> Result<SegmentScan, RestoreError> {
        if let Some((_, scan)) = self.scans.get(&index) {
            return Ok(scan.clone());
        }
        Ok(self.fetch_and_scan(index).await?.1)
    }

    async fn fetch_and_scan(&self, index: usize) -> Result<(u32, SegmentScan), RestoreError> {
        let fetched = self.archive.fetch(index).await?;
        let scan = scan_segment(&fetched.key, &fetched.bytes, Some(&self.ring))?;
        Ok((fetched.crc32c, scan))
    }
}

/// Plan a restore of `life` to `target`.
pub async fn plan_restore(
    env: &RestoreEnv,
    life: &Life,
    archive: &Archive<'_>,
    target: &RestoreTarget,
) -> Result<RestorePlan, RestoreError> {
    let mut scanner = Scanner::new(archive, &env.key);
    let (target_lsn, requested_us) = match target {
        RestoreTarget::Lsn(lsn) => (*lsn, None),
        RestoreTarget::Time { micros, .. } => {
            let lsn = resolve_time(
                &mut scanner,
                &life.bases,
                target_ns(*micros),
                &life.incarnation,
            )
            .await?;
            (lsn, Some(*micros))
        }
    };
    // A base whose catalogs hold a metadata entry the target drops cannot
    // serve: only a base captured before that entry does.
    let mut bases = life.bases.clone();
    loop {
        let mut plan = plan_over(&mut scanner, env, life, &bases, target_lsn, requested_us).await?;
        match plan_node_metadata(env, &plan).await? {
            MetadataStep::Absent => return Ok(plan),
            MetadataStep::Ready(metadata) => {
                plan.metadata = Some(metadata);
                return Ok(plan);
            }
            MetadataStep::BaseHolds { index } => {
                bases.retain(|base| base.metadata_captured_index < index);
                if bases.is_empty() {
                    return Err(RestoreError::NoBaseBeforeCatalogChange {
                        entry_index: index,
                        target_lsn,
                    });
                }
            }
        }
    }
}

/// Plan the WAL of a restore to `target_lsn` from the newest of `bases` that
/// serves it.
async fn plan_over(
    scanner: &mut Scanner<'_, '_>,
    env: &RestoreEnv,
    life: &Life,
    bases: &[Base],
    target_lsn: u64,
    requested_us: Option<u64>,
) -> Result<RestorePlan, RestoreError> {
    let base = find_base(bases, target_lsn)?;
    let mut plan = RestorePlan::new(env, life, base, target_lsn, requested_us);
    plan_wal(scanner, &mut plan, &[], env.alignment).await?;
    // A snapshot install replaced rows no WAL record carries, so a base from
    // before it cannot replay across it. The install forced a base after it.
    if let Some(install_lsn) = install_after_base(scanner, &plan).await? {
        let later: Vec<Base> = bases
            .iter()
            .filter(|base| base.meta.begin_lsn.as_u64() >= install_lsn)
            .cloned()
            .collect();
        let base = find_base(&later, target_lsn).map_err(|_| {
            RestoreError::NoBaseAfterSnapshotInstall {
                install_lsn,
                target_lsn,
            }
        })?;
        plan = RestorePlan::new(env, life, base, target_lsn, requested_us);
        plan_wal(scanner, &mut plan, &[], env.alignment).await?;
    }
    Ok(plan)
}

impl RestorePlan {
    /// A plan of `life` from `base` to `target_lsn`, with no WAL planned yet.
    fn new(
        env: &RestoreEnv,
        life: &Life,
        base: Base,
        target_lsn: u64,
        requested_us: Option<u64>,
    ) -> Self {
        Self {
            node_id: env.node_id,
            incarnation: life.incarnation.clone(),
            base,
            target_lsn,
            requested_us,
            target_commit_ns: None,
            segments: Vec::new(),
            cut: None,
            refused: Vec::new(),
            aborts_scanned_through: None,
            surrogate_hwm: None,
            metadata: None,
        }
    }
}

/// The LSN of the newest snapshot install above the base's floor and at or
/// below the target, `None` when `plan` replays across no install.
pub(super) async fn install_after_base(
    scanner: &Scanner<'_, '_>,
    plan: &RestorePlan,
) -> Result<Option<u64>, RestoreError> {
    let floor = plan.base.meta.begin_lsn.as_u64();
    let mut newest = None;
    for planned in &plan.segments {
        for lsn in scanner.scan_once(planned.index).await?.installs {
            if lsn > floor && lsn <= plan.target_lsn {
                newest = newest.max(Some(lsn));
            }
        }
    }
    Ok(newest)
}

/// Plan the archived WAL that brings `plan.base` to `plan.target_lsn`: the
/// segments, the records refused above the target and in `dropped`, and the
/// cut of the last segment.
pub(super) async fn plan_wal(
    scanner: &mut Scanner<'_, '_>,
    plan: &mut RestorePlan,
    dropped: &[u64],
    alignment: usize,
) -> Result<(), RestoreError> {
    if plan.replay_start() > plan.target_lsn {
        plan.target_commit_ns = commit_time_of(scanner, plan.target_lsn).await?;
        return Ok(());
    }
    let archive = scanner.archive;
    let last = cover(scanner, plan).await?;
    scan_after_target(scanner, plan, last.index).await?;
    if !dropped.is_empty() {
        let first_written = plan
            .segments
            .first()
            .map_or(plan.target_lsn, |seg| seg.first_lsn);
        let mut refused: BTreeSet<u64> = plan.refused.iter().copied().collect();
        refused.extend(
            dropped
                .iter()
                .copied()
                .filter(|lsn| (first_written..=plan.target_lsn).contains(lsn)),
        );
        plan.refused = refused.into_iter().collect();
    }
    let fetched = archive.fetch(last.index).await?;
    if fetched.crc32c != last.crc32c {
        return Err(RestoreError::SegmentChanged {
            key: fetched.key,
            planned: last.crc32c,
            actual: fetched.crc32c,
        });
    }
    let cut = cut_segment(
        &fetched.key,
        &fetched.bytes,
        plan.target_lsn,
        &plan.refused,
        plan.surrogate_hwm,
        alignment,
    )?;
    plan.cut = Some(CutSummary {
        first_lsn: last.first_lsn,
        kept_bytes: cut.bytes.len() as u64,
        dropped_records: cut.dropped_records,
    });
    Ok(())
}

/// Commit time of the batch holding `target`: the first archived anchor at
/// or above it. `None` when the archive holds no such anchor.
async fn commit_time_of(
    scanner: &Scanner<'_, '_>,
    target: u64,
) -> Result<Option<u64>, RestoreError> {
    let start = scanner.archive.containing(target).unwrap_or(0);
    for index in start..scanner.archive.segments().len() {
        let scan = scanner.scan_once(index).await?;
        if let Some(anchor) = scan.anchors.iter().find(|a| a.lsn >= target) {
            return Ok(Some(anchor.hlc_wall_ns));
        }
    }
    Ok(None)
}

/// The newest base holding no write above `target_lsn`.
fn find_base(bases: &[Base], target_lsn: u64) -> Result<Base, RestoreError> {
    let mut catalog = SnapshotCatalog::new();
    for base in bases {
        catalog.add(base.meta.clone());
    }
    let oldest_applied_high_lsn = bases
        .iter()
        .map(|b| b.meta.applied_high_lsn.as_u64())
        .min()
        .unwrap_or(0);
    catalog
        .find_base(crate::types::Lsn::new(target_lsn))
        .and_then(|meta| {
            bases
                .iter()
                .find(|b| b.meta.snapshot_id == meta.snapshot_id)
        })
        .cloned()
        .ok_or(RestoreError::NoBaseAtOrBelow {
            target_lsn,
            oldest_applied_high_lsn,
        })
}

/// Walk the archive from the segment holding the replay start until every
/// LSN through the target is held. Returns the last segment.
pub(super) async fn cover(
    scanner: &mut Scanner<'_, '_>,
    plan: &mut RestorePlan,
) -> Result<PlannedSegment, RestoreError> {
    let archive = scanner.archive;
    let mut coverage = WalCoverage::new(plan.replay_start(), plan.target_lsn);
    let mut index = archive.containing(plan.replay_start()).unwrap_or(0);
    loop {
        let Some(segment) = archive.segments().get(index) else {
            return Err(coverage.missing(None).into());
        };
        if segment.first_lsn > plan.target_lsn {
            return Err(coverage.missing(Some(segment.first_lsn)).into());
        }
        let (crc32c, scan) = scanner.scan(index).await?;
        let step = coverage.feed(segment.first_lsn, scan.last_lsn)?;
        let planned = PlannedSegment {
            index,
            first_lsn: segment.first_lsn,
            size: segment.size,
            crc32c,
        };
        plan.segments.push(planned.clone());
        if step == CoverageStep::Covered {
            return Ok(planned);
        }
        index += 1;
    }
}

/// Read every segment from the one holding the target to the end of the
/// archive: collect write-abort markers above the target that name a record
/// the restore writes, and the commit time of the target's batch.
///
/// A write's record group is kept only whole. Every record the restore
/// writes of a group with a part above the target, or a part missing from
/// the archive, is refused, so replay applies all of the write or none of it.
pub(super) async fn scan_after_target(
    scanner: &Scanner<'_, '_>,
    plan: &mut RestorePlan,
    target_index: usize,
) -> Result<(), RestoreError> {
    let target = plan.target_lsn;
    let first_written = plan.segments.first().map_or(target, |seg| seg.first_lsn);
    let mut refused = BTreeSet::new();
    let mut groups = GroupMembership::default();
    for planned in plan.segments.iter().filter(|seg| seg.index < target_index) {
        for (lsn, group) in scanner.scan_once(planned.index).await?.groups {
            groups.observe(lsn, group);
        }
    }
    for index in target_index..scanner.archive.segments().len() {
        let scan = scanner.scan_once(index).await?;
        for (lsn, group) in &scan.groups {
            groups.observe(*lsn, *group);
        }
        for marker in &scan.aborts {
            if marker.marker_lsn > target && (first_written..=target).contains(&marker.aborted_lsn)
            {
                refused.insert(marker.aborted_lsn);
            }
        }
        if plan.target_commit_ns.is_none() {
            plan.target_commit_ns = scan
                .anchors
                .iter()
                .find(|a| a.lsn >= target)
                .map(|a| a.hlc_wall_ns);
        }
        if scan.last_lsn.is_some() {
            plan.aborts_scanned_through = scan.last_lsn;
        }
    }
    refused.extend(
        groups
            .broken_through(target)
            .into_iter()
            .filter(|lsn| (first_written..=target).contains(lsn)),
    );
    plan.refused = refused.into_iter().collect();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_wal::crypto::WalEncryptionKey;
    use nodedb_wal::record::{RecordType, TimeAnchorPayload, WriteAbortedPayload};
    use nodedb_wal::writer::{WalWriter, WalWriterConfig};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;
    use crate::storage::cold::{ColdStorage, ColdStorageConfig};
    use crate::storage::snapshot::{SNAPSHOT_FORMAT_VERSION, SnapshotKind, SnapshotMeta};
    use crate::storage::snapshot_restore::CoverageError;
    use crate::types::Lsn;

    const NODE: u64 = 0;
    const INC: &str = "0123456789abcdef0123456789abcdef";

    /// One record of a test segment.
    enum Rec {
        Put,
        Anchor(u64),
        Abort(u64),
        /// A `WriteGroup` record with this descriptor and one row.
        Group(crate::wal::WriteGroup),
        /// A `SnapshotInstalled` record.
        Install,
    }

    fn key() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x3C; 32]).unwrap()
    }

    fn env(dir: &std::path::Path) -> RestoreEnv {
        RestoreEnv {
            data_dir: dir.join("data"),
            node_id: NODE,
            key: key(),
            snapshot_root: Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
            cold: ColdStorage::new(ColdStorageConfig {
                local_dir: Some(dir.join("cold")),
                ..Default::default()
            })
            .unwrap(),
            alignment: 4096,
        }
    }

    /// Write an encrypted segment starting at `first` and archive it.
    async fn archive_segment(env: &RestoreEnv, dir: &std::path::Path, first: u64, recs: &[Rec]) {
        let path = dir.join(format!("seg-{first}"));
        let config = WalWriterConfig {
            use_direct_io: false,
            ..Default::default()
        };
        let mut writer = WalWriter::open_with_start_lsn(&path, config, first).unwrap();
        writer.set_encryption_key(key()).unwrap();
        for rec in recs {
            let (kind, payload) = match rec {
                Rec::Put => (RecordType::Put, b"row".to_vec()),
                Rec::Anchor(ns) => (
                    RecordType::TimeAnchor,
                    TimeAnchorPayload::new(*ns).to_bytes().to_vec(),
                ),
                Rec::Abort(lsn) => (
                    RecordType::WriteAborted,
                    WriteAbortedPayload::new(*lsn).to_bytes().to_vec(),
                ),
                Rec::Group(group) => (
                    RecordType::WriteGroup,
                    crate::wal::WriteGroupRecord {
                        schemas: Vec::new(),
                        group: *group,
                        ops: vec![crate::wal::RedoSubRecord {
                            record_type: RecordType::Put as u32,
                            payload: b"row".to_vec(),
                        }],
                        redo: None,
                    }
                    .to_bytes()
                    .unwrap(),
                ),
                Rec::Install => (RecordType::SnapshotInstalled, 4u64.to_le_bytes().to_vec()),
            };
            writer.append(kind as u32, 1, 0, 0, &payload).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);
        env.cold
            .upload_wal_segment(&path, NODE, INC, first, &[])
            .await
            .unwrap();
    }

    fn base(id: u64, begin: u64, applied_high: u64) -> Base {
        Base {
            prefix: format!("snap-{id}"),
            meta: SnapshotMeta {
                format_version: SNAPSHOT_FORMAT_VERSION,
                snapshot_id: id,
                begin_lsn: Lsn::new(begin),
                end_lsn: Lsn::new(begin),
                applied_high_lsn: Lsn::new(applied_high),
                created_at_us: id,
                created_by: "node-0".into(),
                kind: SnapshotKind::Base,
                parent_id: None,
                data_bytes: 0,
            },
            metadata_applied_index: 0,
            metadata_captured_index: 0,
            metadata_timeline: 0,
        }
    }

    fn life(bases: Vec<Base>) -> Life {
        Life {
            incarnation: INC.into(),
            store: Arc::new(InMemory::new()),
            bases,
        }
    }

    const MS: u64 = 1_000_000;

    /// LSNs 1..=5 with anchors at 3 (1 ms) and 5 (2 ms), 6..=9 with anchors
    /// at 7 (3 ms) and 9 (4 ms), and 10..=12 whose abort marker at 11
    /// refuses LSN 7.
    async fn three_segments(env: &RestoreEnv, dir: &std::path::Path) {
        use Rec::*;
        archive_segment(env, dir, 1, &[Put, Put, Anchor(MS), Put, Anchor(2 * MS)]).await;
        archive_segment(env, dir, 6, &[Put, Anchor(3 * MS), Put, Anchor(4 * MS)]).await;
        archive_segment(env, dir, 10, &[Put, Abort(7), Anchor(5 * MS)]).await;
    }

    /// A time target at `ns` nanoseconds since the epoch.
    fn time(ns: u64) -> RestoreTarget {
        RestoreTarget::Time {
            input: String::new(),
            micros: ns / 1_000,
        }
    }

    #[tokio::test]
    async fn a_time_target_resolves_from_archived_anchors() {
        let dir = tempfile::tempdir().unwrap();
        let env = env(dir.path());
        three_segments(&env, dir.path()).await;
        let archive = Archive::list(&env.cold, NODE, INC).await.unwrap();
        // The newer base replays from LSN 7, whose first anchor is later than
        // 2.5 ms, so the older base serves that target.
        let life = life(vec![base(1, 0, 2), base(2, 6, 7)]);

        let plan = plan_restore(&env, &life, &archive, &time(2 * MS + MS / 2))
            .await
            .unwrap();
        assert_eq!(plan.target_lsn, 5);
        assert_eq!(plan.base.meta.snapshot_id, 1);
        assert_eq!(plan.target_commit_ns, Some(2 * MS));

        let late = plan_restore(&env, &life, &archive, &time(3 * MS + MS / 2))
            .await
            .unwrap();
        assert_eq!(late.target_lsn, 7);
        assert_eq!(late.base.meta.snapshot_id, 2);

        assert!(matches!(
            plan_restore(&env, &life, &archive, &time(MS - 1_000)).await,
            Err(RestoreError::TargetBeforeFirstAnchor {
                first_anchor_ns: MS,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn an_lsn_target_plans_the_segments_the_cut_and_refused_writes() {
        let dir = tempfile::tempdir().unwrap();
        let env = env(dir.path());
        three_segments(&env, dir.path()).await;
        let archive = Archive::list(&env.cold, NODE, INC).await.unwrap();
        let life = life(vec![base(1, 0, 2)]);

        let plan = plan_restore(&env, &life, &archive, &RestoreTarget::Lsn(8))
            .await
            .unwrap();
        let firsts: Vec<u64> = plan.segments.iter().map(|s| s.first_lsn).collect();
        assert_eq!(firsts, [1, 6]);
        assert_eq!(
            plan.refused,
            [7],
            "LSN 7 was refused by a marker above the target"
        );
        assert_eq!(plan.aborts_scanned_through, Some(12));
        let cut = plan.cut.as_ref().unwrap();
        assert_eq!(cut.first_lsn, 6);
        assert_eq!(cut.dropped_records, 1);
        assert_eq!(cut.kept_bytes % 4096, 0);

        assert!(matches!(
            plan_restore(&env, &life, &archive, &RestoreTarget::Lsn(13)).await,
            Err(RestoreError::Coverage(CoverageError::EndsBeforeTarget {
                archived_through: 12,
                target: 13
            }))
        ));
    }

    #[tokio::test]
    async fn a_target_inside_a_missing_segment_names_the_range() {
        let dir = tempfile::tempdir().unwrap();
        let env = env(dir.path());
        three_segments(&env, dir.path()).await;
        let crc = crate::wal::archiver::segment_crc32c(&dir.path().join("seg-6")).unwrap();
        env.cold
            .delete_archived_wal_segment(NODE, INC, 6, &[crc])
            .await
            .unwrap();
        let archive = Archive::list(&env.cold, NODE, INC).await.unwrap();
        let life = life(vec![base(1, 0, 2)]);

        let err = plan_restore(&env, &life, &archive, &RestoreTarget::Lsn(8))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                RestoreError::Coverage(CoverageError::Gap {
                    from: 6,
                    to: 8,
                    replay_start: 1,
                    target: 8
                })
            ),
            "{err}"
        );
        assert!(err.to_string().contains("6..=8"), "{err}");
    }

    fn part(origin: u64, part: u32, parts: u32) -> Rec {
        Rec::Group(crate::wal::WriteGroup::part_of(origin, part, parts))
    }

    /// A batch insert whose two rows landed in two parts of its group: a cut
    /// between them refuses the whole insert, and a cut after both keeps it.
    #[tokio::test]
    async fn a_cut_between_two_rows_of_one_batch_insert_refuses_the_whole_insert() {
        let dir = tempfile::tempdir().unwrap();
        let env = env(dir.path());
        use Rec::*;
        archive_segment(
            &env,
            dir.path(),
            1,
            &[
                Group(crate::wal::WriteGroup::OPENING),
                part(1, 1, 2),
                Put,
                part(1, 2, 2),
                Put,
                Anchor(MS),
            ],
        )
        .await;
        let archive = Archive::list(&env.cold, NODE, INC).await.unwrap();
        let life = life(vec![base(1, 0, 0)]);

        let split = plan_restore(&env, &life, &archive, &RestoreTarget::Lsn(3))
            .await
            .unwrap();
        assert_eq!(
            split.refused,
            [1, 2],
            "the opening record and the first row go with the second"
        );
        let whole = plan_restore(&env, &life, &archive, &RestoreTarget::Lsn(5))
            .await
            .unwrap();
        assert!(whole.refused.is_empty(), "{:?}", whole.refused);
    }

    /// A restore across a snapshot install starts from a base taken after it,
    /// and is refused when there is none.
    #[tokio::test]
    async fn a_restore_across_a_snapshot_install_starts_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let env = env(dir.path());
        use Rec::*;
        archive_segment(
            &env,
            dir.path(),
            1,
            &[Put, Put, Install, Put, Anchor(MS), Put],
        )
        .await;
        let archive = Archive::list(&env.cold, NODE, INC).await.unwrap();

        let before_only = life(vec![base(1, 0, 1)]);
        assert!(matches!(
            plan_restore(&env, &before_only, &archive, &RestoreTarget::Lsn(4)).await,
            Err(RestoreError::NoBaseAfterSnapshotInstall {
                install_lsn: 3,
                target_lsn: 4
            })
        ));
        let early = plan_restore(&env, &before_only, &archive, &RestoreTarget::Lsn(2))
            .await
            .unwrap();
        assert_eq!(
            early.base.meta.snapshot_id, 1,
            "a target before the install replays from the earlier base"
        );

        let both = life(vec![base(1, 0, 1), base(2, 3, 3)]);
        let plan = plan_restore(&env, &both, &archive, &RestoreTarget::Lsn(4))
            .await
            .unwrap();
        assert_eq!(plan.base.meta.snapshot_id, 2);
    }

    #[tokio::test]
    async fn a_target_below_every_base_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let env = env(dir.path());
        three_segments(&env, dir.path()).await;
        let archive = Archive::list(&env.cold, NODE, INC).await.unwrap();
        let life = life(vec![base(1, 4, 5)]);
        assert!(matches!(
            plan_restore(&env, &life, &archive, &RestoreTarget::Lsn(3)).await,
            Err(RestoreError::NoBaseAtOrBelow {
                target_lsn: 3,
                oldest_applied_high_lsn: 5
            })
        ));
    }
}
