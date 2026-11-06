// SPDX-License-Identifier: BUSL-1.1

//! How one source entry maps onto the mirror's groups, and what a cut raises.

use std::collections::BTreeMap;

use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};

/// The odd step between two groups' share keys.
const SHARE_KEY_STEP: u64 = 0x9E37_79B9_7F4A_7C15;

/// Split an entry that names its group's vShards along the mirror's groups: a
/// tenant purge or a retention drop. The source lists its own group's vShards,
/// and each mirror group takes its share at this log position.
///
/// A vShard belongs to one source group, so each share keeps its place against
/// every other write of its vShards. Each share of a split takes its own key, and
/// a re-proposal of that share derives the same one.
pub(super) fn split_by_mirror_group(
    entry: &ReplicatedEntry,
    group_of: impl Fn(u32) -> crate::Result<u64>,
) -> crate::Result<Vec<ReplicatedEntry>> {
    let vshards = match &entry.write {
        ReplicatedWrite::TenantPurge { vshards, .. }
        | ReplicatedWrite::RetentionDrop { vshards, .. } => vshards,
        _ => return Ok(vec![entry.clone()]),
    };
    let mut shares: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
    for vshard in vshards {
        shares.entry(group_of(*vshard)?).or_default().push(*vshard);
    }
    if shares.len() <= 1 {
        return Ok(vec![entry.clone()]);
    }
    Ok(shares
        .into_iter()
        .filter_map(|(group, vshards)| {
            let mut share = entry.clone();
            share.vshard_id = vshards.first().copied()?;
            share.idempotency_key = entry
                .idempotency_key
                .wrapping_add(group.wrapping_mul(SHARE_KEY_STEP))
                | 1;
            // The clone holds the rest of the entry. Only the vShards differ.
            if let ReplicatedWrite::TenantPurge {
                vshards: shared, ..
            }
            | ReplicatedWrite::RetentionDrop {
                vshards: shared, ..
            } = &mut share.write
            {
                *shared = vshards;
            }
            Some(share)
        })
        .collect())
}

/// The surrogate floor a cut needs: its mark's high-water mark, or a key the
/// cut binds above it. A bind the mark's node has not seen reaches the mirror
/// in the same cut.
pub(super) fn surrogate_floor<'e>(
    mark: u32,
    entries: impl Iterator<Item = &'e ReplicatedEntry>,
) -> u32 {
    entries
        .filter_map(|entry| match &entry.write {
            ReplicatedWrite::SurrogateBind { binds, .. } => {
                binds.iter().map(|(_, surrogate)| *surrogate).max()
            }
            _ => None,
        })
        .fold(mark, u32::max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::types::RetentionOp;

    fn bind(candidates: &[u32]) -> ReplicatedEntry {
        ReplicatedEntry::new(
            1,
            1400,
            2,
            ReplicatedWrite::SurrogateBind {
                collection: "docs".into(),
                binds: candidates
                    .iter()
                    .map(|candidate| (candidate.to_le_bytes().to_vec(), *candidate))
                    .collect(),
            },
        )
    }

    #[test]
    fn a_floor_covers_the_mark_and_each_bind() {
        let entries = [bind(&[3, 90]), bind(&[7])];
        assert_eq!(surrogate_floor(10, entries.iter()), 90);
        assert_eq!(surrogate_floor(500, entries.iter()), 500);
    }

    /// One source group's drop lands in each mirror group its vShards reach, at
    /// one decision. A re-proposal derives the same keys.
    #[test]
    fn a_retention_drop_splits_along_the_mirror_groups() {
        let entry = ReplicatedEntry::new(
            1,
            1400,
            0,
            ReplicatedWrite::RetentionDrop {
                op: RetentionOp::TimeseriesPartitions {
                    collection: "metrics".into(),
                    max_age_ms: 5,
                    decided_ms: 100,
                },
                vshards: vec![0, 1, 2, 3],
            },
        );
        let group_of = |vshard: u32| Ok(u64::from(vshard % 2) + 1);
        let shares = split_by_mirror_group(&entry, group_of).expect("split");
        assert_eq!(shares.len(), 2);
        let again = split_by_mirror_group(&entry, group_of).expect("split");
        for (share, repeat) in shares.iter().zip(&again) {
            assert_eq!(share.idempotency_key, repeat.idempotency_key);
            let ReplicatedWrite::RetentionDrop { op, vshards } = &share.write else {
                panic!("a share stays a drop");
            };
            assert!(vshards.contains(&share.vshard_id));
            assert!(matches!(
                op,
                RetentionOp::TimeseriesPartitions {
                    decided_ms: 100,
                    ..
                }
            ));
        }
        assert_ne!(shares[0].idempotency_key, shares[1].idempotency_key);
    }

    #[test]
    fn a_drop_in_one_group_stays_whole() {
        let entry = ReplicatedEntry::new(
            1,
            1400,
            0,
            ReplicatedWrite::RetentionDrop {
                op: RetentionOp::ContinuousAggregates { decided_ms: 100 },
                vshards: vec![0, 1],
            },
        );
        let shares = split_by_mirror_group(&entry, |_| Ok(1)).expect("split");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].idempotency_key, entry.idempotency_key);
    }
}
