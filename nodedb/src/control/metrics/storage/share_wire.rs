// SPDX-License-Identifier: BUSL-1.1

//! Convert a [`StorageShare`] to and from its wire form.
//!
//! The wire carries the sample's age, never a clock reading. The receiver
//! places the sample on its own clock by subtracting the age from the
//! receive time, so node clocks never compare.

use std::collections::BTreeMap;
use std::time::Instant;

use nodedb_cluster::storage_share::{GroupShare, ShareSample, StorageShareResponse, TenantBytes};
use nodedb_types::{DatabaseId, TenantId};

use super::shares::{GroupTotals, StorageShare, TenantTotals};

/// The answer of node `node_id` from its latest share, aged at `now`.
pub fn share_to_wire(
    node_id: u64,
    share: Option<&StorageShare>,
    now: Instant,
) -> StorageShareResponse {
    let sample = share.map(|share| ShareSample {
        sample_age_ms: millis(now.saturating_duration_since(share.sampled_at)),
        groups: share
            .groups
            .iter()
            .map(|(group_id, group)| GroupShare {
                group_id: *group_id,
                term: group.term,
                totals: tenant_bytes(&group.totals),
            })
            .collect(),
    });
    StorageShareResponse { node_id, sample }
}

/// The share a peer answered, placed on this node's clock at `received_at`.
pub fn share_from_wire(sample: ShareSample, received_at: Instant) -> StorageShare {
    let age = std::time::Duration::from_millis(sample.sample_age_ms);
    let sampled_at = received_at.checked_sub(age).unwrap_or(received_at);
    let groups: BTreeMap<u64, GroupTotals> = sample
        .groups
        .into_iter()
        .map(|group| {
            let mut totals = TenantTotals::new();
            for row in group.totals {
                let total = totals
                    .entry((
                        DatabaseId::new(row.database_id),
                        TenantId::new(row.tenant_id),
                    ))
                    .or_insert(0);
                *total = total.saturating_add(row.bytes);
            }
            (
                group.group_id,
                GroupTotals {
                    term: group.term,
                    totals,
                },
            )
        })
        .collect();
    StorageShare {
        groups,
        led_vshards: Vec::new(),
        sampled_at,
    }
}

fn tenant_bytes(totals: &TenantTotals) -> Vec<TenantBytes> {
    let mut rows: Vec<TenantBytes> = totals
        .iter()
        .map(|((database_id, tenant_id), bytes)| TenantBytes {
            database_id: database_id.as_u64(),
            tenant_id: tenant_id.as_u64(),
            bytes: *bytes,
        })
        .collect();
    rows.sort_unstable_by_key(|row| (row.database_id, row.tenant_id));
    rows
}

fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_share_survives_the_wire() {
        let sampled_at = Instant::now();
        let mut totals = TenantTotals::new();
        totals.insert((DatabaseId::new(1024), TenantId::new(2)), 300);
        totals.insert((DatabaseId::new(0), TenantId::new(1)), 40);
        let mut groups = BTreeMap::new();
        groups.insert(1, GroupTotals { term: 5, totals });
        groups.insert(2, GroupTotals::default());
        let share = StorageShare {
            groups,
            led_vshards: vec![0, 1, 2],
            sampled_at,
        };

        let answered_at = sampled_at + Duration::from_millis(700);
        let wire = share_to_wire(3, Some(&share), answered_at);
        assert_eq!(wire.node_id, 3);
        let sample = wire.sample.expect("a sample");
        assert_eq!(sample.sample_age_ms, 700);

        let received_at = answered_at + Duration::from_millis(5);
        let back = share_from_wire(sample, received_at);
        assert_eq!(back.groups, share.groups);
        assert!(back.led_vshards.is_empty());
        assert_eq!(back.sampled_at, received_at - Duration::from_millis(700));
    }

    #[test]
    fn no_share_answers_no_sample() {
        assert!(share_to_wire(4, None, Instant::now()).sample.is_none());
    }
}
