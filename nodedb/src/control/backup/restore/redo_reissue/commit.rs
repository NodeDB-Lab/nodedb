// SPDX-License-Identifier: BUSL-1.1

//! Commit restored units as Calvin transactions.
//!
//! A RESTORE re-issues its rows and edge versions in the Calvin sequence, the
//! one ordering domain every other write of them takes. Each batch is a
//! `MetaOp::RestoreRedo` plan. Its transaction locks the rows and edges it
//! writes, as their live writers lock them. Its resolve appends them to the
//! transaction's redo record, each edge version applied at the transaction's
//! ordinal, so a TRUNCATE sequenced before the RESTORE leaves them visible
//! and one sequenced after it hides them. Restored node labels go to each
//! node's home, every label version at its system time. Every replica binds
//! the batch's
//! identities, installs the record as a RESTORE, and raises the tenant's
//! restore mark under the restore's id. The call returns once the
//! transaction committed.

use std::collections::{BTreeMap, HashSet};

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RestoredIdentity, RestoredRedo};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::planner::calvin::submit::submit_calvin_routed;
use crate::control::planner::calvin::tx_class::build_single_vshard_tx_class;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::RedoRecord;

use super::labels::{HomedLabels, label_weight};
use super::units::{CollectionEdges, CollectionUnits, EdgeUnit, RowUnit};

/// Most sub-records or edge versions one restore batch carries.
const MAX_UNITS_PER_BATCH: usize = 512;

/// Most encoded bytes one restore batch carries. A single unit larger than
/// this still commits, alone in its own batch.
const MAX_BYTES_PER_BATCH: usize = 4 * 1024 * 1024;

/// The (collection, key) identities one plan already carries.
type SeenIdentities = HashSet<(String, Vec<u8>)>;

/// Split `units` into batch-sized groups, in order. A unit never splits.
/// `weight` names a unit's count toward [`MAX_UNITS_PER_BATCH`] and its
/// encoded size.
fn batch_units<T>(units: Vec<T>, weight: impl Fn(&T) -> (usize, usize)) -> Vec<Vec<T>> {
    let mut batches = Vec::new();
    let mut current: Vec<T> = Vec::new();
    let (mut count, mut bytes) = (0usize, 0usize);
    for unit in units {
        let (unit_count, unit_bytes) = weight(&unit);
        if !current.is_empty()
            && (count + unit_count > MAX_UNITS_PER_BATCH
                || bytes + unit_bytes > MAX_BYTES_PER_BATCH)
        {
            batches.push(std::mem::take(&mut current));
            (count, bytes) = (0, 0);
        }
        count += unit_count;
        bytes += unit_bytes;
        current.push(unit);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

/// The identities of `carried`, each once, in order.
fn push_identities(
    out: &mut Vec<RestoredIdentity>,
    seen: &mut SeenIdentities,
    carried: Vec<CarriedIdentity>,
) {
    for identity in carried {
        if seen.insert((identity.collection.clone(), identity.pk_bytes.clone())) {
            out.push(RestoredIdentity {
                collection: identity.collection,
                pk_bytes: identity.pk_bytes,
                surrogate: identity.surrogate.as_u32(),
            });
        }
    }
}

/// One batch of rows of the collection stored as `stored`, on `vshard_id`.
fn row_batch(
    stored: &str,
    vshard_id: VShardId,
    batch: Vec<RowUnit>,
) -> crate::Result<RestoredRedo> {
    let mut ops = Vec::new();
    let mut row_changes = Vec::new();
    let mut rows = Vec::new();
    let mut identities = Vec::new();
    let mut seen = HashSet::new();
    for unit in batch {
        ops.extend(unit.ops);
        row_changes.extend(unit.changes);
        rows.extend(unit.rows);
        push_identities(&mut identities, &mut seen, unit.identities);
    }
    let rows_redo = RedoRecord {
        version: 1,
        ops,
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes,
        // A restored row was judged when it was first written.
        user_roles: Vec::new(),
    }
    .to_bytes()?;
    Ok(RestoredRedo {
        vshard: vshard_id.as_u32(),
        rows_redo,
        rows,
        edges: Vec::new(),
        node_labels: Vec::new(),
        collections: vec![stored.to_string()],
        identities,
    })
}

/// One batch of edge versions of the collection stored as `stored`, as one
/// plan per home: each version on every home it lives on, in order.
fn edge_batch(stored: &str, batch: Vec<EdgeUnit>) -> Vec<RestoredRedo> {
    let mut homes: BTreeMap<VShardId, (RestoredRedo, SeenIdentities)> = BTreeMap::new();
    for unit in batch {
        for home in unit.homes.iter() {
            let (plan, seen) = homes.entry(home).or_insert_with(|| {
                (
                    RestoredRedo {
                        vshard: home.as_u32(),
                        rows_redo: Vec::new(),
                        rows: Vec::new(),
                        edges: Vec::new(),
                        node_labels: Vec::new(),
                        collections: vec![stored.to_string()],
                        identities: Vec::new(),
                    },
                    HashSet::new(),
                )
            });
            plan.edges.push(unit.version.clone());
            push_identities(&mut plan.identities, seen, unit.identities.clone());
        }
    }
    homes.into_values().map(|(plan, _)| plan).collect()
}

/// Commit `plans` as one Calvin transaction of RESTORE `restore_id`, and
/// wait until it committed.
async fn commit_batch(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    restore_id: u64,
    plans: Vec<RestoredRedo>,
) -> crate::Result<()> {
    let tasks: Vec<PhysicalTask> = plans
        .into_iter()
        .map(|plan| PhysicalTask {
            tenant_id,
            vshard_id: VShardId::new(plan.vshard),
            database_id,
            plan: PhysicalPlan::Meta(MetaOp::RestoreRedo(Box::new(plan))),
            post_set_op: PostSetOp::None,
            txn_id: None,
        })
        .collect();
    let mut tx_class = build_single_vshard_tx_class(&tasks, tenant_id, &[])?;
    // Every replica installs the rows as restored: AFTER triggers fired when
    // the rows were first written, and do not fire again.
    tx_class.set_event_source(EventSource::Restore.wal_code());
    tx_class.set_restore_id(restore_id);
    if let Some(response) = submit_calvin_routed(state, tx_class).await? {
        crate::control::local_dispatch::reject_data_plane_error(&response)?;
    }
    Ok(())
}

/// A machinery failure with the restore's context. A classified error keeps
/// its class.
fn in_context(e: crate::Error, what: &str) -> crate::Error {
    if crate::error_classify::is_unclassified_failure(&e) {
        crate::Error::Internal {
            detail: format!("restore: re-issuing {what} failed: {e}"),
        }
    } else {
        e
    }
}

/// Commit every row unit of `units` in order, marked under `restore_id`.
/// Returns the transactions committed.
pub(super) async fn commit_collection(
    state: &SharedState,
    tenant_id: TenantId,
    restore_id: u64,
    units: CollectionUnits,
) -> crate::Result<usize> {
    let CollectionUnits {
        database_id,
        collection,
        vshard_id,
        units,
    } = units;
    let stored = nodedb_types::QualifiedCollection::new(database_id, &collection);
    let mut transactions = 0usize;
    for batch in batch_units(units, |unit| (unit.ops.len(), unit.byte_len())) {
        let plan = row_batch(stored.as_str(), vshard_id, batch)?;
        super::super::durable::log_reissue_step(
            state,
            "redo",
            stored.as_str(),
            vshard_id,
            plan.rows.len(),
        );
        commit_batch(state, tenant_id, database_id, restore_id, vec![plan])
            .await
            .map_err(|e| {
                in_context(
                    e,
                    &format!("rows of '{collection}' to vShard {}", vshard_id.as_u32()),
                )
            })?;
        transactions += 1;
    }
    Ok(transactions)
}

/// Commit every edge version of `edges` in order, marked under
/// `restore_id`. Each transaction writes a batch of versions on every home
/// they live on, so an error never leaves a version on one home only.
/// Returns the transactions committed.
pub(super) async fn commit_edges(
    state: &SharedState,
    tenant_id: TenantId,
    restore_id: u64,
    edges: CollectionEdges,
) -> crate::Result<usize> {
    let CollectionEdges {
        database_id,
        collection,
        units,
    } = edges;
    let stored = nodedb_types::QualifiedCollection::new(database_id, &collection);
    let mut transactions = 0usize;
    for batch in batch_units(units, |unit| (1, unit.byte_len())) {
        let versions = batch.len();
        let plans = edge_batch(stored.as_str(), batch);
        if let Some(first) = plans.first() {
            super::super::durable::log_reissue_step(
                state,
                "edges",
                stored.as_str(),
                VShardId::new(first.vshard),
                versions,
            );
        }
        commit_batch(state, tenant_id, database_id, restore_id, plans)
            .await
            .map_err(|e| in_context(e, &format!("edges of '{collection}'")))?;
        transactions += 1;
    }
    Ok(transactions)
}

/// Commit every restored node of `homed` in `database_id`, marked under
/// `restore_id`: one batch of a home's nodes per transaction. Returns the
/// transactions committed.
pub(super) async fn commit_labels(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    restore_id: u64,
    homed: HomedLabels,
) -> crate::Result<usize> {
    let mut transactions = 0usize;
    for (vshard_id, nodes) in homed {
        for batch in batch_units(nodes, label_weight) {
            super::super::durable::log_reissue_step(
                state,
                "node labels",
                "",
                vshard_id,
                batch.len(),
            );
            let plan = RestoredRedo {
                vshard: vshard_id.as_u32(),
                rows_redo: Vec::new(),
                rows: Vec::new(),
                edges: Vec::new(),
                node_labels: batch,
                collections: Vec::new(),
                identities: Vec::new(),
            };
            commit_batch(state, tenant_id, database_id, restore_id, vec![plan])
                .await
                .map_err(|e| {
                    in_context(e, &format!("node labels to vShard {}", vshard_id.as_u32()))
                })?;
            transactions += 1;
        }
    }
    Ok(transactions)
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::RestoredEdgeVersion;
    use nodedb_types::Surrogate;

    use super::*;
    use crate::types::RecordHomes;
    use crate::wal::RedoSubRecord;

    fn unit(ops: usize, payload_len: usize, pk: &str) -> RowUnit {
        RowUnit {
            ops: (0..ops)
                .map(|_| RedoSubRecord {
                    record_type: 0,
                    payload: vec![0; payload_len],
                })
                .collect(),
            identities: vec![CarriedIdentity {
                collection: "c".into(),
                pk_bytes: pk.as_bytes().to_vec(),
                surrogate: Surrogate::new(1),
            }],
            changes: Vec::new(),
            rows: Vec::new(),
        }
    }

    fn edge(dst: &str, system_from: i64) -> EdgeUnit {
        EdgeUnit {
            version: RestoredEdgeVersion {
                collection: "follows".into(),
                src_id: "n0".into(),
                label: "L".into(),
                dst_id: dst.into(),
                src_surrogate: 1,
                dst_surrogate: 2,
                system_from,
                properties: Some(Vec::new()),
                valid_time: Some((0, i64::MAX)),
            },
            identities: vec![
                CarriedIdentity {
                    collection: "follows".into(),
                    pk_bytes: b"n0".to_vec(),
                    surrogate: Surrogate::new(1),
                },
                CarriedIdentity {
                    collection: "follows".into(),
                    pk_bytes: dst.as_bytes().to_vec(),
                    surrogate: Surrogate::new(2),
                },
            ],
            homes: RecordHomes::edge("n0", dst),
        }
    }

    fn cross_shard_peer() -> String {
        (1..4096)
            .map(|i| format!("n{i}"))
            .find(|peer| !RecordHomes::edge("n0", peer).is_single())
            .expect("a cross-shard edge")
    }

    #[test]
    fn batches_cut_between_units_at_the_unit_limit() {
        let units = (0..3)
            .map(|i| unit(MAX_UNITS_PER_BATCH / 2, 1, &i.to_string()))
            .collect();
        let batches = batch_units(units, |unit: &RowUnit| (unit.ops.len(), unit.byte_len()));
        let sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
        assert_eq!(sizes, vec![2, 1]);
    }

    #[test]
    fn an_oversized_unit_commits_alone() {
        let units = vec![
            unit(1, 1, "a"),
            unit(1, MAX_BYTES_PER_BATCH + 1, "b"),
            unit(1, 1, "c"),
        ];
        let sizes: Vec<usize> =
            batch_units(units, |unit: &RowUnit| (unit.ops.len(), unit.byte_len()))
                .iter()
                .map(Vec::len)
                .collect();
        assert_eq!(sizes, vec![1, 1, 1]);
    }

    #[test]
    fn a_row_batch_carries_each_identity_once() {
        let plan = row_batch(
            "c",
            VShardId::new(3),
            vec![unit(1, 1, "a"), unit(1, 1, "a")],
        )
        .expect("row batch");
        assert_eq!(plan.vshard, 3);
        assert_eq!(plan.identities.len(), 1);
        let redo = RedoRecord::from_bytes(&plan.rows_redo).expect("decode rows");
        assert_eq!(redo.ops.len(), 2);
        assert!(plan.edges.is_empty());
    }

    /// A cross-shard edge's versions go to both endpoint homes, each in
    /// order, within one transaction's plans.
    #[test]
    fn a_cross_shard_edge_batch_writes_both_homes() {
        let peer = cross_shard_peer();
        let homes = RecordHomes::edge("n0", &peer);
        let plans = edge_batch("follows", vec![edge(&peer, 1), edge(&peer, 2)]);
        let mut want: Vec<u32> = homes.iter().map(VShardId::as_u32).collect();
        want.sort();
        let targets: Vec<u32> = plans.iter().map(|plan| plan.vshard).collect();
        assert_eq!(targets, want);
        for plan in &plans {
            let order: Vec<i64> = plan.edges.iter().map(|v| v.system_from).collect();
            assert_eq!(order, vec![1, 2], "versions keep their order");
            assert_eq!(plan.identities.len(), 2, "each endpoint binds once");
        }
    }

    #[test]
    fn a_same_shard_edge_batch_writes_one_home() {
        let plans = edge_batch("follows", vec![edge("n0", 1)]);
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].vshard,
            RecordHomes::edge("n0", "n0").owner().as_u32()
        );
    }

    /// Every plan of a restore transaction names its batch's vShard, and
    /// the transaction's write set locks the batch's edges on both homes.
    #[test]
    fn a_restore_transaction_locks_its_edges_on_both_homes() {
        let peer = cross_shard_peer();
        let plans = edge_batch("follows", vec![edge(&peer, 1)]);
        let tasks: Vec<PhysicalTask> = plans
            .into_iter()
            .map(|plan| PhysicalTask {
                tenant_id: TenantId::new(1),
                vshard_id: VShardId::new(plan.vshard),
                database_id: DatabaseId::DEFAULT,
                plan: PhysicalPlan::Meta(MetaOp::RestoreRedo(Box::new(plan))),
                post_set_op: PostSetOp::None,
                txn_id: None,
            })
            .collect();
        let tx_class =
            build_single_vshard_tx_class(&tasks, TenantId::new(1), &[]).expect("tx class");
        let mut participants: Vec<u32> = tx_class
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        participants.sort();
        let mut homes: Vec<u32> = RecordHomes::edge("n0", &peer)
            .iter()
            .map(VShardId::as_u32)
            .collect();
        homes.sort();
        assert_eq!(participants, homes);
    }
}
