// SPDX-License-Identifier: BUSL-1.1

//! Settling a CROSS-SHARD materialized-sum balance for a write whose delta is a
//! difference between two row images.
//!
//! # Why the insert path is not enough
//!
//! [`append_cross_shard_balance_tasks`](super::cross_shard) settles the shapes
//! whose delta the plan already determines: an insert's rows are new by
//! construction, so the whole of each row's value is credited and there is no
//! pre-image to subtract. Every other shape's delta is a difference — an
//! UPDATE's `new − old`, a DELETE's `−old`, a join-key MOVE's `−old` on one
//! target and `+new` on another, a PUT or UPSERT onto an existing row — and the
//! plan carries only one of the two images.
//!
//! The Control Plane already reads the other one. [`super::stored`] does a
//! routed point read for the point shapes and [`super::recon`] scans the
//! predicate for the bulk ones, both so the join values can be resolved at all.
//! Those images are folded HERE, in the same pass, from the same read: a second
//! pass would fold a different snapshot, and two snapshots is two totals.
//!
//! # Deferral is the ABSENCE of a resolution
//!
//! A settled binding's `(target collection, join value)` pairs are removed from
//! the source op's `resolved_sum_targets` before the plan leaves the Control
//! Plane — the pair, so a settled binding never strips the resolution of a
//! sibling binding that happens to read the same join column. The Data
//! Plane skips a delta whose binding is not co-resident AND whose join value it
//! holds no resolution for, so the delta lands exactly once — on the
//! [`ApplyBalanceDelta`](DocumentOp::ApplyBalanceDelta) task appended here,
//! homed on the target's vShard and dual-homed with the source write through
//! Calvin.
//!
//! Nothing is added to the source op to say so. A marker field would have to be
//! spelled on seven more plan variants and seven more replicated-write
//! variants, and every one of them is a place the marker and the appended task
//! can disagree. The resolution IS the marker: the plane-neutral
//! [`sum_target_is_co_resident`](crate::query::sum_target_is_co_resident) tells
//! both planes which bindings can ride the source transaction, and for the rest
//! the presence of a resolved surrogate is what says "nobody else is applying
//! this one".
//!
//! That leaves the Control-Plane orchestrators — `MERGE`, `UPDATE ... FROM`,
//! `INSERT ... SELECT` and the staged-transaction expanders — untouched: they
//! resolve their rows through
//! [`resolve_sum_targets_for_bodies`](super::resolve::resolve_sum_targets_for_bodies)
//! and dispatch through `dispatch_local` without ever reaching this pass, so
//! every join value they resolve stays resolved and the Data Plane keeps
//! applying their deltas exactly as before.
//!
//! # The images are only as good as the version they were read at
//!
//! The read happens before execution, so the source rows can move underneath
//! it, and a delta folded from a moved image is a wrong total that nothing
//! downstream can detect. So every settlement stamps the read it was folded
//! from as a [`ReadSetEntry`]. The entry travels on the transaction's Calvin
//! read-set, and the source core's `read_set_still_current` check votes ABORT —
//! before any row is mutated — when the rows it was folded from have been
//! written since. The statement then retries against a fresh read.

use rust_decimal::Decimal;

use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, PhysicalPlan, ResolvedSumTarget, SumTargetKey,
    resolved_sum_surrogate,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::Surrogate;
use nodedb_types::id::TxnId;

use crate::control::server::shared::session::read_set::{
    EngineTag, ReadKey, ReadOrigin, ReadSetEntry,
};
use crate::engine::document::store::StorageKey;
use crate::query::{sum_target_is_co_resident, sum_target_vshard};
use crate::types::{DatabaseId, KeyRepr, Lsn, TenantId};

/// One source write's plan-time images, ready to settle.
pub(super) struct SettleInput<'a> {
    /// Source collection as it appears on the plan (db-qualified).
    pub source_collection: &'a str,
    /// The write's pre-/post-image pairs, one per row it touches.
    ///
    /// `None` on the left is an insert-shaped row and `None` on the right a
    /// delete-shaped one; a join-key MOVE is one pair whose two images carry
    /// different join values, and the fold derives the two-target split itself.
    pub images: &'a [(Option<serde_json::Value>, Option<serde_json::Value>)],
    /// The ONE source row a point-shaped write rewrites, so the OCC entry names
    /// that row. `None` for a predicate-shaped write, whose observation is the
    /// whole collection and whose entry is collection-scoped accordingly.
    pub source_row: Option<Surrogate>,
    /// Version the images were read at.
    pub read_version_lsn: Lsn,
}

/// What settling produced.
pub(super) struct Settlement {
    /// Balance tasks to append to the plan.
    pub tasks: Vec<PhysicalTask>,
    /// `(target collection, join value)` pairs whose delta now travels on one
    /// of those tasks. The caller removes them from the source op's resolution
    /// — that removal is the whole of the deferral signal.
    ///
    /// Keyed on the PAIR, never on the value: two bindings of one source can
    /// share a join column, and shipping one binding's value would otherwise
    /// strip the other binding's resolution and silently drop its delta.
    pub shipped: Vec<SumTargetKey>,
    /// Read-set entries covering the images the deltas were folded from.
    pub reads: Vec<ReadSetEntry>,
}

impl Settlement {
    /// A settlement that settled nothing — the answer for a source collection
    /// that drives no binding, and for one whose targets are all co-resident.
    pub(super) fn empty() -> Self {
        Self {
            tasks: Vec::new(),
            shipped: Vec::new(),
            reads: Vec::new(),
        }
    }
}

/// Ship every CROSS-SHARD binding's balance for `input` on a task of its own.
///
/// Issues no lookup: `resolved` is the resolution the caller's own pass already
/// produced, and every join value these images can address is in it — both
/// derive their join values from the same plane-neutral rule over the same
/// rows.
///
/// A join value the resolution does not carry is
/// [`MaterializedSumTargetNotFound`](crate::Error::MaterializedSumTargetNotFound):
/// the row addresses a target that does not exist, and failing the statement is
/// what the resolution pass itself does with the same finding. Shipping nothing
/// instead would leave the stored total short of the `SUM(...)` over the source
/// rows.
pub(super) fn settle_cross_shard_images(
    bindings: &[MaterializedSumBinding],
    input: &SettleInput<'_>,
    resolved: &[ResolvedSumTarget],
    txn_id: Option<TxnId>,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<Settlement> {
    let mut settlement = Settlement::empty();
    for binding in bindings {
        if sum_target_is_co_resident(
            database_id,
            input.source_collection,
            &binding.target_collection,
        ) {
            // One core owns both rows: the balance rides the source write's own
            // transaction and is atomic for free.
            continue;
        }
        let mut folded: Vec<crate::query::BindingDelta> = Vec::new();
        for (old, new) in input.images {
            folded.extend(crate::query::binding_image_deltas(
                binding,
                old.as_ref(),
                new.as_ref(),
            )?);
        }
        for (join_value, delta) in crate::query::coalesce_binding_deltas(folded) {
            // A zero net delta leaves the stored total unchanged, so the
            // read-modify-write on the target would rewrite the row
            // byte-for-byte. Shipping a task for it would also make an
            // otherwise single-shard statement multi-shard for nothing. The
            // join value is still recorded as shipped: the source core must not
            // apply it either, and applying nothing is what it does when the
            // resolution is absent.
            let shipped_key = SumTargetKey::new(&binding.target_collection, &join_value);
            if !settlement.shipped.contains(&shipped_key) {
                settlement.shipped.push(shipped_key);
            }
            if delta == Decimal::ZERO {
                continue;
            }
            let surrogate =
                resolved_sum_surrogate(resolved, &binding.target_collection, &join_value)
                    .ok_or_else(|| crate::Error::MaterializedSumTargetNotFound {
                        target_collection: binding.target_collection.clone(),
                        join_column: binding.join_column.clone(),
                        join_value: join_value.clone(),
                    })?;
            settlement.tasks.push(balance_task(BalanceTaskSpec {
                txn_id,
                database_id,
                tenant_id,
                binding,
                surrogate,
                join_value,
                delta,
            }));
        }
    }

    if !settlement.tasks.is_empty() || !settlement.shipped.is_empty() {
        settlement
            .reads
            .push(image_read_entry(input, tenant_id, database_id));
    }
    Ok(settlement)
}

/// Everything one appended balance task needs.
pub(super) struct BalanceTaskSpec<'a> {
    /// Inherited from the source write: the balance belongs to the same
    /// statement, and a task that lost the transaction would commit on its own.
    pub txn_id: Option<TxnId>,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub binding: &'a MaterializedSumBinding,
    /// Target row's identity, resolved on the Control Plane.
    pub surrogate: Surrogate,
    pub join_value: String,
    pub delta: Decimal,
}

/// Build one balance task, homed on the TARGET collection's vShard.
///
/// Shared with the insert path so a balance shipped for an UPDATE and a balance
/// shipped for an INSERT are the same task built the same way.
pub(super) fn balance_task(spec: BalanceTaskSpec<'_>) -> PhysicalTask {
    PhysicalTask {
        tenant_id: spec.tenant_id,
        vshard_id: sum_target_vshard(spec.database_id, &spec.binding.target_collection),
        database_id: spec.database_id,
        plan: PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: nodedb_types::QualifiedCollection::new(
                spec.database_id,
                &spec.binding.target_collection,
            ),
            document_id: StorageKey::for_surrogate(spec.surrogate).to_string(),
            surrogate: spec.surrogate,
            column: spec.binding.target_column.clone(),
            // The exact decimal, as a string: the balance is stored as one for
            // the same reason, and `f64` loses precision past 15 significant
            // digits.
            delta: spec.delta.to_string(),
            join_column: spec.binding.join_column.clone(),
            join_value: spec.join_value,
        }),
        post_set_op: PostSetOp::None,
        txn_id: spec.txn_id,
    }
}

/// The read-set entry covering the images a settlement was folded from.
///
/// A point-shaped write names the ONE row it rewrites, so the entry is that
/// row's surrogate and only a write to THAT row invalidates it. A
/// predicate-shaped write observed which rows matched, which is a
/// collection-scoped observation: any write to the source collection can add a
/// row the scan never saw or move one it did, so the entry is the
/// collection-scoped predicate — the phantom-safe floor, never an
/// under-approximation.
fn image_read_entry(
    input: &SettleInput<'_>,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> ReadSetEntry {
    ReadSetEntry {
        engine: EngineTag::Document,
        database_id,
        tenant_id,
        collection: input.source_collection.to_string(),
        key: match input.source_row {
            Some(surrogate) => ReadKey::Point {
                repr: KeyRepr::Surrogate(surrogate.as_u32()),
            },
            None => ReadKey::Predicate,
        },
        read_lsn: input.read_version_lsn,
        read_version_lsn: input.read_version_lsn,
        // A DERIVATION read, never a read-your-own-write. The image was read
        // from committed base state before this transaction existed, and the
        // delta shipped to the target rests entirely on it; the statement writes
        // the source collection too, so an entry marked `Session` here would be
        // dropped by the own-write exclusion and the fold would never be
        // validated against a concurrent writer.
        origin: ReadOrigin::PlanDerivation,
    }
}

/// Remove every shipped `(target collection, join value)` pair from a
/// resolution, unless a CO-RESIDENT binding of the same source still needs the
/// same pair.
///
/// The removal is the deferral signal, so it has to be exact in both
/// directions: a pair left behind is applied twice, and a pair removed that a
/// co-resident binding still addresses is a delta dropped on the floor. Matching
/// on the join value alone would do both at once when a source drives two
/// bindings that share a join column — the cross-shard one's shipment would
/// strip the co-resident one's entry.
pub(super) fn omit_shipped(
    resolved: &mut Vec<ResolvedSumTarget>,
    shipped: &[SumTargetKey],
    still_needed: &[SumTargetKey],
) {
    resolved.retain(|entry| {
        !shipped.iter().any(|key| entry.matches_key(key))
            || still_needed.iter().any(|key| entry.matches_key(key))
    });
}

/// The `(target collection, join value)` pairs the CO-RESIDENT bindings of
/// `bindings` still address for `images` — the resolution entries
/// [`omit_shipped`] must keep.
pub(super) fn co_resident_target_keys(
    bindings: &[MaterializedSumBinding],
    input: &SettleInput<'_>,
    database_id: DatabaseId,
) -> crate::Result<Vec<SumTargetKey>> {
    let mut keep: Vec<SumTargetKey> = Vec::new();
    for binding in bindings {
        if !sum_target_is_co_resident(
            database_id,
            input.source_collection,
            &binding.target_collection,
        ) {
            continue;
        }
        for (old, new) in input.images {
            for entry in crate::query::binding_image_deltas(binding, old.as_ref(), new.as_ref())? {
                let key = SumTargetKey::new(&binding.target_collection, entry.join_value);
                if !keep.contains(&key) {
                    keep.push(key);
                }
            }
        }
    }
    Ok(keep)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::VShardId;

    const TENANT: TenantId = TenantId::new(7);
    const DB: DatabaseId = DatabaseId::DEFAULT;

    /// A source and a target that provably do NOT share a vShard, so the tests
    /// below exercise the path this module exists for.
    fn cross_shard_pair() -> (String, String) {
        let source = "settle_entries".to_string();
        let home = VShardId::from_collection_in_database(DB, &source);
        let target = (0..2048)
            .map(|i| format!("settle_accounts_{i}"))
            .find(|candidate| VShardId::from_collection_in_database(DB, candidate) != home)
            .unwrap_or_else(|| panic!("the routing domain must hold more than one vShard"));
        (source, target)
    }

    fn binding(target: &str) -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: target.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    fn row(account: &str, amount: i64) -> serde_json::Value {
        serde_json::json!({"account_id": account, "amount": amount})
    }

    /// The resolution the resolve pass produces for `target`: every entry names
    /// the target collection it was resolved against.
    fn resolution(target: &str, entries: &[(&str, u32)]) -> Vec<ResolvedSumTarget> {
        entries
            .iter()
            .map(|(join_value, surrogate)| {
                ResolvedSumTarget::new(target, *join_value, Surrogate::new(*surrogate))
            })
            .collect()
    }

    fn delta_of(task: &PhysicalTask) -> (String, String) {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
                join_value, delta, ..
            }) => (join_value.clone(), delta.clone()),
            other => panic!("a settled balance must be an ApplyBalanceDelta: {other:?}"),
        }
    }

    /// A DELETE ships the row's whole value back OFF the target, homed on the
    /// target's own vShard.
    #[test]
    fn a_delete_ships_a_negative_delta_homed_on_the_target() {
        let (source, target) = cross_shard_pair();
        let old = row("acc-1", 25);
        let images = vec![(Some(old), None)];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            read_version_lsn: Lsn::new(42),
        };
        let settlement = settle_cross_shard_images(
            &[binding(&target)],
            &input,
            &resolution(&target, &[("acc-1", 500)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");

        assert_eq!(settlement.tasks.len(), 1);
        assert_eq!(delta_of(&settlement.tasks[0]).1, "-25");
        assert_eq!(
            settlement.tasks[0].vshard_id,
            sum_target_vshard(DB, &target),
            "the balance must be homed where the TARGET's rows live"
        );
        assert_eq!(
            settlement.shipped,
            vec![SumTargetKey::new(&target, "acc-1")]
        );
    }

    /// The join-key MOVE ships TWO tasks: the abandoned target loses the old
    /// value, the joined target gains the new one.
    #[test]
    fn a_join_key_move_ships_two_sibling_tasks() {
        let (source, target) = cross_shard_pair();
        let images = vec![(Some(row("acc-1", 25)), Some(row("acc-2", 40)))];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            read_version_lsn: Lsn::new(42),
        };
        let settlement = settle_cross_shard_images(
            &[binding(&target)],
            &input,
            &resolution(&target, &[("acc-1", 500), ("acc-2", 501)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");

        assert_eq!(settlement.tasks.len(), 2, "a move touches two target rows");
        assert_eq!(
            delta_of(&settlement.tasks[0]),
            ("acc-1".to_string(), "-25".to_string())
        );
        assert_eq!(
            delta_of(&settlement.tasks[1]),
            ("acc-2".to_string(), "40".to_string())
        );
        assert_eq!(
            settlement.shipped,
            vec![
                SumTargetKey::new(&target, "acc-1"),
                SumTargetKey::new(&target, "acc-2")
            ],
            "BOTH sides must be deferred; a side left resolved is applied twice"
        );
    }

    /// An UPDATE that keeps its join key ships only the DIFFERENCE.
    #[test]
    fn an_in_place_update_ships_the_difference() {
        let (source, target) = cross_shard_pair();
        let images = vec![(Some(row("acc-1", 25)), Some(row("acc-1", 40)))];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            read_version_lsn: Lsn::new(42),
        };
        let settlement = settle_cross_shard_images(
            &[binding(&target)],
            &input,
            &resolution(&target, &[("acc-1", 500)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");
        assert_eq!(delta_of(&settlement.tasks[0]).1, "15");
    }

    /// A net-zero UPDATE ships no task — and STILL defers, because the source
    /// core applying its own non-zero halves would double-count them.
    #[test]
    fn a_net_zero_update_defers_without_shipping() {
        let (source, target) = cross_shard_pair();
        let images = vec![(Some(row("acc-1", 25)), Some(row("acc-1", 25)))];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            read_version_lsn: Lsn::new(42),
        };
        let settlement = settle_cross_shard_images(
            &[binding(&target)],
            &input,
            &resolution(&target, &[("acc-1", 500)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");
        assert!(settlement.tasks.is_empty());
        assert_eq!(
            settlement.shipped,
            vec![SumTargetKey::new(&target, "acc-1")]
        );
    }

    /// A CO-RESIDENT binding is left entirely alone: nothing shipped, nothing
    /// deferred, no read entry — the source transaction still owns it.
    #[test]
    fn a_co_resident_binding_is_not_settled_here() {
        let (source, _) = cross_shard_pair();
        let images = vec![(Some(row("acc-1", 25)), None)];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            read_version_lsn: Lsn::new(42),
        };
        // A binding whose target IS the source collection is co-resident by
        // construction, whatever the hash function does.
        let settlement = settle_cross_shard_images(
            &[binding(&source)],
            &input,
            &resolution(&source, &[("acc-1", 500)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");
        assert!(settlement.tasks.is_empty());
        assert!(settlement.shipped.is_empty());
        assert!(settlement.reads.is_empty());
    }

    /// The settled images are stamped as a read of the ONE source row, so a
    /// concurrent write to that row aborts the statement rather than committing
    /// a delta folded from an image that has moved.
    #[test]
    fn a_settlement_stamps_the_image_it_was_folded_from() {
        let (source, target) = cross_shard_pair();
        let images = vec![(Some(row("acc-1", 25)), None)];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            read_version_lsn: Lsn::new(42),
        };
        let settlement = settle_cross_shard_images(
            &[binding(&target)],
            &input,
            &resolution(&target, &[("acc-1", 500)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");
        assert_eq!(settlement.reads.len(), 1);
        assert_eq!(settlement.reads[0].collection, source);
        assert_eq!(
            settlement.reads[0].key,
            ReadKey::Point {
                repr: KeyRepr::Surrogate(11)
            }
        );
        assert_eq!(settlement.reads[0].read_version_lsn, Lsn::new(42));
    }

    /// A predicate-shaped settlement observes the whole collection, so its
    /// entry is collection-scoped: a row that JOINS the match set after the
    /// scan must invalidate it too.
    #[test]
    fn a_predicate_settlement_stamps_a_collection_scoped_read() {
        let (source, target) = cross_shard_pair();
        let images = vec![(Some(row("acc-1", 25)), None)];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: None,
            read_version_lsn: Lsn::new(42),
        };
        let settlement = settle_cross_shard_images(
            &[binding(&target)],
            &input,
            &resolution(&target, &[("acc-1", 500)]),
            None,
            TENANT,
            DB,
        )
        .expect("settle");
        assert_eq!(settlement.reads[0].key, ReadKey::Predicate);
    }

    /// A pair a shipped task addresses is removed from the resolution — that
    /// removal IS the instruction that stops the source core applying it —
    /// unless a co-resident binding still needs the same pair resolved.
    #[test]
    fn shipping_removes_the_resolution_but_keeps_what_is_still_needed() {
        let mut resolved = resolution("accounts", &[("acc-1", 500), ("acc-2", 501)]);
        omit_shipped(
            &mut resolved,
            &[
                SumTargetKey::new("accounts", "acc-1"),
                SumTargetKey::new("accounts", "acc-2"),
            ],
            &[SumTargetKey::new("accounts", "acc-2")],
        );
        assert_eq!(resolved, resolution("accounts", &[("acc-2", 501)]));
    }

    /// Shipping a CROSS-SHARD binding's join value must not strip the
    /// resolution of a SIBLING binding that reads the same join column into a
    /// different target.
    ///
    /// Keyed on the join value alone, the shipment below would remove both
    /// entries and the co-resident target's delta would be dropped: no error,
    /// a total silently short by the row's whole value.
    #[test]
    fn shipping_one_target_leaves_a_sibling_targets_resolution_intact() {
        let mut resolved = vec![
            ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(500)),
            ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(900)),
        ];
        omit_shipped(
            &mut resolved,
            &[SumTargetKey::new("accounts", "acc-1")],
            &[SumTargetKey::new("audit_totals", "acc-1")],
        );
        assert_eq!(
            resolved,
            vec![ResolvedSumTarget::new(
                "audit_totals",
                "acc-1",
                Surrogate::new(900)
            )]
        );
    }
}
