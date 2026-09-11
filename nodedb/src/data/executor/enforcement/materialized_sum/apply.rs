// SPDX-License-Identifier: BUSL-1.1

//! Applying folded materialized-sum deltas to their target rows, for every
//! binding the plan did not defer onto a task of its own.
//!
//! The target write is a full document write, not a byte poke at the store. It
//! goes through [`CoreLoop::apply_point_put`] inside the CALLER'S transaction,
//! so the target row gets everything any other write of that row would get —
//! WAL-consistent transaction membership, inverted-index maintenance, secondary
//! and versioned index maintenance, column statistics, document-cache
//! population, aggregate-cache invalidation — and lands or rolls back together
//! with the source row that caused it. The read-modify-write itself lives in
//! [`super::rmw`], shared with the cross-shard handler so the two paths cannot
//! total differently.
//!
//! Writing with a bare `sparse.put` has none of those. A balance updated that
//! way leaves the target's FTS postings, secondary indexes, and column
//! statistics asserting the stale value, and puts the row's new bytes outside
//! the transaction the source row is landing in.
//!
//! # A DEFERRED target is not applied here
//!
//! This transaction belongs to the source collection's core, and a target that
//! homes to a different vShard has no rows on it — a write here would land the
//! balance in a store no reader of the target collection ever looks at. When the
//! Control Plane can settle such a binding's delta at plan time it appends an
//! [`ApplyBalanceDelta`](nodedb_physical::physical_plan::DocumentOp::ApplyBalanceDelta)
//! task homed on the target's vShard, dual-homed with the source write through
//! Calvin. Two things say so here, and between them the delta is applied
//! exactly once:
//!
//! * an INSERT-shaped write names the binding on the plan's deferral list, and
//!   a named binding is skipped;
//! * every other shape's balance is settled from row IMAGES, and the settlement
//!   REMOVES that binding's `(target collection, join value)` pair from the
//!   plan's resolution — so a cross-shard target with no resolved surrogate is
//!   one somebody else is applying.
//!
//! Neither is re-derived: a second derivation of "did the Control Plane defer
//! this?" is free to disagree with the first, and disagreement is a
//! double-counted or a dropped balance. Only the co-residency question is asked
//! on both planes, and it is asked through the one plane-neutral function
//! ([`sum_target_is_co_resident`](crate::query::sum_target_is_co_resident))
//! that exists so the two answers cannot differ.
//!
//! A cross-shard target the plan DOES resolve is still applied here: the
//! Control-Plane orchestrators (`MERGE`, `UPDATE ... FROM`, `INSERT ... SELECT`,
//! and the staged-transaction expanders) resolve their rows and dispatch their
//! own concrete work without appending a sibling balance task, so for them the
//! resolution still means "this transaction owns it".
//!
//! # Identity comes from the plan, never from a store probe
//!
//! Rows are keyed by an 8-hex surrogate
//! ([`surrogate_to_doc_id`](crate::engine::document::store::surrogate_to_doc_id)),
//! so a join-key VALUE is not a storage key. The Control Plane resolves each
//! join value to its target row's surrogate at plan time and the resolution
//! arrives on
//! [`EnforcementCtx::resolved_targets`](crate::data::executor::enforcement::images::EnforcementCtx).
//! Deriving it here would mean a Data-Plane copy of the primary-key → surrogate
//! map, which is Control-Plane catalog state.
//!
//! A replica reaches this code too — replication ships the SOURCE row and every
//! node re-executes the plan, so every node folds its own delta — and it gets
//! the same resolution the same way: carried on the replicated record, decided
//! once by the node that accepted the statement. Nothing on this path resolves
//! anything, on a leader or anywhere else.

use redb::WriteTransaction;
use rust_decimal::Decimal;

use nodedb_physical::physical_plan::MaterializedSumBinding;
use nodedb_types::Surrogate;

use super::delta::fold_sum_deltas;
use super::rmw::BalanceRmw;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
use crate::types::DatabaseId;

/// A target row this write updated, captured so a transactional caller can
/// reverse it.
pub(in crate::data::executor) struct TargetWrite {
    /// Target collection name.
    pub collection: String,
    /// Storage key of the target row — the hex-encoded surrogate.
    pub document_id: String,
    /// The target row's surrogate, so an undo entry addresses the same identity
    /// the forward write used. The old code had no surrogate to record and
    /// pushed `Surrogate::ZERO`.
    pub surrogate: Surrogate,
    /// The MessagePack body this write handed to `apply_point_put` — NOT the
    /// bytes that reached storage.
    ///
    /// A durable redo record replays through `apply_point_put`, which encodes
    /// the body into whatever the target collection stores. Journalling the
    /// STORED bytes would hand a strict target's Binary Tuple back to the
    /// encoder on replay and store a tuple of a tuple.
    pub body: Vec<u8>,
    /// Everything the derived write mutated: the pre-image, the versioned and
    /// secondary index tuples, the vector and spatial inserts, the column-stats
    /// pre-images. A transactional caller reverses a target write with exactly
    /// the undo entries it uses for the source row — anything less leaves the
    /// target's indexes asserting a balance a rollback removed.
    pub outcome: PointPutOutcome,
}

impl CoreLoop {
    /// Apply every binding's folded deltas to their target rows.
    ///
    /// Each binding is folded (see
    /// [`fold_sum_deltas`](super::delta::fold_sum_deltas)) into signed deltas,
    /// the deltas for one target are summed so a single row is read and written
    /// once, and each surviving non-zero total is applied as a read-modify-write
    /// through `apply_point_put` inside `txn`.
    ///
    /// A `&mut CoreLoop` because the target write is a real document write.
    /// Bindings are passed in rather than read from `doc_configs` here for the
    /// same reason: the caller owns the immutable borrow of the config.
    pub(in crate::data::executor) fn apply_materialized_sums(
        &mut self,
        txn: &WriteTransaction,
        ctx: &EnforcementCtx<'_>,
        bindings: &[MaterializedSumBinding],
        images: &RowImages<'_>,
    ) -> crate::Result<Vec<TargetWrite>> {
        let mut writes: Vec<TargetWrite> = Vec::new();
        for binding in bindings {
            // Whether one core owns both rows. The Control Plane asked the SAME
            // question, from the same plane-neutral function, when it decided
            // whether the balance could ride this transaction — so the two
            // cannot disagree about which bindings this core is responsible
            // for.
            let co_resident = crate::query::sum_target_is_co_resident(
                DatabaseId::new(ctx.database_id),
                ctx.collection,
                &binding.target_collection,
            );
            // A binding the plan DEFERRED is applied by its own
            // `ApplyBalanceDelta` task on the target's core. Applying it here as
            // well would double-count it — and this transaction belongs to the
            // source's core, so the row it wrote would land in a store no reader
            // of the target collection consults.
            //
            // The deferral is read off the plan, never re-derived here, for the
            // same reason the target's identity is: the Control Plane decided it
            // when it appended the sibling task, and a second derivation is free
            // to disagree with the first.
            if ctx
                .deferred_sum_targets
                .contains(&binding.target_collection)
            {
                continue;
            }
            for (join_value, delta) in coalesce(fold_sum_deltas(binding, images)?) {
                // A zero net delta leaves the stored total unchanged, so the
                // read-modify-write would rewrite the row byte-for-byte. An
                // UPDATE that touched no amount produces exactly this.
                if delta == Decimal::ZERO {
                    continue;
                }
                // A cross-shard target the plan carries NO resolution for was
                // settled at plan time and travels on its own
                // `ApplyBalanceDelta` task, homed where the target's rows
                // actually live. Applying it here would count it twice — and
                // this transaction belongs to the source's core, so the row it
                // wrote would land in a store no reader of the target
                // collection consults.
                //
                // The absence of the resolution IS the instruction; nothing is
                // re-derived. A cross-shard target the plan DID resolve was
                // resolved by a Control-Plane orchestrator that ships no
                // sibling task — `MERGE`, `UPDATE ... FROM`, `INSERT ... SELECT`
                // and the staged-transaction expanders all dispatch their own
                // concrete work — so it is still this transaction's to apply.
                if !co_resident
                    && resolved_target(ctx, &binding.target_collection, &join_value).is_none()
                {
                    continue;
                }
                match self.apply_one_delta(txn, ctx, binding, &join_value, delta) {
                    Ok(write) => writes.push(write),
                    Err(e) => {
                        // The caller drops `txn`, which reverses every target
                        // row this pass already wrote — but not the read-through
                        // cache entries those writes populated. Left behind, they
                        // serve balances that no longer exist in storage.
                        for write in &writes {
                            let key = crate::engine::document::store::StorageKey::for_surrogate(
                                write.surrogate,
                            );
                            self.doc_cache.invalidate(
                                ctx.database_id,
                                ctx.tid,
                                &write.collection,
                                &key,
                            );
                        }
                        return Err(e);
                    }
                }
            }
        }
        Ok(writes)
    }

    /// Resolve the target row and hand the balance move to the shared
    /// read-modify-write.
    ///
    /// Identity comes from the plan and only from the plan: the surrogate
    /// resolved on the Control Plane is the one thing that may address the
    /// target row.
    ///
    /// A join value the plan carries no entry for is
    /// [`MaterializedSumResolutionMissing`](crate::Error::MaterializedSumResolutionMissing),
    /// NOT `MaterializedSumTargetNotFound`. "The target row does not exist" is a
    /// verdict on the user's statement and it is reached on the Control Plane,
    /// where the resolution pass fails the statement before any row is written.
    /// Whatever reaches here has already passed that gate — including every
    /// write a replica re-executes, which the leader accepted and resolved — so
    /// an absent entry means the resolution and the fold disagree about which
    /// rows participate. On a replica there is additionally no user to report a
    /// user error to, and reporting one would blame the application for a row
    /// the leader found.
    fn apply_one_delta(
        &mut self,
        txn: &WriteTransaction,
        ctx: &EnforcementCtx<'_>,
        binding: &MaterializedSumBinding,
        join_value: &str,
        delta: Decimal,
    ) -> crate::Result<TargetWrite> {
        let surrogate =
            resolved_target(ctx, &binding.target_collection, join_value).ok_or_else(|| {
                crate::Error::MaterializedSumResolutionMissing {
                    target_collection: binding.target_collection.clone(),
                    join_column: binding.join_column.clone(),
                    join_value: join_value.to_string(),
                }
            })?;
        self.apply_balance_delta(
            txn,
            &BalanceRmw {
                database_id: ctx.database_id,
                tid: ctx.tid,
                target_collection: &binding.target_collection,
                target_column: &binding.target_column,
                surrogate,
                delta,
                join_column: &binding.join_column,
                join_value,
                wal_lsn: ctx.wal_lsn,
            },
        )
    }
}

/// The surrogate the Control Plane resolved THIS BINDING's join value to.
///
/// The binding's target collection is half the lookup key. One source may drive
/// two bindings that read the same join column into different targets: keyed on
/// the value alone, the second binding would find the first binding's entry and
/// this pass would write its balance into a row of the wrong collection —
/// silently, since a resolution IS present.
fn resolved_target(
    ctx: &EnforcementCtx<'_>,
    target_collection: &str,
    join_value: &str,
) -> Option<Surrogate> {
    nodedb_physical::physical_plan::resolved_sum_surrogate(
        ctx.resolved_targets,
        target_collection,
        join_value,
    )
}

/// Sum the deltas that address the same target, preserving first-seen order.
///
/// Two deltas against one row would otherwise be two read-modify-writes, and
/// the second would have to observe the first — which is the whole reason the
/// plain read goes through the caller's transaction. Summing first makes the
/// question moot for deltas produced by a single fold.
fn coalesce(deltas: Vec<super::delta::SumDelta>) -> Vec<(String, Decimal)> {
    let mut totals: Vec<(String, Decimal)> = Vec::with_capacity(deltas.len());
    for delta in deltas {
        match totals
            .iter()
            .position(|(join_value, _)| join_value.as_str() == delta.join_value.as_str())
        {
            Some(index) => totals[index].1 += delta.delta,
            None => totals.push((delta.join_value, delta.delta)),
        }
    }
    totals
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;
    use crate::data::executor::enforcement::funnel::{
        WriteEnforcementOutcome, run_write_enforcement,
    };
    use crate::data::executor::handlers::bulk_dml::{
        BulkDeleteParams, BulkUpdateParams, OllpPrediction,
    };
    use crate::data::executor::handlers::document::write::DocumentBatchInsertParams;
    use crate::data::executor::handlers::update_from_join::UpdateFromJoinParams;
    use crate::data::executor::strict_format;
    use crate::engine::document::store::{CollectionConfig, surrogate_to_doc_id};
    use crate::types::TenantId;
    use nodedb_physical::physical_plan::{ResolvedSumTarget, StorageMode, UpdateValue};
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::{QualifiedCollection, Value};

    const DB: u64 = 0;
    const TID: u64 = 1;
    /// The source collection. Every test below drives the INLINE fold, whose
    /// entire premise is that one core owns both rows: the target is seeded into
    /// and read back out of THIS core's own store, which is only a meaningful
    /// assertion when the two collections are co-resident.
    const SOURCE: &str = "local_charges";
    /// A target that shares `SOURCE`'s vShard — asserted by
    /// [`the_local_fixture_is_co_resident`], not assumed.
    const TARGET: &str = "local_balances";
    /// A target that does NOT share `SOURCE`'s vShard, for the one test that
    /// pins the opposite rule.
    const REMOTE_TARGET: &str = "remote_balances";
    const ACCOUNT: &str = "a1";
    const TARGET_SURROGATE: Surrogate = Surrogate(4242);

    /// A strict target: `owner` is untouched by the sum and exists purely to
    /// prove the whole row survived the write-back.
    fn strict_target_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::required("owner", ColumnType::String),
            ColumnDef::required("balance", ColumnType::String),
        ])
        .expect("schema")
    }

    fn binding_onto(target: &str) -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: target.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        }
    }

    /// Register the source collection so the funnel finds the binding on it.
    fn register_source(core: &mut CoreLoop) {
        register_source_onto(core, TARGET);
    }

    /// Register the source collection with a binding onto `target`.
    fn register_source_onto(core: &mut CoreLoop, target: &str) {
        let mut config = CollectionConfig::new(SOURCE);
        config.enforcement.materialized_sum_sources = vec![binding_onto(target)];
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), SOURCE.to_string()),
            config,
        );
    }

    /// The premise every other test in this module rests on.
    ///
    /// The inline fold writes the target row inside the SOURCE write's
    /// transaction, on the source's core — and each core opens its own document
    /// store, so that write is only visible to a reader of the target collection
    /// when both collections home to the same vShard. Asserted rather than
    /// assumed: a change to the collection hash that silently made this pair
    /// cross-shard would otherwise turn every assertion below into a test of the
    /// DEFERRED path wearing the inline path's name.
    #[test]
    fn the_local_fixture_is_co_resident() {
        assert!(
            crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
            "'{SOURCE}' and '{TARGET}' must share a vShard for the inline fold to be observable"
        );
        assert!(
            !crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, REMOTE_TARGET),
            "'{REMOTE_TARGET}' must NOT share '{SOURCE}'s vShard; it pins the deferred path"
        );
    }

    /// Drive one source INSERT through the funnel and commit its transaction.
    fn insert_source_row(core: &mut CoreLoop, new_doc: &serde_json::Value) {
        let txn = core.sparse.begin_write().expect("begin write");
        let outcome: WriteEnforcementOutcome = run_write_enforcement(
            core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &[ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc },
        )
        .expect("enforcement must apply the materialized sum");
        assert_eq!(
            outcome.target_writes.len(),
            1,
            "the source row credits exactly one target"
        );
        assert_eq!(outcome.target_writes[0].surrogate, TARGET_SURROGATE);
        txn.commit().expect("commit");
    }

    /// MATERIALIZED SUM over a `document_strict` target must total correctly AND
    /// leave the row a Binary Tuple.
    ///
    /// The target is a different collection from the source, so its encoding has
    /// to be resolved from `doc_configs` on BOTH halves of the read-modify-write.
    /// Reading with the schemaless decoder failed on every strict row (the
    /// feature was simply broken there); writing msgpack back would have been
    /// worse — the row survives the statement and is unreadable to every strict
    /// reader afterwards.
    ///
    /// The target row is seeded under `surrogate_to_doc_id`, the key every
    /// reader of that collection uses. Seeding under the raw join VALUE would
    /// only prove that a lookup keyed by the same wrong value finds it.
    #[test]
    fn a_strict_target_totals_correctly_and_stays_a_binary_tuple() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());

        let schema = strict_target_schema();
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET).with_storage_mode(StorageMode::Strict {
                schema: schema.clone(),
            }),
        );
        register_source(&mut core);

        // Seed the target row in the encoding a strict collection actually
        // stores: a Binary Tuple, not msgpack.
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), Value::String(ACCOUNT.into()));
        row.insert("owner".to_string(), Value::String("alice".into()));
        row.insert("balance".to_string(), Value::String("100".into()));
        let tuple = strict_format::value_to_binary_tuple(&Value::Object(row), &schema, TARGET)
            .expect("encode seed tuple");
        let target_key = nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE);
        core.sparse
            .put(DB, TID, TARGET, &target_key, &tuple)
            .expect("seed target row");

        insert_source_row(
            &mut core,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 25}),
        );
        insert_source_row(
            &mut core,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 75}),
        );

        let stored = core
            .sparse
            .get(DB, TID, TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");

        // The stored bytes must still be a Binary Tuple. `binary_tuple_to_json`
        // is what every reader of this collection uses; if the write-back had
        // emitted msgpack this returns `None` and the row is lost.
        let decoded = strict_format::binary_tuple_to_json(&stored, &schema)
            .expect("the stored row must still be a readable Binary Tuple");

        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("200"),
            "100 + 25 + 75 must be totalled onto the strict row: {decoded:?}"
        );
        assert_eq!(
            decoded.get("owner").and_then(|v| v.as_str()),
            Some("alice"),
            "columns the sum does not touch must survive the re-encode"
        );
        assert_eq!(decoded.get("id").and_then(|v| v.as_str()), Some(ACCOUNT));
    }

    /// The schemaless target keeps working — the encoding is chosen per
    /// collection, so fixing strict must not have moved schemaless onto the
    /// strict encoder.
    #[test]
    fn a_schemaless_target_still_totals_and_stays_msgpack() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());

        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );
        register_source(&mut core);

        let seed = serde_json::json!({"id": ACCOUNT, "owner": "alice", "balance": "100"});
        let body = doc_format::encode_to_msgpack(&seed);
        let target_key = nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE);
        core.sparse
            .put(DB, TID, TARGET, &target_key, &body)
            .expect("seed target row");

        insert_source_row(
            &mut core,
            &serde_json::json!({"account_id": ACCOUNT, "amount": 50}),
        );

        let stored = core
            .sparse
            .get(DB, TID, TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");
        let decoded =
            doc_format::decode_document(&stored).expect("a schemaless row must stay msgpack");
        assert_eq!(decoded.get("balance").and_then(|v| v.as_str()), Some("150"));
        assert_eq!(decoded.get("owner").and_then(|v| v.as_str()), Some("alice"));
    }

    /// A DELETE subtracts, against the SAME storage key an insert credited.
    #[test]
    fn a_delete_subtracts_from_the_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );
        register_source(&mut core);

        let target_key = nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE);
        let seed = serde_json::json!({"id": ACCOUNT, "balance": "100"});
        core.sparse
            .put(
                DB,
                TID,
                TARGET,
                &target_key,
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");

        let old_doc = serde_json::json!({"account_id": ACCOUNT, "amount": 30});
        let txn = core.sparse.begin_write().expect("begin write");
        run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &[ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)],
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Delete { old_doc: &old_doc },
        )
        .expect("a delete must be applied, not ignored");
        txn.commit().expect("commit");

        let stored = core
            .sparse
            .get(DB, TID, TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");
        let decoded = doc_format::decode_document(&stored).expect("decode");
        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("70"),
            "a deleted row's contribution must come back off the total"
        );
    }

    /// A join value the plan carries no resolution for fails the write with the
    /// INTERNAL error, not the user-facing "target not found".
    ///
    /// CO-RESIDENT, and that is load-bearing: a co-resident binding is one this
    /// core applies itself, so nothing ever removes its join values from the
    /// resolution and an absent one can only mean the plan is short. The
    /// cross-shard half of the rule is
    /// [`an_unresolved_cross_shard_target_is_deferred_rather_than_refused`].
    ///
    /// Nothing here says the target row is missing — the plan simply never
    /// carried its identity. A replica re-executing a write the leader accepted
    /// hits exactly this shape when the resolution fails to reach it, and
    /// reporting it as `MaterializedSumTargetNotFound` would tell an operator
    /// the application referenced an account that does not exist.
    #[test]
    fn an_unresolved_target_fails_as_an_internal_shortfall() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), TARGET.to_string()),
            CollectionConfig::new(TARGET),
        );
        register_source(&mut core);

        let new_doc = serde_json::json!({"account_id": "a-missing", "amount": 5});
        let txn = core.sparse.begin_write().expect("begin write");
        let error = run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &[],
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc: &new_doc },
        )
        .err()
        .unwrap_or_else(|| panic!("an unresolvable target must fail the write"));

        match error {
            crate::Error::MaterializedSumResolutionMissing {
                target_collection,
                join_column,
                join_value,
            } => {
                assert_eq!(target_collection, TARGET);
                assert_eq!(join_column, "account_id");
                assert_eq!(join_value, "a-missing");
            }
            other => panic!(
                "an absent resolution must surface as MaterializedSumResolutionMissing — \
                 'target not found' is the Control Plane's verdict on the user's statement, \
                 got {other:?}"
            ),
        }
    }

    /// The MIRROR of the test above, and the rule that makes it precise: for a
    /// CROSS-SHARD binding an absent resolution is not a shortfall at all — it
    /// is how the Control Plane says "this one travels on its own task".
    ///
    /// Residency is what separates the two, and it separates them totally. A
    /// co-resident binding is never omitted from the resolution, so absent means
    /// the plan is short and the write must refuse. A cross-shard binding is
    /// always omitted once it has been settled, so absent means somebody else is
    /// applying it and this core must stand down. There is no third state for
    /// the check to guess at.
    ///
    /// Standing down has to be silent AND total: refusing would fail a write the
    /// Control Plane deliberately split, and applying would put the balance in
    /// this core's store — which no reader of the target collection opens — and
    /// then count it a second time when the sibling task ran.
    #[test]
    fn an_unresolved_cross_shard_target_is_deferred_rather_than_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.doc_configs.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                REMOTE_TARGET.to_string(),
            ),
            CollectionConfig::new(REMOTE_TARGET),
        );
        register_source_onto(&mut core, REMOTE_TARGET);

        // Seeded so that an accidental apply would be VISIBLE as a moved total
        // rather than failing on an absent row and looking like a refusal.
        let target_key = nodedb_types::StorageKey::for_surrogate(TARGET_SURROGATE);
        let seed = serde_json::json!({"id": ACCOUNT, "balance": "100"});
        core.sparse
            .put(
                DB,
                TID,
                REMOTE_TARGET,
                &target_key,
                &doc_format::encode_to_msgpack(&seed),
            )
            .expect("seed target row");

        let new_doc = serde_json::json!({"account_id": ACCOUNT, "amount": 5});
        let txn = core.sparse.begin_write().expect("begin write");
        let outcome: WriteEnforcementOutcome = run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                // Empty: the Control Plane settled this binding and removed the
                // join value when it appended the sibling balance task.
                resolved_targets: &[],
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Insert { new_doc: &new_doc },
        )
        .expect("a settled cross-shard binding must not fail the source write");
        assert!(
            outcome.target_writes.is_empty(),
            "the balance travels on its own task; this core must write no target row"
        );
        txn.commit().expect("commit");

        let stored = core
            .sparse
            .get(DB, TID, REMOTE_TARGET, &target_key)
            .expect("read back")
            .expect("row must still exist");
        let decoded = doc_format::decode_document(&stored).expect("decode");
        assert_eq!(
            decoded.get("balance").and_then(|v| v.as_str()),
            Some("100"),
            "the total must be untouched here — the sibling task owns it"
        );
    }

    // ── Every write path that folds materialized sums, driven end to end
    // through its own handler ───────────────────────────────────────────────
    //
    // The tests above prove the arithmetic and the write-back; these prove
    // that each PATH reaches them: a bulk update, a bulk delete, a `TRUNCATE`,
    // an `UPDATE ... FROM` and a batch insert each match their rows
    // differently, and a path that folds nothing leaves a stored total that
    // silently disagrees with the `SUM(...)` over the source rows.

    /// The collection whose `balance` column a second, path-level fixture set
    /// maintains, sharing `SOURCE`'s vShard — see `TARGET` for the identical
    /// premise these tests share with the ones above.
    const ACCOUNT_A: &str = "a1";
    const ACCOUNT_B: &str = "a2";
    const SURROGATE_A: Surrogate = Surrogate(4242);
    const SURROGATE_B: Surrogate = Surrogate(4343);
    /// A third collection, read-only, standing in as the FROM side of a joined
    /// update.
    const JOIN_SOURCE: &str = "local_rates";

    fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    /// Register the three collections: the binding-driving source, its target, and
    /// the read-only join side.
    fn register_collections(core: &mut CoreLoop) {
        register_collections_onto(core, TARGET);
    }

    /// Register the three collections with the source's binding pointed at
    /// `target`.
    fn register_collections_onto(core: &mut CoreLoop, target: &str) {
        let mut source = CollectionConfig::new(SOURCE);
        source.enforcement.materialized_sum_sources = vec![binding_onto(target)];
        core.doc_configs.insert(config_key(SOURCE), source);
        core.doc_configs
            .insert(config_key(target), CollectionConfig::new(target));
        core.doc_configs
            .insert(config_key(JOIN_SOURCE), CollectionConfig::new(JOIN_SOURCE));
    }

    /// The premise every path-level test in this section rests on. Identical
    /// to [`the_local_fixture_is_co_resident`]'s premise; kept as its own test
    /// (rather than folded into that one) because it was moved here from a
    /// separate file whose own fixtures depend on it independently.
    #[test]
    fn the_local_fixture_is_co_resident_for_path_level_tests() {
        assert!(
            crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, TARGET),
            "'{SOURCE}' and '{TARGET}' must share a vShard: the inline fold writes the target \
             inside the source's transaction, on the source's core, and each core opens its own \
             document store"
        );
        assert!(
            !crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, REMOTE_TARGET),
            "'{REMOTE_TARGET}' must NOT share '{SOURCE}'s vShard; it pins the deferred rule"
        );
    }

    fn seed_target(core: &mut CoreLoop, surrogate: Surrogate, id: &str, balance: &str) {
        seed_target_in(core, TARGET, surrogate, id, balance);
    }

    fn seed_target_in(
        core: &mut CoreLoop,
        collection: &str,
        surrogate: Surrogate,
        id: &str,
        balance: &str,
    ) {
        let row = serde_json::json!({"id": id, "balance": balance});
        core.sparse
            .put(
                DB,
                TID,
                collection,
                &nodedb_types::StorageKey::for_surrogate(surrogate),
                &doc_format::encode_to_msgpack(&row),
            )
            .expect("seed target row");
    }

    fn seed_source(core: &mut CoreLoop, surrogate: Surrogate, account: &str, amount: i64) {
        let row = serde_json::json!({"account_id": account, "amount": amount});
        core.sparse
            .put(
                DB,
                TID,
                SOURCE,
                &nodedb_types::StorageKey::for_surrogate(surrogate),
                &doc_format::encode_to_msgpack(&row),
            )
            .expect("seed source row");
    }

    fn balance_of(core: &CoreLoop, surrogate: Surrogate) -> String {
        balance_in(core, TARGET, surrogate)
    }

    fn balance_in(core: &CoreLoop, collection: &str, surrogate: Surrogate) -> String {
        let stored = core
            .sparse
            .get(
                DB,
                TID,
                collection,
                &nodedb_types::StorageKey::for_surrogate(surrogate),
            )
            .expect("read target row")
            .expect("target row must still exist");
        doc_format::decode_document(&stored)
            .expect("target row must decode")
            .get("balance")
            .and_then(|v| v.as_str())
            .expect("target row must carry a balance")
            .to_string()
    }

    /// The resolution the Control Plane produces for the local binding: every entry
    /// names `TARGET`, the collection it was resolved against.
    fn resolved_onto_target(entries: &[(&str, Surrogate)]) -> Vec<ResolvedSumTarget> {
        entries
            .iter()
            .map(|(join_value, surrogate)| ResolvedSumTarget::new(TARGET, *join_value, *surrogate))
            .collect()
    }

    fn literal(value: serde_json::Value) -> UpdateValue {
        UpdateValue::Literal(nodedb_types::json_to_msgpack(&value).expect("encode literal"))
    }

    /// A bulk UPDATE contributes each matched row's DIFFERENCE, not its whole new
    /// value: two rows moved from 30 and 20 to 50 add 20 + 30 to the total, never
    /// 100.
    #[test]
    fn bulk_update_moves_the_total_by_the_difference() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
        seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
        seed_source(&mut core, Surrogate(2), ACCOUNT_A, 20);

        let updates = vec![("amount".to_string(), literal(serde_json::json!(50)))];
        let resolved = resolved_onto_target(&[(ACCOUNT_A, SURROGATE_A)]);
        let task = make_default_task();
        let response = core.execute_bulk_update(
            &task,
            TID,
            BulkUpdateParams {
                collection: SOURCE,
                filter_bytes: &[],
                updates: &updates,
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &resolved,
                declared_primary_key: None,
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(balance_of(&core, SURROGATE_A), "150");
    }

    /// A bulk DELETE takes every removed row's contribution back off the total.
    #[test]
    fn bulk_delete_subtracts_every_removed_rows_contribution() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
        seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
        seed_source(&mut core, Surrogate(2), ACCOUNT_A, 20);

        let resolved = resolved_onto_target(&[(ACCOUNT_A, SURROGATE_A)]);
        let task = make_default_task();
        let response = core.execute_bulk_delete(
            &task,
            TID,
            BulkDeleteParams {
                collection: SOURCE,
                filter_bytes: &[],
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &resolved,
                ollp: OllpPrediction {
                    surrogates: None,
                    edges: None,
                },
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(balance_of(&core, SURROGATE_A), "50");
    }

    /// `TRUNCATE` on the source zeroes EVERY target the collection's rows
    /// contributed to — it must leave the totals exactly where N individual deletes
    /// would.
    #[test]
    fn truncate_zeroes_every_target_balance() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "30");
        seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");
        seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
        seed_source(&mut core, Surrogate(2), ACCOUNT_B, 50);

        let resolved = resolved_onto_target(&[(ACCOUNT_A, SURROGATE_A), (ACCOUNT_B, SURROGATE_B)]);
        let task = make_default_task();
        let response = core.execute_truncate(&task, TID, SOURCE, &resolved);

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(balance_of(&core, SURROGATE_A), "0");
        assert_eq!(
            balance_of(&core, SURROGATE_B),
            "0",
            "a target only the SECOND removed row contributed to must be zeroed too"
        );
    }

    /// `UPDATE ... FROM` folds the difference of every row the join matched.
    #[test]
    fn update_from_join_moves_the_total_by_the_difference() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
        // The written row carries the join column of the FROM side as well.
        let entry = serde_json::json!({"account_id": ACCOUNT_A, "amount": 30, "rate_id": "r1"});
        core.sparse
            .put(
                DB,
                TID,
                SOURCE,
                &nodedb_types::StorageKey::for_surrogate(Surrogate(1)),
                &doc_format::encode_to_msgpack(&entry),
            )
            .expect("seed written row");

        let rate = serde_json::json!({"rate_id": "r1", "amount": 80});
        let source_rows = vec![(
            surrogate_to_doc_id(Surrogate(9)),
            doc_format::encode_to_msgpack(&rate),
        )];

        let updates = vec![("amount".to_string(), literal(serde_json::json!(80)))];
        let resolved = resolved_onto_target(&[(ACCOUNT_A, SURROGATE_A)]);
        let task = make_default_task();
        let response = core.execute_update_from_join(
            &task,
            TID,
            UpdateFromJoinParams {
                target_collection: SOURCE,
                source_collection: JOIN_SOURCE,
                source_alias: "r",
                target_join_col: "rate_id",
                source_join_col: "rate_id",
                updates: &updates,
                target_filter_bytes: &[],
                returning: None,
                resolve_only: false,
                source_rows: Some(source_rows.as_slice()),
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &resolved,
                declared_primary_key: None,
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(balance_of(&core, SURROGATE_A), "150");
    }

    /// The batch insert an `INSERT ... SELECT` page ships credits its targets.
    ///
    /// The orchestrator re-issues the copy through `dispatch_local`, which never
    /// passes through the statement-level resolution pass — so a page shipping an
    /// empty resolution would leave the total short of the rows it inserted.
    #[test]
    fn insert_select_page_credits_its_targets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");

        let documents = vec![
            (
                surrogate_to_doc_id(Surrogate(1)),
                doc_format::encode_to_msgpack(
                    &serde_json::json!({"account_id": ACCOUNT_A, "amount": 25}),
                ),
            ),
            (
                surrogate_to_doc_id(Surrogate(2)),
                doc_format::encode_to_msgpack(
                    &serde_json::json!({"account_id": ACCOUNT_A, "amount": 75}),
                ),
            ),
        ];
        let surrogates = vec![Surrogate(1), Surrogate(2)];
        let resolved = resolved_onto_target(&[(ACCOUNT_A, SURROGATE_A)]);
        let task = make_default_task();
        let response = core.execute_document_batch_insert(
            &task,
            DocumentBatchInsertParams {
                tid: TID,
                collection: SOURCE,
                documents: &documents,
                surrogates: &surrogates,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &resolved,
                deferred_sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(balance_of(&core, SURROGATE_A), "200");
    }

    /// A resolution that no longer covers the matched rows is REFUSED before
    /// anything is written.
    ///
    /// This is the drift the recon scan cannot rule out: a row that joined the match
    /// set after the Control Plane resolved its targets. Folding it would fail
    /// mid-statement with earlier rows already removed, leaving a stored total that
    /// still counts rows the statement deleted. The leader answers
    /// `OllpRetryRequired` instead, having removed nothing, and the coordinator
    /// re-resolves.
    ///
    /// CO-RESIDENT, and that is what makes an uncovered value a defect here: this
    /// core applies the binding itself, so nothing ever removes a join value from
    /// the resolution and a missing one can only mean the plan no longer covers the
    /// rows. The cross-shard half of the rule, where a missing value is the
    /// deliberate deferral signal, is
    /// [`an_uncovered_cross_shard_join_value_is_deferred_rather_than_retried`].
    #[test]
    fn an_uncovered_join_value_retries_instead_of_writing_a_wrong_total() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
        seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");
        seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
        // The row the resolution below does not know about — it arrived after the
        // Control Plane scanned.
        seed_source(&mut core, Surrogate(2), ACCOUNT_B, 20);

        let resolved = resolved_onto_target(&[(ACCOUNT_A, SURROGATE_A)]);
        let task = make_default_task();
        let response = core.execute_bulk_delete(
            &task,
            TID,
            BulkDeleteParams {
                collection: SOURCE,
                filter_bytes: &[],
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &resolved,
                ollp: OllpPrediction {
                    surrogates: None,
                    edges: None,
                },
            },
        );

        assert_eq!(
            response.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "an uncovered join value must ask for a retry, not write a partial total"
        );
        assert_eq!(
            balance_of(&core, SURROGATE_A),
            "100",
            "the covered target must be untouched: the statement wrote nothing"
        );
        assert_eq!(balance_of(&core, SURROGATE_B), "50");
        assert!(
            core.sparse
                .get(
                    DB,
                    TID,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(Surrogate(1))
                )
                .expect("read source row")
                .is_some(),
            "no source row may be removed on a refused statement"
        );
    }

    /// The MIRROR of the retry test, and the rule that keeps the guard honest: for a
    /// CROSS-SHARD binding an uncovered join value is not drift at all.
    ///
    /// The Control Plane settles such a binding at plan time and REMOVES its join
    /// values from the resolution — that removal is how this core is told to stand
    /// down. Demanding coverage for them anyway would report a divergence on every
    /// single cross-shard predicate write, and the coordinator would re-recon,
    /// resolve, remove them again and resubmit: a livelock, not a retry, on a
    /// dataset nobody else is touching.
    ///
    /// Residency is the discriminator, and it is total. A co-resident binding is
    /// never omitted, so an uncovered value there is a genuine shortfall and
    /// `an_uncovered_join_value_retries_instead_of_writing_a_wrong_total` still
    /// demands the retry. A cross-shard binding is always omitted once settled, so
    /// an uncovered value there is the deferral. Drift on the cross-shard side is
    /// caught by the settlement's own read-set entry over the images the shipped
    /// deltas were folded from, which the Calvin OCC check validates on this core
    /// before any mutation — a different guard, not a missing one.
    ///
    /// The balance assertion is what makes "stood down" mean something: the
    /// statement must proceed AND leave the total alone, because the sibling task
    /// owns it.
    #[test]
    fn an_uncovered_cross_shard_join_value_is_deferred_rather_than_retried() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections_onto(&mut core, REMOTE_TARGET);
        seed_target_in(&mut core, REMOTE_TARGET, SURROGATE_A, ACCOUNT_A, "100");
        seed_target_in(&mut core, REMOTE_TARGET, SURROGATE_B, ACCOUNT_B, "50");
        seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);
        seed_source(&mut core, Surrogate(2), ACCOUNT_B, 20);

        // Empty, not partial: a settled cross-shard binding has every one of its
        // join values removed, so this is the ORDINARY shape of the plan, not a
        // damaged one.
        let task = make_default_task();
        let response = core.execute_bulk_delete(
            &task,
            TID,
            BulkDeleteParams {
                collection: SOURCE,
                filter_bytes: &[],
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &[],
                ollp: OllpPrediction {
                    surrogates: None,
                    edges: None,
                },
            },
        );

        assert_eq!(
            response.status,
            Status::Ok,
            "a settled cross-shard binding must not be reported as drift: {:?}",
            response.error_code
        );
        assert_ne!(
            response.error_code.as_deref(),
            Some(&ErrorCode::OllpRetryRequired),
            "demanding coverage for a deliberately-omitted join value livelocks the statement"
        );
        assert_eq!(
            balance_in(&core, REMOTE_TARGET, SURROGATE_A),
            "100",
            "the balance travels on its own task; this core must not move it"
        );
        assert_eq!(balance_in(&core, REMOTE_TARGET, SURROGATE_B), "50");
        assert!(
            core.sparse
                .get(
                    DB,
                    TID,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(Surrogate(1))
                )
                .expect("read source row")
                .is_none(),
            "the statement itself must have run: the matched source rows are gone"
        );
    }

    /// A covered resolution is NOT treated as drift — the guard is coverage, so an
    /// entry the statement turns out not to need costs one unused surrogate and
    /// never a spurious retry.
    #[test]
    fn an_over_resolved_plan_is_not_a_divergence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
        seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");
        seed_source(&mut core, Surrogate(1), ACCOUNT_A, 30);

        let resolved = resolved_onto_target(&[
            (ACCOUNT_A, SURROGATE_A),
            // Resolved, then the row that needed it was removed by someone else.
            (ACCOUNT_B, SURROGATE_B),
        ]);
        let task = make_default_task();
        let response = core.execute_bulk_delete(
            &task,
            TID,
            BulkDeleteParams {
                collection: SOURCE,
                filter_bytes: &[],
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &resolved,
                ollp: OllpPrediction {
                    surrogates: None,
                    edges: None,
                },
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(balance_of(&core, SURROGATE_A), "70");
        assert_eq!(
            balance_of(&core, SURROGATE_B),
            "50",
            "an unused resolution entry must move no total"
        );
    }

    /// The MERGE update arm that REWRITES the join key debits the target the row
    /// leaves and credits the one it joins — one arm, two targets, opposite signs.
    ///
    /// The insert and delete arms are covered above; this is the arm whose
    /// two-sided split is derived rather than carried, and accounting it as a single
    /// positive contribution leaves the abandoned target permanently overstated.
    #[test]
    fn a_merge_update_arm_that_moves_the_join_key_touches_both_targets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");
        seed_target(&mut core, SURROGATE_B, ACCOUNT_B, "50");

        let old_doc = serde_json::json!({"account_id": ACCOUNT_A, "amount": 30});
        let new_doc = serde_json::json!({"account_id": ACCOUNT_B, "amount": 40});
        let txn = core.sparse.begin_write().expect("begin write");
        run_write_enforcement(
            &mut core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: &resolved_onto_target(&[
                    (ACCOUNT_A, SURROGATE_A),
                    (ACCOUNT_B, SURROGATE_B),
                ]),
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            RowImages::Update {
                old_doc: &old_doc,
                new_doc: &new_doc,
            },
        )
        .expect("a join-key move must be applied to both targets");
        txn.commit().expect("commit");

        assert_eq!(
            balance_of(&core, SURROGATE_A),
            "70",
            "the target the row left loses its old value"
        );
        assert_eq!(
            balance_of(&core, SURROGATE_B),
            "90",
            "the target the row joined gains its new value"
        );
    }

    /// The surrogate of the SECOND target's row for `ACCOUNT_A`.
    ///
    /// Deliberately not `SURROGATE_A`: the two bindings read the same join VALUE, so
    /// the only thing that can tell their target rows apart is the collection each
    /// was resolved against. Giving both targets the same surrogate would let a
    /// resolution that lost the collection still land on the right row and hide the
    /// defect this fixture exists to catch.
    const SECOND_TARGET_SURROGATE: Surrogate = Surrogate(7777);

    /// Letters the second target's generated suffix is drawn from.
    ///
    /// LETTERS, not digits, and that is the whole point — see
    /// [`co_resident_second_target`].
    const SECOND_TARGET_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    /// Name prefix the second target is generated under.
    const SECOND_TARGET_PREFIX: &str = "local_audit_totals_";

    /// A SECOND target collection that shares `SOURCE`'s vShard.
    ///
    /// Solved for rather than named, for the same reason
    /// [`the_local_fixture_is_co_resident_for_path_level_tests`] asserts rather
    /// than assumes: every test in this section drives the INLINE fold and reads
    /// the target row back out of the SOURCE core's own store, which only
    /// observes the write when one core owns both rows.
    ///
    /// # Why the suffix is letters and not a counter
    ///
    /// vShard homing is a base-31 polynomial over the name's bytes reduced modulo
    /// the vShard count, which is a power of two — so only the polynomial's low bits
    /// survive. A fixed prefix with a short DECIMAL suffix varies just ten
    /// contiguous byte values per position, which leaves that residue space only
    /// partly covered, and WHICH part depends entirely on the prefix. Counting is
    /// therefore not a widening: this prefix with a four-digit counter reaches
    /// roughly seven tenths of the vShards and `SOURCE`'s own is not among them, so
    /// a counter-based search finds nothing however far it counts.
    ///
    /// A 26-letter alphabet over suffixes of length one to three reaches EVERY
    /// vShard, with no fewer than thirteen names landing on each. So the search
    /// terminates for any source collection, not just this one, and keeps a wide
    /// margin if the hash is ever changed.
    fn co_resident_second_target() -> String {
        let alphabet = SECOND_TARGET_ALPHABET.len();
        for width in 1..=3u32 {
            for code in 0..alphabet.pow(width) {
                let mut suffix = String::with_capacity(width as usize);
                let mut rest = code;
                for _ in 0..width {
                    suffix.push(char::from(SECOND_TARGET_ALPHABET[rest % alphabet]));
                    rest /= alphabet;
                }
                let candidate = format!("{SECOND_TARGET_PREFIX}{suffix}");
                // A name already registered by this fixture would collapse the two
                // bindings into one collection and prove nothing.
                if [SOURCE, TARGET, JOIN_SOURCE].contains(&candidate.as_str()) {
                    continue;
                }
                if crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, &candidate)
                {
                    return candidate;
                }
            }
        }
        panic!(
            "no '{SECOND_TARGET_PREFIX}*' name of up to three letters shares '{SOURCE}'s vShard. \
             That alphabet reaches every vShard under the current homing hash, so the hash has \
             changed and this fixture can no longer build the two-co-resident-target \
             configuration it exists to pin"
        )
    }

    /// Register `SOURCE` driving TWO bindings that read the SAME join column into
    /// DIFFERENT target collections, plus both targets.
    fn register_two_binding_source(core: &mut CoreLoop, second_target: &str) {
        let mut source = CollectionConfig::new(SOURCE);
        source.enforcement.materialized_sum_sources =
            vec![binding_onto(TARGET), binding_onto(second_target)];
        core.doc_configs.insert(config_key(SOURCE), source);
        core.doc_configs
            .insert(config_key(TARGET), CollectionConfig::new(TARGET));
        core.doc_configs.insert(
            config_key(second_target),
            CollectionConfig::new(second_target),
        );
    }

    /// Fold one write's images through the enforcement funnel and commit.
    fn fold_images(core: &mut CoreLoop, resolved: &[ResolvedSumTarget], images: RowImages<'_>) {
        let txn = core.sparse.begin_write().expect("begin write");
        run_write_enforcement(
            core,
            &txn,
            EnforcementCtx {
                database_id: DB,
                tid: TID,
                collection: SOURCE,
                resolved_targets: resolved,
                deferred_sum_targets: &[],
                wal_lsn: None,
            },
            images,
        )
        .expect("both bindings must fold");
        txn.commit().expect("commit");
    }

    /// Two materialized sums on ONE source, reading the SAME join column into
    /// DIFFERENT target collections, each keep their own correct total — across an
    /// insert, an update and a delete.
    ///
    /// This is the configuration a value-keyed resolution cannot express. Resolving
    /// by join value alone, the first binding claims `"a1"` and the second is
    /// skipped as a duplicate; the fold then looks `"a1"` up, gets the FIRST
    /// binding's target row surrogate, and applies the SECOND binding's delta to a
    /// row of that surrogate inside the second target collection — a row nothing
    /// else writes or reads. No error is raised on either plane. The first target
    /// ends up correct by luck, the second permanently frozen at its seed value, and
    /// a phantom row accumulates the deltas that should have landed on it.
    ///
    /// The two target rows deliberately carry DIFFERENT surrogates and DIFFERENT
    /// seed balances, so neither a lost collection nor a swapped row can pass.
    #[test]
    fn two_bindings_sharing_a_join_column_each_receive_their_own_total() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let second_target = co_resident_second_target();
        // Premise one: the two bindings really do point at two DIFFERENT
        // collections. A fixture that collapsed them would pass without ever
        // building the configuration under test.
        assert_ne!(
            second_target.as_str(),
            TARGET,
            "the two bindings must name different target collections"
        );
        // Premise two: BOTH targets are co-resident with the source, so both inline
        // folds land in this core's own store and are observable here. Asserted,
        // never assumed — a cross-shard target would silently exercise the DEFERRED
        // path instead and leave the collision this test exists for uncovered.
        for target in [TARGET, second_target.as_str()] {
            assert!(
                crate::query::sum_target_is_co_resident(DatabaseId::DEFAULT, SOURCE, target),
                "'{target}' must share '{SOURCE}'s vShard for its inline fold to be observable"
            );
        }

        register_two_binding_source(&mut core, &second_target);
        seed_target_in(&mut core, TARGET, SURROGATE_A, ACCOUNT_A, "100");
        seed_target_in(
            &mut core,
            &second_target,
            SECOND_TARGET_SURROGATE,
            ACCOUNT_A,
            "500",
        );

        // One join value, TWO resolutions — one per binding. Keyed on the value
        // alone only the first of these could exist.
        let resolved = vec![
            ResolvedSumTarget::new(TARGET, ACCOUNT_A, SURROGATE_A),
            ResolvedSumTarget::new(&second_target, ACCOUNT_A, SECOND_TARGET_SURROGATE),
        ];

        let inserted = serde_json::json!({"account_id": ACCOUNT_A, "amount": 30});
        fold_images(
            &mut core,
            &resolved,
            RowImages::Insert { new_doc: &inserted },
        );
        assert_eq!(balance_in(&core, TARGET, SURROGATE_A), "130");
        assert_eq!(
            balance_in(&core, &second_target, SECOND_TARGET_SURROGATE),
            "530",
            "the second binding must credit its OWN target row, not the first binding's"
        );

        let updated = serde_json::json!({"account_id": ACCOUNT_A, "amount": 50});
        fold_images(
            &mut core,
            &resolved,
            RowImages::Update {
                old_doc: &inserted,
                new_doc: &updated,
            },
        );
        assert_eq!(balance_in(&core, TARGET, SURROGATE_A), "150");
        assert_eq!(
            balance_in(&core, &second_target, SECOND_TARGET_SURROGATE),
            "550",
            "an update moves each target by the difference, on its own row"
        );

        fold_images(
            &mut core,
            &resolved,
            RowImages::Delete { old_doc: &updated },
        );
        assert_eq!(
            balance_in(&core, TARGET, SURROGATE_A),
            "100",
            "the first target returns to its seed"
        );
        assert_eq!(
            balance_in(&core, &second_target, SECOND_TARGET_SURROGATE),
            "500",
            "so does the second — a delete debits the row the credit landed on"
        );
    }

    /// The CALVIN apply path honours a deferral, and it is the only path that ever
    /// sees one.
    ///
    /// `execute_calvin_flush` replays every staged plan through
    /// `execute_transaction_batch`, which intercepts `PointInsert` for undo
    /// tracking instead of re-dispatching it. That interception must forward both
    /// `resolved_sum_targets` and `deferred_sum_targets`; dropping the latter lets
    /// the source core fold a balance the Control Plane already shipped on its own
    /// `ApplyBalanceDelta` task, moving the total twice.
    ///
    /// A dropped marker is invisible under two compounding conditions: a deferral
    /// is only ever set on a CROSS-SHARD statement, and a cross-shard statement
    /// only ever commits through Calvin — so a bug here fires on every write that
    /// carries the marker, and never on a write that could notice. The
    /// direct-dispatch path forwards it correctly, which is why a regression here
    /// leaves every single-shard test green.
    ///
    /// Asserted through `execute_transaction_batch` rather than through the funnel:
    /// the funnel was never where the field was lost.
    #[test]
    fn the_transaction_batch_path_honours_a_deferred_binding() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections_onto(&mut core, REMOTE_TARGET);
        seed_target_in(&mut core, REMOTE_TARGET, SURROGATE_A, ACCOUNT_A, "100");

        let plans = vec![nodedb_physical::physical_plan::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE),
                document_id: "e1".to_string(),
                value: doc_format::encode_to_msgpack(
                    &serde_json::json!({"account_id": ACCOUNT_A, "amount": 25}),
                ),
                if_absent: false,
                surrogate: Surrogate(1),
                returning: None,
                rls_filters: Vec::new(),
                // Resolved AND deferred: an insert's balance is settled at plan
                // time, so the resolution stays on the plan and the deferral is
                // what stops this core applying it.
                resolved_sum_targets: vec![ResolvedSumTarget::new(
                    REMOTE_TARGET,
                    ACCOUNT_A,
                    SURROGATE_A,
                )],
                deferred_sum_targets: vec![REMOTE_TARGET.to_string()],
            },
        )];

        let task = make_default_task();
        let response = core.execute_transaction_batch(&task, TID, &plans, &[], None);
        assert_eq!(
            response.status,
            Status::Ok,
            "the source write itself must succeed: {:?}",
            response.error_code
        );

        assert_eq!(
            balance_in(&core, REMOTE_TARGET, SURROGATE_A),
            "100",
            "the sibling ApplyBalanceDelta task owns this delta; folding it here too \
             is the double-count the deferral exists to prevent"
        );
        assert!(
            core.sparse
                .get(
                    DB,
                    TID,
                    SOURCE,
                    &nodedb_types::StorageKey::for_surrogate(Surrogate(1))
                )
                .expect("read source row")
                .is_some(),
            "the source row must still have been written"
        );
    }

    /// The same path, with the deferral ABSENT, must still apply the balance.
    ///
    /// Without this the fix above could be "never fold on the transaction batch
    /// path", which would silently drop every CO-RESIDENT balance committed through
    /// Calvin — a wrong total in the other direction, and one no cross-shard test
    /// would catch.
    #[test]
    fn the_transaction_batch_path_still_folds_an_undeferred_binding() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");

        let plans = vec![nodedb_physical::physical_plan::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE),
                document_id: "e1".to_string(),
                value: doc_format::encode_to_msgpack(
                    &serde_json::json!({"account_id": ACCOUNT_A, "amount": 25}),
                ),
                if_absent: false,
                surrogate: Surrogate(1),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: vec![ResolvedSumTarget::new(TARGET, ACCOUNT_A, SURROGATE_A)],
                deferred_sum_targets: Vec::new(),
            },
        )];

        let task = make_default_task();
        let response = core.execute_transaction_batch(&task, TID, &plans, &[], None);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            balance_of(&core, SURROGATE_A),
            "125",
            "a co-resident binding is this core's to apply, on every path"
        );
    }

    /// A source write replayed on the transaction-batch path reports its
    /// affected-row count.
    ///
    /// `execute_calvin_flush` replays a participant's staged plans through
    /// `execute_transaction_batch` and returns the LAST sub-plan's payload as that
    /// participant's applied response. The scheduler deposits it, and the
    /// coordinator shapes the statement's `INSERT <n>` tag from it — so a bare `Ok`
    /// from the sub-plan leaves an autocommit CROSS-SHARD insert with no count at
    /// all and the statement fails with "write response carried no affected-row
    /// count".
    ///
    /// It stayed hidden because the other consumer of this payload is the
    /// single-shard COMMIT flush, whose tag is `COMMIT` and which discards the
    /// count entirely. The cross-shard materialized-sum pair is the first shape
    /// that makes a ONE-STATEMENT autocommit write commit through Calvin, and it
    /// only appeared to work while the sibling balance participant — whose handler
    /// does report a count — happened to win the race to deposit first.
    #[test]
    fn the_transaction_batch_path_reports_its_affected_count() {
        use crate::control::server::shared::sql::staging_predicates::require_affected_count;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register_collections(&mut core);
        seed_target(&mut core, SURROGATE_A, ACCOUNT_A, "100");

        let plans = vec![nodedb_physical::physical_plan::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::PointInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE),
                document_id: "e1".to_string(),
                value: doc_format::encode_to_msgpack(
                    &serde_json::json!({"account_id": ACCOUNT_A, "amount": 25}),
                ),
                if_absent: false,
                surrogate: Surrogate(1),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: vec![ResolvedSumTarget::new(TARGET, ACCOUNT_A, SURROGATE_A)],
                deferred_sum_targets: Vec::new(),
            },
        )];

        let task = make_default_task();
        let response = core.execute_transaction_batch(&task, TID, &plans, &[], None);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            require_affected_count(response.payload.as_bytes())
                .expect("a batch whose last sub-plan renders an INSERT tag must carry its count"),
            1,
            "one row was inserted, so the batch's applied response reports one"
        );
    }
}
