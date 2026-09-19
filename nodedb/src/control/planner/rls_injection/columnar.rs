// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for the three peer engines sharing the columnar storage
//! core: plain columnar, timeseries, and spatial.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TimeseriesOp};

use super::context::RlsCtx;

/// Exhaustive over [`ColumnarOp`].
pub(super) fn inject_columnar(ctx: &RlsCtx<'_>, op: &mut ColumnarOp) -> crate::Result<()> {
    match op {
        // Inject: the policy occupies the dedicated post-scan slot, applied
        // after block pruning and before rows are returned.
        ColumnarOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: the clone materializer streams raw `(surrogate, row bytes)`
        // pairs through a cursor payload that carries no row filter.
        ColumnarOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored rows through a cursor payload that carries \
             no row filter",
        ),

        // Plain insert: admit its rows now. Conflict branch: ship the
        // predicate instead — its merged post-image doesn't exist yet.
        ColumnarOp::Insert {
            collection,
            payload,
            on_conflict_updates,
            rls_write_check,
            rls_filters,
            ..
        } => {
            if on_conflict_updates.is_empty() {
                ctx.admit_write_batch(collection, payload, rls_write_check)?;
            } else {
                ctx.set_write_check(collection, rls_write_check)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the predicate: the image exists only after the handler scans
        // the row (update's post-image, delete's pre-image).
        ColumnarOp::Update {
            collection,
            rls_write_check,
            ..
        }
        | ColumnarOp::Delete {
            collection,
            rls_write_check,
            ..
        } => ctx.set_write_check(collection, rls_write_check),

        // No injection: already decided while the writing identity was
        // live; the check slot already carries `DecidedEarlierInRequest`.
        ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. } => Ok(()),

        // Refuse: removes every row without reading one, so no image
        // exists to evaluate against. Mirrors `KvOp::Truncate`.
        ColumnarOp::Truncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),
    }
}

/// Exhaustive over [`TimeseriesOp`].
pub(super) fn inject_timeseries(ctx: &RlsCtx<'_>, op: &mut TimeseriesOp) -> crate::Result<()> {
    match op {
        // Inject: the policy is applied after time-range pruning, on the rows
        // the scan actually produced.
        TimeseriesOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Ship the predicate: normalization retypes values, so the
        // submitted body is not the image that gets stored.
        TimeseriesOp::Ingest {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Recurse: the resolve pass carries the ingest it is about to decide,
        // and that ingest's own slots are the ones the policy fills.
        TimeseriesOp::ResolveIngest(inner) => inject_timeseries(ctx, inner),

        // Refuse: removes every row without reading one, so no image
        // exists to evaluate against. Mirrors `KvOp::Truncate`.
        TimeseriesOp::Truncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),
    }
}

/// Exhaustive over [`SpatialOp`].
pub(super) fn inject_spatial(ctx: &RlsCtx<'_>, op: &mut SpatialOp) -> crate::Result<()> {
    match op {
        // Inject: the policy is applied to the R-tree candidates before they
        // are returned, alongside the query's own attribute filters.
        SpatialOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: carries geometry + surrogate, not column values — edge
        // sync, not user SQL, so refusing loses no user-facing write.
        SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. } => ctx
            .refuse_if_write_policy(
                collection,
                "the R-tree entry carries a geometry and a surrogate rather than the column values \
                 a policy predicate names",
            ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        ColumnarInsertIntent, ColumnarOp, SpatialOp, TimeseriesOp, UpdateValue,
    };

    use super::super::plan::test_support::{
        assert_write_refused, inject, inject_without_policy, store_with_read_policy,
        store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// `alice` in the shared fixture has user id 42, so a row carrying
    /// `owner_id = "42"` satisfies `owner_id = $auth.id` and any other does not.
    fn rows(owner_ids: &[&str]) -> Vec<u8> {
        let rows: Vec<serde_json::Value> = owner_ids
            .iter()
            .map(|owner| serde_json::json!({ "owner_id": owner, "amount": 100 }))
            .collect();
        nodedb_types::json_to_msgpack_or_empty(&serde_json::Value::Array(rows))
    }

    fn columnar_insert(collection: &str, owner_ids: &[&str]) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            payload: rows(owner_ids),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn columnar_update(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            filters: Vec::new(),
            updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })
    }

    fn columnar_truncate(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            restart_identity: false,
        })
    }

    fn timeseries_truncate(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            restart_identity: false,
        })
    }

    /// A truncate reads no row, so nothing exists for the write policy to
    /// decide against; it is refused like `KvOp::Truncate`.
    #[test]
    fn columnar_family_truncate_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("docs");
        for mut plan in [columnar_truncate("docs"), timeseries_truncate("docs")] {
            assert_write_refused(inject(&mut plan, &store), "docs");
        }
    }

    #[test]
    fn columnar_family_truncate_without_a_policy_is_untouched() {
        for mut plan in [columnar_truncate("docs"), timeseries_truncate("docs")] {
            let before = plan.clone();
            assert!(inject_without_policy(&mut plan).is_ok());
            assert_eq!(plan, before);
        }
    }

    fn ingest(collection: &str, format: &str) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            payload: Vec::new(),
            format: format.into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// The compiled write predicate a plan carries into the Data Plane.
    fn write_check(plan: &PhysicalPlan) -> &nodedb_types::RlsWriteCheck {
        match plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Update {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                rls_write_check, ..
            }) => rls_write_check,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// The plan carries every row a plain insert will persist, so the policy
    /// decides them at plan time: a conforming batch is admitted and a batch
    /// holding one violating row fails the whole statement.
    #[test]
    fn columnar_insert_is_admitted_or_rejected_on_its_own_rows() {
        let store = store_with_write_policy("events");

        let mut conforming = columnar_insert("events", &["42", "42"]);
        assert!(inject(&mut conforming, &store).is_ok());

        let mut violating = columnar_insert("events", &["42", "99"]);
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A plain insert is admitted at plan time, so nothing ships to the
    /// Data Plane gate — but the slot must still record that injection ran,
    /// or `PendingInjection` reads as "never ran" and the write is refused.
    #[test]
    fn a_plain_insert_records_that_its_rows_were_decided() {
        let store = store_with_write_policy("events");
        let mut plan = columnar_insert("events", &["42"]);
        assert!(inject(&mut plan, &store).is_ok());
        assert_eq!(
            write_check(&plan),
            &nodedb_types::RlsWriteCheck::DecidedEarlierInRequest
        );
    }

    /// …and with no policy the slot says so, rather than staying un-injected.
    #[test]
    fn a_plain_insert_without_a_policy_records_that_injection_ran() {
        let mut plan = columnar_insert("events", &["42"]);
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());

        assert_eq!(
            write_check(&before),
            &nodedb_types::RlsWriteCheck::PendingInjection
        );
        assert_eq!(
            write_check(&plan),
            &nodedb_types::RlsWriteCheck::NoPolicyApplies
        );

        let mut normalized = plan.clone();
        super::super::plan::test_support::reset_write_check(&mut normalized);
        assert_eq!(normalized, before, "nothing else may change");
    }

    /// A payload that is not a decodable row batch fails closed rather than
    /// being waved through as "not rows".
    #[test]
    fn an_undecodable_insert_payload_is_rejected_under_a_write_policy() {
        let store = store_with_write_policy("events");
        let mut plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "events",
            ),
            payload: vec![0xC1],
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// The merged row an ON CONFLICT DO UPDATE stores exists only in the
    /// handler, so the predicate travels with the plan instead of the incoming
    /// body being admitted in its place.
    #[test]
    fn on_conflict_update_carries_the_write_predicate() {
        let store = store_with_write_policy("events");
        let mut plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "events",
            ),
            payload: rows(&["42"]),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Put,
            on_conflict_updates: vec![("amount".into(), UpdateValue::Literal(Vec::new()))],
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            write_check(&plan).has_predicate(),
            "write policy must reach the Data-Plane gate"
        );
    }

    /// An update's post-image and a delete's pre-image both exist only inside
    /// the handler, so both ship the compiled predicate.
    #[test]
    fn columnar_update_and_delete_carry_the_write_predicate() {
        let store = store_with_write_policy("events");

        let mut update = columnar_update("events");
        assert!(inject(&mut update, &store).is_ok());
        assert!(write_check(&update).has_predicate());

        let mut delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "events",
            ),
            filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(inject(&mut delete, &store).is_ok());
        assert!(write_check(&delete).has_predicate());
    }

    /// Not decided here even though the plan carries its rows: the ingest
    /// handler retypes values before storing, so the predicate ships and
    /// the one post-normalization gate decides the stored rows.
    #[test]
    fn timeseries_msgpack_ingest_carries_the_write_predicate_rather_than_deciding_here() {
        let store = store_with_write_policy("metrics");
        let mut plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "metrics",
            ),
            payload: rows(&["99"]),
            format: "msgpack".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(
            inject(&mut plan, &store).is_ok(),
            "the violating row must be left for the Data-Plane gate, not refused here"
        );
        assert!(
            write_check(&plan).has_predicate(),
            "the predicate must reach the gate that sees the stored image"
        );
    }

    /// Every payload shape carries the predicate to the handler's per-row gate.
    #[test]
    fn timeseries_ilp_ingest_carries_the_write_predicate() {
        let store = store_with_write_policy("metrics");
        for format in ["ilp", "ilp-msgpack", "json"] {
            let mut plan = ingest("metrics", format);
            assert!(inject(&mut plan, &store).is_ok());
            assert!(
                write_check(&plan).has_predicate(),
                "{format} ingest must carry the predicate to the gate"
            );
        }
    }

    /// …and runs untouched when no policy applies.
    #[test]
    fn timeseries_ingest_without_a_policy_records_that_injection_ran() {
        let mut plan = ingest("metrics", "ilp");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());

        // Stamps `NoPolicyApplies` so the gate can tell the pass ran.
        assert_eq!(
            write_check(&before),
            &nodedb_types::RlsWriteCheck::PendingInjection
        );
        assert_eq!(
            write_check(&plan),
            &nodedb_types::RlsWriteCheck::NoPolicyApplies
        );

        let mut normalized = plan.clone();
        super::super::plan::test_support::reset_write_check(&mut normalized);
        assert_eq!(normalized, before, "nothing else may change");
    }

    /// A read policy alone must not start rejecting or gating writes.
    #[test]
    fn a_read_policy_alone_leaves_the_columnar_write_gate_empty() {
        let store = store_with_read_policy("events");

        let mut insert = columnar_insert("events", &["99"]);
        assert!(inject(&mut insert, &store).is_ok());

        let mut update = columnar_update("events");
        assert!(inject(&mut update, &store).is_ok());
        assert!(!write_check(&update).has_predicate());
    }

    /// A spatial write carries geometry and a surrogate, not the row body the
    /// policy names.
    #[test]
    fn spatial_delete_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("places");
        let mut plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "places",
            ),
            field: "geom".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            provenance: None,
        });
        assert_write_refused(inject(&mut plan, &store), "places");
    }
}
