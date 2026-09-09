// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane resolution of the materialized-sum targets a write addresses
//! through its STORED row rather than through anything the plan carries.
//!
//! `PointDelete` carries no body and `PointUpdate` carries only field
//! assignments, so neither can say which target it contributes to on its own —
//! an empty resolution must not fail such statements outright. `PointPut` /
//! `Upsert` onto an existing row have the reverse gap: a join-column
//! rewrite moves value between TWO targets, but only the body-named one
//! resolves from the plan; the one the row is leaving is readable only from
//! the stored image.
//!
//! [`recon_point_row`] is the only plan-time source read: two readers could
//! disagree about where a collection lives, and the Data Plane never
//! resolves this itself (the pk→surrogate map is catalog state, off-limits
//! cross-plane). [`binding_join_keys`](crate::query::binding_join_keys)
//! resolves BOTH the pre- and post-image's join value — one side alone
//! leaves the other target's total wrong by the row's whole value. It reads
//! images, not assignments, because `Put`/`Upsert` form their post-image
//! from the submitted body too.

use std::sync::Arc;

use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, ResolvedSumTarget, UpdateValue,
};
use nodedb_types::Surrogate;

use super::recon::recon_point_row;
use super::resolve::lookup_join_value;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};

/// How a point-shaped write's POST-image is formed.
///
/// The four shapes form it four different ways, and the difference decides what
/// a cross-shard balance is settled from — so it is spelled out rather than
/// inferred from whether a body or an assignment list happens to be empty. An
/// `ON CONFLICT DO UPDATE` with no assignment and a whole-row `PUT` are
/// indistinguishable by that test, and they produce different post-images.
pub(super) enum PostImage<'a> {
    /// The row is removed; there is no post-image.
    Removed,
    /// The stored row with the statement's assignments applied.
    Assigned,
    /// The submitted body, replacing the stored row wholesale.
    Body(&'a [u8]),
    /// The submitted body when no row is stored; when one is, the CONFLICT
    /// branch's merge of the stored row with that same body — the body overlaid
    /// on it when the statement carries no conflict assignments, and the stored
    /// row rewritten by those assignments (with the body answering
    /// `EXCLUDED.col`) when it does.
    BodyOrAssigned(&'a [u8]),
}

/// The one stored row a point-shaped write rewrites or removes.
pub(super) struct StoredRowScope<'a> {
    /// Source collection as it appears on the plan (db-qualified).
    pub collection: &'a str,
    /// The row's user-facing primary key. Carried for the read plan; identity
    /// for storage addressing is `surrogate` and only `surrogate`.
    pub document_id: &'a str,
    /// Catalog-bound identity of the row, which is what actually addresses it.
    pub surrogate: Surrogate,
    /// The statement's assignments, so a write that rewrites the join column
    /// resolves the target it moves the row ONTO as well as the one it moves it
    /// off. Empty for the shapes that carry no assignments — a delete, and a put
    /// whose post-image is the submitted body.
    pub updates: &'a [(String, UpdateValue)],
    /// How this shape's post-image is formed.
    pub post_image: PostImage<'a>,
}

/// What reading the stored row produced: the write's pre-/post-image pairs
/// and the version they were read at — from the SAME read that resolved
/// the join values, so a re-read would fold a different snapshot.
pub(super) struct StoredImages {
    /// One pair per row this write touches — at most one, for a point shape.
    pub images: Vec<(Option<serde_json::Value>, Option<serde_json::Value>)>,
    /// The source collection's write floor at read time.
    pub read_version_lsn: Lsn,
}

/// The stored row an op rewrites or removes, or `None` for every op that
/// rewrites none. Exhaustive so a new `DocumentOp` variant must state which
/// side it is on. Covers all four point shapes, including `PointPut`/
/// `Upsert` (a join-column rewrite debits the leaving target, named only by
/// the stored image). `PointInsert`/`BatchInsert` are absent: rows are new
/// by construction, so there's no pre-image to read.
pub(super) fn stored_row_scope(op: &DocumentOp) -> Option<StoredRowScope<'_>> {
    match op {
        DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            updates,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: updates.as_slice(),
            post_image: PostImage::Assigned,
        }),
        // A delete assigns nothing: the row's pre-image join value is the whole
        // of what it owes.
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: &[],
            post_image: PostImage::Removed,
        }),
        // A put replaces the row wholesale, so its post-image is the submitted
        // body — already resolved from the body — and the stored row is needed
        // only for the value it held before.
        DocumentOp::PointPut {
            collection,
            document_id,
            surrogate,
            value,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: &[],
            post_image: PostImage::Body(value.as_slice()),
        }),
        // On the conflict branch an upsert's post-image is the stored row with
        // `on_conflict_updates` applied, so those assignments decide the target
        // the row moves ONTO exactly as an update's `SET` does. With no stored
        // row it inserts instead, and the submitted body is the post-image.
        DocumentOp::Upsert {
            collection,
            document_id,
            surrogate,
            on_conflict_updates,
            value,
            ..
        } => Some(StoredRowScope {
            collection: collection.as_str(),
            document_id: document_id.as_str(),
            surrogate: *surrogate,
            updates: on_conflict_updates.as_slice(),
            post_image: PostImage::BodyOrAssigned(value.as_slice()),
        }),
        DocumentOp::PointInsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::MaterializeScan { .. }
        // Already resolved on the plan the resolve pass read.
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Add every target the stored row addresses to `resolved`, and return the
/// write's pre-/post-image pairs so a CROSS-SHARD target's delta can be settled
/// from the SAME read.
///
/// A row that is not there resolves nothing for the shapes that need one: an
/// update or delete whose key matches no row rewrites no stored row and so owes
/// no target anything. That is the ordinary answer, not a failure — the write
/// path reaches the same conclusion about the same absent row. A put or upsert
/// with no stored row still writes: it INSERTS, and its post-image is the
/// submitted body, so it still owes its target the row's whole value.
pub(super) async fn extend_with_stored_row(
    state: &SharedState,
    bindings: &Arc<Vec<MaterializedSumBinding>>,
    scope: &StoredRowScope<'_>,
    resolved: &mut Vec<ResolvedSumTarget>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<StoredImages> {
    let read = recon_point_row(
        state,
        tenant_id,
        database_id,
        scope.collection,
        scope.document_id,
        scope.surrogate,
    )
    .await?;

    let outcome = StoredImages {
        images: images_of(scope, read.rows.as_ref())?,
        read_version_lsn: read.read_version_lsn,
    };

    if read.rows.is_none() {
        // Nothing stored: the body-driven resolution already covers the only
        // target such a write can address.
        return Ok(outcome);
    }

    // Both sides of every image pair, so a write that rewrites the join column
    // resolves the target it LEAVES and the one it JOINS. The join values are
    // read off the images the deltas will be folded from rather than re-derived
    // from the assignments: the two shapes that carry a body form their
    // post-image from the body as well as from the assignments, and a
    // re-derivation that saw only the assignments would resolve a target the
    // fold never addresses — or miss the one it does.
    let mut rows: Vec<serde_json::Value> = Vec::with_capacity(outcome.images.len() * 2);
    for (old, new) in &outcome.images {
        rows.extend(old.iter().cloned());
        rows.extend(new.iter().cloned());
    }
    for binding in bindings.iter() {
        for join_value in crate::query::binding_join_keys(binding, &[], &rows)? {
            // One entry per DISTINCT `(target collection, join value)` PAIR
            // across every binding and every source of join values, mirroring
            // the body-driven resolution: a write whose old and new join keys
            // are the same resolves that target once, while two bindings that
            // share a join column and name different targets each keep their
            // own entry.
            if resolved
                .iter()
                .any(|entry| entry.addresses(&binding.target_collection, &join_value))
            {
                continue;
            }
            let surrogate = lookup_join_value(
                state,
                binding,
                &join_value,
                tenant_id,
                database_id,
                trace_id,
            )
            .await?;
            resolved.push(ResolvedSumTarget::new(
                &binding.target_collection,
                join_value,
                surrogate,
            ));
        }
    }
    Ok(outcome)
}

/// The pre-/post-image pairs `scope` produces against `stored`.
///
/// One pair at most: a point write touches one row. An empty result is a write
/// that rewrites nothing at all — an update or delete whose key matched no row.
fn images_of(
    scope: &StoredRowScope<'_>,
    stored: Option<&serde_json::Value>,
) -> crate::Result<Vec<(Option<serde_json::Value>, Option<serde_json::Value>)>> {
    let assigned =
        |old: &serde_json::Value| crate::query::apply_update_assignments(old, scope.updates);
    Ok(match (&scope.post_image, stored) {
        (PostImage::Removed, Some(old)) => vec![(Some(old.clone()), None)],
        (PostImage::Assigned, Some(old)) => vec![(Some(old.clone()), Some(assigned(old)?))],
        (PostImage::Body(body), Some(old)) => {
            vec![(Some(old.clone()), decode_body(body))]
        }
        (PostImage::Body(body), None) => vec![(None, decode_body(body))],
        (PostImage::BodyOrAssigned(body), Some(old)) => {
            // The CONFLICT branch. The submitted body is half of the answer —
            // it is what `EXCLUDED.col` resolves to, and it is what an upsert
            // with no conflict assignments merges over the stored row. A body
            // that will not decode carries no column either form can read, so
            // it stands in as an empty row: `EXCLUDED.col` is then NULL, which
            // is what the evaluator answers for a column the excluded row does
            // not carry.
            let submitted =
                decode_body(body).unwrap_or_else(|| serde_json::Value::Object(Default::default()));
            vec![(
                Some(old.clone()),
                Some(crate::query::apply_conflict_assignments(
                    old,
                    &submitted,
                    scope.updates,
                )?),
            )]
        }
        (PostImage::BodyOrAssigned(body), None) => vec![(None, decode_body(body))],
        // Nothing stored and nothing submitted: the write rewrites no row.
        (PostImage::Removed | PostImage::Assigned, None) => Vec::new(),
    })
}

/// Decode a submitted MessagePack body.
///
/// A body that will not decode carries no column any binding can read, so it
/// contributes no delta — the same conclusion the Data-Plane hook reaches for a
/// submitted body it cannot decode.
fn decode_body(body: &[u8]) -> Option<serde_json::Value> {
    nodedb_types::json_from_msgpack(body).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::QualifiedCollection;

    const ROW: Surrogate = Surrogate(77);

    fn literal(value: serde_json::Value) -> UpdateValue {
        UpdateValue::Literal(nodedb_types::json_to_msgpack(&value).expect("encode literal"))
    }

    fn assignments() -> Vec<(String, UpdateValue)> {
        vec![(
            "account_id".to_string(),
            literal(serde_json::json!("acc-2")),
        )]
    }

    fn point_delete() -> DocumentOp {
        DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn point_update() -> DocumentOp {
        DocumentOp::PointUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            updates: assignments(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        }
    }

    fn point_put() -> DocumentOp {
        DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            value: Vec::new(),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn upsert() -> DocumentOp {
        DocumentOp::Upsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            value: Vec::new(),
            on_conflict_updates: assignments(),
            surrogate: ROW,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn point_insert() -> DocumentOp {
        DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            value: Vec::new(),
            if_absent: false,
            surrogate: ROW,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }
    }

    /// Every point-shaped write that can land on an EXISTING row names that row
    /// for the resolution — including the two that carry a body, whose body
    /// names only the target the row moves ONTO.
    #[test]
    fn every_point_shape_names_the_stored_row_it_rewrites() {
        for op in [point_delete(), point_update(), point_put(), upsert()] {
            let scope = stored_row_scope(&op)
                .unwrap_or_else(|| panic!("a point write must name its stored row: {op:?}"));
            assert_eq!(scope.collection, "entries");
            assert_eq!(scope.document_id, "e1");
            assert_eq!(
                scope.surrogate, ROW,
                "the row is addressed by its surrogate, never by the primary-key string"
            );
        }
    }

    /// An insert's row is new by construction, so there is no stored image to
    /// read and no round trip to spend learning that.
    #[test]
    fn an_insert_names_no_stored_row() {
        assert!(stored_row_scope(&point_insert()).is_none());
    }

    /// The assignments travel on the scope, so a write that rewrites the join
    /// column resolves the target it moves the row ONTO as well as the one it
    /// moves it off. An upsert's conflict-branch assignments count for exactly
    /// the same reason.
    #[test]
    fn assignments_that_rewrite_the_join_column_travel_on_the_scope() {
        for op in [point_update(), upsert()] {
            let scope = stored_row_scope(&op).expect("a point write names its stored row");
            assert_eq!(
                scope.updates.len(),
                1,
                "the statement's assignments must reach the resolution: {op:?}"
            );
            assert_eq!(scope.updates[0].0, "account_id");
        }
    }

    /// A delete and a whole-row put assign nothing: the stored row's own join
    /// value is the whole of what each owes.
    #[test]
    fn the_shapes_without_assignments_carry_none() {
        for op in [point_delete(), point_put()] {
            let scope = stored_row_scope(&op).expect("a point write names its stored row");
            assert!(scope.updates.is_empty(), "{op:?}");
        }
    }

    fn body(account: &str, amount: i64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({
            "account_id": account,
            "amount": amount,
        }))
        .expect("encode body")
    }

    fn upsert_with(value: Vec<u8>, on_conflict_updates: Vec<(String, UpdateValue)>) -> DocumentOp {
        DocumentOp::Upsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            value,
            on_conflict_updates,
            surrogate: ROW,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }
    }

    fn excluded(column: &str) -> UpdateValue {
        UpdateValue::Expr(nodedb_query::expr::SqlExpr::ExcludedColumn(
            column.to_string(),
        ))
    }

    fn amount_of(image: Option<&serde_json::Value>) -> Option<i64> {
        image
            .and_then(|row| row.get("amount"))
            .and_then(serde_json::Value::as_i64)
    }

    /// The conflict branch's post-image takes its value from the SUBMITTED
    /// body. `SET amount = EXCLUDED.amount` is the ordinary way to write the
    /// statement, and evaluating it without that body resolves `EXCLUDED.amount`
    /// to NULL — the fold then reads the post-image's value as zero and debits
    /// the target the row's whole old contribution instead of crediting the
    /// difference.
    #[test]
    fn an_upsert_conflict_branch_folds_the_submitted_body() {
        let op = upsert_with(
            body("acc-1", 60),
            vec![("amount".to_string(), excluded("amount"))],
        );
        let scope = stored_row_scope(&op).expect("an upsert names its stored row");
        let stored = serde_json::json!({"account_id": "acc-1", "amount": 25});
        let images = images_of(&scope, Some(&stored)).expect("images");

        assert_eq!(images.len(), 1);
        assert_eq!(images[0].0.as_ref(), Some(&stored));
        assert_eq!(amount_of(images[0].1.as_ref()), Some(60));
    }

    /// An upsert with NO conflict assignments merges the submitted body over
    /// the stored row, so a body that raises the value settles the difference
    /// rather than nothing at all.
    #[test]
    fn an_upsert_without_conflict_assignments_merges_the_body() {
        let op = upsert_with(body("acc-1", 60), Vec::new());
        let scope = stored_row_scope(&op).expect("an upsert names its stored row");
        let stored = serde_json::json!({"account_id": "acc-1", "amount": 25, "memo": "kept"});
        let images = images_of(&scope, Some(&stored)).expect("images");

        assert_eq!(amount_of(images[0].1.as_ref()), Some(60));
        assert_eq!(
            images[0]
                .1
                .as_ref()
                .and_then(|row| row.get("memo"))
                .and_then(serde_json::Value::as_str),
            Some("kept"),
            "a column the body omits survives the merge"
        );
    }

    /// A conflict assignment that rewrites the join column from `EXCLUDED`
    /// moves the row between two targets, and the post-image names the one it
    /// moves ONTO.
    #[test]
    fn an_upsert_conflict_branch_can_move_the_join_key() {
        let op = upsert_with(
            body("acc-2", 60),
            vec![
                ("account_id".to_string(), excluded("account_id")),
                ("amount".to_string(), excluded("amount")),
            ],
        );
        let scope = stored_row_scope(&op).expect("an upsert names its stored row");
        let stored = serde_json::json!({"account_id": "acc-1", "amount": 25});
        let images = images_of(&scope, Some(&stored)).expect("images");

        assert_eq!(
            images[0]
                .1
                .as_ref()
                .and_then(|row| row.get("account_id"))
                .and_then(serde_json::Value::as_str),
            Some("acc-2")
        );
        assert_eq!(amount_of(images[0].1.as_ref()), Some(60));
    }

    /// With no stored row the upsert INSERTS, so its post-image is the
    /// submitted body and it owes the target the row's whole value.
    #[test]
    fn an_upsert_onto_no_row_folds_the_body_as_an_insert() {
        let op = upsert_with(
            body("acc-1", 60),
            vec![("amount".to_string(), excluded("amount"))],
        );
        let scope = stored_row_scope(&op).expect("an upsert names its stored row");
        let images = images_of(&scope, None).expect("images");

        assert_eq!(images.len(), 1);
        assert!(images[0].0.is_none(), "an insert has no pre-image");
        assert_eq!(amount_of(images[0].1.as_ref()), Some(60));
    }

    /// A whole-row PUT does not share the conflict branch's gap: it carries no
    /// assignments at all, so its post-image is the submitted body and there is
    /// no `EXCLUDED` reference to resolve.
    #[test]
    fn a_put_onto_an_existing_row_folds_the_body_wholesale() {
        let op = DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "entries"),
            document_id: "e1".to_string(),
            value: body("acc-1", 60),
            surrogate: ROW,
            pk_bytes: b"e1".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        };
        let scope = stored_row_scope(&op).expect("a put names its stored row");
        let stored = serde_json::json!({"account_id": "acc-1", "amount": 25});
        let images = images_of(&scope, Some(&stored)).expect("images");

        assert_eq!(images[0].0.as_ref(), Some(&stored));
        assert_eq!(amount_of(images[0].1.as_ref()), Some(60));
    }
}
