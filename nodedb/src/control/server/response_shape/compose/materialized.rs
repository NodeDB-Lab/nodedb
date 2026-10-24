// SPDX-License-Identifier: BUSL-1.1

//! Composed, protocol-neutral materialized response shaping.
//!
//! `shape_response_materialized` is the canonical SELECT-read shaping used by
//! every protocol entrypoint. It performs the full per-payload shaping order
//! (`apply_kv_wrap` -> `apply_walk_wrap` -> `translate_search_response` ->
//! decode -> scan-envelope
//! unwrap -> optional SELECT-list projection) as a single call, producing an
//! already-shaped, already-projected [`ShapeOutcome`]. Every SELECT-read
//! producer — pgwire's non-streaming dispatch, native's dispatch loop — calls
//! this directly and hands the resulting `ShapedRows` to its own protocol
//! encoder; each protocol then encodes those rows in its own wire format
//! (pgwire's RowDescription/DataRow, native's MessagePack, http's JSON).
//!
//! Gateway forwarding shapes through this same call with the forwarded task's
//! plan, so a forwarded payload yields the rows a local one does. Producers
//! with no `PhysicalPlan` in scope (ClusterArray, set-op merges, clone merges)
//! call [`shape_payload_no_plan`], which
//! skips the plan-dependent `apply_kv_wrap` / `translate_search_response`
//! transforms those callers never ran. The pure kernel `shape_decoded_rows`
//! is shared with per-batch lazy streaming callers, which have an
//! already-decoded batch and only need the envelope-unwrap + projection logic.

use crate::control::sequence::SequenceAccess;
use crate::control::server::response_translate::dispatch::translate_search_response;
use crate::data::executor::response_codec::decode_payload_value;
use nodedb_types::NodeDbError;

use super::super::kv::apply_kv_wrap;
use super::super::redaction::RedactionCtx;
use super::super::request::MaterializedShapeRequest;
use super::super::returning::shape_returning_rows;
use super::super::schema::OutputSchema;
use super::super::types::{PlanKind, ShapedRows};
use super::super::walk::apply_walk_wrap;
use super::array_slice::shape_array_slice;
use super::kernel::{empty_shaped, shape_decoded_rows, single_result_row};

/// Outcome of materialized response shaping.
///
/// Row-producing plan kinds (`SingleDocument`, `MultiRow`, `ReturningRows`,
/// `ArraySlice`) yield `Rows`. Tag/execution kinds (`Execution`,
/// `DmlResult`, `DmlResultByOp`) yield `Passthrough` — a `ShapedRows` cannot
/// represent a bare `CommandComplete` tag or affected-row count, so callers
/// keep their existing tag / `rows_affected` handling for those.
pub enum ShapeOutcome {
    Rows(ShapedRows),
    Passthrough,
}

/// Shape a single Data-Plane payload into protocol-neutral rows, applying
/// the canonical shaping order: KV point-get wrap, walk wrap, search
/// surrogate->PK naming, payload decode, scan-envelope unwrap, and (when
/// `projection` names columns) SELECT-list column selection.
///
/// Async: the search naming step can ask a collection's home leader for
/// keys this node's catalog lacks.
pub async fn shape_response_materialized(
    request: MaterializedShapeRequest<'_>,
) -> Result<ShapeOutcome, NodeDbError> {
    let MaterializedShapeRequest {
        payload,
        plan,
        plan_kind,
        projection,
        state,
        database_id,
        tenant_id,
        redaction,
        sequences,
    } = request;

    match plan_kind {
        PlanKind::Execution | PlanKind::DmlResult(_) | PlanKind::DmlResultByOp => {
            return Ok(ShapeOutcome::Passthrough);
        }
        PlanKind::ArraySlice
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => {}
    }

    // The plan-dependent transforms run before any decode or shape step:
    // KV wrap, then walk wrap, then search naming.
    let wrapped = apply_walk_wrap(plan, &apply_kv_wrap(plan, payload))?;
    let translated =
        translate_search_response(&wrapped, plan, state, database_id, tenant_id).await?;
    shape_translated(
        &translated,
        plan_kind,
        projection,
        redaction,
        sequences.map(|s| s as &dyn SequenceAccess),
        &crate::control::crs_context::crs_set_for(state),
    )
}

/// Shape a write's `RETURNING` payload. Synchronous: a `RETURNING` payload
/// holds stored rows, never search hits, so it has no surrogate to name.
/// Any other plan kind is refused, since it can carry search hits.
pub fn shape_returning_materialized(
    request: MaterializedShapeRequest<'_>,
) -> Result<ShapeOutcome, NodeDbError> {
    let MaterializedShapeRequest {
        payload,
        plan,
        plan_kind,
        projection,
        state,
        redaction,
        sequences,
        ..
    } = request;
    if !matches!(plan_kind, PlanKind::ReturningRows) {
        return Err(crate::Error::Internal {
            detail: format!(
                "returning shaping received a {plan_kind:?} payload, which needs search naming"
            ),
        }
        .into());
    }
    let wrapped = apply_walk_wrap(plan, &apply_kv_wrap(plan, payload))?;
    shape_translated(
        &wrapped,
        plan_kind,
        projection,
        redaction,
        sequences.map(|s| s as &dyn SequenceAccess),
        &crate::control::crs_context::crs_set_for(state),
    )
}

/// Shape a payload whose plan-dependent transforms already ran.
fn shape_translated(
    translated: &[u8],
    plan_kind: PlanKind,
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
    sequences: Option<&dyn SequenceAccess>,
    crs_set: &nodedb_crs::CustomCrsSet,
) -> Result<ShapeOutcome, NodeDbError> {
    let shaped = match plan_kind {
        PlanKind::ArraySlice => {
            shape_array_slice(translated, projection, redaction, sequences, crs_set)?
        }
        // `RETURNING` rows are held to the columns already announced to the
        // client, when any were — see `super::returning`.
        PlanKind::ReturningRows => {
            shape_returning_rows(translated, projection, redaction, sequences, crs_set)?
        }
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            shape_generic_rows(translated, projection, redaction, sequences, crs_set)?
        }
        // The callers return early for these; kept exhaustive (no catch-all,
        // no panic) so a future PlanKind desync degrades to passthrough
        // rather than crashing the connection.
        PlanKind::Execution | PlanKind::DmlResult(_) | PlanKind::DmlResultByOp => {
            return Ok(ShapeOutcome::Passthrough);
        }
    };
    Ok(ShapeOutcome::Rows(shaped))
}

/// Shape a Data-Plane payload with no `PhysicalPlan` in scope.
///
/// Producers that never had a plan to KV-wrap or vector-translate
/// (ClusterArray, set-op merges, clone merges) call this
/// instead of [`shape_response_materialized`]: it applies only the decode +
/// scan-envelope unwrap + optional SELECT-list projection steps, skipping the
/// plan-dependent `apply_kv_wrap` / `translate_search_response` transforms those
/// callers never ran.
///
/// `sequences` resolves the sequence accessors of the projection's
/// Control-Plane computed columns. A caller with no session in scope passes
/// `None`. A column that calls an accessor then fails rather than shipping
/// NULL. A column that calls none evaluates.
pub fn shape_payload_no_plan(
    payload: &[u8],
    plan_kind: PlanKind,
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
    sequences: Option<&dyn SequenceAccess>,
    crs_set: &nodedb_crs::CustomCrsSet,
) -> Result<ShapeOutcome, NodeDbError> {
    Ok(match plan_kind {
        PlanKind::Execution | PlanKind::DmlResult(_) | PlanKind::DmlResultByOp => {
            ShapeOutcome::Passthrough
        }
        PlanKind::ArraySlice => ShapeOutcome::Rows(shape_array_slice(
            payload, projection, redaction, sequences, crs_set,
        )?),
        PlanKind::ReturningRows => ShapeOutcome::Rows(shape_returning_rows(
            payload, projection, redaction, sequences, crs_set,
        )?),
        PlanKind::SingleDocument | PlanKind::MultiRow => ShapeOutcome::Rows(shape_generic_rows(
            payload, projection, redaction, sequences, crs_set,
        )?),
    })
}

/// Shape a `SingleDocument` / `MultiRow` response: decode the payload to a
/// typed value, then hand it to the pure [`shape_decoded_rows`] core.
///
/// A payload that decodes as neither msgpack nor JSON falls back to a single
/// "result" column holding its lossy UTF-8 text, matching pgwire's
/// single-row fallback.
fn shape_generic_rows(
    payload: &[u8],
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
    sequences: Option<&dyn SequenceAccess>,
    crs_set: &nodedb_crs::CustomCrsSet,
) -> crate::Result<ShapedRows> {
    if payload.is_empty() {
        return Ok(empty_shaped());
    }
    match decode_payload_value(payload) {
        Ok(value) => shape_decoded_rows(value, projection, redaction, sequences, crs_set),
        Err(_) => Ok(single_result_row(
            String::from_utf8_lossy(payload).into_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::wal::WalManager;
    use nodedb_types::CrdtPreviewResult;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::response_shape::types::describe_plan;
    use crate::control::state::SharedState;
    use nodedb_types::{DatabaseId, TenantId};

    fn preview_plan() -> PhysicalPlan {
        PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::PreviewApply {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "tasks"),
            document_id: "task-1".to_string(),
            delta: vec![0x92, 0x01],
        })
    }

    fn preview_payload() -> (CrdtPreviewResult, Vec<u8>) {
        let result = CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            pre_image_msgpack: vec![0xc0],
            imported_ops: 17,
            trimmed_ops: 0,
            frontier_digest: [0x5a; 32],
        };
        let payload = zerompk::to_msgpack_vec(&result).expect("preview result serializes");
        (result, payload)
    }

    /// Build the minimum real shared state needed by the materialized entry
    /// point. Execution plans return before consulting it, which is exactly
    /// the property this test protects.
    fn shared_state() -> Arc<SharedState> {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("response-shape.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, _) = Dispatcher::new(1, 1);
        SharedState::new(dispatcher, wal).expect("test shared state")
    }

    #[tokio::test]
    async fn crdt_preview_is_byte_preserving_through_both_shaping_entry_points() {
        let plan = preview_plan();
        let kind = describe_plan(&plan);
        assert!(matches!(kind, PlanKind::Execution));
        let (expected, payload) = preview_payload();
        let original_payload = payload.clone();

        let state = shared_state();
        let materialized = shape_response_materialized(MaterializedShapeRequest {
            payload: &payload,
            plan: &plan,
            plan_kind: kind,
            projection: None,
            state: &state,
            database_id: DatabaseId::new(1),
            tenant_id: TenantId::new(1),
            redaction: None,
            sequences: None,
        })
        .await
        .expect("execution plan passthrough");
        assert!(matches!(materialized, ShapeOutcome::Passthrough));
        assert_eq!(
            payload, original_payload,
            "materialized path must not rewrite bytes"
        );
        assert_eq!(
            zerompk::from_msgpack::<CrdtPreviewResult>(&payload)
                .expect("materialized passthrough remains decodable"),
            expected
        );

        let no_plan = shape_payload_no_plan(
            &payload,
            kind,
            None,
            None,
            None,
            nodedb_crs::CustomCrsSet::empty(),
        );
        assert!(matches!(no_plan, Ok(ShapeOutcome::Passthrough)));
        assert_eq!(
            payload, original_payload,
            "no-plan path must not rewrite bytes"
        );
        assert_eq!(
            zerompk::from_msgpack::<CrdtPreviewResult>(&payload)
                .expect("no-plan passthrough remains decodable"),
            expected
        );
    }

    /// A msgpack `bin` cell reaches the shaped row as `Value::Bytes`; its
    /// text form is decided at the protocol edge.
    #[test]
    fn a_byte_cell_shapes_as_bytes() {
        let mut row = std::collections::HashMap::new();
        row.insert(
            "blob".to_string(),
            nodedb_types::Value::Bytes(vec![0, 255, 7]),
        );
        let payload = nodedb_types::value_to_msgpack(&nodedb_types::Value::Array(vec![
            nodedb_types::Value::Object(row),
        ]))
        .expect("encode");

        let ShapeOutcome::Rows(shaped) = shape_payload_no_plan(
            &payload,
            PlanKind::MultiRow,
            None,
            None,
            None,
            nodedb_crs::CustomCrsSet::empty(),
        )
        .expect("shape") else {
            panic!("multi-row plan must yield rows");
        };
        assert_eq!(
            shaped.rows[0]["blob"],
            nodedb_types::Value::Bytes(vec![0, 255, 7])
        );
    }
}
