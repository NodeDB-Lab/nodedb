// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for `INSERT ... SELECT`.
//!
//! Every target row gets its OWN registered surrogate, never the source
//! row's, so cross-engine search resolves back to the target identity.
//! Since surrogate registration is Control-Plane-only, the copy runs as a
//! DP→CP→DP round trip: read the source batch by batch
//! (`control::source_read`), assign each row a fresh target-keyed
//! surrogate, write each batch as one atomic `BatchInsert`.
//!
//! The source is read like a SELECT: its engine, WHERE, ORDER BY, LIMIT,
//! OFFSET, DISTINCT, windows, `AS OF`, and read policy all hold. A plain
//! document scan is read page by page with no row bound. Any other source is
//! read once, bounded by the scan byte budget, which fails loudly.
//!
//! Each batch is atomic (a violation aborts only that batch); across batches
//! writes are separate transactions with `BatchInsert`'s usual partial
//! visibility. Source read and target write are separated by the
//! assignment round trip, so this is NOT globally serializable against
//! concurrent source mutation — a deliberate, documented relaxation. A copy
//! into its own source is planned as a one-shot read, so it never reads the
//! rows it writes.

use nodedb_types::{DatabaseId, Lsn, Surrogate, TenantId};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::orchestrated_write::apply_orchestrated_write;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::source_read::{SourceReadContext, SourceReader};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, RowSource};

/// Consume an authorized `INSERT ... SELECT` task at the orchestration boundary.
pub async fn run_authorized_insert_select(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let identity = authorized.identity().clone();
    let task = authorized.into_physical_task();
    let PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection,
        source,
    }) = task.plan
    else {
        return Err(crate::Error::BadRequest {
            detail: "authorized task is not INSERT ... SELECT".into(),
        });
    };
    run_insert_select(
        state,
        &identity,
        task.tenant_id,
        task.database_id,
        target_collection.as_str(),
        &source,
    )
    .await
}

/// Drive an `INSERT ... SELECT` from `source` into `target_collection`, the
/// db-qualified name as it appears in the `DocumentOp::InsertSelect` plan.
///
/// Returns a `{"inserted": N}` response mirroring the shape the autocommit
/// dispatch loops shape as an `INSERT` command tag.
async fn run_insert_select(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
    source: &RowSource,
) -> crate::Result<Response> {
    let spec = resolve_copy_spec(state, tenant_id, database_id, target_collection)?;
    let ctx = SourceReadContext {
        state,
        identity,
        tenant_id,
        database_id,
        txn_id: None,
    };
    let mut reader = SourceReader::new(source)?;
    let mut total_inserted: usize = 0;
    let mut max_lsn = Lsn::ZERO;

    while let Some(batch) = reader.next_batch(&ctx).await? {
        if batch.is_empty() {
            continue;
        }
        // Assign fresh target surrogates to the shaped source rows.
        let rows = assign_page_rows(
            state,
            tenant_id,
            database_id,
            target_collection,
            &spec,
            batch,
        )
        .await?;

        // One atomic batch write per source batch.
        let mut documents: Vec<(String, Vec<u8>)> = Vec::with_capacity(rows.len());
        let mut surrogates: Vec<Surrogate> = Vec::with_capacity(rows.len());
        for (document_id, value, surrogate) in rows {
            documents.push((document_id, value));
            surrogates.push(surrogate);
        }
        // Resolve this batch's sum targets: the orchestrated apply bypasses
        // the statement-level resolution pass, so without this the fold has
        // no target to credit. Resolved per batch since each is its own
        // atomic write.
        let page_bodies: Vec<&[u8]> = documents.iter().map(|(_, body)| body.as_slice()).collect();
        let mut resolved_sum_targets =
            crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies(
                state,
                &page_bodies,
                target_collection,
                tenant_id,
                database_id,
                crate::types::TraceId::ZERO,
            )
            .await?;
        // Period-lock target for the same batch, in the SAME slot — a target
        // collection under a period lock reads its reference row's surrogate
        // off `resolved_sum_targets` exactly like a materialized-sum fold does.
        resolved_sum_targets.extend(
            crate::control::planner::period_lock::resolve_period_lock_targets_for_bodies(
                state,
                &page_bodies,
                target_collection,
                tenant_id,
                database_id,
                crate::types::TraceId::ZERO,
            )
            .await?,
        );

        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: nodedb_types::QualifiedCollection::from_stored(
                database_id,
                target_collection.to_string(),
            )?,
            documents,
            // A copied row whose key the target already holds is refused,
            // as a VALUES row is.
            if_absent: false,
            surrogates,
            // `INSERT ... SELECT` is batched across many of these writes, so
            // no single batch owns the statement's answer; the clause is
            // refused at planning rather than half-answered here.
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets,
            // Every batch applies on the target's own vShard, so a binding
            // whose target is co-resident folds here; nothing is deferred to
            // a sibling task.
            deferred_sum_targets: Vec::new(),
        });
        // Each batch lands on the target's owner and every replica.
        let resp = apply_orchestrated_write(
            state,
            tenant_id,
            database_id,
            target_collection,
            plan,
            identity.role_names(),
        )
        .await?;
        if resp.status != Status::Ok {
            // Atomic batch failure (e.g. constraint violation): the batch's
            // rows did not land. Surface the DP error verbatim.
            return Ok(resp);
        }
        total_inserted += decode_inserted(&resp.payload)?;
        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }
    }

    let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": total_inserted }))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("insert-select response: {e}"),
        })?;

    Ok(Response {
        request_id: crate::types::RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload),
        watermark_lsn: max_lsn,
        error_code: None,
        stage_vote: None,
        read_vshards: crate::types::ReadShards::new(),
        write_set: Vec::new(),
    })
}

/// Read the `"inserted"` count from a `BatchInsert` response payload. A
/// payload without the count is an error: the statement tag would otherwise
/// report rows the write never confirmed.
fn decode_inserted(payload: &[u8]) -> crate::Result<usize> {
    let malformed = || crate::Error::Internal {
        detail: "INSERT ... SELECT: batch insert answered without an 'inserted' count".into(),
    };
    let json = nodedb_types::json_from_msgpack(payload).map_err(|_| malformed())?;
    json.get("inserted")
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_inserted_count_is_read_from_the_payload() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "inserted": 3 }))
            .expect("encode payload");
        assert_eq!(decode_inserted(&payload).expect("count"), 3);
    }

    #[test]
    fn a_payload_without_the_count_is_an_error() {
        let payload = nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": 3 }))
            .expect("encode payload");
        assert!(decode_inserted(&payload).is_err());
        assert!(decode_inserted(&[]).is_err());
    }
}
