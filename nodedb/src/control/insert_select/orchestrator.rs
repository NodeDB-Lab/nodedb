// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for `INSERT ... SELECT`.
//!
//! Every target row gets its OWN registered surrogate, never the source
//! row's, so cross-engine search resolves back to the target identity.
//! Since surrogate registration is Control-Plane-only, the copy runs as a
//! DP→CP→DP round trip: scan the source page-by-page
//! (`DocumentOp::MaterializeScan`), assign each row a fresh target-keyed
//! surrogate, write each page as one atomic `BatchInsert`.
//!
//! Each page is atomic (a violation aborts only that page); across pages
//! writes are separate transactions with `BatchInsert`'s usual partial
//! visibility. Source scan and target write are separated by the
//! assignment round trip, so this is NOT globally serializable against
//! concurrent source mutation — a deliberate, documented relaxation.

use nodedb_types::{DatabaseId, Lsn, Surrogate, TenantId};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::maintenance::clone_materializer::{dispatch_local, scan_source_page};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::DocumentOp;

/// Consume an authorized `INSERT ... SELECT` task at the orchestration boundary.
pub async fn run_authorized_insert_select(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    let PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection,
        source_collection,
        source_filters,
        source_limit,
        column_map,
    }) = task.plan
    else {
        return Err(crate::Error::BadRequest {
            detail: "authorized task is not INSERT ... SELECT".into(),
        });
    };
    run_insert_select(
        state,
        task.tenant_id,
        task.database_id,
        &CopyRequest {
            target_collection: target_collection.as_str(),
            source_collection: source_collection.as_str(),
            source_filters: &source_filters,
            source_limit,
            column_map: &column_map,
        },
    )
    .await
}

/// The plan-derived operands of one `INSERT ... SELECT` copy.
pub(crate) struct CopyRequest<'a> {
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    /// Serialized `Vec<ScanFilter>` residual `WHERE` predicate.
    pub source_filters: &'a [u8],
    /// Bounds how many source rows are copied.
    pub source_limit: usize,
    /// Serialized `Vec<ComputedColumn>` shaping each copied row. Empty for a
    /// plain copy.
    pub column_map: &'a [u8],
}

/// Drive an `INSERT ... SELECT` from `source_collection` into `target_collection`.
///
/// The copy operands travel in `CopyRequest`: `target_collection` /
/// `source_collection` are the (db-qualified) collection names as they
/// appear in the `DocumentOp::InsertSelect` plan.
///
/// Returns a `{"inserted": N}` response mirroring the shape the autocommit
/// dispatch loops shape as an `INSERT` command tag.
pub(crate) async fn run_insert_select(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    req: &CopyRequest<'_>,
) -> crate::Result<Response> {
    let spec = resolve_copy_spec(
        state,
        tenant_id,
        database_id,
        req.target_collection,
        req.source_filters,
        req.column_map,
    )?;

    let mut cursor: Vec<u8> = Vec::new();
    let mut remaining = req.source_limit;
    let mut total_inserted: usize = 0;
    let mut max_lsn = Lsn::ZERO;

    while remaining > 0 {
        // Phase 1: scan one source page (point-in-time snapshot).
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            database_id,
            req.source_collection,
            &cursor,
            None,
            None,
        )
        .await?;

        // Phase 2: normalize (strict → msgpack), filter surviving rows, and
        // assign fresh target surrogates via the shared copy pipeline.
        let rows = assign_page_rows(
            state,
            tenant_id,
            database_id,
            req.target_collection,
            &spec,
            entries,
            &mut remaining,
        )?;

        // Phase 3: one atomic batch write for this page.
        if !rows.is_empty() {
            let page_len = rows.len();
            let mut documents: Vec<(String, Vec<u8>)> = Vec::with_capacity(page_len);
            let mut surrogates: Vec<Surrogate> = Vec::with_capacity(page_len);
            for (document_id, value, surrogate) in rows {
                documents.push((document_id, value));
                surrogates.push(surrogate);
            }
            // Resolve this page's sum targets: `dispatch_local` bypasses the
            // statement-level resolution pass, so without this the fold has
            // no target to credit. Resolved per page since each is its own
            // atomic write.
            let page_bodies: Vec<&[u8]> =
                documents.iter().map(|(_, body)| body.as_slice()).collect();
            let mut resolved_sum_targets =
                crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies(
                    state,
                    &page_bodies,
                    req.target_collection,
                    tenant_id,
                    database_id,
                    crate::types::TraceId::ZERO,
                )
                .await?;
            // Period-lock target for the same page, in the SAME slot — a
            // target collection under a period lock reads its reference row's
            // surrogate off `resolved_sum_targets` exactly like a
            // materialized-sum fold does.
            resolved_sum_targets.extend(
                crate::control::planner::period_lock::resolve_period_lock_targets_for_bodies(
                    state,
                    &page_bodies,
                    req.target_collection,
                    tenant_id,
                    database_id,
                    crate::types::TraceId::ZERO,
                )
                .await?,
            );

            let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    req.target_collection.to_string(),
                ),
                documents,
                surrogates,
                // `INSERT ... SELECT` is paged across many of these writes, so
                // no single page owns the statement's answer; the clause is
                // refused at planning rather than half-answered here.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets,
                // Every page dispatches locally to the target's own core, so a
                // binding whose target is co-resident folds here; nothing is
                // deferred to a sibling task.
                deferred_sum_targets: Vec::new(),
            });
            let resp = dispatch_local(
                state,
                tenant_id,
                database_id,
                req.target_collection,
                plan,
                None,
            )
            .await?;
            if resp.status != Status::Ok {
                // Atomic page failure (e.g. constraint violation): the page's
                // rows did not land. Surface the DP error verbatim.
                return Ok(resp);
            }
            // `dispatch_local` bypasses the funnel's post-apply redo minting,
            // so a vector-indexed target's write-set arrives unconsumed. Mint
            // it now — without it, a WAL-only restart rebuilds the HNSW from
            // nothing for these rows: the vectors are lost, not just stale.
            crate::control::server::wal_dispatch::mint_dispatch_local_redo(
                &state.wal,
                tenant_id,
                database_id,
                req.target_collection,
                &resp,
            )?;
            total_inserted += decode_inserted(&resp.payload).unwrap_or(page_len);
            if resp.watermark_lsn > max_lsn {
                max_lsn = resp.watermark_lsn;
            }
        }

        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
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
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    })
}

/// Read the `"inserted"` count from a `BatchInsert` response payload.
fn decode_inserted(payload: &[u8]) -> Option<usize> {
    if payload.is_empty() {
        return None;
    }
    let json: serde_json::Value = nodedb_types::json_from_msgpack(payload)
        .ok()
        .or_else(|| sonic_rs::from_slice(payload).ok())?;
    json.get("inserted")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
}
