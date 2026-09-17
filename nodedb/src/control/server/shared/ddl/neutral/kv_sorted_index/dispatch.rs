// SPDX-License-Identifier: BUSL-1.1

//! Data Plane dispatch and response shaping for the sorted-index family.
//!
//! Every plan here is hand-built and reaches the Data Plane through
//! `dispatch_utils`, which accepts a trusted internal plan and runs neither the
//! RBAC check nor RLS injection. Authorization therefore happens before a plan
//! gets this far — see [`super::gate`].

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

use super::super::super::result::{DdlError, DdlResult};
use super::parse::ddl_err;

/// Where one sorted index's Data Plane state lives.
///
/// A sorted index is an order-statistic tree built from, and maintained by, the
/// rows of the collection it covers. Both of those happen inside the `KvEngine`
/// of a single Data Plane core, which owns nothing but the vShards routed to it:
/// the backfill can only see rows on its own core, and `KvEngine::put` /
/// `delete` can only update index trees registered on its own core. So the
/// index has exactly one correct home — the vShard the *collection* hashes to,
/// in the caller's database, which is where every KV write to that collection
/// already lands.
///
/// Registration, maintenance, query and teardown must therefore all resolve the
/// same coordinates from the same place. This type is that place: it carries the
/// owning collection (resolved from the index registry — see [`super::gate`])
/// rather than the index name, so no route can derive a different vShard or a
/// different `database_id` than the rows do.
pub struct SortedIndexTarget<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The collection the index was built over.
    pub collection: &'a str,
}

impl SortedIndexTarget<'_> {
    /// The vShard holding both the collection's rows and its index trees.
    fn vshard(&self) -> VShardId {
        VShardId::from_collection_in_database(self.database_id, self.collection)
    }
}

/// Turn a refused Data-Plane reply into an error.
///
/// A `Response` carries its verdict in `status`, not in the transport `Result`,
/// so a caller that checks only the `Result` reports success over a refusal and
/// then renders the empty payload as an empty answer. For this family that is
/// indistinguishable from a real result — `TOPK` legitimately returns no rows
/// for an empty index — which is exactly how a sorted index that was never
/// built anywhere kept answering "no rows" instead of saying so.
///
/// `NotFound` here means the index is absent from the core that owns its
/// collection while the catalog still lists it, so it is surfaced as an
/// undefined object rather than swallowed.
fn refusal(target: &SortedIndexTarget<'_>, resp: &Response) -> Option<DdlError> {
    if resp.status != Status::Error {
        return None;
    }
    Some(match resp.error_code.as_deref() {
        Some(ErrorCode::NotFound) => ddl_err(
            "42704",
            format!(
                "sorted index is registered in the catalog but absent from the engine \
                 state of '{}'",
                target.collection
            ),
        ),
        Some(other) => ddl_err("XX000", format!("{other:?}")),
        None => ddl_err("XX000", String::from_utf8_lossy(&resp.payload).into_owned()),
    })
}

/// Dispatch a sorted-index read (`RANK` / `TOPK` / `RANGE` / `SORTED_COUNT` /
/// `ZSCORE`), which mints no durable record.
async fn dispatch_read(
    state: &SharedState,
    target: &SortedIndexTarget<'_>,
    plan: PhysicalPlan,
) -> Result<Response, DdlError> {
    let resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        target.tenant_id,
        target.database_id,
        target.vshard(),
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    match refusal(target, &resp) {
        Some(error) => Err(error),
        None => Ok(resp),
    }
}

/// Dispatch a sorted-index registration or teardown.
///
/// These go through the autocommit write funnel rather than the read path so
/// the funnel appends their WAL record (`kv_register_sorted_index` /
/// `kv_drop_sorted_index`) under the write-admission guard. The manager holds
/// the tree only in memory, so that record plus the KV checkpoint is all that
/// carries a registration across a restart: dispatched as a read, the catalog
/// would keep listing an index whose tree no longer exists anywhere.
async fn dispatch_durable(
    state: &SharedState,
    target: &SortedIndexTarget<'_>,
    plan: PhysicalPlan,
) -> Result<Response, DdlError> {
    crate::control::server::dispatch_utils::dispatch_autocommit_write(
        state,
        crate::control::server::dispatch_utils::AutocommitWrite {
            tenant_id: target.tenant_id,
            database_id: target.database_id,
            vshard_id: target.vshard(),
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
        },
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))
}

/// Decode a row-shaped sorted-index reply.
///
/// The Data Plane encodes these rows with
/// `response_codec::encode_json_vec_as_msgpack`, so `decode_payload` is their
/// counterpart. A JSON parser on those bytes fails on the first byte, and a
/// decoder that defaults that failure into an empty row set would
/// report an empty leaderboard for every query, whatever the index held.
fn decode_rows(payload: &[u8]) -> Result<Vec<serde_json::Value>, DdlError> {
    crate::data::executor::response_codec::decode_payload(payload)
        .map_err(|e| ddl_err("XX000", format!("sorted index reply: {e}")))
}

/// Build the index's tree on the core that owns its collection's rows, and
/// return a DDL tag response.
///
/// A refused apply fails the statement. The caller writes the catalog registry
/// record only after this returns, and that record is what every later read
/// resolves the index through — reporting `CREATE SORTED INDEX` successful over
/// an apply that did not happen files a record for an index that exists
/// nowhere, and every read of it then answers from an index that was never
/// built.
pub(super) async fn register_in_engine(
    state: &SharedState,
    target: &SortedIndexTarget<'_>,
    plan: PhysicalPlan,
    tag: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let resp = dispatch_durable(state, target, plan).await?;
    if let Some(error) = refusal(target, &resp) {
        return Err(error);
    }
    Ok(vec![DdlResult::Status {
        command: tag.to_string(),
        rows_affected: None,
    }])
}

/// Remove a sorted index's Data Plane state.
///
/// Shared by `DROP SORTED INDEX` and by the generic `DROP INDEX` teardown, so
/// both remove the same state through the same route.
///
/// `NotFound` is success here, unlike on the read and register paths: removal
/// is idempotent, and the state this is asked to reclaim being already gone is
/// the outcome the caller wanted. Every other refusal fails the drop, so a
/// teardown never reports an index reclaimed while its tree is still live.
pub async fn drop_in_engine(
    state: &SharedState,
    target: &SortedIndexTarget<'_>,
    index_name: &str,
) -> Result<(), DdlError> {
    let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.to_string(),
    });
    let resp = dispatch_durable(state, target, plan).await?;
    if !matches!(resp.error_code.as_deref(), Some(ErrorCode::NotFound))
        && let Some(error) = refusal(target, &resp)
    {
        return Err(error);
    }
    Ok(())
}

/// Dispatch plan and return a single-row JSON response.
pub(super) async fn dispatch_and_respond_json(
    state: &SharedState,
    target: &SortedIndexTarget<'_>,
    plan: PhysicalPlan,
    col_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let resp = dispatch_read(state, target, plan).await?;
    let payload_text = crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
    let mut row = Map::new();
    row.insert(col_name.to_string(), JsonValue::String(payload_text));
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec![col_name.to_string()],
        vec![row],
    ))])
}

/// Dispatch plan and return multi-row response (for TOPK, RANGE).
pub(super) async fn dispatch_and_respond_rows(
    state: &SharedState,
    target: &SortedIndexTarget<'_>,
    plan: PhysicalPlan,
) -> Result<Vec<DdlResult>, DdlError> {
    let resp = dispatch_read(state, target, plan).await?;
    let rows_json = decode_rows(&resp.payload)?;

    let mut rows = Vec::with_capacity(rows_json.len());
    for row_json in &rows_json {
        let rank = row_json
            .get("rank")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            .to_string();
        let key = row_json
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut row = Map::new();
        row.insert("rank".to_string(), JsonValue::String(rank));
        row.insert("key".to_string(), JsonValue::String(key));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["rank".to_string(), "key".to_string()],
        rows,
    ))])
}
