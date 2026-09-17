// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CONTINUOUS AGGREGATES [FOR <source>]` handler.
//!
//! Ported from the pgwire `ddl::continuous_agg::show` handler. The catalog read
//! (durable source of truth), the best-effort runtime-stats merge from the local
//! Data Plane manager, the optional `FOR <source>` filter, the decode-failure
//! skip, and the exact column set are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral [`DdlResult::Rows`] over [`ShapedRows`]. The mixed
//! text/`int8` column OIDs (`watermark_ts`, `rows_aggregated`,
//! `materialized_buckets` are `int8`) are reproduced by building `column_types`
//! manually so the RowDescription stays byte-identical; the `int8` cells are
//! emitted as their decimal text form, the same bytes the pgwire
//! `DataRowEncoder::encode_field(&i64)` produced.

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::StoredContinuousAggregate;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{AggregateInfo, ContinuousAggregateDef};
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::super::result::{DdlError, DdlResult};

/// `SHOW CONTINUOUS AGGREGATES [FOR <source>]`.
///
/// Reads the catalog (the source of truth: replicated, persisted,
/// survives restart) and merges in best-effort runtime stats from the
/// local Data Plane manager. A node that has just restarted but hasn't
/// finished replaying registers will still show the aggregate via the
/// catalog row; the runtime columns surface as zero / blank until the
/// manager catches up.
pub async fn show_continuous_aggregates(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let source_filter = if parts.len() >= 5 && parts[3].to_uppercase() == "FOR" {
        Some(parse_ident_token(parts[4])?)
    } else {
        None
    };

    let tenant_id = identity.tenant_id;

    // Catalog rows — the durable source of truth.
    let stored_aggs: Vec<StoredContinuousAggregate> = state
        .credentials
        .catalog()
        .list_continuous_aggregates(database_id.as_u64(), tenant_id.as_u64())
        .ok()
        .unwrap_or_default();

    // Best-effort runtime stats from the local manager.
    //
    // "Best effort" covers the dispatch not answering — a node still replaying
    // registers has no stats to give, and the catalog rows below carry the
    // listing regardless. It does NOT cover a payload that arrived and could
    // not be read: `MetaOp::ListContinuousAggregates` encodes with
    // `response_codec::encode_serde`, which is MessagePack, so a JSON parser
    // here would fail on every one of those payloads and default the failure
    // away — reporting every aggregate at watermark 0, zero rows
    // aggregated, zero materialized buckets, and the catalog's stale flag
    // instead of the live one, on every node, always.
    let runtime_infos: Vec<AggregateInfo> = match sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::CatalogMaintenance,
            tenant_id,
            database_id,
            "__system",
            PhysicalPlan::Meta(MetaOp::ListContinuousAggregates),
        ),
        Duration::from_secs(5),
    )
    .await
    {
        Ok(payload) => {
            crate::data::executor::response_codec::decode_payload(&payload).map_err(|e| {
                DdlError::new("XX000", format!("continuous aggregate runtime stats: {e}"))
            })?
        }
        Err(_) => Vec::new(),
    };

    let columns = vec![
        "name".to_string(),
        "source".to_string(),
        "bucket_interval".to_string(),
        "refresh_policy".to_string(),
        "watermark_ts".to_string(),
        "rows_aggregated".to_string(),
        "materialized_buckets".to_string(),
        "stale".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    let mut rows = Vec::new();
    for stored in &stored_aggs {
        if let Some(ref filter) = source_filter
            && stored.source != *filter
        {
            continue;
        }

        // Decode the catalog-stored runtime def for the static columns
        // (bucket interval, refresh policy). Skip the row on a decode
        // failure rather than poisoning the whole listing.
        let Ok(def) = zerompk::from_msgpack::<ContinuousAggregateDef>(&stored.def_bytes) else {
            tracing::warn!(
                cagg = %stored.name,
                tenant = stored.tenant_id,
                "continuous aggregate row has unreadable def_bytes; \
                 skipping in SHOW (the row is still durable in the catalog)"
            );
            continue;
        };

        let runtime = runtime_infos.iter().find(|i| i.name == stored.name);
        let watermark = runtime.map(|i| i.watermark_ts).unwrap_or(0);
        let rows_agg = runtime.map(|i| i.rows_aggregated as i64).unwrap_or(0);
        let buckets = runtime.map(|i| i.materialized_buckets as i64).unwrap_or(0);
        let stale = runtime.map(|i| i.stale).unwrap_or(def.stale).to_string();

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(stored.name.clone()));
        row.insert(
            "source".to_string(),
            JsonValue::String(stored.source.clone()),
        );
        row.insert(
            "bucket_interval".to_string(),
            JsonValue::String(def.bucket_interval.clone()),
        );
        row.insert(
            "refresh_policy".to_string(),
            JsonValue::String(format!("{:?}", def.refresh_policy)),
        );
        row.insert(
            "watermark_ts".to_string(),
            JsonValue::String(watermark.to_string()),
        );
        row.insert(
            "rows_aggregated".to_string(),
            JsonValue::String(rows_agg.to_string()),
        );
        row.insert(
            "materialized_buckets".to_string(),
            JsonValue::String(buckets.to_string()),
        );
        row.insert("stale".to_string(), JsonValue::String(stale));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
