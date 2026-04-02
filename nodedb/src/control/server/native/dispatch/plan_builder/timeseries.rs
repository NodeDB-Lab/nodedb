//! Timeseries plan builders.

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::TimeseriesOp;

pub(crate) fn build_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let start = fields.time_range_start.unwrap_or(0);
    let end = fields.time_range_end.unwrap_or(i64::MAX);
    let limit = fields.limit.unwrap_or(10_000) as usize;
    let bucket_interval_ms = fields
        .bucket_interval
        .as_deref()
        .map(|s| {
            nodedb_types::kv_parsing::parse_interval_to_ms(s)
                .map(|ms| ms as i64)
                .unwrap_or(0)
        })
        .unwrap_or(0);

    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Scan {
        collection: collection.to_string(),
        time_range: (start, end),
        projection: Vec::new(),
        limit,
        filters: Vec::new(),
        bucket_interval_ms,
        group_by: Vec::new(),
        aggregates: Vec::new(),
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_ingest(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let payload = fields
        .payload
        .as_ref()
        .or(fields.data.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'payload' or 'data'".to_string(),
        })?
        .clone();
    let format = fields.format.as_deref().unwrap_or("ilp").to_string();

    Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: collection.to_string(),
        payload,
        format,
        wal_lsn: None,
    }))
}
