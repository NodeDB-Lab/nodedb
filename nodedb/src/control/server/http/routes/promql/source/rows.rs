// SPDX-License-Identifier: BUSL-1.1

//! Stored metric rows → PromQL series.
//!
//! A row is one sample. Its label set is `__name__` (the collection name)
//! plus every non-empty string cell. The time key, the `value` column and
//! the bitemporal system columns are not labels. Rows with the same label
//! set form one series. Its samples are sorted ascending and de-duplicated
//! by timestamp, because the scan returns live memtable rows before flushed
//! partition rows and a retried remote write stores a sample twice.

use std::collections::BTreeMap;
use std::io::Cursor;

use nodedb_types::columnar::schema::{TS_SERIES, TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};

use crate::control::promql::LabelMatcher;
use crate::control::promql::label::matches_all;
use crate::control::promql::types::{Labels, Sample, Series};

use super::discovery::{MetricCollection, VALUE_COLUMN};

/// Rows are flat maps of scalar cells; nothing deeper is ever emitted.
const ROW_MAX_DEPTH: usize = 8;

/// Decode a raw-scan reply into its rows.
///
/// The reply is one MessagePack array of row maps. Rows are decoded one at
/// a time, so the reply's size is bounded by the scan's row ceiling and not
/// by a whole-document value count.
pub(crate) fn decode_rows(metric: &str, payload: &[u8]) -> crate::Result<Vec<rmpv::Value>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let (count, mut offset) = nodedb_query::msgpack_scan::array_header(payload, 0)
        .ok_or_else(|| codec(metric, "the scan reply is not an array"))?;
    // The header's count is untrusted, so the vector grows as rows decode.
    let mut rows = Vec::new();
    for _ in 0..count {
        let rest = payload
            .get(offset..)
            .ok_or_else(|| codec(metric, "the scan reply ends inside its row array"))?;
        let mut cursor = Cursor::new(rest);
        let row = rmpv::decode::read_value_with_max_depth(&mut cursor, ROW_MAX_DEPTH)
            .map_err(|e| codec(metric, &format!("a scan row does not decode: {e}")))?;
        let used = usize::try_from(cursor.position())
            .map_err(|_| codec(metric, "a scan row is larger than memory"))?;
        offset += used;
        rows.push(row);
    }
    if offset != payload.len() {
        return Err(codec(
            metric,
            "the scan reply has bytes after its row array",
        ));
    }
    Ok(rows)
}

/// Group `rows` of `metric` into series, keeping the rows `matchers` admit.
pub(crate) fn group_rows(
    metric: &MetricCollection,
    rows: Vec<rmpv::Value>,
    matchers: &[LabelMatcher],
) -> crate::Result<Vec<Series>> {
    let mut by_labels: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
    for row in rows {
        let rmpv::Value::Map(cells) = row else {
            return Err(codec(&metric.name, "a scan row is not a map"));
        };
        let mut labels = Labels::new();
        labels.insert("__name__".to_owned(), metric.name.clone());
        let mut timestamp_ms = None;
        let mut value = None;
        for (key, cell) in cells {
            let Some(column) = key.as_str() else {
                return Err(codec(
                    &metric.name,
                    "a scan row has a non-string column name",
                ));
            };
            if column == metric.time_key {
                timestamp_ms =
                    Some(cell_millis(&cell).ok_or_else(|| {
                        codec(&metric.name, "the time key cell is not an instant")
                    })?);
            } else if column == VALUE_COLUMN {
                value = cell_f64(&cell).map_err(|detail| codec(&metric.name, detail))?;
            } else if column == TS_SYSTEM
                || column == TS_SERIES
                || column == TS_VALID_FROM
                || column == TS_VALID_UNTIL
            {
                continue;
            } else if let Some(text) = cell.as_str()
                && !text.is_empty()
            {
                labels.insert(column.to_owned(), text.to_owned());
            }
        }
        let timestamp_ms =
            timestamp_ms.ok_or_else(|| codec(&metric.name, "a scan row has no time key cell"))?;
        // A NULL value is a row that stored no sample.
        let Some(value) = value else {
            continue;
        };
        if matches_all(matchers, &labels) {
            by_labels.entry(labels).or_default().push(Sample {
                timestamp_ms,
                value,
            });
        }
    }
    Ok(by_labels
        .into_iter()
        .map(|(labels, samples)| Series {
            labels,
            samples: sorted_unique(samples),
        })
        .collect())
}

/// Sort samples ascending by timestamp and keep the first of each timestamp.
pub(crate) fn sorted_unique(mut samples: Vec<Sample>) -> Vec<Sample> {
    samples.sort_by_key(|s| s.timestamp_ms);
    samples.dedup_by_key(|s| s.timestamp_ms);
    samples
}

/// Milliseconds since the epoch of a time cell: an integer for a
/// millisecond time key, a typed instant ext for a declared `TIMESTAMP`.
fn cell_millis(cell: &rmpv::Value) -> Option<i64> {
    match cell {
        rmpv::Value::Integer(n) => n.as_i64(),
        rmpv::Value::Ext(ext_type, payload) => {
            nodedb_types::json_msgpack::instant_from_ext(*ext_type, payload)
                .map(|(_, micros)| micros.div_euclid(1000))
        }
        _ => None,
    }
}

fn cell_f64(cell: &rmpv::Value) -> Result<Option<f64>, &'static str> {
    match cell {
        rmpv::Value::F64(v) => Ok(Some(*v)),
        rmpv::Value::F32(v) => Ok(Some(f64::from(*v))),
        rmpv::Value::Integer(n) => n
            .as_f64()
            .map(Some)
            .ok_or("the value cell is not representable as f64"),
        rmpv::Value::Nil => Ok(None),
        _ => Err("the value cell is not numeric"),
    }
}

fn codec(metric: &str, detail: &str) -> crate::Error {
    crate::Error::Codec {
        detail: format!("promql scan of metric '{metric}': {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::promql::LabelMatchOp;

    fn metric() -> MetricCollection {
        MetricCollection {
            name: "up".into(),
            time_key: "timestamp".into(),
            tiering: None,
        }
    }

    fn row(cells: &[(&str, rmpv::Value)]) -> rmpv::Value {
        rmpv::Value::Map(
            cells
                .iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        )
    }

    fn sample_row(ts: i64, job: &str, value: f64) -> rmpv::Value {
        row(&[
            ("timestamp", rmpv::Value::Integer(ts.into())),
            ("job", rmpv::Value::String(job.into())),
            ("value", rmpv::Value::F64(value)),
        ])
    }

    #[test]
    fn rows_group_by_label_set_sorted_and_unique() {
        let rows = vec![
            sample_row(3000, "api", 3.0),
            sample_row(1000, "api", 1.0),
            sample_row(3000, "api", 3.0),
            sample_row(2000, "web", 7.0),
        ];
        let series = group_rows(&metric(), rows, &[]).unwrap();
        assert_eq!(series.len(), 2);
        let api = &series[0];
        assert_eq!(api.labels.get("job").map(String::as_str), Some("api"));
        assert_eq!(api.labels.get("__name__").map(String::as_str), Some("up"));
        let ts: Vec<i64> = api.samples.iter().map(|s| s.timestamp_ms).collect();
        assert_eq!(ts, vec![1000, 3000]);
    }

    #[test]
    fn null_value_and_empty_label_are_skipped() {
        let rows = vec![
            row(&[
                ("timestamp", rmpv::Value::Integer(1000.into())),
                ("job", rmpv::Value::String("".into())),
                ("value", rmpv::Value::F64(1.0)),
            ]),
            row(&[
                ("timestamp", rmpv::Value::Integer(2000.into())),
                ("value", rmpv::Value::Nil),
            ]),
        ];
        let series = group_rows(&metric(), rows, &[]).unwrap();
        assert_eq!(series.len(), 1);
        assert!(!series[0].labels.contains_key("job"));
        assert_eq!(series[0].samples.len(), 1);
    }

    #[test]
    fn matchers_apply_after_the_scan() {
        let rows = vec![sample_row(1000, "api", 1.0), sample_row(1000, "web", 2.0)];
        let m = LabelMatcher::new("job".into(), LabelMatchOp::NotEqual, "api".into()).unwrap();
        let series = group_rows(&metric(), rows, &[m]).unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].labels.get("job").map(String::as_str), Some("web"));
    }

    #[test]
    fn instant_ext_time_key_decodes_to_millis() {
        let millis: i64 = 1_700_000_000_123;
        let ext = rmpv::Value::Ext(
            nodedb_types::InstantKind::Naive.ext_type(),
            (millis * 1000).to_be_bytes().to_vec(),
        );
        let rows = vec![row(&[("timestamp", ext), ("value", rmpv::Value::F64(1.0))])];
        let series = group_rows(&metric(), rows, &[]).unwrap();
        assert_eq!(series[0].samples[0].timestamp_ms, millis);
    }

    #[test]
    fn non_numeric_value_is_an_error() {
        let rows = vec![row(&[
            ("timestamp", rmpv::Value::Integer(1000.into())),
            ("value", rmpv::Value::String("x".into())),
        ])];
        assert!(group_rows(&metric(), rows, &[]).is_err());
    }

    #[test]
    fn decode_rows_reads_an_array_of_maps() {
        let array = rmpv::Value::Array(vec![sample_row(1000, "api", 1.0)]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &array).unwrap();
        assert_eq!(decode_rows("up", &buf).unwrap().len(), 1);
        assert!(decode_rows("up", &[]).unwrap().is_empty());
        buf.push(0xc0);
        assert!(decode_rows("up", &buf).is_err());
    }
}
