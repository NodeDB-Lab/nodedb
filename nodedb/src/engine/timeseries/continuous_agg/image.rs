// SPDX-License-Identifier: BUSL-1.1

//! On-disk image of every continuous aggregate rooted at one timeseries
//! collection: `<collection dir>/cagg.state`.
//!
//! One file holds every tier, written by one `atomic_write_fsync`, so a
//! crash never publishes one tier ahead of another. Each entry's buckets
//! equal the fold of exactly the partitions in its `folded` set. A name in
//! `folded` always names a partition that was durable when it was folded,
//! because a partition's files are fsynced before its flush folds it.
//!
//! A pending refresh cut is in the image from the moment it applies. The
//! cut's apply writes the image before it answers, so the data group's
//! applied index never passes a cut the image does not hold.
//!
//! The file is a format-version byte followed by MessagePack:
//! `[[entries...], [orphan cuts...], raw_dropped_through, [[name,
//! rewritten_at]...]]`, each entry
//! `[def, watermark_ts, rows_aggregated, last_refresh_ms, time_kind,
//! folded, buckets, cut]`, and each `cut`
//! `[pending_ids, pending, completed, last_slot, parked_rows,
//! parked_max_ts, parked]`.

use std::collections::{HashSet, VecDeque};

use nodedb_types::timeseries::continuous_agg::ContinuousAggregateDef;
use zerompk::{FromMessagePack, ToMessagePack};

use super::cut::{CutState, OrphanCut};
use super::partial::PartialAggregate;
use super::refresh::Buckets;
use crate::engine::timeseries::columnar_memtable::TimeKind;

/// File name of the image inside the root collection's directory. It does
/// not start with `ts-`, so the partition scan and the orphan sweeper skip
/// it.
pub const CAGG_IMAGE_FILE: &str = "cagg.state";
/// Format version byte.
pub const CAGG_IMAGE_FORMAT_VERSION: u8 = 1;
const IMAGE_FIELDS: usize = 4;
const ENTRY_FIELDS: usize = 8;
const CUT_FIELDS: usize = 7;

/// One aggregate's persisted state.
#[derive(Debug)]
pub struct CaggImageEntry {
    pub def: ContinuousAggregateDef,
    pub watermark_ts: i64,
    pub rows_aggregated: u64,
    pub last_refresh_ms: i64,
    pub time_kind: Option<TimeKind>,
    pub folded: Vec<String>,
    pub buckets: Vec<PartialAggregate>,
    pub cut: CutState,
}

/// Every aggregate's persisted state for one root.
#[derive(Debug, Default)]
pub struct CaggImage {
    pub entries: Vec<CaggImageEntry>,
    pub orphans: Vec<OrphanCut>,
    /// The newest time-key value of a root partition retention removed,
    /// `None` when retention removed none. Raw rows at or below it can be
    /// gone, so only the tiers hold them.
    pub raw_dropped_through: Option<i64>,
    /// Each aggregate's newest rewrite ordinal: the system time of its
    /// last refresh cut or bucket retention pass.
    pub rewritten: Vec<(String, i64)>,
}

/// One aggregate's state borrowed from the manager for encoding.
pub struct CaggImageEntryView<'a> {
    pub def: &'a ContinuousAggregateDef,
    pub watermark_ts: i64,
    pub rows_aggregated: u64,
    pub last_refresh_ms: i64,
    pub time_kind: Option<TimeKind>,
    /// Sorted, so equal state encodes to equal bytes.
    pub folded: Vec<&'a str>,
    /// Sorted by `(bucket_ts, group_key)`.
    pub buckets: Vec<&'a PartialAggregate>,
    /// `None` encodes an empty cut state.
    pub cut: Option<&'a CutState>,
}

/// Every aggregate's borrowed state for one root.
pub struct CaggImageView<'a> {
    pub entries: Vec<CaggImageEntryView<'a>>,
    pub orphans: &'a [OrphanCut],
    /// See [`CaggImage::raw_dropped_through`].
    pub raw_dropped_through: Option<i64>,
    /// See [`CaggImage::rewritten`]. Sorted by name.
    pub rewritten: Vec<(&'a str, i64)>,
}

fn sorted_buckets(buckets: &Buckets) -> Vec<&PartialAggregate> {
    let mut sorted: Vec<&PartialAggregate> = buckets.values().collect();
    sorted.sort_by(|a, b| (a.bucket_ts, &a.group_key).cmp(&(b.bucket_ts, &b.group_key)));
    sorted
}

fn write_cut<W: zerompk::Write>(cut: &CutState, writer: &mut W) -> zerompk::Result<()> {
    writer.write_array_len(CUT_FIELDS)?;
    cut.pending_ids.write(writer)?;
    let mut pending: Vec<&str> = cut.pending.iter().map(String::as_str).collect();
    pending.sort_unstable();
    writer.write_array_len(pending.len())?;
    for name in pending {
        writer.write_string(name)?;
    }
    writer.write_array_len(cut.completed.len())?;
    for id in &cut.completed {
        writer.write_u64(*id)?;
    }
    writer.write_u64(cut.last_slot)?;
    writer.write_u64(cut.parked_rows)?;
    writer.write_i64(cut.parked_max_ts)?;
    let parked = sorted_buckets(&cut.parked);
    writer.write_array_len(parked.len())?;
    for bucket in parked {
        bucket.write(writer)?;
    }
    Ok(())
}

fn read_cut<'a, R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<CutState> {
    reader.check_array_len(CUT_FIELDS)?;
    let pending_ids = Vec::<u64>::read(reader)?;
    let pending: HashSet<String> = Vec::<String>::read(reader)?.into_iter().collect();
    let completed: VecDeque<u64> = Vec::<u64>::read(reader)?.into_iter().collect();
    let last_slot = reader.read_u64()?;
    let parked_rows = reader.read_u64()?;
    let parked_max_ts = reader.read_i64()?;
    let parked = Vec::<PartialAggregate>::read(reader)?
        .into_iter()
        .map(|b| ((b.bucket_ts, b.group_key.clone()), b))
        .collect();
    Ok(CutState {
        pending_ids,
        pending,
        completed,
        last_slot,
        parked,
        parked_rows,
        parked_max_ts,
    })
}

impl zerompk::ToMessagePack for CaggImageView<'_> {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(IMAGE_FIELDS)?;
        writer.write_array_len(self.entries.len())?;
        for entry in &self.entries {
            writer.write_array_len(ENTRY_FIELDS)?;
            entry.def.write(writer)?;
            writer.write_i64(entry.watermark_ts)?;
            writer.write_u64(entry.rows_aggregated)?;
            writer.write_i64(entry.last_refresh_ms)?;
            entry.time_kind.write(writer)?;
            writer.write_array_len(entry.folded.len())?;
            for name in &entry.folded {
                writer.write_string(name)?;
            }
            writer.write_array_len(entry.buckets.len())?;
            for bucket in &entry.buckets {
                bucket.write(writer)?;
            }
            match entry.cut {
                Some(cut) => write_cut(cut, writer)?,
                None => write_cut(&CutState::default(), writer)?,
            }
        }
        writer.write_array_len(self.orphans.len())?;
        for orphan in self.orphans {
            orphan.write(writer)?;
        }
        self.raw_dropped_through.write(writer)?;
        writer.write_array_len(self.rewritten.len())?;
        for (name, ordinal) in &self.rewritten {
            writer.write_array_len(2)?;
            writer.write_string(name)?;
            writer.write_i64(*ordinal)?;
        }
        Ok(())
    }
}

impl<'a> zerompk::FromMessagePack<'a> for CaggImage {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(IMAGE_FIELDS)?;
        let count = reader.read_array_len()?;
        let mut entries = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            reader.check_array_len(ENTRY_FIELDS)?;
            let def = ContinuousAggregateDef::read(reader)?;
            let watermark_ts = reader.read_i64()?;
            let rows_aggregated = reader.read_u64()?;
            let last_refresh_ms = reader.read_i64()?;
            let time_kind = Option::<TimeKind>::read(reader)?;
            let folded = Vec::<String>::read(reader)?;
            let buckets = Vec::<PartialAggregate>::read(reader)?;
            let cut = read_cut(reader)?;
            entries.push(CaggImageEntry {
                def,
                watermark_ts,
                rows_aggregated,
                last_refresh_ms,
                time_kind,
                folded,
                buckets,
                cut,
            });
        }
        let orphans = Vec::<OrphanCut>::read(reader)?;
        let raw_dropped_through = Option::<i64>::read(reader)?;
        let count = reader.read_array_len()?;
        let mut rewritten = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            reader.check_array_len(2)?;
            let name = String::read(reader)?;
            let ordinal = reader.read_i64()?;
            rewritten.push((name, ordinal));
        }
        Ok(Self {
            entries,
            orphans,
            raw_dropped_through,
            rewritten,
        })
    }
}

/// Error decoding an image file.
#[derive(Debug, thiserror::Error)]
pub enum CaggImageError {
    #[error("continuous aggregate image is empty")]
    Empty,
    #[error("continuous aggregate image format version {found}, expected {expected}")]
    Version { found: u8, expected: u8 },
    #[error("continuous aggregate image body: {0}")]
    Body(String),
}

/// Version byte followed by the MessagePack body.
pub fn encode_image(view: &CaggImageView<'_>) -> Result<Vec<u8>, CaggImageError> {
    let body = zerompk::to_msgpack_vec(view).map_err(|e| CaggImageError::Body(e.to_string()))?;
    let mut bytes = Vec::with_capacity(body.len() + 1);
    bytes.push(CAGG_IMAGE_FORMAT_VERSION);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Decode the bytes [`encode_image`] wrote.
pub fn decode_image(bytes: &[u8]) -> Result<CaggImage, CaggImageError> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(CaggImageError::Empty);
    };
    if version != CAGG_IMAGE_FORMAT_VERSION {
        return Err(CaggImageError::Version {
            found: version,
            expected: CAGG_IMAGE_FORMAT_VERSION,
        });
    }
    zerompk::from_msgpack(body).map_err(|e| CaggImageError::Body(e.to_string()))
}

impl From<CaggImageError> for crate::Error {
    fn from(e: CaggImageError) -> Self {
        crate::Error::Codec {
            detail: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::definition::{AggFunction, AggregateExpr};
    use crate::engine::timeseries::continuous_agg::partial::{ColumnLayout, GroupCell};
    use crate::engine::timeseries::continuous_agg::test_support::exact_def;

    #[test]
    fn image_round_trips_every_partial_part() {
        let mut def = exact_def("v_1m", "metrics", "1m");
        def.aggregates.extend([
            AggregateExpr {
                function: AggFunction::CountDistinct,
                source_column: "v".into(),
                output_column: "d".into(),
            },
            AggregateExpr {
                function: AggFunction::Percentile(0.5),
                source_column: "v".into(),
                output_column: "p".into(),
            },
            AggregateExpr {
                function: AggFunction::TopK(2),
                source_column: "v".into(),
                output_column: "k".into(),
            },
        ]);
        let layout = ColumnLayout::of(&def);
        let mut bucket = PartialAggregate::new(60_000, vec![GroupCell::Text("a".into())], &layout);
        for (ts, v) in [(1, 5), (2, i64::MAX), (3, 7)] {
            bucket.count += 1;
            bucket.columns[0].add_int(ts, v);
        }
        let mut cut = CutState {
            pending_ids: vec![4, 5],
            pending: HashSet::from(["p3".to_string()]),
            completed: VecDeque::from([1, 2]),
            last_slot: 9,
            parked_rows: 3,
            parked_max_ts: 7,
            ..CutState::default()
        };
        let parked = PartialAggregate::new(60_000, vec![GroupCell::Text("a".into())], &layout);
        cut.parked
            .insert((parked.bucket_ts, parked.group_key.clone()), parked);
        let orphans = vec![OrphanCut {
            cut: nodedb_physical::physical_plan::RefreshCut {
                aggregate: "later".into(),
                incarnation: nodedb_types::Hlc::new(5, 0),
                refresh_id: 11,
                periodic: false,
            },
            partitions: vec!["p1".into()],
        }];
        let view = CaggImageView {
            entries: vec![CaggImageEntryView {
                def: &def,
                watermark_ts: 3,
                rows_aggregated: 3,
                last_refresh_ms: 9,
                time_kind: Some(TimeKind::Millis),
                folded: vec!["p1", "p2"],
                buckets: vec![&bucket],
                cut: Some(&cut),
            }],
            orphans: &orphans,
            raw_dropped_through: Some(42),
            rewritten: vec![("by_host", 7)],
        };
        let bytes = encode_image(&view).expect("encode");
        let image = decode_image(&bytes).expect("decode");
        assert_eq!(image.entries.len(), 1);
        assert_eq!(image.orphans, orphans);
        assert_eq!(image.raw_dropped_through, Some(42));
        assert_eq!(image.rewritten, vec![("by_host".to_string(), 7)]);
        let entry = &image.entries[0];
        assert_eq!(entry.def, def);
        assert_eq!(entry.folded, vec!["p1".to_string(), "p2".to_string()]);
        assert_eq!(entry.time_kind, Some(TimeKind::Millis));
        assert_eq!(entry.cut.pending_ids, cut.pending_ids);
        assert_eq!(entry.cut.pending, cut.pending);
        assert_eq!(entry.cut.completed, cut.completed);
        assert_eq!(
            (
                entry.cut.last_slot,
                entry.cut.parked_rows,
                entry.cut.parked_max_ts
            ),
            (9, 3, 7)
        );
        assert_eq!(entry.cut.parked.len(), 1);
        let back = &entry.buckets[0];
        assert_eq!(back.group_key, bucket.group_key);
        for expr in &def.aggregates {
            assert_eq!(
                back.finalize(expr, &layout).expect("finalize back"),
                bucket.finalize(expr, &layout).expect("finalize original"),
                "{expr:?}"
            );
        }
    }

    #[test]
    fn a_foreign_version_is_refused() {
        assert!(matches!(
            decode_image(&[CAGG_IMAGE_FORMAT_VERSION + 1]),
            Err(CaggImageError::Version { .. })
        ));
        assert!(matches!(decode_image(&[]), Err(CaggImageError::Empty)));
    }
}
