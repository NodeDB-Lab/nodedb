// SPDX-License-Identifier: BUSL-1.1

//! Read one block index from every source: fetch the ranges on the Control
//! Plane, decode on Tokio's blocking pool.
//!
//! The block ranges of every source merge where they lie close together,
//! so a block costs one request per source when its data and mask chunks
//! sit near each other. The producer reads block indexes in order into a
//! queue of [`BLOCK_QUEUE`], so a load holds at most that many decoded
//! block sets, one in decode and one in the load.

use std::sync::Arc;

use nodedb_raster::{ByteRange, COALESCE_GAP, RasterBlock, RasterReader, TiffReader, coalesce};
use tokio::sync::mpsc;

use super::super::fetch::{FetchError, RangeFetch};
use super::error::DriveError;

/// Decoded block sets queued between the producer and the load.
pub const BLOCK_QUEUE: usize = 2;

/// One block index, read from every source in source order.
pub type BlockSet = Vec<RasterBlock>;

/// Fetch `ranges`, merging close ones, and answer each range in order. An
/// empty range answers with no bytes and no fetch.
pub async fn fetch_ranges<F: RangeFetch>(
    fetch: &F,
    ranges: &[ByteRange],
) -> Result<Vec<Option<Vec<u8>>>, FetchError> {
    let merged = coalesce(ranges.to_vec(), COALESCE_GAP);
    let answers = if merged.is_empty() {
        Vec::new()
    } else {
        fetch.fetch(&merged).await?
    };
    Ok(ranges
        .iter()
        .map(|range| {
            if range.is_empty() {
                return Some(Vec::new());
            }
            let (outer, answer) = merged.iter().zip(&answers).find(|(outer, _)| {
                outer.object == range.object && outer.start <= range.start && range.end <= outer.end
            })?;
            let bytes = answer.as_ref()?;
            let from = usize::try_from(range.start - outer.start).unwrap_or(usize::MAX);
            let to = usize::try_from(range.end - outer.start).unwrap_or(usize::MAX);
            let (from, to) = (from.min(bytes.len()), to.min(bytes.len()));
            Some(bytes.get(from..to.max(from)).unwrap_or_default().to_vec())
        })
        .collect())
}

/// Read block `index` from every reader: fetch, then decode on the blocking
/// pool.
pub async fn read_index<F: RangeFetch>(
    readers: &Arc<Vec<TiffReader>>,
    fetch: &F,
    index: u32,
) -> Result<BlockSet, DriveError> {
    let mut counts = Vec::with_capacity(readers.len());
    let mut ranges = Vec::new();
    for (source, reader) in readers.iter().enumerate() {
        let block = reader
            .block_ranges(index)
            .map_err(|error| DriveError::Raster {
                index: source,
                error,
            })?;
        counts.push(block.len());
        ranges.extend(block);
    }
    let bytes = fetch_ranges(fetch, &ranges).await?;
    let readers = Arc::clone(readers);
    tokio::task::spawn_blocking(move || decode_set(&readers, index, &counts, &bytes))
        .await
        .map_err(|e| DriveError::Task(e.to_string()))?
}

/// Decode block `index` of each reader from its share of `bytes`.
fn decode_set(
    readers: &[TiffReader],
    index: u32,
    counts: &[usize],
    bytes: &[Option<Vec<u8>>],
) -> Result<BlockSet, DriveError> {
    let mut set = Vec::with_capacity(readers.len());
    let mut rest = bytes;
    for (source, (reader, count)) in readers.iter().zip(counts).enumerate() {
        let (mine, after) = rest.split_at((*count).min(rest.len()));
        rest = after;
        let borrowed: Vec<Option<&[u8]>> = mine.iter().map(Option::as_deref).collect();
        let block = reader
            .decode_block(index, &borrowed)
            .map_err(|error| DriveError::Raster {
                index: source,
                error,
            })?;
        set.push(block);
    }
    Ok(set)
}

/// Read `indexes` in order into `tx`. Ends at the first error, after
/// sending it, or when the load drops its side.
pub async fn produce_blocks<F: RangeFetch + 'static>(
    readers: Arc<Vec<TiffReader>>,
    fetch: Arc<F>,
    indexes: Vec<u32>,
    tx: mpsc::Sender<Result<BlockSet, DriveError>>,
) {
    for index in indexes {
        let read = read_index(&readers, &*fetch, index).await;
        let failed = read.is_err();
        if tx.send(read).await.is_err() || failed {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_raster::{RasterLimits, TiffOpener};
    use nodedb_raster_load::PixelWindow;

    use super::super::super::fetch::memory::MemoryFetch;
    use super::super::open::open_sources;
    use super::*;

    /// A 64x48 cloud-optimised GeoTIFF in 16x16 tiles with two overviews
    /// and an internal mask.
    const COG: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../nodedb-raster/tests/fixtures/cog-mask.tif"
    ));

    const LIMITS: RasterLimits = RasterLimits::new(1 << 20);

    fn opener(object: u32) -> TiffOpener {
        TiffOpener::new().with_object(nodedb_raster::ObjectId(object))
    }

    #[tokio::test]
    async fn a_cog_opens_in_one_round() {
        let fetch = MemoryFetch::new(vec![COG.to_vec(), COG.to_vec()]);
        let readers = open_sources(&fetch, &[opener(0), opener(1)], LIMITS)
            .await
            .expect("opens");
        assert_eq!(readers.len(), 2);
        let calls = fetch.calls();
        assert_eq!(calls.len(), 1, "one round");
        assert_eq!(calls[0].len(), 2, "one request per source");
        assert_eq!(readers[0].header().width, 64);
    }

    #[tokio::test]
    async fn a_window_fetches_only_its_tiles() {
        let fetch = MemoryFetch::new(vec![COG.to_vec()]);
        let readers = open_sources(&fetch, &[opener(0)], LIMITS)
            .await
            .expect("opens");
        let opened = fetch.calls().len();
        let window = PixelWindow {
            col0: 16,
            row0: 16,
            col1: 32,
            row1: 32,
        };
        let reader = &readers[0];
        let wanted: Vec<u32> = (0..reader.block_count())
            .filter(|index| window.overlaps(&reader.block_window(*index).expect("window")))
            .collect();
        assert_eq!(wanted, vec![5], "one 16x16 tile of twelve");
        let expected = coalesce(reader.block_ranges(5).expect("ranges"), COALESCE_GAP);
        let readers = Arc::new(readers);
        let set = read_index(&readers, &fetch, 5).await.expect("reads");
        let calls = fetch.calls();
        assert_eq!(calls.len(), opened + 1);
        assert_eq!(calls[opened], expected);
        assert_eq!((set[0].window.col_off, set[0].window.row_off), (16, 16));
    }

    #[tokio::test]
    async fn every_source_reads_the_same_block_in_lockstep() {
        let fetch = MemoryFetch::new(vec![COG.to_vec(), COG.to_vec()]);
        let readers = Arc::new(
            open_sources(&fetch, &[opener(0), opener(1)], LIMITS)
                .await
                .expect("opens"),
        );
        let set = read_index(&readers, &fetch, 3).await.expect("reads");
        assert_eq!(set.len(), 2);
        assert_eq!(set[0].window, set[1].window);
        assert_eq!(set[0].bands, set[1].bands);
        assert_eq!(set[0].masks, set[1].masks);
        let objects: Vec<u32> = fetch
            .calls()
            .last()
            .expect("a block fetch")
            .iter()
            .map(|r| r.object.0)
            .collect();
        assert!(objects.contains(&0) && objects.contains(&1), "{objects:?}");
    }

    #[test]
    fn close_ranges_merge_and_slice_back() {
        let r = |object, start, end| ByteRange::new(nodedb_raster::ObjectId(object), start, end);
        let fetch = MemoryFetch::new(vec![(0u8..=255).collect()]);
        let answers = futures::executor::block_on(fetch_ranges(
            &fetch,
            &[
                r(0, 10, 12),
                r(0, 4, 4),
                r(0, 2, 5),
                r(0, 250, 300),
                r(1, 0, 3),
            ],
        ))
        .expect("reads");
        assert_eq!(
            answers,
            vec![
                Some(vec![10, 11]),
                Some(vec![]),
                Some(vec![2, 3, 4]),
                Some(vec![250, 251, 252, 253, 254, 255]),
                None
            ]
        );
        assert_eq!(fetch.calls().len(), 1);
        assert_eq!(fetch.calls()[0].len(), 2, "object 0 merges into one range");
    }
}
