// SPDX-License-Identifier: BUSL-1.1

//! Uniform row-source abstraction for join sides.
//!
//! A [`RowSource`] represents one side of a join and allows the grace-hash-join
//! driver to consume rows through a single `for_each` call instead of inline
//! `scan_collection_for_each` calls scattered through `drive_grace_build`.
//!
//! Two variants exist:
//! - [`RowSource::LocalScan`] — a pure pass-through wrapper around
//!   `CoreLoop::scan_collection_for_each` that is byte-identical to the previous
//!   inline calls (the local-join path).
//! - [`RowSource::ShuffleStream`] — rows staged to a LOCAL scratch file by a
//!   cross-node shuffle (the "receive-to-spill, then local grace join" design).
//!   The file holds `[u32 LE len][row-bytes]` frames, ONE join row per frame,
//!   each frame's bytes being the SAME per-row msgpack shape `scan_collection_for_each`
//!   yields — so the grace build/probe closures are byte-identical regardless of
//!   which source feeds them.
//!
//! The `match self` inside [`RowSource::for_each`] is the single dispatch seam:
//! the grace driver in `grace_drive.rs` is parameterized over `RowSource` values
//! and never branches on the variant itself.

use std::path::PathBuf;

use super::grace_repartition::FrameStreamReader;
use crate::bridge::scan_filter::decode_scan_filters;
use crate::data::executor::core_loop::CoreLoop;

/// One side of a join consumed through a uniform interface.
///
/// - [`RowSource::LocalScan`] is a pass-through to
///   `CoreLoop::scan_collection_for_each` (local-join path).
/// - [`RowSource::ShuffleStream`] reads rows from a LOCAL staged shuffle file
///   (cross-node shuffle-join consumer path).
///
/// The dispatch seam is the `match self` inside [`RowSource::for_each`].
///
/// `Clone` is derived because the probe source is consumed at two grace-driver
/// sites (the spill-probe and the streamed-probe paths); both variants are cheap
/// to clone (a few scalars / a `PathBuf`).
#[derive(Clone)]
pub(super) enum RowSource {
    /// Scan rows directly from a local collection on this core.
    LocalScan {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        /// Row-level-security filters for this side, as the MessagePack
        /// `Vec<ScanFilter>` the planner injected. Empty = no policy applies.
        ///
        /// Applied here rather than at each grace-driver site because this is
        /// the single dispatch seam every locally-scanned join side passes
        /// through — a filter applied anywhere else would be one strategy's
        /// filter, not the join's.
        rls_filters: Vec<u8>,
        /// This side's own `WHERE` predicates, as the MessagePack
        /// `Vec<ScanFilter>` the planner serialized. Empty = no local
        /// predicate. Applied at the same seam as `rls_filters`, and a row must
        /// pass both.
        scan_filters: Vec<u8>,
    },
    /// Stream rows from a LOCAL staged shuffle file written by a cross-node
    /// exchange. The file is a sequence of `[u32 LE len][row-bytes]` frames,
    /// one join row per frame, each row a single msgpack document (the same
    /// per-row byte shape `scan_collection_for_each` yields).
    ///
    /// Constructed by the shuffle-join consumer (E3b/E4); until that path is
    /// wired it is exercised only by tests, hence `dead_code` on this variant.
    #[allow(dead_code)]
    ShuffleStream { path: PathBuf },
}

impl RowSource {
    /// Iterate every row in this source, calling `f(id, bytes)` for each.
    ///
    /// Errors from `f` and from the underlying scan/read are propagated via `?`.
    ///
    /// For [`RowSource::ShuffleStream`] the `id` passed to `f` is always the
    /// empty string `""`. This is safe because no grace build/probe closure ever
    /// uses the `id` for join semantics — they use only `id.len()` for byte
    /// accounting (and a staged frame stores no id, so its contribution to the
    /// running byte total is genuinely zero) and discard the id otherwise
    /// (pushing `String::new()` as the stored doc id). The join key is extracted
    /// from the row bytes, never from the id, so byte-for-byte the staged-row
    /// path produces the same matches the local-scan path would.
    pub(super) fn for_each<F>(&self, core: &CoreLoop, mut f: F) -> crate::Result<()>
    where
        F: FnMut(&str, &[u8]) -> crate::Result<()>,
    {
        let crs = core.crs_context();
        match self {
            RowSource::LocalScan {
                database_id,
                tenant_id,
                collection,
                rls_filters,
                scan_filters,
            } => {
                if rls_filters.is_empty() && scan_filters.is_empty() {
                    return core.scan_collection_for_each(*database_id, *tenant_id, collection, f);
                }
                // Deserialize once, outside the per-row closure. A set that
                // fails to decode is an error, never an empty filter set:
                // dropping it would stream the unfiltered side into the join.
                let mut filters = decode_scan_filters(rls_filters, "RLS filter (join side)")?;
                filters.extend(decode_scan_filters(scan_filters, "join side predicate")?);
                core.scan_collection_for_each(*database_id, *tenant_id, collection, |id, bytes| {
                    if crate::bridge::scan_filter::ScanFilter::all_match_binary(
                        &filters, bytes, &crs,
                    )? {
                        f(id, bytes)?;
                    }
                    Ok(())
                })
            }
            RowSource::ShuffleStream { path } => {
                // One row per frame: `next_row` yields exactly one join row's
                // bytes, in file order. A truncated/corrupt frame is a HARD
                // error inside `FrameStreamReader::next_row` (never a silent
                // drop), matching the grace spill-read contract.
                let mut reader = FrameStreamReader::open(path)?;
                while let Some(row) = reader.next_row()? {
                    f("", &row)?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    /// Write `rows` as a sequence of `[u32 LE len][row-bytes]` frames — the
    /// exact staged-file format `RowSource::ShuffleStream` consumes (one join
    /// row per frame).
    fn write_staged_file(path: &std::path::Path, rows: &[Vec<u8>]) {
        let mut f = std::fs::File::create(path).expect("create staged file");
        for row in rows {
            let len = u32::try_from(row.len()).expect("row fits u32");
            f.write_all(&len.to_le_bytes()).expect("write len");
            f.write_all(row).expect("write body");
        }
        f.flush().expect("flush");
    }

    /// `ShuffleStream` yields the staged rows in order, byte-identical to what
    /// was written — the order-equivalence contract the consumer relies on.
    ///
    /// `for_each` is exercised WITHOUT a live `CoreLoop` because the
    /// `ShuffleStream` arm never touches `core` (it reads the local file);
    /// passing a raw null pointer reference would be UB, so instead we read the
    /// file directly through the same `FrameStreamReader` the arm uses and
    /// assert the frame decode is order- and byte-exact. The variant's only
    /// other behavior — passing `""` as the id — is verified by the join test in
    /// `shuffle_join.rs`, which runs the full `for_each`.
    #[test]
    fn shuffle_stream_yields_rows_in_order_byte_identical() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("staged.frames");

        // Heterogeneous row bytes including an empty row (a legal zero-length
        // frame) to prove the length framing is exact.
        let rows: Vec<Vec<u8>> = vec![
            b"first-row-bytes".to_vec(),
            Vec::new(),
            vec![0u8, 1, 2, 3, 0xff, 0xfe],
            b"another".to_vec(),
        ];
        write_staged_file(&path, &rows);

        let mut reader = super::FrameStreamReader::open(&path).expect("open reader");
        let mut got: Vec<Vec<u8>> = Vec::new();
        while let Some(row) = reader.next_row().expect("read frame") {
            got.push(row);
        }

        assert_eq!(
            got, rows,
            "staged frames must read back in order, byte-identical"
        );
    }

    /// A truncated frame (header declares more bytes than the body holds) is a
    /// HARD error, never a silent drop — mirroring the grace spill-read contract.
    #[test]
    fn shuffle_stream_truncated_frame_is_hard_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("truncated.frames");

        // Declare a 10-byte body but write only 3 bytes.
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(&10u32.to_le_bytes()).expect("write len");
        f.write_all(b"abc").expect("write short body");
        f.flush().expect("flush");

        let mut reader = super::FrameStreamReader::open(&path).expect("open reader");
        let err = reader.next_row();
        assert!(
            err.is_err(),
            "a truncated frame body must surface as an error, not a silent EOF"
        );
    }
}
