//! External sort infrastructure: sort helpers, run files, and k-way merge.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read as _, Write as _};

use tracing::debug;

use crate::data::executor::core_loop::CoreLoop;

use nodedb_query::msgpack_scan;

impl CoreLoop {
    /// External sort: split filtered rows into sorted runs, spill each run
    /// to a temp file, then k-way merge to produce the final sorted output.
    pub(super) fn external_sort(
        &self,
        rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[(String, bool)],
        output_limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // Spill directory for temporary sort run files. Temp files are
        // auto-deleted on Drop; the directory persists but is cleaned up
        // on the next external_sort call or server restart.
        let spill_dir = self
            .data_dir
            .join(format!("sort-spill/core-{}", self.core_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("failed to create sort spill dir: {e}"),
        })?;

        let total_rows = rows.len();

        let mut run_files = Vec::new();
        for chunk in rows.chunks(self.query_tuning.sort_run_size) {
            let mut run: Vec<(String, Vec<u8>)> = chunk.to_vec();
            sort_rows(&mut run, sort_keys);

            let file = tempfile::tempfile_in(&spill_dir).map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("failed to create sort temp file: {e}"),
            })?;
            let mut writer = BufWriter::new(file);

            let count = run.len() as u32;
            writer
                .write_all(&count.to_le_bytes())
                .map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill write: {e}"),
                })?;
            for (id, val) in &run {
                let id_bytes = id.as_bytes();
                writer
                    .write_all(&(id_bytes.len() as u32).to_le_bytes())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer
                    .write_all(id_bytes)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer
                    .write_all(&(val.len() as u32).to_le_bytes())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sort".into(),
                        detail: format!("sort spill write: {e}"),
                    })?;
                writer.write_all(val).map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill write: {e}"),
                })?;
            }
            writer.flush().map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("sort spill flush: {e}"),
            })?;

            let mut file = writer.into_inner().map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("sort spill into_inner: {e}"),
            })?;
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(0))
                .map_err(|e| crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("sort spill seek: {e}"),
                })?;

            run_files.push(file);
        }

        debug!(
            core = self.core_id,
            runs = run_files.len(),
            total_rows,
            "external sort: spilled runs"
        );

        let mut readers: Vec<RunReader> = run_files
            .into_iter()
            .enumerate()
            .filter_map(|(idx, file)| RunReader::new(file, idx).ok())
            .collect();

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(row) = reader.next_row() {
                heap.push(Reverse(MergeEntry {
                    row,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        let mut result = Vec::with_capacity(output_limit.min(total_rows));
        while let Some(Reverse(entry)) = heap.pop() {
            result.push(entry.row);
            if result.len() >= output_limit {
                break;
            }
            if let Some(next_row) = readers[entry.run_idx].next_row() {
                heap.push(Reverse(MergeEntry {
                    row: next_row,
                    run_idx: entry.run_idx,
                    sort_keys: sort_keys.to_vec(),
                }));
            }
        }

        Ok(result)
    }
}

/// Compare two raw msgpack documents by a list of sort keys.
///
/// Uses binary field extraction — no decode. Used by both in-memory
/// sort and external merge sort for consistent ordering.
pub(super) fn compare_docs_by_keys_binary(
    a_bytes: &[u8],
    b_bytes: &[u8],
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    for (field, asc) in sort_keys {
        let a_range = msgpack_scan::extract_field(a_bytes, 0, field);
        let b_range = msgpack_scan::extract_field(b_bytes, 0, field);

        let cmp = match (a_range, b_range) {
            (Some(ar), Some(br)) => msgpack_scan::compare_field_bytes(a_bytes, ar, b_bytes, br),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        };

        let ordered = if *asc { cmp } else { cmp.reverse() };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

/// Pre-extracted sort key offsets for a single row.
/// Each entry is `Option<(usize, usize)>` — byte range of the sort key value.
type SortKeyOffsets = Vec<Option<(usize, usize)>>;

pub(in crate::data::executor) fn sort_rows(rows: &mut [(String, Vec<u8>)], sort_keys: &[(String, bool)]) {
    if sort_keys.is_empty() {
        return;
    }

    // Pre-extract sort key offsets for all rows — one scan per row instead
    // of O(N log N) scans during comparisons.
    let mut key_offsets: Vec<SortKeyOffsets> = rows
        .iter()
        .map(|(_, bytes)| {
            sort_keys
                .iter()
                .map(|(field, _)| msgpack_scan::extract_field(bytes, 0, field))
                .collect()
        })
        .collect();

    // Sort indices using pre-extracted offsets.
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices.sort_by(|&ai, &bi| {
        compare_with_preextracted(
            &rows[ai].1,
            &key_offsets[ai],
            &rows[bi].1,
            &key_offsets[bi],
            sort_keys,
        )
    });

    // Apply permutation in-place.
    apply_permutation(rows, &mut key_offsets, indices);
}

/// Compare two docs using pre-extracted sort key offsets.
fn compare_with_preextracted(
    a_bytes: &[u8],
    a_offsets: &[Option<(usize, usize)>],
    b_bytes: &[u8],
    b_offsets: &[Option<(usize, usize)>],
    sort_keys: &[(String, bool)],
) -> std::cmp::Ordering {
    for (i, (_, asc)) in sort_keys.iter().enumerate() {
        let cmp = match (a_offsets[i], b_offsets[i]) {
            (Some(ar), Some(br)) => msgpack_scan::compare_field_bytes(a_bytes, ar, b_bytes, br),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        };
        let ordered = if *asc { cmp } else { cmp.reverse() };
        if ordered != std::cmp::Ordering::Equal {
            return ordered;
        }
    }
    std::cmp::Ordering::Equal
}

/// Apply a permutation to rows (and key_offsets) in-place using swaps.
fn apply_permutation(
    rows: &mut [(String, Vec<u8>)],
    _key_offsets: &mut [SortKeyOffsets],
    mut indices: Vec<usize>,
) {
    for i in 0..indices.len() {
        while indices[i] != i {
            let j = indices[i];
            rows.swap(i, j);
            indices.swap(i, j);
        }
    }
}

pub(super) struct RunReader {
    pub(super) reader: BufReader<std::fs::File>,
    pub(super) remaining: u32,
    pub(super) run_idx: usize,
}

impl RunReader {
    pub(super) fn new(file: std::fs::File, run_idx: usize) -> crate::Result<Self> {
        let mut reader = BufReader::new(file);
        let mut buf4 = [0u8; 4];
        reader
            .read_exact(&mut buf4)
            .map_err(|e| crate::Error::Storage {
                engine: "sort".into(),
                detail: format!("run reader init: {e}"),
            })?;
        let count = u32::from_le_bytes(buf4);
        Ok(Self {
            reader,
            remaining: count,
            run_idx,
        })
    }

    pub(super) fn next_row(&mut self) -> Option<(String, Vec<u8>)> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let mut buf4 = [0u8; 4];

        if self.reader.read_exact(&mut buf4).is_err() {
            return None;
        }
        let id_len = u32::from_le_bytes(buf4) as usize;
        let mut id_buf = vec![0u8; id_len];
        if self.reader.read_exact(&mut id_buf).is_err() {
            return None;
        }
        let id = String::from_utf8(id_buf).unwrap_or_default();

        if self.reader.read_exact(&mut buf4).is_err() {
            return None;
        }
        let val_len = u32::from_le_bytes(buf4) as usize;
        let mut val_buf = vec![0u8; val_len];
        if self.reader.read_exact(&mut val_buf).is_err() {
            return None;
        }

        Some((id, val_buf))
    }
}

pub(super) struct MergeEntry {
    pub(super) row: (String, Vec<u8>),
    pub(super) run_idx: usize,
    pub(super) sort_keys: Vec<(String, bool)>,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_docs_by_keys_binary(&self.row.1, &other.row.1, &self.sort_keys)
    }
}
