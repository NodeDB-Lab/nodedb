// SPDX-License-Identifier: BUSL-1.1

//! External sort: spill sorted runs to per-core files, then k-way merge.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{BufReader, Read as _};
use std::path::{Path, PathBuf};

use nodedb_physical::physical_plan::SortKeySpec;
use tracing::debug;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::io::uring_seq_reader::UringSeqReader;
use crate::data::io::uring_writer::UringWriter;

use super::compare::{
    SortValues, all_column_keys, compare_docs_by_keys_binary, compare_sort_values,
    decode_sort_values, encode_sort_values, eval_sort_values,
};
use super::in_memory::sort_rows;

/// A row on its way through the sort: the document plus, when the ORDER BY has
/// a computed key, that row's evaluated key values.
///
/// The evaluated keys travel *with* the row into the spill file and back out
/// again, so the k-way merge orders runs by exactly the values the in-memory
/// run sort used. Re-deriving them at merge time would mean re-evaluating the
/// expression against a row that no longer carries the columns it referenced.
struct SortRecord {
    id: String,
    doc: Vec<u8>,
    keys: SortValues,
}

impl CoreLoop {
    /// External sort: split filtered rows into sorted runs, spill each run
    /// to a named per-run file written via io_uring, then k-way merge to
    /// produce the final sorted output.
    ///
    /// Spill files are named (`run-N.spill`) and written through [`UringWriter`]
    /// so the per-core io_uring reactor is never stalled by blocking `std::fs`
    /// content writes. They are unlinked by [`SortSpillCleanup`] (a Drop guard),
    /// not by tempfile auto-delete. The merge reads each run back incrementally
    /// via [`UringSeqReader`] — one row at a time — so peak read memory is one
    /// refill buffer per run, not the whole run.
    ///
    /// `decimal_keys` flags the keys `decimal_sort_keys` found `DECIMAL`.
    pub(in crate::data::executor) fn external_sort(
        &self,
        rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[SortKeySpec],
        decimal_keys: &[bool],
        output_limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        // Spill directory for the named sort run files. `create_dir_all` is a
        // bounded metadata op (not bulk content I/O), so it stays `std::fs`.
        let spill_dir = self
            .data_dir
            .join(format!("sort-spill/core-{}", self.core_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("failed to create sort spill dir: {e}"),
        })?;

        let total_rows = rows.len();
        let computed_keys = !all_column_keys(sort_keys);

        // Declared FIRST so it Drops LAST — after the readers below close their
        // fds — guaranteeing the spill files are unlinked only once no reader
        // still holds them open.
        let mut cleanup = SortSpillCleanup {
            dir: spill_dir.clone(),
            paths: Vec::new(),
        };

        for (run_idx, chunk) in rows.chunks(self.query_tuning.sort_run_size).enumerate() {
            let mut run: Vec<(String, Vec<u8>)> = chunk.to_vec();
            sort_rows(&mut run, sort_keys, decimal_keys)?;

            // Build the framed run into one buffer and write it in a single
            // pass. Writing each tiny frame field separately would be hundreds
            // of thousands of micro io_uring writes.
            let mut framed = Vec::new();
            framed.extend_from_slice(&(run.len() as u32).to_le_bytes());
            for (id, val) in &run {
                let key_bytes = if computed_keys {
                    encode_sort_values(&eval_sort_values(val, sort_keys)?)?
                } else {
                    Vec::new()
                };
                let id_bytes = id.as_bytes();
                framed.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
                framed.extend_from_slice(id_bytes);
                framed.extend_from_slice(&(val.len() as u32).to_le_bytes());
                framed.extend_from_slice(val);
                framed.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                framed.extend_from_slice(&key_bytes);
            }

            let run_path = spill_dir.join(format!("run-{run_idx}.spill"));
            write_sort_run(&run_path, &framed)?;
            cleanup.paths.push(run_path);
        }

        debug!(
            core = self.core_id,
            runs = cleanup.paths.len(),
            total_rows,
            "external sort: spilled runs"
        );

        // Build readers propagating errors — a run whose reader fails to init is
        // a hard error, never a silently dropped run.
        let mut readers: Vec<RunReader> = Vec::with_capacity(cleanup.paths.len());
        for (idx, path) in cleanup.paths.iter().enumerate() {
            readers.push(RunReader::open(path, idx)?);
        }

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(record) = reader.next_row()? {
                heap.push(Reverse(MergeEntry {
                    record,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.to_vec(),
                    decimal_keys: decimal_keys.to_vec(),
                }));
            }
        }

        let mut result = Vec::with_capacity(output_limit.min(total_rows));
        while let Some(Reverse(entry)) = heap.pop() {
            let run_idx = entry.run_idx;
            result.push((entry.record.id, entry.record.doc));
            if result.len() >= output_limit {
                break;
            }
            if let Some(next) = readers[run_idx].next_row()? {
                heap.push(Reverse(MergeEntry {
                    record: next,
                    run_idx,
                    sort_keys: sort_keys.to_vec(),
                    decimal_keys: decimal_keys.to_vec(),
                }));
            }
        }

        Ok(result)
    }
}

/// Drop guard that unlinks named sort spill files (and their directory).
///
/// Named spill files do not auto-unlink (unlike tempfile handles), so each is
/// removed explicitly. Declared before the [`RunReader`]s in `external_sort` so
/// it Drops last — after the readers' fds close. Unlink is a bounded metadata
/// op, not bulk content I/O, so it stays plain `std::fs`.
struct SortSpillCleanup {
    dir: PathBuf,
    paths: Vec<PathBuf>,
}

impl Drop for SortSpillCleanup {
    fn drop(&mut self) {
        for p in &self.paths {
            let _ = std::fs::remove_file(p);
        }
        let _ = std::fs::remove_dir(&self.dir);
    }
}

/// Write one framed sort-run blob to `path`.
///
/// Uses [`UringWriter`] when io_uring is available; otherwise falls back to a
/// blocking `std::fs::write` (on a non-io_uring platform there is no per-core
/// reactor to stall, so the blocking call is plane-safe).
fn write_sort_run(path: &Path, bytes: &[u8]) -> crate::Result<()> {
    match UringWriter::new(path) {
        Some(mut w) => {
            w.append(bytes)?;
            w.finish()?;
            Ok(())
        }
        None => std::fs::write(path, bytes).map_err(|e| crate::Error::Storage {
            engine: "sort".into(),
            detail: format!("sort spill write error: {e}"),
        }),
    }
}

/// Read backend for a sort run: io_uring streaming on Linux, blocking
/// `std::fs` (`BufReader`) when io_uring is unavailable.
enum RunBackend {
    // Boxed: `UringSeqReader` carries an io_uring ring + chunk buffer and is far
    // larger than the `BufReader` variant; box it to keep the enum compact.
    Uring(Box<UringSeqReader>),
    Std(BufReader<std::fs::File>),
}

struct RunReader {
    backend: RunBackend,
    remaining: u32,
    run_idx: usize,
}

impl RunReader {
    fn open(path: &Path, run_idx: usize) -> crate::Result<Self> {
        let mut backend = match UringSeqReader::open_default(path) {
            Some(r) => RunBackend::Uring(Box::new(r)),
            None => RunBackend::Std(BufReader::new(std::fs::File::open(path).map_err(|e| {
                crate::Error::Storage {
                    engine: "sort".into(),
                    detail: format!("run reader open: {e}"),
                }
            })?)),
        };

        let mut buf4 = [0u8; 4];
        if !Self::read_full(&mut backend, &mut buf4)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: missing count header".into(),
            });
        }
        let count = u32::from_le_bytes(buf4);

        Ok(Self {
            backend,
            remaining: count,
            run_idx,
        })
    }

    /// Read exactly `dst.len()` bytes. `Ok(true)` = filled; `Ok(false)` = clean
    /// EOF before fill; `Err` = io failure. Bridges the two backends to one
    /// uniform contract.
    fn read_full(backend: &mut RunBackend, dst: &mut [u8]) -> crate::Result<bool> {
        match backend {
            RunBackend::Uring(r) => r.read_exact(dst),
            RunBackend::Std(r) => match r.read_exact(dst) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(e) => Err(crate::Error::Io(e)),
            },
        }
    }

    /// Read one length-prefixed field. A short read mid-record is corruption —
    /// error, never silently drop rows.
    fn read_field(&mut self) -> crate::Result<Vec<u8>> {
        let mut buf4 = [0u8; 4];
        if !Self::read_full(&mut self.backend, &mut buf4)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: expected row frame".into(),
            });
        }
        let len = u32::from_le_bytes(buf4) as usize;
        let mut buf = vec![0u8; len];
        if len > 0 && !Self::read_full(&mut self.backend, &mut buf)? {
            return Err(crate::Error::Storage {
                engine: "sort".into(),
                detail: "sort run truncated: expected row frame".into(),
            });
        }
        Ok(buf)
    }

    fn next_row(&mut self) -> crate::Result<Option<SortRecord>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;

        let id_buf = self.read_field()?;
        let id = String::from_utf8(id_buf).map_err(|_| crate::Error::Storage {
            engine: "sort".into(),
            detail: "sort run corrupt: id not valid utf-8".into(),
        })?;
        let doc = self.read_field()?;
        let keys = decode_sort_values(&self.read_field()?)?;

        Ok(Some(SortRecord { id, doc, keys }))
    }
}

struct MergeEntry {
    record: SortRecord,
    run_idx: usize,
    sort_keys: Vec<SortKeySpec>,
    decimal_keys: Vec<bool>,
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
        // Rows spilled with evaluated keys are merged by those keys; a
        // column-only sort carries none and compares straight from the bytes.
        if self.record.keys.is_empty() && other.record.keys.is_empty() {
            compare_docs_by_keys_binary(
                &self.record.doc,
                &other.record.doc,
                &self.sort_keys,
                &self.decimal_keys,
            )
        } else {
            compare_sort_values(
                &self.record.keys,
                &other.record.keys,
                &self.sort_keys,
                &self.decimal_keys,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    /// Build a framed run blob byte-identical to `external_sort`'s spill
    /// layout, including the trailing (here empty) evaluated-keys field.
    fn frame(rows: &[(String, Vec<u8>)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for (id, val) in rows {
            let idb = id.as_bytes();
            out.extend_from_slice(&(idb.len() as u32).to_le_bytes());
            out.extend_from_slice(idb);
            out.extend_from_slice(&(val.len() as u32).to_le_bytes());
            out.extend_from_slice(val);
            out.extend_from_slice(&0u32.to_le_bytes());
        }
        out
    }

    fn row(id: &str, val: i64) -> (String, Vec<u8>) {
        (
            id.to_string(),
            encode(&serde_json::json!({"id": id, "val": val})),
        )
    }

    /// Write several internally-sorted runs, open them via `RunReader`, drive
    /// the same heap merge `external_sort` uses, and assert the output is
    /// globally sorted and contains exactly every row (no drops).
    #[test]
    fn spill_then_kway_merge_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sort_keys = vec![SortKeySpec::column("val", true)];

        // Three runs, each internally sorted ascending by `val`.
        let runs = [
            vec![row("a", 1), row("d", 4), row("g", 7)],
            vec![row("b", 2), row("e", 5), row("h", 8)],
            vec![row("c", 3), row("f", 6), row("i", 9)],
        ];

        let mut readers: Vec<RunReader> = Vec::new();
        for (idx, run) in runs.iter().enumerate() {
            let path = dir.path().join(format!("run-{idx}.spill"));
            write_sort_run(&path, &frame(run)).unwrap();
            readers.push(RunReader::open(&path, idx).unwrap());
        }

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        for reader in &mut readers {
            if let Some(r) = reader.next_row().unwrap() {
                heap.push(Reverse(MergeEntry {
                    record: r,
                    run_idx: reader.run_idx,
                    sort_keys: sort_keys.clone(),
                    decimal_keys: Vec::new(),
                }));
            }
        }

        let mut out: Vec<String> = Vec::new();
        while let Some(Reverse(entry)) = heap.pop() {
            let run_idx = entry.run_idx;
            out.push(entry.record.id.clone());
            if let Some(next) = readers[run_idx].next_row().unwrap() {
                heap.push(Reverse(MergeEntry {
                    record: next,
                    run_idx,
                    sort_keys: sort_keys.clone(),
                    decimal_keys: Vec::new(),
                }));
            }
        }

        // Globally sorted by val: a..i, and every row present exactly once.
        assert_eq!(out, vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
    }

    /// A run whose count header claims more rows than its bytes provide must
    /// surface an `Err` from `next_row` — never silently return fewer rows.
    #[test]
    fn truncated_run_errors_not_silent_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trunc.spill");

        // Header says 3 rows, but only 1 row of frame bytes follows.
        let one = vec![row("x", 1)];
        let mut bytes = frame(&one);
        // Overwrite the count header (first 4 bytes) with 3.
        bytes[0..4].copy_from_slice(&3u32.to_le_bytes());
        write_sort_run(&path, &bytes).unwrap();

        let mut reader = RunReader::open(&path, 0).unwrap();
        // First row reads back fine.
        assert!(reader.next_row().unwrap().is_some());
        // Second row: bytes exhausted but remaining > 0 → must error.
        assert!(
            reader.next_row().is_err(),
            "truncated run must error, not silently drop rows"
        );
    }
}
