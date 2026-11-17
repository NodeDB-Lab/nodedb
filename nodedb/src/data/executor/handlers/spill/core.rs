// SPDX-License-Identifier: BUSL-1.1

//! Generic spill-to-disk file I/O shared by all GROUP BY spillers.
//!
//! `SpillCore<K, V>` handles serializing an in-memory map to spill files and
//! merging them back.  All application-specific logic (governor integration,
//! feed routing, merge semantics) lives in the typed wrapper in `groupby.rs`.
//!
//! ## Plane-safe I/O
//!
//! The Data Plane runs on a `!Send` per-core io_uring reactor; a blocking
//! `std::fs` read/write of file *contents* stalls the entire core's event loop.
//! Run contents are therefore written via [`UringWriter`] and read via
//! [`UringReader`] — the same io_uring primitives the grace-hash join uses.
//! When io_uring is unavailable ([`UringWriter::new`] returns `None`: a non-Linux
//! build or a kernel too old), the helpers fall back to blocking `std::fs`.
//! That fallback is correct: on a platform without io_uring there is no per-core
//! io_uring reactor to stall, so the blocking call does not violate the
//! Data-Plane no-blocking-content-IO rule it is replacing on Linux.
//!
//! Directory and unlink metadata ops (`create_dir_all`, `remove_file`,
//! `remove_dir`) remain plain `std::fs`: they are bounded, fast, and do not
//! scale with the spilled data volume.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::data::io::uring_reader::UringReader;
use crate::data::io::uring_writer::UringWriter;

/// Maximum output cardinality as a multiple of the in-memory cap.
///
/// Grace-hash recursive partitioning is deferred to v0.2.0; a deterministic
/// error is returned rather than OOMing.
const FINALIZE_CAP_FACTOR: usize = 10;

/// Generic spill-to-disk manager for a `HashMap<K, V>`.
///
/// Each spill run is encoded as MessagePack (via `zerompk`) into its own
/// file inside `spill_dir`. MessagePack keeps every float exact, NaN and
/// ±Infinity included. On `merge()`, all runs plus any remaining in-memory
/// entries are folded together using a caller-supplied merge function.
pub(super) struct SpillCore<K, V> {
    spill_dir: PathBuf,
    /// Paths of the spill-run files written so far (one serialized
    /// `Vec<(K, V)>` blob per file). `merge` unlinks them, or `Drop` when no merge ran.
    runs: Vec<PathBuf>,
    pub(super) spilled_runs: u64,
    /// Set once the run files and the directory are removed.
    removed: bool,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> SpillCore<K, V>
where
    K: zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de> + Eq + Hash,
    V: zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
{
    pub(super) fn new(spill_dir: PathBuf) -> crate::Result<Self> {
        std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
            engine: "groupby_spill".into(),
            detail: format!("failed to create spill dir {}: {e}", spill_dir.display()),
        })?;
        Ok(Self {
            spill_dir,
            runs: Vec::new(),
            spilled_runs: 0,
            removed: false,
            _marker: PhantomData,
        })
    }

    /// Serialize `entries` to a spill file and append it to the run list.
    ///
    /// Returns immediately without writing if `entries` is empty.
    pub(super) fn flush_run(&mut self, entries: impl Iterator<Item = (K, V)>) -> crate::Result<()> {
        let entries: Vec<(K, V)> = entries.collect();
        if entries.is_empty() {
            return Ok(());
        }

        let encoded = zerompk::to_msgpack_vec(&entries).map_err(|e| crate::Error::Storage {
            engine: "groupby_spill".into(),
            detail: format!("spill serialize error: {e}"),
        })?;

        let run_path = self
            .spill_dir
            .join(format!("run-{}.spill", self.spilled_runs));
        write_run_file(&run_path, &encoded)?;

        self.runs.push(run_path);
        self.spilled_runs += 1;
        Ok(())
    }

    /// Merge all spill runs and the remaining `in_mem` entries into a single
    /// consolidated `HashMap<K, V>`.
    ///
    /// `merge_fn(dst, src)` is called when `src`'s key already exists in the
    /// output.  Returns `Err` if the final cardinality exceeds
    /// `cap × FINALIZE_CAP_FACTOR`.
    ///
    /// Runs are read back one at a time so the transient read buffer is bounded
    /// to a single run rather than the sum of all spilled runs. The spill files
    /// are unlinked at the end of this call, and a removal that fails is an
    /// error.
    pub(super) fn merge<F>(
        mut self,
        in_mem: &mut HashMap<K, V>,
        cap: usize,
        merge_fn: F,
    ) -> crate::Result<HashMap<K, V>>
    where
        F: Fn(&mut V, V),
    {
        let output_cap = cap.saturating_mul(FINALIZE_CAP_FACTOR);
        let mut output: HashMap<K, V> = HashMap::new();

        // One reader reused across runs, built only when there is something to
        // read back. `None` => no spill occurred, or io_uring is unavailable
        // (the read helper then falls back to blocking `std::fs::read`). Runs
        // larger than the pool buffer get a dedicated allocation inside
        // `read_files`, so this does not cap run size.
        let mut reader = if self.runs.is_empty() {
            None
        } else {
            UringReader::with_config(8, 2, 4 * 1024 * 1024)
        };

        for run_path in &self.runs {
            let buf = read_run_file(&mut reader, run_path)?;
            let entries: Vec<(K, V)> =
                zerompk::from_msgpack(&buf).map_err(|e| crate::Error::Storage {
                    engine: "groupby_spill".into(),
                    detail: format!("spill run deserialize error: {e}"),
                })?;

            merge_entries(&mut output, entries, output_cap, &merge_fn)?;
        }

        let in_mem_entries: Vec<(K, V)> = in_mem.drain().collect();
        merge_entries(&mut output, in_mem_entries, output_cap, &merge_fn)?;

        self.remove_files()?;
        Ok(output)
    }
}

/// Write one serialized spill-run blob to `path`.
///
/// Uses [`UringWriter`] when io_uring is available; otherwise falls back to a
/// blocking `std::fs::write` (see the module doc for why that is plane-safe on
/// a non-io_uring platform).
fn write_run_file(path: &Path, bytes: &[u8]) -> crate::Result<()> {
    match UringWriter::new(path) {
        Some(mut writer) => {
            writer.append(bytes)?;
            writer.finish()?;
            Ok(())
        }
        None => std::fs::write(path, bytes).map_err(|e| crate::Error::Storage {
            engine: "groupby_spill".into(),
            detail: format!("spill run write error: {e}"),
        }),
    }
}

/// Read one spill-run blob back from `path`.
///
/// Uses the shared [`UringReader`] when available; otherwise falls back to
/// blocking `std::fs::read`. An empty read-back of a run we wrote non-empty is
/// surfaced as an error rather than silently dropping that run's groups.
fn read_run_file(reader: &mut Option<UringReader>, path: &Path) -> crate::Result<Vec<u8>> {
    let buf = match reader.as_mut() {
        Some(r) => {
            let mut bufs = r.read_files(&[path]);
            bufs.pop().unwrap_or_default()
        }
        None => std::fs::read(path).map_err(|e| crate::Error::Storage {
            engine: "groupby_spill".into(),
            detail: format!("spill run read error: {e}"),
        })?,
    };

    if buf.is_empty() {
        // Runs are only ever written non-empty (`flush_run` early-returns on an
        // empty iterator), so an empty read-back is a read failure, not a valid
        // empty run. Erroring here prevents silently dropping a run's rows.
        return Err(crate::Error::Storage {
            engine: "groupby_spill".into(),
            detail: format!(
                "spill run {} read back empty (read failure)",
                path.display()
            ),
        });
    }
    Ok(buf)
}

fn merge_entries<K, V, F>(
    output: &mut HashMap<K, V>,
    entries: Vec<(K, V)>,
    output_cap: usize,
    merge_fn: &F,
) -> crate::Result<()>
where
    K: Eq + Hash,
    F: Fn(&mut V, V),
{
    for (key, value) in entries {
        if output.len() >= output_cap && !output.contains_key(&key) {
            return Err(crate::Error::Storage {
                engine: "groupby_spill".into(),
                detail: format!(
                    "finalized group cardinality exceeds {FINALIZE_CAP_FACTOR}x cap \
                     ({output_cap}), query result cardinality limit reached"
                ),
            });
        }
        match output.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                merge_fn(e.get_mut(), value);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(value);
            }
        }
    }
    Ok(())
}

impl<K, V> SpillCore<K, V> {
    /// Remove every run file, then the spill directory. Unlink is a bounded
    /// metadata op, not bulk content I/O, so it stays plain `std::fs`. Each
    /// removal that fails files its faultbox report at the removal site.
    fn remove_files(&mut self) -> Result<(), super::error::SpillError> {
        let mut first_error = None;
        for path in self.runs.drain(..) {
            if let Err(e) =
                super::cleanup::remove_spill_file(super::cleanup::SpillKind::GroupBy, &path)
            {
                first_error.get_or_insert(e);
            }
        }
        if let Err(e) = super::cleanup::remove_empty_spill_dir(
            super::cleanup::SpillKind::GroupBy,
            &self.spill_dir,
        ) {
            first_error.get_or_insert(e);
        }
        self.removed = true;
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<K, V> Drop for SpillCore<K, V> {
    fn drop(&mut self) {
        if self.removed {
            return;
        }
        // Reached when the aggregation stops before `merge`, with its own
        // error or with no group left to merge.
        if let Err(e) = self.remove_files() {
            tracing::error!(error = %e, "groupby_spill: spill files left after the aggregation");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs + in-memory entries are folded together with the merge function;
    /// duplicate keys across runs and the in-memory map combine correctly.
    #[test]
    fn merge_consolidates_runs_and_in_mem() {
        let dir = tempfile::tempdir().unwrap();
        let mut core: SpillCore<String, u64> = SpillCore::new(dir.path().join("sc")).unwrap();

        core.flush_run(vec![("a".to_string(), 1u64), ("b".to_string(), 2)].into_iter())
            .unwrap();
        core.flush_run(vec![("a".to_string(), 10u64), ("c".to_string(), 3)].into_iter())
            .unwrap();
        assert_eq!(core.spilled_runs, 2);

        let mut in_mem: HashMap<String, u64> = HashMap::new();
        in_mem.insert("b".to_string(), 20);
        in_mem.insert("d".to_string(), 4);

        let out = core
            .merge(&mut in_mem, 100, |dst, src| *dst += src)
            .unwrap();

        // a: 1 + 10 = 11, b: 2 + 20 = 22, c: 3, d: 4.
        assert_eq!(out.get("a"), Some(&11));
        assert_eq!(out.get("b"), Some(&22));
        assert_eq!(out.get("c"), Some(&3));
        assert_eq!(out.get("d"), Some(&4));
        assert_eq!(out.len(), 4);
    }

    /// An empty run is a no-op: nothing is written and no run file is created.
    #[test]
    fn empty_flush_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut core: SpillCore<String, u64> = SpillCore::new(dir.path().join("sc")).unwrap();

        core.flush_run(std::iter::empty()).unwrap();
        assert_eq!(core.spilled_runs, 0);

        let mut in_mem: HashMap<String, u64> = HashMap::new();
        in_mem.insert("x".to_string(), 1);
        let out = core
            .merge(&mut in_mem, 100, |dst, src| *dst += src)
            .unwrap();
        assert_eq!(out.get("x"), Some(&1));
        assert_eq!(out.len(), 1);
    }

    /// A spill-only round-trip (no in-memory residue) reads every run back.
    #[test]
    fn merge_with_empty_in_mem_returns_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut core: SpillCore<String, u64> = SpillCore::new(dir.path().join("sc")).unwrap();
        core.flush_run(vec![("k1".to_string(), 5u64)].into_iter())
            .unwrap();
        core.flush_run(vec![("k2".to_string(), 7u64)].into_iter())
            .unwrap();

        let mut in_mem: HashMap<String, u64> = HashMap::new();
        let out = core
            .merge(&mut in_mem, 100, |dst, src| *dst += src)
            .unwrap();
        assert_eq!(out.get("k1"), Some(&5));
        assert_eq!(out.get("k2"), Some(&7));
        assert_eq!(out.len(), 2);
    }

    /// Float state holding NaN and ±Infinity spills, reads back, and merges
    /// with every non-finite value intact. Each value is `[sum, count]`.
    #[test]
    fn spill_keeps_non_finite_values() {
        fn state(v: f64) -> Vec<f64> {
            vec![v, 1.0]
        }

        let dir = tempfile::tempdir().unwrap();
        let mut core: SpillCore<String, Vec<f64>> = SpillCore::new(dir.path().join("sc")).unwrap();
        core.flush_run(
            vec![
                ("g".to_string(), state(f64::INFINITY)),
                ("n".to_string(), state(f64::NAN)),
                ("o".to_string(), state(1e308)),
                ("i".to_string(), state(f64::NEG_INFINITY)),
            ]
            .into_iter(),
        )
        .unwrap();
        core.flush_run(
            vec![
                ("g".to_string(), state(f64::NEG_INFINITY)),
                ("n".to_string(), state(1.0)),
            ]
            .into_iter(),
        )
        .unwrap();
        let mut in_mem: HashMap<String, Vec<f64>> = HashMap::new();
        in_mem.insert("o".to_string(), state(1e308));

        let out = core
            .merge(&mut in_mem, 100, |dst, src| {
                for (d, s) in dst.iter_mut().zip(src) {
                    *d += s;
                }
            })
            .unwrap();

        // Infinity plus -Infinity is NaN.
        assert!(out["g"][0].is_nan());
        assert_eq!(out["g"][1], 2.0);
        // NaN survives the run.
        assert!(out["n"][0].is_nan());
        // A run total plus the in-memory total overflows to Infinity.
        assert_eq!(out["o"][0], f64::INFINITY);
        assert_eq!(out["o"][1], 2.0);
        // An unmerged -Infinity reads back exactly.
        assert_eq!(out["i"][0], f64::NEG_INFINITY);
    }

    /// Exceeding `cap × FINALIZE_CAP_FACTOR` distinct keys returns a
    /// deterministic error rather than growing unbounded.
    #[test]
    fn cardinality_cap_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut core: SpillCore<String, u64> = SpillCore::new(dir.path().join("sc")).unwrap();

        // cap = 1 → output_cap = 10. Spill 11 distinct keys → must error.
        let entries: Vec<(String, u64)> = (0..11).map(|i| (format!("k{i}"), i as u64)).collect();
        core.flush_run(entries.into_iter()).unwrap();

        let mut in_mem: HashMap<String, u64> = HashMap::new();
        let res = core.merge(&mut in_mem, 1, |dst, src| *dst += src);
        assert!(res.is_err(), "expected cardinality-cap error, got {res:?}");
    }
}
