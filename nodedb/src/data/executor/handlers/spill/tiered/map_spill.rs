// SPDX-License-Identifier: BUSL-1.1

//! Writing a tiered map's hot tier as a run, merging runs, and rolling the
//! map back to a journal position.
//!
//! A merge keeps every version, ordered by key bytes and then by seq
//! newest first, so a later rollback can still drop the versions written
//! after its marker. Runs merge by level: [`MERGE_FANOUT`] runs of one
//! level at the newest end merge into one run of the next level, so a key
//! is rewritten once per level and the run count stays logarithmic.

use super::map::{Hot, TieredKey, TieredMap, TieredValue, encode_key};
use super::space::SpillSpace;
use crate::data::executor::handlers::spill::run::record::{RunRecordRef, encode_value};
use crate::data::executor::handlers::spill::run::{
    RunRecord, RunStream, SortedRun, SortedRunWriter,
};

/// Runs of one level that merge into one run of the next level.
const MERGE_FANOUT: usize = 8;

impl<K: TieredKey, V: TieredValue> TieredMap<K, V> {
    /// Write the hot tier as one sorted run tagged `tag`, clear the hot
    /// tier, and merge runs that reached [`MERGE_FANOUT`]. A hot tier with
    /// no entry writes nothing.
    pub(in crate::data::executor) fn spill(
        &mut self,
        space: &mut SpillSpace,
        tag: &str,
    ) -> crate::Result<()> {
        if self.hot.is_empty() {
            return Ok(());
        }
        let path = space.next_path(tag)?;
        let run = {
            let mut keyed: Vec<(Vec<u8>, &Hot<V>)> = self
                .hot
                .iter()
                .map(|(key, hot)| Ok((encode_key(key)?, hot)))
                .collect::<crate::Result<_>>()?;
            keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let mut writer = SortedRunWriter::create(path, 0)?;
            for (key_bytes, hot) in &keyed {
                let value = encode_value(std::path::Path::new(super::map::HOT_TIER), &hot.value)?;
                writer.push(RunRecordRef {
                    key: key_bytes,
                    seq: hot.seq,
                    value: &value,
                })?;
            }
            writer.finish()?
        };
        self.hot.clear();
        self.hot_bytes = 0;
        if let Some(run) = run {
            self.runs.push(run);
        }
        self.merge_full_levels(space, tag)
    }

    /// Merge the newest runs while [`MERGE_FANOUT`] of them share a level.
    fn merge_full_levels(&mut self, space: &mut SpillSpace, tag: &str) -> crate::Result<()> {
        loop {
            let Some(level) = self.runs.last().map(SortedRun::level) else {
                return Ok(());
            };
            let same = self
                .runs
                .iter()
                .rev()
                .take_while(|run| run.level() == level)
                .count();
            if same < MERGE_FANOUT {
                return Ok(());
            }
            let start = self.runs.len() - same;
            let merged = merge_runs(
                self.runs.get(start..).unwrap_or_default(),
                space.next_path(tag)?,
                level + 1,
            )?;
            let replaced: Vec<SortedRun> = self.runs.drain(start..).collect();
            if let Some(run) = merged {
                self.runs.push(run);
            }
            for run in replaced {
                run.remove()?;
            }
        }
    }

    /// Drop every version written at or after journal position `bound`: the
    /// hot versions, and the run records. A run with no version left goes;
    /// a run with some versions left is rewritten without the others.
    pub(in crate::data::executor) fn rollback(
        &mut self,
        bound: u64,
        space: &mut SpillSpace,
        tag: &str,
    ) -> crate::Result<()> {
        let late: Vec<K> = self
            .hot
            .iter()
            .filter(|(_, hot)| hot.seq >= bound)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &late {
            self.remove_hot(key);
        }
        if self.runs.iter().all(|run| run.max_seq() < bound) {
            return Ok(());
        }
        let runs = std::mem::take(&mut self.runs);
        let mut obsolete = Vec::new();
        for run in runs {
            if run.max_seq() < bound {
                self.runs.push(run);
            } else if run.min_seq() >= bound {
                obsolete.push(run);
            } else {
                let filtered = filter_run(&run, bound, space.next_path(tag)?)?;
                if let Some(filtered) = filtered {
                    self.runs.push(filtered);
                }
                obsolete.push(run);
            }
        }
        for run in obsolete {
            run.remove()?;
        }
        Ok(())
    }
}

/// Rewrite `run` keeping only the versions written before `bound`.
fn filter_run(
    run: &SortedRun,
    bound: u64,
    path: std::path::PathBuf,
) -> crate::Result<Option<SortedRun>> {
    let mut stream = RunStream::from_start(run)?;
    let mut writer = SortedRunWriter::create(path, run.level())?;
    while let Some(record) = stream.next_record()? {
        if record.seq < bound {
            writer.push(record.as_ref())?;
        }
    }
    writer.finish()
}

/// Merge `runs` (oldest first) into one run of `level`, keeping every
/// version.
fn merge_runs(
    runs: &[SortedRun],
    path: std::path::PathBuf,
    level: u8,
) -> crate::Result<Option<SortedRun>> {
    let mut writer = SortedRunWriter::create(path, level)?;
    let mut cursors: Vec<(RunStream, Option<RunRecord>)> = Vec::with_capacity(runs.len());
    for run in runs {
        let mut stream = RunStream::from_start(run)?;
        let head = stream.next_record()?;
        cursors.push((stream, head));
    }
    loop {
        // The smallest key, newest version first. Across runs, versions of
        // one key grow newer with the run index, and seq orders them.
        let mut best: Option<usize> = None;
        for (index, (_, head)) in cursors.iter().enumerate() {
            let Some(record) = head else {
                continue;
            };
            let better = match best.and_then(|b| cursors.get(b)?.1.as_ref()) {
                None => true,
                Some(current) => match record.key.cmp(&current.key) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Equal => record.seq > current.seq,
                    std::cmp::Ordering::Greater => false,
                },
            };
            if better {
                best = Some(index);
            }
        }
        let Some(index) = best else {
            break;
        };
        let Some((stream, head)) = cursors.get_mut(index) else {
            break;
        };
        if let Some(record) = head.take() {
            writer.push(record.as_ref())?;
        }
        *head = stream.next_record()?;
    }
    writer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn space(dir: &std::path::Path) -> SpillSpace {
        SpillSpace::new(dir.join("txn"), "t", 0)
    }

    type PairKey = (u32, String);

    fn pair_key(state: &mut u64) -> PairKey {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let a = (*state >> 40) as u32 % 40;
        let b = (*state >> 20) as u32 % 12;
        (a, format!("s{b}"))
    }

    fn scan_all(
        map: &TieredMap<PairKey, u64>,
        bounds: crate::data::executor::handlers::spill::tiered::ScanBounds,
        reader: &crate::data::executor::handlers::spill::run::BlockReader,
    ) -> Vec<(PairKey, u64)> {
        map.scan(bounds, reader)
            .map(|item| item.map(|(k, v)| (k, *v)))
            .collect::<crate::Result<_>>()
            .unwrap()
    }

    #[test]
    fn a_map_spilled_at_threshold_zero_answers_like_an_unspilled_map() {
        use crate::data::executor::handlers::spill::tiered::ScanBounds;

        let dir = tempfile::tempdir().unwrap();
        let mut space = space(dir.path());
        let mut never = SpillSpace::never_spills();
        let mut spilled: TieredMap<PairKey, u64> = TieredMap::default();
        let mut resident: TieredMap<PairKey, u64> = TieredMap::default();
        let mut history: std::collections::HashMap<PairKey, Vec<(u64, u64)>> =
            std::collections::HashMap::new();
        let mut state = 7u64;
        for seq in 0..3_000u64 {
            let key = pair_key(&mut state);
            let value = seq * 3;
            history.entry(key.clone()).or_default().push((seq, value));
            spilled.insert(key.clone(), value, seq);
            resident.insert(key, value, seq);
            if spilled.hot_bytes() > space.threshold() {
                spilled.spill(&mut space, "v").unwrap();
            }
            if resident.hot_bytes() > never.threshold() {
                resident.spill(&mut never, "v").unwrap();
            }
        }
        assert!(spilled.has_runs());
        assert!(!resident.has_runs());
        assert!(spilled.runs.len() < MERGE_FANOUT * 4);
        let reader = space.reader();
        let resident_reader = never.reader();

        for a in 0..42u32 {
            for b in 0..13u32 {
                let key = (a, format!("s{b}"));
                let want = resident.get(&key, resident_reader).unwrap().map(|v| *v);
                assert_eq!(spilled.get(&key, reader).unwrap().map(|v| *v), want);
                assert_eq!(spilled.contains(&key, reader).unwrap(), want.is_some());
                for bound in [0u64, 1, 500, 1_500, 2_999, u64::MAX] {
                    let before = history.get(&key).and_then(|versions| {
                        versions
                            .iter()
                            .rev()
                            .find(|(seq, _)| *seq < bound)
                            .map(|(_, v)| *v)
                    });
                    assert_eq!(spilled.get_before(&key, bound, reader).unwrap(), before);
                }
            }
        }

        let all = |map: &TieredMap<PairKey, u64>| -> Vec<(PairKey, u64)> {
            map.iter()
                .map(|item| item.map(|(k, v)| (k, *v)))
                .collect::<crate::Result<_>>()
                .unwrap()
        };
        assert_eq!(all(&spilled), all(&resident));

        for a in [0u32, 7, 39, 41] {
            let mut prefix = vec![0x92];
            prefix.extend(encode_key(&a).unwrap());
            let bounds = ScanBounds {
                after: None,
                prefix: Some(prefix),
            };
            assert_eq!(
                scan_all(&spilled, bounds.clone(), reader),
                scan_all(&resident, bounds, resident_reader),
                "prefix {a}"
            );
        }
        for after in [
            (0u32, "s0".to_string()),
            (20, "s5".into()),
            (39, "s9".into()),
        ] {
            let bounds = ScanBounds {
                after: Some(encode_key(&after).unwrap()),
                prefix: None,
            };
            assert_eq!(
                scan_all(&spilled, bounds.clone(), reader),
                scan_all(&resident, bounds, resident_reader),
                "after {after:?}"
            );
        }
    }

    #[test]
    fn spill_then_get_reads_runs_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let mut space = space(dir.path());
        let mut map: TieredMap<u32, String> = TieredMap::default();
        map.insert(1, "a".into(), 0);
        map.insert(2, "b".into(), 1);
        map.spill(&mut space, "v").unwrap();
        map.insert(1, "c".into(), 2);
        map.spill(&mut space, "v").unwrap();
        assert_eq!(map.hot_bytes(), 0);

        let reader = space.reader();
        assert_eq!(map.get(&1, reader).unwrap().unwrap().as_str(), "c");
        assert_eq!(map.get(&2, reader).unwrap().unwrap().as_str(), "b");
        assert!(map.get(&3, reader).unwrap().is_none());
        assert_eq!(map.get_before(&1, 2, reader).unwrap().unwrap(), "a");
    }

    #[test]
    fn a_lookup_reads_nothing_outside_a_run_and_one_page_and_block_inside() {
        let dir = tempfile::tempdir().unwrap();
        let mut space = space(dir.path());
        let mut map: TieredMap<u32, u32> = TieredMap::default();
        for key in (0..400u32).step_by(2) {
            map.insert(key, key, u64::from(key));
        }
        map.spill(&mut space, "v").unwrap();
        let reader = space.reader();

        // Keys past the run's last key cost no read.
        for key in 10_000..10_200u32 {
            let before = reader.reads();
            assert!(map.get(&key, reader).unwrap().is_none());
            assert!(!map.contains(&key, reader).unwrap());
            assert!(map.get_before(&key, u64::MAX, reader).unwrap().is_none());
            assert_eq!(reader.reads(), before, "key {key} read a block");
        }

        // A key inside the range reads one index page and one block.
        for key in [1u32, 41, 42, 201, 398] {
            let before = reader.reads();
            let found = map.get(&key, reader).unwrap().map(|v| *v);
            assert_eq!(found, (key % 2 == 0).then_some(key));
            assert_eq!(reader.reads() - before, 2, "key {key}");
        }
    }

    #[test]
    fn merging_runs_keeps_every_version_for_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let mut space = space(dir.path());
        let mut map: TieredMap<u32, u32> = TieredMap::default();
        for seq in 0..(MERGE_FANOUT as u64 * 2) {
            map.insert(7, seq as u32, seq);
            map.spill(&mut space, "v").unwrap();
        }
        assert!(map.runs.len() < MERGE_FANOUT);
        let reader = space.reader();
        assert_eq!(*map.get(&7, reader).unwrap().unwrap(), 15);

        map.rollback(5, &mut space, "v").unwrap();
        assert_eq!(*map.get(&7, space.reader()).unwrap().unwrap(), 4);
        map.rollback(0, &mut space, "v").unwrap();
        assert!(map.is_empty());
    }
}
