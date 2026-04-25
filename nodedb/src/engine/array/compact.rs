//! Compaction trigger for `ArrayEngine`.

use nodedb_array::types::ArrayId;

use super::compaction::{CompactionMerger, CompactionPicker};
use super::engine::{ArrayEngine, ArrayEngineResult};

impl ArrayEngine {
    /// Run compaction on the array if the picker chooses one. Returns
    /// `true` if a merge happened.
    pub fn maybe_compact(&mut self, id: &ArrayId) -> ArrayEngineResult<bool> {
        let plan = match CompactionPicker::pick(self.store(id)?) {
            Some(p) => p,
            None => return Ok(false),
        };
        let store = self.store(id)?;
        let out = CompactionMerger::run(store, &plan.inputs, plan.output_level)?;
        let store = self.store_mut(id)?;
        let removed = out.removed.clone();
        store.replace_segments(&removed, vec![out.segment_ref])?;
        store.persist_manifest()?;
        for old in removed {
            // Best-effort unlink — the manifest is already authoritative.
            let _ = store.unlink_segment(&old);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::engine::array::test_support::{aid, put_one, schema};
    use tempfile::TempDir;

    #[test]
    fn compaction_merges_l0_segments() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        for i in 0..4 {
            put_one(&mut e, i, 0, i, (i as u64) + 1);
        }
        assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 4);
        let merged = e.maybe_compact(&aid()).unwrap();
        assert!(merged);
        let m = e.store(&aid()).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].level, 1);
    }
}
