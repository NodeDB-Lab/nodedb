//! Background merge cycle: pick mergeable groups, merge, atomically update registry.

use std::path::{Path, PathBuf};

use crate::engine::timeseries::columnar_segment::SegmentError;
use crate::engine::timeseries::partition_registry::{PartitionRegistry, format_partition_dir};

use super::partitions::merge_partitions;

/// Execute a merge cycle with crash-safe persistence.
///
/// Three-step atomic protocol per merge:
/// 1. Write merged partition to new directory (crash → orphan, no manifest entry)
/// 2. Commit to registry + persist manifest atomically (crash → write-rename atomic)
/// 3. Background cleanup of source directories (crash → cleanup on next startup)
pub fn run_merge_cycle(
    registry: &mut PartitionRegistry,
    base_dir: &Path,
    now_ms: i64,
) -> Result<usize, SegmentError> {
    let groups = registry.find_mergeable(now_ms);
    let mut merge_count = 0;

    for group_starts in &groups {
        // Collect source directories.
        let source_dirs: Vec<PathBuf> = group_starts
            .iter()
            .filter_map(|&start| registry.get(start).map(|e| base_dir.join(&e.dir_name)))
            .collect();

        if source_dirs.len() != group_starts.len() {
            continue; // Some partitions already gone.
        }

        // Mark sources as merging.
        for &start in group_starts {
            registry.mark_merging(start);
        }

        // Determine output name.
        let (Some(&first_start), Some(&last_start)) = (group_starts.first(), group_starts.last())
        else {
            continue;
        };
        let last_entry = registry.get(last_start);
        let last_end = last_entry.map(|e| e.meta.max_ts).unwrap_or(first_start);
        let output_name = format_partition_dir(first_start, last_end);

        // Execute merge.
        match merge_partitions(base_dir, &source_dirs, &output_name) {
            Ok(result) => {
                // Step 2: Atomic manifest update.
                registry.commit_merge(result.meta, result.dir_name, group_starts);

                // Persist manifest (atomic write-rename).
                let manifest_path = base_dir.join("partition_manifest.json");
                if let Err(e) = registry.persist(&manifest_path) {
                    tracing::warn!(error = %e, "failed to persist partition manifest after merge");
                }

                merge_count += 1;
            }
            Err(_e) => {
                // Merge failed — roll back to Sealed state.
                for &start in group_starts {
                    registry.rollback_merging(start);
                }
            }
        }
    }

    Ok(merge_count)
}
