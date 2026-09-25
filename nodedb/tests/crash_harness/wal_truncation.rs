// SPDX-License-Identifier: BUSL-1.1

//! Observe WAL truncation from the segment names on disk and the checkpoint
//! manager's log lines.

use super::log_fields::{log_field, strip_ansi};

/// The checkpoint manager's line for a cycle that wrote its marker. A debug
/// line.
pub const MARKER_WRITTEN: &str = "checkpoint WAL marker written";
/// The line for a cycle whose truncation step removed segments.
pub const WAL_TRUNCATED: &str = "WAL truncated after checkpoint";
/// The line for a cycle whose truncation step removed nothing. A debug line.
pub const NOTHING_TRUNCATED: &str = "checkpoint complete (no segments to truncate)";

/// The first LSN of a WAL segment, from its `wal-<first LSN>.seg` name.
pub fn segment_first_lsn(segment: &str) -> u64 {
    segment
        .trim_start_matches("wal-")
        .trim_end_matches(".seg")
        .parse()
        .unwrap_or_else(|e| panic!("WAL segment name {segment} carries no first LSN: {e}"))
}

/// Whether a checkpoint whose marker is at or above `lsn` finished its
/// truncation step in `log`. Cycles run one at a time, so the first
/// truncation line after that marker belongs to the same cycle.
pub fn truncation_finished_from(log: &str, lsn: u64) -> bool {
    let log = strip_ansi(log);
    let mut lines = log.lines();
    let marked = lines.by_ref().any(|line| {
        log_field(line, MARKER_WRITTEN, "marker_lsn")
            .first()
            .is_some_and(|marker| *marker >= lsn)
    });
    marked && lines.any(|line| line.contains(WAL_TRUNCATED) || line.contains(NOTHING_TRUNCATED))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cycle_counts_only_once_its_truncation_step_finished() {
        let marker = "DEBUG checkpoint WAL marker written marker_lsn=40 checkpoint_lsn=9";
        let done = "DEBUG checkpoint complete (no segments to truncate)";
        assert!(!truncation_finished_from(marker, 40));
        assert!(truncation_finished_from(&format!("{marker}\n{done}"), 40));
        assert!(!truncation_finished_from(&format!("{marker}\n{done}"), 41));
        assert!(!truncation_finished_from(&format!("{done}\n{marker}"), 40));
        assert_eq!(segment_first_lsn("wal-00000000000000000019.seg"), 19);
    }
}
