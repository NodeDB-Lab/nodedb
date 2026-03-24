//! PITR/restore utilities: timestamp parsing, dry-run validation, and restore planning.
//!
//! Extracted from `snapshot.rs` — contains standalone functions and types
//! used for Point-In-Time Recovery planning and validation.

use crate::types::Lsn;

use super::snapshot::SnapshotMeta;

/// Result of a PITR target resolution.
#[derive(Debug, Clone)]
pub struct PitrTarget {
    /// The closest base snapshot to restore from.
    pub base_snapshot: SnapshotMeta,
    /// Delta snapshots to apply in order (oldest first).
    pub deltas: Vec<SnapshotMeta>,
    /// Target LSN resolved from the requested UTC timestamp.
    pub replay_lsn: Lsn,
    /// Number of WAL records to replay after snapshot restore.
    pub wal_records_to_replay: u64,
}

/// Dry-run result for restore validation.
#[derive(Debug, Clone)]
pub struct RestoreDryRun {
    /// Whether the restore plan is valid.
    pub valid: bool,
    /// Human-readable description of what would happen.
    pub plan_description: String,
    /// Estimated time for restore (microseconds).
    pub estimated_duration_us: u64,
    /// Number of snapshot files to read.
    pub files_to_read: usize,
    /// Number of WAL records to replay.
    pub wal_records: u64,
    /// Issues found during validation.
    pub issues: Vec<String>,
}

/// Validate a restore plan without executing it.
pub fn dry_run_restore(target: &PitrTarget) -> RestoreDryRun {
    let mut issues = Vec::new();
    let files_to_read = 1 + target.deltas.len(); // base + deltas

    // Validate delta chain continuity.
    let mut expected_lsn = target.base_snapshot.end_lsn;
    for delta in &target.deltas {
        if delta.begin_lsn > expected_lsn {
            issues.push(format!(
                "gap in delta chain: expected begin_lsn <= {}, got {}",
                expected_lsn.as_u64(),
                delta.begin_lsn.as_u64()
            ));
        }
        expected_lsn = delta.end_lsn;
    }

    // Check that replay LSN is reachable.
    if target.replay_lsn < target.base_snapshot.begin_lsn {
        issues.push(format!(
            "replay LSN {} is before base snapshot begin {}",
            target.replay_lsn.as_u64(),
            target.base_snapshot.begin_lsn.as_u64()
        ));
    }

    let plan_description = format!(
        "Restore base snapshot #{} (LSN {}-{}), apply {} deltas, replay {} WAL records to LSN {}",
        target.base_snapshot.snapshot_id,
        target.base_snapshot.begin_lsn.as_u64(),
        target.base_snapshot.end_lsn.as_u64(),
        target.deltas.len(),
        target.wal_records_to_replay,
        target.replay_lsn.as_u64(),
    );

    // Rough estimate: 100MB/s for snapshot reads + 10K WAL records/sec.
    let total_snapshot_bytes: u64 =
        target.base_snapshot.data_bytes + target.deltas.iter().map(|d| d.data_bytes).sum::<u64>();
    let snapshot_us = (total_snapshot_bytes as f64 / 100_000_000.0 * 1_000_000.0) as u64;
    let wal_us = target.wal_records_to_replay * 100; // 100us per record

    RestoreDryRun {
        valid: issues.is_empty(),
        plan_description,
        estimated_duration_us: snapshot_us + wal_us,
        files_to_read,
        wal_records: target.wal_records_to_replay,
        issues,
    }
}

/// Parse a UTC timestamp from various input formats.
///
/// Supports:
/// - Unix epoch microseconds: `"1710509400000000"`
/// - Unix epoch seconds: `"1710509400"`
/// - ISO 8601: `"2024-03-15T14:30:00Z"` (basic parsing, no full chrono dependency)
pub fn parse_utc_timestamp(input: &str) -> crate::Result<u64> {
    let trimmed = input.trim();

    // Try parsing as integer (epoch micros or seconds).
    if let Ok(n) = trimmed.parse::<u64>() {
        // Heuristic: values > 1e15 are microseconds, otherwise seconds.
        if n > 1_000_000_000_000_000 {
            return Ok(n); // Already microseconds.
        }
        return Ok(n * 1_000_000); // Convert seconds to microseconds.
    }

    // Try ISO 8601 basic parsing: "YYYY-MM-DDTHH:MM:SSZ"
    // This is a simplified parser — production should use chrono or time crate.
    if trimmed.len() >= 19 && trimmed.contains('T') {
        let date_part = &trimmed[..10]; // "YYYY-MM-DD"
        let time_part = &trimmed[11..19]; // "HH:MM:SS"

        let parts: Vec<u64> = date_part
            .split('-')
            .chain(time_part.split(':'))
            .filter_map(|s| s.parse().ok())
            .collect();

        if parts.len() == 6 {
            let (year, month, day, hour, min, sec) =
                (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);

            // Days from Unix epoch to the given date.
            // Leap year: divisible by 4, except centuries unless divisible by 400.
            let leap_days = |y: u64| -> u64 {
                if y == 0 {
                    return 0;
                }
                let y = y - 1; // count leap years before this year
                y / 4 - y / 100 + y / 400 - (1969 / 4 - 1969 / 100 + 1969 / 400)
            };
            let is_leap =
                |y: u64| y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400));
            let leap_adj = if is_leap(year) && month > 2 { 1 } else { 0 };
            let days_since_epoch =
                (year - 1970) * 365 + leap_days(year) + month_to_days(month) + leap_adj + day - 1;
            let epoch_secs = days_since_epoch * 86400 + hour * 3600 + min * 60 + sec;
            return Ok(epoch_secs * 1_000_000);
        }
    }

    Err(crate::Error::BadRequest {
        detail: format!(
            "cannot parse UTC timestamp: '{trimmed}'. Expected epoch micros, epoch seconds, or ISO 8601"
        ),
    })
}

/// Approximate days from Jan 1 to the start of the given month (non-leap year).
fn month_to_days(month: u64) -> u64 {
    match month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        12 => 334,
        _ => 0,
    }
}
