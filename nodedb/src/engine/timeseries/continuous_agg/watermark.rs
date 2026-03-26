//! Per-aggregate watermark tracking.
//!
//! Tracks the highest timestamp fully aggregated and detects out-of-order
//! data that needs re-aggregation.

use serde::{Deserialize, Serialize};

/// Per-aggregate watermark state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkState {
    /// Highest timestamp that has been fully aggregated.
    pub watermark_ts: i64,
    /// Oldest out-of-order timestamp that needs re-aggregation.
    /// When O3 data arrives below the watermark, this is set to the
    /// oldest O3 timestamp. On next refresh, re-aggregate buckets
    /// between o3_watermark and watermark.
    pub o3_watermark_ts: Option<i64>,
    /// Total rows aggregated.
    pub rows_aggregated: u64,
    /// Last refresh timestamp (wall clock).
    pub last_refresh_ms: i64,
}

impl Default for WatermarkState {
    fn default() -> Self {
        Self {
            watermark_ts: i64::MIN,
            o3_watermark_ts: None,
            rows_aggregated: 0,
            last_refresh_ms: 0,
        }
    }
}

impl WatermarkState {
    /// Record O3 data below the watermark.
    pub fn record_o3(&mut self, ts: i64) {
        if ts <= self.watermark_ts {
            match self.o3_watermark_ts {
                Some(current) if ts < current => self.o3_watermark_ts = Some(ts),
                None => self.o3_watermark_ts = Some(ts),
                _ => {}
            }
        }
    }

    /// Advance the watermark after successful refresh.
    pub fn advance(&mut self, max_ts: i64, rows: u64, now_ms: i64) {
        if max_ts > self.watermark_ts {
            self.watermark_ts = max_ts;
        }
        self.rows_aggregated += rows;
        self.last_refresh_ms = now_ms;
    }

    /// Clear the O3 watermark after re-aggregation completes.
    pub fn clear_o3(&mut self) {
        self.o3_watermark_ts = None;
    }
}
