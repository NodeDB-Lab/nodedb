//! Codec auto-detection from column type and data distribution.
//!
//! Analyzes up to the first 1024 values of a column to select the optimal
//! codec chain. Called at flush time when `ColumnCodec::Auto` is configured.
//!
//! Selection strategy:
//! - Partitions ≥ 1024 values → cascading codecs (ALP, FastLanes, etc.)
//! - Partitions < 1024 values → single-step codecs (Gorilla, Delta, etc.)
//! - f64 with >95% ALP encodability → `AlpFastLanesLz4`
//! - f64 with ≤95% ALP encodability → `Gorilla` (fallback)
//! - i64 timestamps/counters → `DeltaFastLanesLz4`
//! - Symbol columns → `FastLanesLz4` (small integer IDs)

use crate::{ColumnCodec, ColumnTypeHint};

use crate::CODEC_SAMPLE_SIZE;

/// Minimum partition size to use cascading codecs.
/// Below this, FastLanes block overhead dominates — use legacy codecs.
const CASCADE_THRESHOLD: usize = 128;

/// Detect the optimal codec for a column based on its type and data.
///
/// When `codec` is not `Auto`, returns it unchanged. When `Auto`,
/// analyzes the column type hint to select the best codec. For
/// data-aware selection (ALP encodability, monotonicity detection),
/// use `detect_f64_codec()` or `detect_i64_codec()` with actual values.
pub fn detect_codec(codec: ColumnCodec, type_hint: ColumnTypeHint) -> ColumnCodec {
    if codec != ColumnCodec::Auto {
        return codec;
    }

    // Default selections (data-unaware). The segment writer calls
    // data-aware variants (detect_f64_codec, detect_i64_codec) when
    // it has actual values.
    match type_hint {
        ColumnTypeHint::Timestamp => ColumnCodec::DeltaFastLanesLz4,
        ColumnTypeHint::Float64 => ColumnCodec::AlpFastLanesLz4,
        ColumnTypeHint::Int64 => ColumnCodec::DeltaFastLanesLz4,
        ColumnTypeHint::Symbol => ColumnCodec::FastLanesLz4,
        ColumnTypeHint::String => ColumnCodec::FsstLz4,
    }
}

/// Detect the optimal codec for an i64 column by analyzing the data.
///
/// For partitions ≥ CASCADE_THRESHOLD values, selects cascading codecs.
/// For smaller partitions, falls back to legacy single-step codecs.
pub fn detect_i64_codec(values: &[i64]) -> ColumnCodec {
    if values.len() < 2 {
        return ColumnCodec::Delta;
    }

    // Large partitions → cascading codec (FastLanes handles all patterns).
    if values.len() >= CASCADE_THRESHOLD {
        return ColumnCodec::DeltaFastLanesLz4;
    }

    // Small partitions → legacy codecs. Analyze data to pick best one.
    let sample_end = values.len().min(CODEC_SAMPLE_SIZE);
    let sample = &values[..sample_end];

    let mut zero_dod_count = 0usize;
    let mut prev_delta: Option<i64> = None;

    for i in 1..sample.len() {
        let delta = sample[i] - sample[i - 1];
        if let Some(pd) = prev_delta
            && delta == pd
        {
            zero_dod_count += 1;
        }
        prev_delta = Some(delta);
    }

    let total_deltas = sample.len() - 1;
    let constant_rate_ratio = zero_dod_count as f64 / total_deltas.max(1) as f64;

    if constant_rate_ratio > 0.8 {
        ColumnCodec::DoubleDelta
    } else {
        ColumnCodec::Delta
    }
}

/// Detect the optimal codec for an f64 column by analyzing the data.
///
/// For partitions ≥ CASCADE_THRESHOLD values with >95% ALP encodability,
/// selects `AlpFastLanesLz4`. Otherwise falls back to `Gorilla`.
pub fn detect_f64_codec(values: &[f64]) -> ColumnCodec {
    if values.len() < 2 {
        return ColumnCodec::Gorilla;
    }

    let use_cascade = values.len() >= CASCADE_THRESHOLD;

    if use_cascade {
        // Check ALP encodability on a sample.
        let encodability = crate::alp::alp_encodability(values);
        if encodability > 0.95 {
            return ColumnCodec::AlpFastLanesLz4;
        }
    }

    // Fallback: Gorilla is the best general-purpose f64 codec.
    ColumnCodec::Gorilla
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_codec_passthrough() {
        assert_eq!(
            detect_codec(ColumnCodec::Lz4, ColumnTypeHint::Timestamp),
            ColumnCodec::Lz4
        );
        assert_eq!(
            detect_codec(ColumnCodec::Zstd, ColumnTypeHint::Float64),
            ColumnCodec::Zstd
        );
    }

    #[test]
    fn auto_timestamp() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Timestamp),
            ColumnCodec::DeltaFastLanesLz4
        );
    }

    #[test]
    fn auto_float64() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Float64),
            ColumnCodec::AlpFastLanesLz4
        );
    }

    #[test]
    fn auto_int64() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Int64),
            ColumnCodec::DeltaFastLanesLz4
        );
    }

    #[test]
    fn auto_symbol() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::Symbol),
            ColumnCodec::FastLanesLz4
        );
    }

    #[test]
    fn auto_string() {
        assert_eq!(
            detect_codec(ColumnCodec::Auto, ColumnTypeHint::String),
            ColumnCodec::FsstLz4
        );
    }

    #[test]
    fn detect_large_i64_uses_cascade() {
        // ≥128 values → cascading DeltaFastLanesLz4 regardless of pattern.
        let values: Vec<i64> = (0..1000).map(|i| i * 100).collect();
        assert_eq!(detect_i64_codec(&values), ColumnCodec::DeltaFastLanesLz4);

        let timestamps: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 10_000).collect();
        assert_eq!(
            detect_i64_codec(&timestamps),
            ColumnCodec::DeltaFastLanesLz4
        );
    }

    #[test]
    fn detect_small_i64_uses_legacy() {
        // <128 values → legacy codecs.
        let constant_rate: Vec<i64> = (0..50).map(|i| i * 100).collect();
        assert_eq!(detect_i64_codec(&constant_rate), ColumnCodec::DoubleDelta);

        let varying: Vec<i64> = vec![1, 3, 7, 15, 22, 30];
        assert_eq!(detect_i64_codec(&varying), ColumnCodec::Delta);
    }

    #[test]
    fn detect_large_f64_decimal_uses_alp() {
        // Decimal-origin f64 with ≥128 values → AlpFastLanesLz4.
        let values: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
        assert_eq!(detect_f64_codec(&values), ColumnCodec::AlpFastLanesLz4);
    }

    #[test]
    fn detect_large_f64_irrational_uses_gorilla() {
        // Non-ALP-encodable f64 → Gorilla fallback.
        let values: Vec<f64> = (1..1000).map(|i| std::f64::consts::PI * i as f64).collect();
        assert_eq!(detect_f64_codec(&values), ColumnCodec::Gorilla);
    }

    #[test]
    fn detect_small_f64_uses_gorilla() {
        let values: Vec<f64> = (0..50).map(|i| i as f64 * 0.1).collect();
        assert_eq!(detect_f64_codec(&values), ColumnCodec::Gorilla);
    }

    #[test]
    fn small_sample() {
        assert_eq!(detect_i64_codec(&[]), ColumnCodec::Delta);
        assert_eq!(detect_i64_codec(&[42]), ColumnCodec::Delta);
        assert_eq!(detect_f64_codec(&[]), ColumnCodec::Gorilla);
    }
}
