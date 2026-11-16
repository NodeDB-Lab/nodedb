// SPDX-License-Identifier: Apache-2.0

//! Bollinger Bands for anomaly detection and volatility analysis.
//!
//! - `BOLLINGER_UPPER(period, k)` = SMA + k × σ
//! - `BOLLINGER_LOWER(period, k)` = SMA − k × σ
//! - `BOLLINGER_MID(period)` = SMA
//! - `BOLLINGER_WIDTH(period, k)` = (upper − lower) / mid

use super::stddev::TsStddevAccum;

/// Complete Bollinger Band computation for a sliding window.
///
/// Returns `(upper, middle, lower, width)` vectors.
/// - `period`: window size for SMA and stddev.
/// - `num_std`: number of standard deviations (typically 2.0).
/// - Positions with fewer than `period` samples return `None`.
pub fn ts_bollinger(values: &[f64], period: usize, num_std: f64) -> BollingerResult {
    let n = values.len();
    if n == 0 || period == 0 {
        return BollingerResult::empty(n);
    }

    let mut upper = Vec::with_capacity(n);
    let mut middle = Vec::with_capacity(n);
    let mut lower = Vec::with_capacity(n);
    let mut width = Vec::with_capacity(n);

    for i in 0..n {
        if i + 1 < period {
            upper.push(None);
            middle.push(None);
            lower.push(None);
            width.push(None);
            continue;
        }

        let start = i + 1 - period;
        let window = &values[start..=i];

        // Compute SMA.
        let valid: Vec<f64> = window.iter().copied().filter(|v| !v.is_nan()).collect();
        if valid.len() < 2 {
            upper.push(None);
            middle.push(None);
            lower.push(None);
            width.push(None);
            continue;
        }

        let mean = valid.iter().sum::<f64>() / valid.len() as f64;

        // Compute population stddev.
        let mut accum = TsStddevAccum::new();
        accum.update_batch(&valid);

        match accum.evaluate_population() {
            Some(stddev) => {
                let u = mean + num_std * stddev;
                let l = mean - num_std * stddev;
                let w = if mean.abs() > f64::EPSILON {
                    (u - l) / mean
                } else {
                    0.0
                };
                upper.push(Some(u));
                middle.push(Some(mean));
                lower.push(Some(l));
                width.push(Some(w));
            }
            None => {
                upper.push(None);
                middle.push(None);
                lower.push(None);
                width.push(None);
            }
        }
    }

    BollingerResult {
        upper,
        middle,
        lower,
        width,
    }
}

/// Individual Bollinger band accessors for SQL function registration.
pub fn ts_bollinger_upper(values: &[f64], period: usize, num_std: f64) -> Vec<Option<f64>> {
    ts_bollinger(values, period, num_std).upper
}

pub fn ts_bollinger_lower(values: &[f64], period: usize, num_std: f64) -> Vec<Option<f64>> {
    ts_bollinger(values, period, num_std).lower
}

pub fn ts_bollinger_mid(values: &[f64], period: usize) -> Vec<Option<f64>> {
    ts_bollinger(values, period, 0.0).middle
}

pub fn ts_bollinger_width(values: &[f64], period: usize, num_std: f64) -> Vec<Option<f64>> {
    ts_bollinger(values, period, num_std).width
}

/// Result of a Bollinger Band computation.
pub struct BollingerResult {
    pub upper: Vec<Option<f64>>,
    pub middle: Vec<Option<f64>>,
    pub lower: Vec<Option<f64>>,
    pub width: Vec<Option<f64>>,
}

impl BollingerResult {
    fn empty(n: usize) -> Self {
        Self {
            upper: vec![None; n],
            middle: vec![None; n],
            lower: vec![None; n],
            width: vec![None; n],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_bollinger() {
        // 5 values, period=5, k=2
        let vals = [2.0, 4.0, 4.0, 4.0, 5.0];
        let b = ts_bollinger(&vals, 5, 2.0);

        // First 4 should be None.
        for upper in b.upper.iter().take(4) {
            assert!(upper.is_none());
        }

        // At position 4: mean = 3.8, stddev ≈ 0.98
        let mid = b.middle[4].unwrap();
        let up = b.upper[4].unwrap();
        let lo = b.lower[4].unwrap();
        assert!((mid - 3.8).abs() < 1e-10);
        assert!(up > mid);
        assert!(lo < mid);
        assert!((up - mid - (mid - lo)).abs() < 1e-10); // symmetric
    }

    #[test]
    fn bollinger_width() {
        let vals = [10.0, 12.0, 11.0, 13.0, 14.0];
        let b = ts_bollinger(&vals, 5, 2.0);
        let w = b.width[4].unwrap();
        // Width = (upper - lower) / mid, should be positive.
        assert!(w > 0.0);
    }

    #[test]
    fn constant_values() {
        let vals = [5.0, 5.0, 5.0, 5.0, 5.0];
        let b = ts_bollinger(&vals, 3, 2.0);
        // stddev = 0, so upper == lower == mid
        let mid = b.middle[4].unwrap();
        let up = b.upper[4].unwrap();
        let lo = b.lower[4].unwrap();
        assert!((up - mid).abs() < 1e-12);
        assert!((lo - mid).abs() < 1e-12);
    }

    #[test]
    fn individual_accessors() {
        let vals = [1.0, 2.0, 3.0, 4.0, 5.0];
        let up = ts_bollinger_upper(&vals, 3, 2.0);
        let lo = ts_bollinger_lower(&vals, 3, 2.0);
        let mid = ts_bollinger_mid(&vals, 3);
        let w = ts_bollinger_width(&vals, 3, 2.0);
        assert_eq!(up.len(), 5);
        assert_eq!(lo.len(), 5);
        assert_eq!(mid.len(), 5);
        assert_eq!(w.len(), 5);
        assert!(up[2].unwrap() > mid[2].unwrap());
        assert!(lo[2].unwrap() < mid[2].unwrap());
    }

    #[test]
    fn empty_input() {
        let b = ts_bollinger(&[], 5, 2.0);
        assert!(b.upper.is_empty());
    }
}
