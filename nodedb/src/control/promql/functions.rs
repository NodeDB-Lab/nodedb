//! PromQL built-in functions.
//!
//! All functions operate on sorted sample vectors and return a scalar or
//! modified sample vector. Pure computation — no I/O.

use super::types::Sample;

/// `rate(v range-vector)` — per-second average rate of increase, counter-aware.
///
/// Handles counter resets by assuming monotonic increase with wraps.
pub fn rate(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let first = samples.first()?;
    let last = samples.last()?;
    let dt_secs = (last.timestamp_ms - first.timestamp_ms) as f64 / 1000.0;
    if dt_secs <= 0.0 {
        return None;
    }
    // Sum increases, handling counter resets.
    let mut total_increase = 0.0;
    for i in 1..samples.len() {
        let delta = samples[i].value - samples[i - 1].value;
        if delta >= 0.0 {
            total_increase += delta;
        } else {
            // Counter reset — current value is the increase since reset.
            total_increase += samples[i].value;
        }
    }
    Some(total_increase / dt_secs)
}

/// `irate(v range-vector)` — instant rate using only the last two samples.
pub fn irate(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let prev = &samples[samples.len() - 2];
    let last = &samples[samples.len() - 1];
    let dt_secs = (last.timestamp_ms - prev.timestamp_ms) as f64 / 1000.0;
    if dt_secs <= 0.0 {
        return None;
    }
    let delta = if last.value >= prev.value {
        last.value - prev.value
    } else {
        last.value // counter reset
    };
    Some(delta / dt_secs)
}

/// `increase(v range-vector)` — total increase over the range, counter-aware.
pub fn increase(samples: &[Sample]) -> Option<f64> {
    let r = rate(samples)?;
    let first = samples.first()?;
    let last = samples.last()?;
    let dt_secs = (last.timestamp_ms - first.timestamp_ms) as f64 / 1000.0;
    Some(r * dt_secs)
}

/// `delta(v range-vector)` — difference between first and last sample (gauge).
pub fn delta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    Some(samples.last()?.value - samples.first()?.value)
}

/// `idelta(v range-vector)` — instant delta using last two samples.
pub fn idelta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let prev = &samples[samples.len() - 2];
    let last = &samples[samples.len() - 1];
    Some(last.value - prev.value)
}

/// `deriv(v range-vector)` — per-second derivative via simple linear regression.
pub fn deriv(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let n = samples.len() as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    let t0 = samples[0].timestamp_ms as f64 / 1000.0;

    for s in samples {
        let x = s.timestamp_ms as f64 / 1000.0 - t0;
        let y = s.value;
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_x2 += x * x;
    }
    let denom = n * sum_x2 - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return None;
    }
    Some((n * sum_xy - sum_x * sum_y) / denom)
}

/// `predict_linear(v range-vector, t scalar)` — extrapolate value at t seconds from now.
pub fn predict_linear(samples: &[Sample], t_secs: f64) -> Option<f64> {
    let slope = deriv(samples)?;
    let last = samples.last()?;
    Some(last.value + slope * t_secs)
}

/// `avg_over_time(v range-vector)` — average value over the range.
pub fn avg_over_time(samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let sum: f64 = samples.iter().map(|s| s.value).sum();
    Some(sum / samples.len() as f64)
}

/// `sum_over_time(v range-vector)` — sum of values over the range.
pub fn sum_over_time(samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    Some(samples.iter().map(|s| s.value).sum())
}

/// `min_over_time(v range-vector)` — minimum value over the range.
pub fn min_over_time(samples: &[Sample]) -> Option<f64> {
    samples.iter().map(|s| s.value).reduce(f64::min)
}

/// `max_over_time(v range-vector)` — maximum value over the range.
pub fn max_over_time(samples: &[Sample]) -> Option<f64> {
    samples.iter().map(|s| s.value).reduce(f64::max)
}

/// `count_over_time(v range-vector)` — number of samples in the range.
pub fn count_over_time(samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    Some(samples.len() as f64)
}

/// `quantile_over_time(scalar, v range-vector)` — φ-quantile over the range.
pub fn quantile_over_time(q: f64, samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let q = q.clamp(0.0, 1.0);
    let mut vals: Vec<f64> = samples.iter().map(|s| s.value).collect();
    vals.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = vals.len();
    if n == 1 {
        return Some(vals[0]);
    }
    let rank = q * (n - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(n - 1);
    let frac = rank - lo as f64;
    Some(vals[lo] * (1.0 - frac) + vals[hi] * frac)
}

// ── Tier 2 range-vector functions ─────────────────────────────────────

/// `changes(v range-vector)` — number of times the value changed.
pub fn changes(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return Some(0.0);
    }
    let count = samples
        .windows(2)
        .filter(|w| (w[1].value - w[0].value).abs() > f64::EPSILON)
        .count();
    Some(count as f64)
}

/// `resets(v range-vector)` — number of counter resets (value decreases).
pub fn resets(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return Some(0.0);
    }
    let count = samples
        .windows(2)
        .filter(|w| w[1].value < w[0].value)
        .count();
    Some(count as f64)
}

/// `absent_over_time(v range-vector)` — returns 1 if no samples, empty otherwise.
pub fn absent_over_time(samples: &[Sample]) -> Option<f64> {
    if samples.is_empty() { Some(1.0) } else { None }
}

/// `stddev_over_time(v range-vector)` — population standard deviation.
pub fn stddev_over_time(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mean = samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64;
    let var = samples
        .iter()
        .map(|s| (s.value - mean).powi(2))
        .sum::<f64>()
        / samples.len() as f64;
    Some(var.sqrt())
}

/// `stdvar_over_time(v range-vector)` — population variance.
pub fn stdvar_over_time(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let mean = samples.iter().map(|s| s.value).sum::<f64>() / samples.len() as f64;
    Some(
        samples
            .iter()
            .map(|s| (s.value - mean).powi(2))
            .sum::<f64>()
            / samples.len() as f64,
    )
}

/// `holt_winters(v range-vector, sf scalar, tf scalar)` — double exponential smoothing.
///
/// `sf` is the smoothing factor (0 < sf < 1), `tf` is the trend factor (0 < tf < 1).
pub fn holt_winters(samples: &[Sample], sf: f64, tf: f64) -> Option<f64> {
    if samples.is_empty() || sf <= 0.0 || sf >= 1.0 || tf <= 0.0 || tf >= 1.0 {
        return None;
    }
    // Initialize level and trend from first two samples.
    let mut level = samples[0].value;
    let mut trend = if samples.len() > 1 {
        samples[1].value - samples[0].value
    } else {
        0.0
    };

    for s in samples.iter().skip(1) {
        let prev_level = level;
        level = sf * s.value + (1.0 - sf) * (prev_level + trend);
        trend = tf * (level - prev_level) + (1.0 - tf) * trend;
    }
    Some(level)
}

/// Dispatch a range-vector function by name.
pub fn call_range_func(name: &str, samples: &[Sample], scalar_arg: Option<f64>) -> Option<f64> {
    match name {
        "rate" => rate(samples),
        "irate" => irate(samples),
        "increase" => increase(samples),
        "delta" => delta(samples),
        "idelta" => idelta(samples),
        "deriv" => deriv(samples),
        "predict_linear" => predict_linear(samples, scalar_arg.unwrap_or(0.0)),
        "avg_over_time" => avg_over_time(samples),
        "sum_over_time" => sum_over_time(samples),
        "min_over_time" => min_over_time(samples),
        "max_over_time" => max_over_time(samples),
        "count_over_time" => count_over_time(samples),
        "quantile_over_time" => quantile_over_time(scalar_arg.unwrap_or(0.5), samples),
        "changes" => changes(samples),
        "resets" => resets(samples),
        "absent_over_time" => absent_over_time(samples),
        "stddev_over_time" => stddev_over_time(samples),
        "stdvar_over_time" => stdvar_over_time(samples),
        _ => None,
    }
}

/// Dispatch `holt_winters` separately (needs two scalar args).
pub fn call_holt_winters(samples: &[Sample], sf: Option<f64>, tf: Option<f64>) -> Option<f64> {
    holt_winters(samples, sf.unwrap_or(0.5), tf.unwrap_or(0.5))
}

/// Check if a function name is a known range-vector function.
pub fn is_range_func(name: &str) -> bool {
    matches!(
        name,
        "rate"
            | "irate"
            | "increase"
            | "delta"
            | "idelta"
            | "deriv"
            | "predict_linear"
            | "avg_over_time"
            | "sum_over_time"
            | "min_over_time"
            | "max_over_time"
            | "count_over_time"
            | "quantile_over_time"
            | "changes"
            | "resets"
            | "absent_over_time"
            | "stddev_over_time"
            | "stdvar_over_time"
            | "holt_winters"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(pairs: &[(i64, f64)]) -> Vec<Sample> {
        pairs
            .iter()
            .map(|&(t, v)| Sample {
                timestamp_ms: t,
                value: v,
            })
            .collect()
    }

    #[test]
    fn test_rate_monotonic() {
        // 0→10→20→30 over 3 seconds = 10/s
        let s = samples(&[(0, 0.0), (1000, 10.0), (2000, 20.0), (3000, 30.0)]);
        assert!((rate(&s).unwrap() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn test_rate_counter_reset() {
        // 100 → 5 (reset) → 15: increase = 5 + 10 = 15 over 2s = 7.5/s
        let s = samples(&[(0, 100.0), (1000, 5.0), (2000, 15.0)]);
        assert!((rate(&s).unwrap() - 7.5).abs() < 1e-9);
    }

    #[test]
    fn test_irate() {
        let s = samples(&[(0, 0.0), (1000, 10.0), (2000, 50.0)]);
        // Only uses last two: (50-10)/1s = 40
        assert!((irate(&s).unwrap() - 40.0).abs() < 1e-9);
    }

    #[test]
    fn test_increase() {
        let s = samples(&[(0, 0.0), (1000, 10.0), (2000, 20.0)]);
        // rate=10/s, dt=2s, increase=20
        assert!((increase(&s).unwrap() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn test_delta() {
        let s = samples(&[(0, 10.0), (1000, 7.0), (2000, 15.0)]);
        assert!((delta(&s).unwrap() - 5.0).abs() < 1e-9);
    }

    #[test]
    fn test_idelta() {
        let s = samples(&[(0, 10.0), (1000, 7.0), (2000, 15.0)]);
        assert!((idelta(&s).unwrap() - 8.0).abs() < 1e-9);
    }

    #[test]
    fn test_deriv_linear() {
        // Perfect linear: y = 2x (x in seconds)
        let s = samples(&[(0, 0.0), (1000, 2.0), (2000, 4.0), (3000, 6.0)]);
        assert!((deriv(&s).unwrap() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn test_predict_linear() {
        let s = samples(&[(0, 0.0), (1000, 2.0), (2000, 4.0)]);
        // slope=2/s, last=4, predict at +5s: 4+2*5=14
        assert!((predict_linear(&s, 5.0).unwrap() - 14.0).abs() < 1e-9);
    }

    #[test]
    fn test_avg_over_time() {
        let s = samples(&[(0, 2.0), (1000, 4.0), (2000, 6.0)]);
        assert!((avg_over_time(&s).unwrap() - 4.0).abs() < 1e-9);
    }

    #[test]
    fn test_sum_over_time() {
        let s = samples(&[(0, 1.0), (1000, 2.0), (2000, 3.0)]);
        assert!((sum_over_time(&s).unwrap() - 6.0).abs() < 1e-9);
    }

    #[test]
    fn test_min_max_over_time() {
        let s = samples(&[(0, 5.0), (1000, 2.0), (2000, 8.0)]);
        assert!((min_over_time(&s).unwrap() - 2.0).abs() < 1e-9);
        assert!((max_over_time(&s).unwrap() - 8.0).abs() < 1e-9);
    }

    #[test]
    fn test_count_over_time() {
        let s = samples(&[(0, 1.0), (1000, 2.0), (2000, 3.0)]);
        assert!((count_over_time(&s).unwrap() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_quantile_over_time() {
        let s = samples(&[(0, 1.0), (1000, 2.0), (2000, 3.0), (3000, 4.0), (4000, 5.0)]);
        assert!((quantile_over_time(0.5, &s).unwrap() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_insufficient_samples() {
        let one = samples(&[(0, 1.0)]);
        assert!(rate(&one).is_none());
        assert!(irate(&one).is_none());
        assert!(delta(&one).is_none());
        assert!(deriv(&one).is_none());
    }
}
