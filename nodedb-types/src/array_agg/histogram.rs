// SPDX-License-Identifier: Apache-2.0

//! Fixed-width histogram bins.
//!
//! `HISTOGRAM(bins, lo, hi)` splits `[lo, hi]` into `bins` bins of width
//! `w = (hi - lo) / bins`. Bin `i` holds the values in
//! `[lo + i*w, lo + (i+1)*w)`, and the last bin also holds `hi`. A value
//! below `lo`, above `hi`, or NaN falls in no bin and is not counted.
//! Bin counts merge by addition.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The most bins one histogram holds.
pub const MAX_HISTOGRAM_BINS: u32 = 65_536;

/// The bins of a histogram.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct HistogramSpec {
    pub bins: u32,
    pub lo: f64,
    pub hi: f64,
}

/// Why a histogram spec is refused.
#[derive(Debug, Clone, PartialEq)]
pub enum HistogramSpecError {
    /// `bins` is 0 or above [`MAX_HISTOGRAM_BINS`].
    Bins(u32),
    /// `lo` or `hi` is not finite, or `lo >= hi`.
    Range { lo: f64, hi: f64 },
}

impl fmt::Display for HistogramSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HistogramSpecError::Bins(bins) => write!(
                f,
                "HISTOGRAM bins {bins} must be between 1 and {MAX_HISTOGRAM_BINS}"
            ),
            HistogramSpecError::Range { lo, hi } => write!(
                f,
                "HISTOGRAM range [{lo}, {hi}] must be finite with lo < hi"
            ),
        }
    }
}

/// One bin of a finalized histogram.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HistogramBin {
    pub lo: f64,
    pub hi: f64,
    pub count: u64,
}

impl HistogramSpec {
    /// A checked spec.
    pub fn new(bins: u32, lo: f64, hi: f64) -> Result<Self, HistogramSpecError> {
        if bins == 0 || bins > MAX_HISTOGRAM_BINS {
            return Err(HistogramSpecError::Bins(bins));
        }
        if !lo.is_finite() || !hi.is_finite() || lo >= hi {
            return Err(HistogramSpecError::Range { lo, hi });
        }
        Ok(Self { bins, lo, hi })
    }

    /// The bin `v` falls in, or `None` outside `[lo, hi]` or for NaN.
    pub fn bin_of(&self, v: f64) -> Option<usize> {
        if v.is_nan() || v < self.lo || v > self.hi {
            return None;
        }
        let last = self.bins as usize - 1;
        let raw = ((v - self.lo) / (self.hi - self.lo) * f64::from(self.bins)).floor();
        // `v == hi` lands on `bins`, and rounding can push a value just
        // below a boundary up one bin past the last. Both go to the last bin.
        Some((raw as usize).min(last))
    }

    /// The lower bound of bin `i`. Bin `bins` gives `hi`.
    pub fn edge(&self, i: usize) -> f64 {
        if i >= self.bins as usize {
            return self.hi;
        }
        self.lo + (self.hi - self.lo) * (i as f64) / f64::from(self.bins)
    }

    /// A zero count per bin.
    pub fn empty_counts(&self) -> Vec<u64> {
        vec![0; self.bins as usize]
    }

    /// `counts` as bins with their bounds. A missing count reads as 0.
    pub fn finish(&self, counts: &[u64]) -> Vec<HistogramBin> {
        (0..self.bins as usize)
            .map(|i| HistogramBin {
                lo: self.edge(i),
                hi: self.edge(i + 1),
                count: counts.get(i).copied().unwrap_or(0),
            })
            .collect()
    }

    /// Add `other` into `into`, bin by bin. A shorter vector grows to the
    /// spec's bin count.
    pub fn merge_counts(&self, into: &mut Vec<u64>, other: &[u64]) {
        let bins = self.bins as usize;
        if into.len() < bins {
            into.resize(bins, 0);
        }
        for (slot, n) in into.iter_mut().zip(other) {
            *slot = slot.saturating_add(*n);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_fall_in_half_open_bins_and_hi_in_the_last() {
        let s = HistogramSpec::new(4, 0.0, 1.0).expect("valid");
        assert_eq!(s.bin_of(0.0), Some(0));
        assert_eq!(s.bin_of(0.2499), Some(0));
        assert_eq!(s.bin_of(0.25), Some(1));
        assert_eq!(s.bin_of(0.99), Some(3));
        assert_eq!(s.bin_of(1.0), Some(3));
        assert_eq!(s.bin_of(-0.01), None);
        assert_eq!(s.bin_of(1.01), None);
        assert_eq!(s.bin_of(f64::NAN), None);
    }

    #[test]
    fn bins_carry_their_bounds() {
        let s = HistogramSpec::new(2, -1.0, 1.0).expect("valid");
        let bins = s.finish(&[3, 4]);
        assert_eq!(
            bins,
            vec![
                HistogramBin {
                    lo: -1.0,
                    hi: 0.0,
                    count: 3
                },
                HistogramBin {
                    lo: 0.0,
                    hi: 1.0,
                    count: 4
                },
            ]
        );
        assert_eq!(s.finish(&[]).iter().map(|b| b.count).sum::<u64>(), 0);
    }

    #[test]
    fn counts_merge_by_addition() {
        let s = HistogramSpec::new(3, 0.0, 3.0).expect("valid");
        let mut a = vec![1, 0, 2];
        s.merge_counts(&mut a, &[0, 5, 1]);
        assert_eq!(a, vec![1, 5, 3]);
        let mut empty = Vec::new();
        s.merge_counts(&mut empty, &[1, 1, 1]);
        assert_eq!(empty, vec![1, 1, 1]);
    }

    #[test]
    fn bad_specs_are_refused() {
        assert!(HistogramSpec::new(0, 0.0, 1.0).is_err());
        assert!(HistogramSpec::new(MAX_HISTOGRAM_BINS + 1, 0.0, 1.0).is_err());
        assert!(HistogramSpec::new(4, 1.0, 1.0).is_err());
        assert!(HistogramSpec::new(4, 0.0, f64::INFINITY).is_err());
    }
}
