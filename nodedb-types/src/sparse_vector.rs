//! Sparse vector type for learned sparse retrieval (SPLADE, SPLADE++).
//!
//! Internal representation: sorted `Vec<(u32, f32)>` — (dimension_index, weight).
//! Only nonzero entries stored — storage proportional to nnz, not total dimensions.

use serde::{Deserialize, Serialize};

/// A sparse vector: a set of (dimension_index, weight) pairs.
///
/// Entries are sorted by dimension index for efficient intersection during
/// dot-product scoring. Dimension indices are non-negative, weights are finite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SparseVector {
    /// Sorted by dimension index (ascending). No duplicate dimensions.
    entries: Vec<(u32, f32)>,
}

impl SparseVector {
    /// Create from unsorted entries. Sorts and deduplicates by dimension.
    /// Last-writer-wins for duplicate dimensions. Validates all weights are finite.
    pub fn from_entries(mut entries: Vec<(u32, f32)>) -> Result<Self, SparseVectorError> {
        for &(_, w) in &entries {
            if !w.is_finite() {
                return Err(SparseVectorError::NonFiniteWeight(w));
            }
        }
        // Sort by dimension, deduplicate (last wins).
        entries.sort_by_key(|&(dim, _)| dim);
        entries.dedup_by_key(|e| e.0);
        // Remove zero-weight entries (they contribute nothing to scoring).
        entries.retain(|&(_, w)| w != 0.0);
        Ok(Self { entries })
    }

    /// Create an empty sparse vector.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Parse from literal syntax: `'{103: 0.85, 2941: 0.42, 15003: 0.91}'`.
    pub fn parse_literal(s: &str) -> Result<Self, SparseVectorError> {
        let trimmed = s.trim().trim_matches('\'').trim_matches('"');
        let inner = trimmed
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or(SparseVectorError::InvalidLiteral(
                "expected '{dim: weight, ...}'".into(),
            ))?;

        if inner.trim().is_empty() {
            return Ok(Self::empty());
        }

        let mut entries = Vec::new();
        for pair in inner.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let (dim_str, weight_str) = pair.split_once(':').ok_or_else(|| {
                SparseVectorError::InvalidLiteral(format!("expected 'dim: weight', got '{pair}'"))
            })?;
            let dim: u32 = dim_str.trim().parse().map_err(|_| {
                SparseVectorError::InvalidLiteral(format!("invalid dimension '{}'", dim_str.trim()))
            })?;
            let weight: f32 = weight_str.trim().parse().map_err(|_| {
                SparseVectorError::InvalidLiteral(format!("invalid weight '{}'", weight_str.trim()))
            })?;
            entries.push((dim, weight));
        }

        Self::from_entries(entries)
    }

    /// Access the sorted entries.
    pub fn entries(&self) -> &[(u32, f32)] {
        &self.entries
    }

    /// Number of nonzero entries.
    pub fn nnz(&self) -> usize {
        self.entries.len()
    }

    /// Whether the vector has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Dot product with another sparse vector.
    ///
    /// `score = Σ self[d] * other[d]` for dimensions present in both vectors.
    /// Runs in O(nnz_self + nnz_other) via sorted merge.
    pub fn dot_product(&self, other: &SparseVector) -> f32 {
        let mut score = 0.0f32;
        let (mut i, mut j) = (0, 0);
        let (a, b) = (&self.entries, &other.entries);

        while i < a.len() && j < b.len() {
            match a[i].0.cmp(&b[j].0) {
                std::cmp::Ordering::Equal => {
                    score += a[i].1 * b[j].1;
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }

        score
    }
}

/// Errors from sparse vector construction or parsing.
#[derive(Debug, Clone)]
pub enum SparseVectorError {
    /// A weight value is NaN or infinite.
    NonFiniteWeight(f32),
    /// Literal syntax is malformed.
    InvalidLiteral(String),
}

impl std::fmt::Display for SparseVectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonFiniteWeight(w) => write!(f, "sparse vector weight must be finite, got {w}"),
            Self::InvalidLiteral(msg) => write!(f, "invalid sparse vector literal: {msg}"),
        }
    }
}

impl std::error::Error for SparseVectorError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_entries_sorts_and_deduplicates() {
        let sv = SparseVector::from_entries(vec![(5, 0.5), (2, 0.3), (5, 0.9)]).unwrap();
        // Sorted, deduped (last-wins for dim 5 after sort → first 0.5, then dedup keeps first).
        assert_eq!(sv.nnz(), 2);
        assert_eq!(sv.entries()[0].0, 2);
        assert_eq!(sv.entries()[1].0, 5);
    }

    #[test]
    fn zero_weights_removed() {
        let sv = SparseVector::from_entries(vec![(1, 0.5), (2, 0.0), (3, 0.3)]).unwrap();
        assert_eq!(sv.nnz(), 2);
        assert!(sv.entries().iter().all(|&(_, w)| w != 0.0));
    }

    #[test]
    fn non_finite_rejected() {
        assert!(SparseVector::from_entries(vec![(1, f32::NAN)]).is_err());
        assert!(SparseVector::from_entries(vec![(1, f32::INFINITY)]).is_err());
    }

    #[test]
    fn parse_literal() {
        let sv = SparseVector::parse_literal("'{103: 0.85, 2941: 0.42, 15003: 0.91}'").unwrap();
        assert_eq!(sv.nnz(), 3);
        assert_eq!(sv.entries()[0], (103, 0.85));
        assert_eq!(sv.entries()[1], (2941, 0.42));
        assert_eq!(sv.entries()[2], (15003, 0.91));
    }

    #[test]
    fn parse_empty_literal() {
        let sv = SparseVector::parse_literal("'{}'").unwrap();
        assert!(sv.is_empty());
    }

    #[test]
    fn dot_product_basic() {
        let a = SparseVector::from_entries(vec![(1, 2.0), (3, 4.0), (5, 6.0)]).unwrap();
        let b = SparseVector::from_entries(vec![(1, 0.5), (5, 0.5), (7, 1.0)]).unwrap();
        // Shared dims: 1 (2.0*0.5=1.0) and 5 (6.0*0.5=3.0) = 4.0
        let score = a.dot_product(&b);
        assert!((score - 4.0).abs() < 1e-6);
    }

    #[test]
    fn dot_product_no_overlap() {
        let a = SparseVector::from_entries(vec![(1, 1.0)]).unwrap();
        let b = SparseVector::from_entries(vec![(2, 1.0)]).unwrap();
        assert_eq!(a.dot_product(&b), 0.0);
    }

    #[test]
    fn serde_roundtrip() {
        let sv = SparseVector::from_entries(vec![(10, 0.5), (20, 0.8)]).unwrap();
        let bytes = rmp_serde::to_vec_named(&sv).unwrap();
        let restored: SparseVector = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(sv, restored);
    }
}
