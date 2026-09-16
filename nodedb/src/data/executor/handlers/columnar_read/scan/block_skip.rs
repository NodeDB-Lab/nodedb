// SPDX-License-Identifier: BUSL-1.1

//! Block-boundary surrogate prefilter for the live memtable.

use nodedb_types::Surrogate;
use nodedb_types::surrogate_bitmap::SurrogateBitmap;

/// Whether the whole live memtable can be skipped before any row decoding.
///
/// True when the prefilter is empty, or when none of the memtable's recorded
/// surrogates fall within the bitmap's `[min, max]` range. A memtable with no
/// recorded surrogate at all cannot be block-skipped: the row-boundary check
/// decides each of its rows.
pub(super) fn memtable_block_skipped(
    bitmap: &SurrogateBitmap,
    surrogates: &[Option<Surrogate>],
) -> bool {
    if bitmap.is_empty() {
        return true;
    }
    let (mt_min, mt_max) = surrogates
        .iter()
        .flatten()
        .fold((u32::MAX, u32::MIN), |(lo, hi), s| {
            (lo.min(s.0), hi.max(s.0))
        });
    if mt_min > mt_max {
        return false;
    }
    // The bitmap is non-empty, so both bounds are `Some`; the `None` arm is
    // the type-level fallback and never skips a block.
    match (bitmap.0.min(), bitmap.0.max()) {
        (Some(bm_min), Some(bm_max)) => bm_max < mt_min || bm_min > mt_max,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap(ids: &[u32]) -> SurrogateBitmap {
        let mut b = SurrogateBitmap::new();
        for id in ids {
            b.insert(Surrogate(*id));
        }
        b
    }

    #[test]
    fn an_empty_prefilter_skips_the_block() {
        assert!(memtable_block_skipped(&bitmap(&[]), &[Some(Surrogate(1))]));
    }

    #[test]
    fn a_disjoint_range_skips_the_block() {
        let mt = [Some(Surrogate(10)), None, Some(Surrogate(20))];
        assert!(memtable_block_skipped(&bitmap(&[1, 5]), &mt));
        assert!(memtable_block_skipped(&bitmap(&[21, 30]), &mt));
        assert!(!memtable_block_skipped(&bitmap(&[15]), &mt));
    }

    #[test]
    fn a_memtable_without_surrogates_is_never_block_skipped() {
        assert!(!memtable_block_skipped(&bitmap(&[1]), &[None, None]));
        assert!(!memtable_block_skipped(&bitmap(&[1]), &[]));
    }
}
