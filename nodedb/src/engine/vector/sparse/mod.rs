// SPDX-License-Identifier: BUSL-1.1

pub mod index;
pub mod search;

pub use index::{SparseDocImage, SparseInvertedIndex};
pub use search::SparseSearchResult;
