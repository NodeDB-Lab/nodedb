//! Tree traversal for [`super::HilbertPackedRTree`].
//!
//! Lives next to [`super::build`] so the build step can stay focused on
//! arena layout and chunking, and traversal can evolve (e.g. parallel
//! descent) independently. The tree owns the actual descent logic — this
//! file is the home for future range/intersect/nn variants.

// Currently empty by design; see `HilbertPackedRTree::query` /
// `HilbertPackedRTree::descend`. Adding sibling traversals (kNN,
// nearest-tile, etc.) at Tier 4+ goes here.
