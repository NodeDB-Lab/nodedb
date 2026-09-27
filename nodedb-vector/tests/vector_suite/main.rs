// SPDX-License-Identifier: Apache-2.0

//! Grouped test target for the vector-engine integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Held as loose files under `tests/`, these cases cost a separate
//! compile and link step each — nine of them, all pulling in the same
//! `nodedb-vector` closure — for one crate's tests. Grouped, they link once.
//!
//! Every case keeps its own module, so a test's path stays
//! `cases::<file>::<test>` and names never collide across files.
//!
//! Native-only: these cases drive `VectorCollection`, `mmap_segment` and their
//! `libc::madvise` path, none of which exists on wasm32 — `src/lib.rs` gates all
//! three off there. They are skipped on that target rather than rewritten,
//! because there is no wasm equivalent to assert against.

#![cfg(not(target_arch = "wasm32"))]

mod cases;
mod support;
