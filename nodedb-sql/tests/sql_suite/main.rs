// SPDX-License-Identifier: Apache-2.0

//! Grouped test target for the nodedb-sql integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Held as loose files under `tests/`, these cases cost a separate
//! compile and link step each — five of them, all pulling in the same
//! `nodedb-sql` closure — for one crate's tests. Grouped, they link once.
//!
//! Every case keeps its own module, so a test's path stays
//! `cases::<file>::<test>` and names never collide across files.

mod cases;
