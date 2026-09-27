// SPDX-License-Identifier: Apache-2.0

//! Grouped test target for the WAL integration tests.
//!
//! Cargo compiles this whole directory into ONE test binary rather than one
//! per file. Held as loose files under `tests/`, these cases cost a separate
//! compile and link step each — sixteen of them, all pulling in the same
//! `nodedb-wal` closure — for one crate's tests. Grouped, they link once.
//!
//! Every case keeps its own module, so a test's path stays
//! `cases::<file>::<test>` and names never collide across files. Cases that
//! only apply to some build configurations carry their `cfg` on the `mod`
//! declaration in `cases/mod.rs`.

mod cases;

/// Create a temporary directory that also works under `wasm32-wasip1`.
///
/// `tempfile::tempdir()` aborts there: `std::env::temp_dir()` is
/// `unimplemented!("not supported by WASI yet")` in std
/// (`library/std/src/sys/paths/wasi.rs`), so it fails before it ever reaches the
/// filesystem and no preopen or `TMPDIR` can help. The wasm runner preopens only
/// the working directory (`wasmtime --dir=.`), so on wasm the directory is
/// created under it; native keeps the exact `tempfile::tempdir()` behaviour.
pub(crate) fn tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_arch = "wasm32")]
    {
        tempfile::Builder::new().tempdir_in(".")
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        tempfile::tempdir()
    }
}
