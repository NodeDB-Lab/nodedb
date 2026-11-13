// Empty placeholder; integration tests live under `tests/`. Cargo
// requires every package to have at least one target.

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
