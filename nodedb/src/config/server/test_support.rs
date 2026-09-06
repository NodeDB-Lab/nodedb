// SPDX-License-Identifier: BUSL-1.1

//! Shared env-var test helper for `config/server` unit tests.
//!
//! Env vars are process-global, so a test that sets one must always remove
//! it, even when the test body panics.

/// Sets an env var for the duration of `f`, then always removes it.
pub(crate) fn with_var<R>(name: &str, value: &str, f: impl FnOnce() -> R) -> R {
    unsafe { std::env::set_var(name, value) };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    unsafe { std::env::remove_var(name) };
    match result {
        Ok(r) => r,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}
