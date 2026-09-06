// SPDX-License-Identifier: BUSL-1.1

//! Env-var scoping and the shared rejection assertion for the startup
//! override tests.
//!
//! Every `NODEDB_*` override is an operator-supplied startup value. A value
//! that fails to parse names sizing, a listener, or a durability setting the
//! process cannot provide. The process refuses to start and names what it
//! refused. A fallback runs the node on a configuration nobody chose.

#![allow(dead_code)] // Not every test binary needs every helper here.

/// Sets env vars for the duration of one test and removes them on drop.
///
/// Env vars are process-global. Each test gets its own process under
/// `cargo nextest run`, which the workspace mandates, so one guard per test
/// isolates enough. The drop still runs, so a panicking test leaves nothing
/// behind for a same-process runner.
pub struct EnvGuard {
    keys: Vec<String>,
}

impl EnvGuard {
    /// Sets one variable for the lifetime of the guard.
    pub fn set(var: &str, value: &str) -> Self {
        unsafe { std::env::set_var(var, value) };
        Self {
            keys: vec![var.to_string()],
        }
    }

    /// Sets several variables for the lifetime of the guard.
    pub fn set_all(pairs: &[(&str, &str)]) -> Self {
        let mut keys = Vec::with_capacity(pairs.len());
        for (var, value) in pairs {
            unsafe { std::env::set_var(var, value) };
            keys.push((*var).to_string());
        }
        Self { keys }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for key in &self.keys {
            unsafe { std::env::remove_var(key) };
        }
    }
}

/// Asserts that an override pass refused `var=value` and said so.
///
/// The two `contains` checks guard against the silent fallback this covers.
/// An error naming neither the variable nor the value reads like a
/// warn-and-continue log. The operator gets no way to find the typo.
pub fn assert_rejected(result: nodedb::Result<()>, var: &str, value: &str) {
    let err = match result {
        Ok(()) => panic!(
            "{var}={value} was accepted; a malformed startup value must fail startup, \
             not fall back to the config value or the compiled default"
        ),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains(var), "error must name the variable: {msg}");
    assert!(
        msg.contains(value),
        "error must name the rejected value: {msg}"
    );
}
