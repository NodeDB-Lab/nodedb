// SPDX-License-Identifier: BUSL-1.1

//! Shared value parsers for `NODEDB_*` env row `apply` functions.
//!
//! Each returns the expectation text as its `Err`. A row's violation message
//! then carries what the process needed, not a parser-specific string that
//! drifts from the row table.

use std::net::{IpAddr, SocketAddr};

pub(super) fn parse_ip(raw: &str) -> Result<IpAddr, &'static str> {
    raw.trim().parse::<IpAddr>().map_err(|_| "an IP address")
}

/// Parses a listener port. Zero is out of domain: no listener binds to it.
pub(super) fn parse_port(raw: &str) -> Result<u16, &'static str> {
    match raw.trim().parse::<u16>() {
        Ok(0) | Err(_) => Err("a port number (1-65535)"),
        Ok(port) => Ok(port),
    }
}

pub(super) fn parse_socket_addr(
    raw: &str,
    expected: &'static str,
) -> Result<SocketAddr, &'static str> {
    raw.trim().parse::<SocketAddr>().map_err(|_| expected)
}

/// Accepts `true`/`1`/`yes` and `false`/`0`/`no`, case-insensitive. Every
/// boolean row shares this vocabulary, observability toggles included.
pub(super) fn parse_bool_lenient(raw: &str) -> Result<bool, &'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err("true or false"),
    }
}

pub(super) fn parse_usize_positive(raw: &str) -> Result<usize, &'static str> {
    match raw.trim().parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("a positive integer"),
    }
}

pub(super) fn parse_usize_nonneg(raw: &str) -> Result<usize, &'static str> {
    raw.trim()
        .parse::<usize>()
        .map_err(|_| "a non-negative integer")
}

pub(super) fn parse_u32_positive(raw: &str) -> Result<u32, &'static str> {
    match raw.trim().parse::<u32>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("a positive integer"),
    }
}

pub(super) fn parse_u64_positive(raw: &str) -> Result<u64, &'static str> {
    match raw.trim().parse::<u64>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("a positive integer"),
    }
}

/// Parses a `u64` no smaller than `min`, for the one row (scope-expiry)
/// whose floor sits above zero.
pub(super) fn parse_u64_at_least(
    raw: &str,
    min: u64,
    expected: &'static str,
) -> Result<u64, &'static str> {
    match raw.trim().parse::<u64>() {
        Ok(n) if n >= min => Ok(n),
        _ => Err(expected),
    }
}
