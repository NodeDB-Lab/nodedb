// SPDX-License-Identifier: BUSL-1.1

//! Capture site for a write window that leaked from the outcome floor.

use std::time::Duration;

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Report a write window dropped without a settle or a hold. Called from the
/// window's drop, the one site that detects the leak.
pub fn write_window_leaked(ticket: u64, horizon: u64, open_for: Duration) {
    let ctx = context::WriteWindowLeaked {
        ticket,
        horizon,
        open_for_ms: u64::try_from(open_for.as_millis()).unwrap_or(u64::MAX),
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "write window leaked: the outcome floor is held until restart",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a write window held until restart. Called from the hold, the one
/// site that decides it.
pub fn write_window_held(
    site: &std::panic::Location<'_>,
    ticket: u64,
    horizon: u64,
    open_for: Duration,
) {
    let ctx = context::WriteWindowHeld {
        site: format!("{}:{}", site.file(), site.line()),
        ticket,
        horizon,
        open_for_ms: u64::try_from(open_for.as_millis()).unwrap_or(u64::MAX),
    };
    let _ = Capture::new(
        EventKind::Error,
        "write window held: the outcome floor stays below it until restart",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
