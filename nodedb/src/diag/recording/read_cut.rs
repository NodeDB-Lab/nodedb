// SPDX-License-Identifier: BUSL-1.1

//! Capture site for an ordinal reservation that outlived its write.

use std::time::Duration;

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Report a reservation that blocked a read cut past the age any write
/// holds one. Called only from the read cut's deadline arm, the one site
/// that detects it.
pub fn ordinal_reservation_leaked(
    site: &std::panic::Location<'_>,
    ordinal: i64,
    open_for: Duration,
) {
    let ctx = context::OrdinalReservationLeaked {
        site: format!("{}:{}", site.file(), site.line()),
        ordinal,
        open_for_ms: u64::try_from(open_for.as_millis()).unwrap_or(u64::MAX),
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "ordinal reservation leaked: a write's reservation outlived the write and holds read cuts",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
