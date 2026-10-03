// SPDX-License-Identifier: BUSL-1.1

//! Capture site for a stored strict row with no MessagePack image.

use faultbox::{Capture, EventKind};

use crate::diag::context;

/// Report a stored strict row that does not render as MessagePack. Called
/// only from the Data Plane's image resolution, where the decode fails.
/// `fault` is the stable class of the failure.
pub fn strict_row_image_unrendered(collection: &str, fault: &'static str) {
    let ctx = context::StrictRowImageUnrendered { collection, fault };
    let _ = Capture::new(
        EventKind::Corruption,
        "stored strict row did not render as MessagePack: its image is withheld",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
