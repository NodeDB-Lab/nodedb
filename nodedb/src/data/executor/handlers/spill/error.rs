// SPDX-License-Identifier: BUSL-1.1

//! Typed errors of the spill tiers. Each converts into `crate::Error` as a
//! storage error that names the spill file.

use std::path::PathBuf;

/// An error of a spill file or directory.
#[derive(Debug, thiserror::Error)]
pub enum SpillError {
    /// A read, write, open or create of a spill file failed.
    #[error("spill {action} {}: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// An io_uring read of a spill file failed.
    #[error("spill io_uring read {}: {detail}", path.display())]
    UringRead { path: PathBuf, detail: String },
    /// A spill record did not encode or decode.
    #[error("spill record codec {}: {detail}", path.display())]
    Codec { path: PathBuf, detail: String },
    /// A spill file read back shorter than its index or out of order.
    #[error("spill file {} corrupt: {detail}", path.display())]
    Corrupt { path: PathBuf, detail: String },
    /// A reader buffer was taken while another use held it: a lookup or
    /// a block read ran inside another.
    #[error("spill reader {buffer} buffer already in use")]
    BufferBusy { buffer: &'static str },
    /// Cleanup could not remove a spill directory or file.
    #[error("spill cleanup of {} failed: {source}", path.display())]
    NotRemoved {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
