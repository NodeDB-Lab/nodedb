//! Reclaim error type and result alias.

use std::path::PathBuf;

use thiserror::Error;

/// A persistent L1 surface could not be fully reclaimed. Callers must not
/// release the collection lifecycle barrier after this error.
#[derive(Debug, Error)]
pub enum ReclaimError {
    #[error("{operation} failed for '{}': {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A checkpoint manifest could not be read, so the live generation — and
    /// therefore the set of files this collection still owns — is unknown.
    /// Fail-closed: releasing the barrier here would let a same-name CREATE
    /// proceed while the predecessor's files stay reachable.
    #[error("{engine} manifest at '{}' is unreadable: {detail}", path.display())]
    Manifest {
        engine: &'static str,
        path: PathBuf,
        detail: String,
    },
    /// A checkpoint file's stored key could not be read, so whether it belongs
    /// to the dropped collection is unknown. Fail-closed for the same reason.
    #[error("{engine} checkpoint key at '{}' is unreadable: {detail}", path.display())]
    Key {
        engine: &'static str,
        path: PathBuf,
        detail: String,
    },
}

pub type Result<T> = std::result::Result<T, ReclaimError>;
