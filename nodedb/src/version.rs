//! NodeDB version and compatibility information.
//!
//! Follows semver: MAJOR.MINOR.PATCH.
//! - MAJOR changes MAY break wire/disk compatibility.
//! - MINOR changes MUST be backward compatible within the same MAJOR.
//! - Rolling upgrades are supported for N-1 within the same MAJOR.

/// Current NodeDB version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Wire format version (incremented when SPSC bridge, WAL, or RPC formats change).
/// Readers MUST reject messages with wire_version > their own.
/// Readers SHOULD accept messages with wire_version == their own or one less (N-1).
pub const WIRE_FORMAT_VERSION: u16 = 1;

/// Minimum wire format version this build can read.
/// Messages below this version are rejected.
pub const MIN_WIRE_FORMAT_VERSION: u16 = 1;

/// Check if a remote node's wire version is compatible.
///
/// Returns `Ok(())` if compatible, `Err(reason)` if not.
pub fn check_wire_compatibility(remote_version: u16) -> crate::Result<()> {
    if remote_version > WIRE_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "remote wire version {remote_version} is newer than local {WIRE_FORMAT_VERSION}; upgrade this node"
            ),
        });
    }
    if remote_version < MIN_WIRE_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "remote wire version {remote_version} is too old (minimum {MIN_WIRE_FORMAT_VERSION}); upgrade the remote node"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_version_compatible() {
        assert!(check_wire_compatibility(WIRE_FORMAT_VERSION).is_ok());
    }

    #[test]
    fn newer_version_rejected() {
        assert!(check_wire_compatibility(WIRE_FORMAT_VERSION + 1).is_err());
    }

    #[test]
    fn version_string_valid() {
        assert!(!VERSION.is_empty());
    }
}
