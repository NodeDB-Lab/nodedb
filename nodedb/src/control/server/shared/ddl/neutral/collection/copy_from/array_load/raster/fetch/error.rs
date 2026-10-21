// SPDX-License-Identifier: BUSL-1.1

//! `FetchError`: why a byte-range fetch of a raster source failed, with
//! the SQLSTATE each failure reports. No variant carries a response body.

use crate::control::security::egress::EgressError;

/// A failed range fetch.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("'{source_name}': cannot read: {error}")]
    Io {
        source_name: String,
        error: std::io::Error,
    },
    #[error("'{source_name}' does not exist")]
    NotFound { source_name: String },
    #[error("'{source_name}': refused by the egress policy: {error}")]
    Egress {
        source_name: String,
        error: EgressError,
    },
    #[error(
        "'{source_name}': the server refused the request with HTTP {status}; check the CREDENTIAL"
    )]
    Unauthorized { source_name: String, status: u16 },
    #[error("'{source_name}' changed during the load ({what}); rerun the COPY to read one version")]
    Changed { source_name: String, what: String },
    #[error(
        "'{source_name}': the server ignored the byte range (HTTP {status}); a remote raster \
         needs a server that answers Range requests with 206"
    )]
    RangeIgnored { source_name: String, status: u16 },
    #[error("'{source_name}': HTTP {status}")]
    Status { source_name: String, status: u16 },
    #[error("'{source_name}': {what}")]
    Protocol { source_name: String, what: String },
    #[error("'{source_name}': the connection failed: {what}")]
    Network { source_name: String, what: String },
    #[error("'{source_name}': {what}")]
    Store { source_name: String, what: String },
    #[error("raster fetch task failed: {0}")]
    Task(String),
}

impl FetchError {
    /// The SQLSTATE the failure reports.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Io { .. } | Self::NotFound { .. } | Self::Status { .. } | Self::Store { .. } => {
                "58030"
            }
            Self::Egress { .. } => "42501",
            Self::Unauthorized { .. } => "28000",
            Self::Changed { .. } => "55000",
            Self::RangeIgnored { .. } => "0A000",
            Self::Protocol { .. } => "22P04",
            Self::Network { .. } => "08006",
            Self::Task(_) => "XX000",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_failure_maps_to_its_sqlstate() {
        let name = || "https://h.example/a.tif".to_string();
        assert_eq!(
            FetchError::Changed {
                source_name: name(),
                what: "ETag".into()
            }
            .sqlstate(),
            "55000"
        );
        assert_eq!(
            FetchError::RangeIgnored {
                source_name: name(),
                status: 200
            }
            .sqlstate(),
            "0A000"
        );
        assert_eq!(
            FetchError::Egress {
                source_name: name(),
                error: EgressError::IpLiteral
            }
            .sqlstate(),
            "42501"
        );
    }
}
