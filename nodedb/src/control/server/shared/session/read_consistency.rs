// SPDX-License-Identifier: BUSL-1.1

//! Session-level `ReadConsistency` — wire `SET` / `SHOW` for the
//! `default_read_consistency` session parameter.
//!
//! Accepted values:
//!
//! - `'strong'`
//! - `'bounded_staleness:<secs>'` or `'bounded_staleness:<secs>s'`
//! - `'eventual'`
//!
//! The value is stored as a plain string in the session parameter
//! map. This module provides the typed parse + accessor.

use super::connection::SessionId;
use std::time::Duration;

use crate::types::ReadConsistency;

use super::store::SessionStore;

/// Session parameter key.
pub const PARAM_KEY: &str = "default_read_consistency";

/// Parse a user-supplied string into a `ReadConsistency`. Returns
/// `None` on unrecognised input so the caller can return a helpful
/// error message.
pub fn parse_value(value: &str) -> Option<ReadConsistency> {
    let lower = value.trim().to_lowercase();
    match lower.as_str() {
        "strong" => Some(ReadConsistency::Strong),
        "eventual" => Some(ReadConsistency::Eventual),
        _ => {
            let stripped = lower.strip_prefix("bounded_staleness:")?;
            let secs_str = stripped.trim_end_matches('s').trim();
            let secs: f64 = secs_str.parse().ok()?;
            if secs <= 0.0 {
                return None;
            }
            Some(ReadConsistency::BoundedStaleness(Duration::from_secs_f64(
                secs,
            )))
        }
    }
}

/// The level a request field names, `Strong` when the request names none.
///
/// A level this cannot parse is refused: reading at another level than the one
/// the client named returns data it did not ask for.
pub fn parse_request_field(value: Option<&str>) -> crate::Result<ReadConsistency> {
    let Some(value) = value else {
        return Ok(ReadConsistency::Strong);
    };
    parse_value(value).ok_or_else(|| crate::Error::BadRequest {
        detail: format!(
            "invalid read_consistency '{value}'; send strong, eventual or \
             bounded_staleness:<secs>, or omit the field"
        ),
    })
}

/// Format a `ReadConsistency` back into the canonical string form
/// so `SHOW default_read_consistency` returns something parseable.
pub fn format_value(rc: &ReadConsistency) -> String {
    match rc {
        ReadConsistency::Strong => "strong".to_string(),
        ReadConsistency::Eventual => "eventual".to_string(),
        ReadConsistency::BoundedStaleness(d) => {
            format!("bounded_staleness:{}s", d.as_secs_f64())
        }
    }
}

impl SessionStore {
    /// Resolve the effective `ReadConsistency` for a session. Falls
    /// back to `Strong` if the parameter is unset or unparseable.
    pub fn read_consistency(&self, addr: impl Into<SessionId>) -> ReadConsistency {
        self.get_parameter(addr, PARAM_KEY)
            .and_then(|v| parse_value(&v))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    #[test]
    fn a_request_field_defaults_to_strong_and_refuses_an_unknown_level() {
        assert_eq!(parse_request_field(None).unwrap(), ReadConsistency::Strong);
        assert_eq!(
            parse_request_field(Some("eventual")).unwrap(),
            ReadConsistency::Eventual
        );
        assert!(matches!(
            parse_request_field(Some("sometimes")),
            Err(crate::Error::BadRequest { detail }) if detail.contains("sometimes")
        ));
    }

    #[test]
    fn parse_strong() {
        assert_eq!(parse_value("strong"), Some(ReadConsistency::Strong));
        assert_eq!(parse_value("STRONG"), Some(ReadConsistency::Strong));
    }

    #[test]
    fn parse_eventual() {
        assert_eq!(parse_value("eventual"), Some(ReadConsistency::Eventual));
    }

    #[test]
    fn parse_bounded_staleness_seconds() {
        let rc = parse_value("bounded_staleness:5").unwrap();
        assert_eq!(
            rc,
            ReadConsistency::BoundedStaleness(Duration::from_secs(5))
        );
    }

    #[test]
    fn parse_bounded_staleness_with_s_suffix() {
        let rc = parse_value("bounded_staleness:5s").unwrap();
        assert_eq!(
            rc,
            ReadConsistency::BoundedStaleness(Duration::from_secs(5))
        );
    }

    #[test]
    fn parse_bounded_staleness_fractional() {
        let rc = parse_value("bounded_staleness:0.5s").unwrap();
        assert_eq!(
            rc,
            ReadConsistency::BoundedStaleness(Duration::from_millis(500))
        );
    }

    #[test]
    fn parse_rejects_zero_staleness() {
        assert!(parse_value("bounded_staleness:0").is_none());
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse_value("foobar").is_none());
        assert!(parse_value("").is_none());
    }

    #[test]
    fn format_roundtrip_strong() {
        let s = format_value(&ReadConsistency::Strong);
        assert_eq!(parse_value(&s), Some(ReadConsistency::Strong));
    }

    #[test]
    fn format_roundtrip_bounded() {
        let rc = ReadConsistency::BoundedStaleness(Duration::from_secs(10));
        let s = format_value(&rc);
        assert_eq!(parse_value(&s), Some(rc));
    }

    #[test]
    fn format_roundtrip_eventual() {
        let s = format_value(&ReadConsistency::Eventual);
        assert_eq!(parse_value(&s), Some(ReadConsistency::Eventual));
    }

    #[test]
    fn session_store_defaults_to_strong() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5432".parse().unwrap();
        store.ensure_session(addr);
        assert_eq!(store.read_consistency(addr), ReadConsistency::Strong);
    }

    #[test]
    fn session_store_reads_set_value() {
        let store = SessionStore::new();
        let addr: SocketAddr = "127.0.0.1:5432".parse().unwrap();
        store.ensure_session(addr);
        store.set_parameter(addr, PARAM_KEY.to_string(), "eventual".to_string());
        assert_eq!(store.read_consistency(addr), ReadConsistency::Eventual);
    }
}
