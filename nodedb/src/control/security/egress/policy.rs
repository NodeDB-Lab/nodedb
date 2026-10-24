// SPDX-License-Identifier: BUSL-1.1

//! `EgressPolicy`: which URLs an outbound request of the server may reach.
//! Remote `COPY` sources, JWKS fetches, change-stream and alert webhooks, SIEM
//! export, OTLP export and the Vault key provider share it.
//!
//! The strict policy allows `https://` only, refuses IP-literal hosts, and
//! refuses any resolved address outside global unicast. A relaxed policy
//! names hosts, and every relaxation is tied to a named host:
//!
//! - `allow_http` admits `http://` only for a host in `hosts`.
//! - A resolved non-global address passes only when the host is in `hosts`
//!   AND the address lies in one of `cidrs`.
//! - An IP-literal host is never admitted.
//!
//! So a typo or a forgotten flag never opens the whole private address
//! space. Error messages never echo the URL: external input flowing into a
//! log line is its own exfiltration path.

use std::collections::HashSet;
use std::net::IpAddr;

use reqwest::Url;

use super::global_unicast::is_global_unicast;
use crate::control::security::net::Cidr;

/// Why a URL or an address is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EgressError {
    #[error("URL is malformed")]
    Malformed,
    #[error("URL must use the https scheme; http needs the host on the egress allow-list")]
    NonHttps,
    #[error("URL has no host component")]
    NoHost,
    #[error("URL must use a DNS name, not an IP literal")]
    IpLiteral,
    #[error("host resolves to a non-global address the egress allow-list does not cover")]
    PrivateAddress,
    #[error("invalid CIDR in the egress allow-list: {0}")]
    BadCidr(String),
}

/// The effective egress allow-list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EgressPolicy {
    /// Allow `http://` for hosts in `hosts`.
    pub allow_http: bool,
    /// Hostnames (exact, lowercase) that may use `http://` when
    /// `allow_http` is set, and may resolve into `cidrs`.
    pub hosts: HashSet<String>,
    /// Blocks `hosts` may resolve into, beyond global unicast.
    pub cidrs: Vec<Cidr>,
}

impl EgressPolicy {
    /// No relaxation: https, DNS names, global unicast addresses.
    pub fn strict() -> Self {
        Self::default()
    }

    /// A policy from configuration strings. A CIDR that does not parse is
    /// an error.
    pub fn from_parts(
        allow_http: bool,
        hosts: &[String],
        cidrs: &[String],
    ) -> Result<Self, EgressError> {
        let hosts = hosts.iter().map(|h| h.to_ascii_lowercase()).collect();
        let cidrs = cidrs
            .iter()
            .map(|s| Cidr::parse(s).ok_or_else(|| EgressError::BadCidr(s.clone())))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            allow_http,
            hosts,
            cidrs,
        })
    }

    /// Check a URL string: the first request and every redirect hop.
    pub fn check_url(&self, url: &str) -> Result<(), EgressError> {
        let parsed = Url::parse(url).map_err(|_| EgressError::Malformed)?;
        self.check_parsed(&parsed)
    }

    /// Check a parsed URL's scheme and host.
    pub fn check_parsed(&self, parsed: &Url) -> Result<(), EgressError> {
        let host = match parsed.domain() {
            Some(d) if !d.is_empty() => d.to_ascii_lowercase(),
            Some(_) => return Err(EgressError::NoHost),
            None if parsed.host_str().is_some() => return Err(EgressError::IpLiteral),
            None => return Err(EgressError::NoHost),
        };
        match parsed.scheme() {
            "https" => Ok(()),
            "http" if self.allow_http && self.hosts.contains(&host) => Ok(()),
            _ => Err(EgressError::NonHttps),
        }
    }

    /// Check every address `host` resolved to. All must pass, so a name
    /// with one public and one private address is refused.
    pub fn check_resolved(&self, host: &str, addrs: &[IpAddr]) -> Result<(), EgressError> {
        if addrs.is_empty() {
            return Err(EgressError::PrivateAddress);
        }
        let allowlisted = self.hosts.contains(&host.to_ascii_lowercase());
        for ip in addrs {
            if is_global_unicast(ip) {
                continue;
            }
            if allowlisted && self.cidrs.iter().any(|c| c.contains(ip)) {
                continue;
            }
            return Err(EgressError::PrivateAddress);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn relaxed(hosts: &[&str], cidrs: &[&str], http: bool) -> EgressPolicy {
        let hosts: Vec<String> = hosts.iter().map(|h| (*h).to_string()).collect();
        let cidrs: Vec<String> = cidrs.iter().map(|c| (*c).to_string()).collect();
        EgressPolicy::from_parts(http, &hosts, &cidrs).expect("valid allow-list")
    }

    #[test]
    fn https_public_dns_passes_strict() {
        assert!(
            EgressPolicy::strict()
                .check_url("https://auth.example.com/.well-known/jwks.json")
                .is_ok()
        );
    }

    #[test]
    fn http_is_refused_unless_the_host_is_allowlisted() {
        assert_eq!(
            EgressPolicy::strict().check_url("http://example.com/"),
            Err(EgressError::NonHttps)
        );
        let p = relaxed(&["mirror.internal"], &[], true);
        assert!(p.check_url("http://mirror.internal/a.tif").is_ok());
        assert_eq!(
            p.check_url("http://evil.example.com/"),
            Err(EgressError::NonHttps)
        );
        assert_eq!(
            p.check_url("ftp://mirror.internal/"),
            Err(EgressError::NonHttps)
        );
    }

    #[test]
    fn ip_literals_are_always_refused() {
        let p = relaxed(&["ignored"], &["0.0.0.0/0", "::/0"], true);
        for u in [
            "https://127.0.0.1/",
            "https://169.254.169.254/latest/meta-data/",
            "https://[::1]/",
            "http://10.0.0.5/",
        ] {
            assert_eq!(p.check_url(u), Err(EgressError::IpLiteral), "{u}");
        }
    }

    #[test]
    fn private_addresses_need_an_allowlisted_host_and_cidr() {
        let p = relaxed(&["mirror.internal"], &["10.42.0.0/16"], false);
        let ip = |a, b, c, d| IpAddr::V4(Ipv4Addr::new(a, b, c, d));
        assert!(
            p.check_resolved("mirror.internal", &[ip(10, 42, 1, 5)])
                .is_ok()
        );
        assert!(
            p.check_resolved("mirror.internal", &[ip(10, 43, 0, 1)])
                .is_err()
        );
        assert!(
            p.check_resolved("evil.example.com", &[ip(10, 42, 0, 1)])
                .is_err()
        );
        assert!(
            p.check_resolved("evil.example.com", &[ip(127, 0, 0, 1)])
                .is_err()
        );
        assert!(p.check_resolved("evil.example.com", &[]).is_err());
        let mixed = [ip(93, 184, 216, 34), ip(169, 254, 169, 254)];
        assert!(p.check_resolved("cdn.example.com", &mixed).is_err());
        let v6 = relaxed(&["v6.internal"], &["fd00::/8"], false);
        let ula = IpAddr::V6(Ipv6Addr::new(0xfd12, 0, 0, 0, 0, 0, 0, 1));
        let link = IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1));
        assert!(v6.check_resolved("v6.internal", &[ula]).is_ok());
        assert!(v6.check_resolved("v6.internal", &[link]).is_err());
    }

    #[test]
    fn a_bad_cidr_is_an_error_at_build_time() {
        assert_eq!(
            EgressPolicy::from_parts(false, &[], &["not-a-cidr".into()]),
            Err(EgressError::BadCidr("not-a-cidr".into()))
        );
    }
}
