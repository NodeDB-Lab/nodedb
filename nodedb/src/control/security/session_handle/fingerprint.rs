//! Client fingerprint captured at session create-time and matched on resolve.
//!
//! A fingerprint is `(tenant_id, ip_bucket)`. The IP bucket normalizes the
//! caller's source address per the configured `FingerprintMode`:
//!
//! - **Strict** — full 32/128-bit IP. Zero tolerance; any host-bit change
//!   rejects. Appropriate for server-to-server or pinned deployments.
//! - **Subnet** (default) — IPv4 `/24`, IPv6 `/64`. Tolerates NAT/egress
//!   jitter without widening the attacker's footprint to a meaningfully
//!   larger set. The most common mobile/desktop default.
//! - **Disabled** — IP ignored; only `tenant_id` is checked. Emergency
//!   knob for networks with aggressive IP churn (e.g. carrier-grade NAT
//!   rotation). Still rejects cross-tenant resolution.
//!
//! Tenant divergence always rejects, regardless of mode — the session was
//! issued to a specific tenant's `AuthContext` and that binding is not
//! configurable away.

use std::net::{IpAddr, SocketAddr};

use crate::types::TenantId;

/// Policy for comparing a caller's fingerprint against the one captured
/// at handle create-time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FingerprintMode {
    /// Exact match on (tenant_id, full IP).
    Strict,
    /// Match on (tenant_id, IPv4 /24 or IPv6 /64).
    Subnet,
    /// Match on tenant_id only; IP ignored.
    Disabled,
}

impl Default for FingerprintMode {
    /// Subnet is the documented default — strict enough to kill
    /// cross-origin theft, lax enough to tolerate mobile egress.
    fn default() -> Self {
        Self::Subnet
    }
}

/// Fingerprint of a client establishing or resolving a session handle.
///
/// `ip_bucket` is a `/24` or `/64` prefix (or the full IP under `Strict`)
/// stored as an `IpAddr` with host bits zeroed in the subnet case. Comparing
/// two fingerprints under the same mode is a bitwise equality check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientFingerprint {
    pub tenant_id: TenantId,
    pub ip: IpAddr,
}

impl ClientFingerprint {
    /// Build a fingerprint from a connected peer's `(tenant, socket)`.
    ///
    /// The full IP is stored verbatim — subnet normalization happens at
    /// compare time, so a single captured fingerprint can be compared
    /// under any mode without re-hashing.
    pub fn from_peer(tenant_id: TenantId, peer: &SocketAddr) -> Self {
        Self {
            tenant_id,
            ip: peer.ip(),
        }
    }

    /// Direct constructor for tests and non-socket contexts.
    pub fn new(tenant_id: TenantId, ip: IpAddr) -> Self {
        Self { tenant_id, ip }
    }

    /// True when `self` (the captured fingerprint) matches `caller`
    /// under the given mode.
    pub fn matches(&self, caller: &ClientFingerprint, mode: FingerprintMode) -> bool {
        if self.tenant_id != caller.tenant_id {
            return false;
        }
        match mode {
            FingerprintMode::Disabled => true,
            FingerprintMode::Strict => self.ip == caller.ip,
            FingerprintMode::Subnet => same_subnet(self.ip, caller.ip),
        }
    }
}

/// IPv4 /24 and IPv6 /64 equality. Mixed families never match.
fn same_subnet(a: IpAddr, b: IpAddr) -> bool {
    match (a, b) {
        (IpAddr::V4(x), IpAddr::V4(y)) => x.octets()[..3] == y.octets()[..3],
        (IpAddr::V6(x), IpAddr::V6(y)) => x.octets()[..8] == y.octets()[..8],
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(a, b, c, d))
    }

    fn v6(s: &str) -> IpAddr {
        IpAddr::V6(s.parse::<Ipv6Addr>().unwrap())
    }

    #[test]
    fn tenant_divergence_rejects_in_all_modes() {
        let captured = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 5));
        let caller = ClientFingerprint::new(TenantId::new(2), v4(10, 0, 0, 5));
        for mode in [
            FingerprintMode::Strict,
            FingerprintMode::Subnet,
            FingerprintMode::Disabled,
        ] {
            assert!(!captured.matches(&caller, mode), "mode {mode:?}");
        }
    }

    #[test]
    fn strict_mode_rejects_any_ip_difference() {
        let captured = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 5));
        let caller = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 6));
        assert!(!captured.matches(&caller, FingerprintMode::Strict));
    }

    #[test]
    fn subnet_mode_tolerates_ipv4_24_host_bits() {
        let captured = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 5));
        let caller = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 99));
        assert!(captured.matches(&caller, FingerprintMode::Subnet));
    }

    #[test]
    fn subnet_mode_rejects_different_ipv4_24() {
        let captured = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 5));
        let caller = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 1, 5));
        assert!(!captured.matches(&caller, FingerprintMode::Subnet));
    }

    #[test]
    fn subnet_mode_tolerates_ipv6_64() {
        let captured = ClientFingerprint::new(TenantId::new(1), v6("2001:db8::1"));
        let caller = ClientFingerprint::new(TenantId::new(1), v6("2001:db8::ffff"));
        assert!(captured.matches(&caller, FingerprintMode::Subnet));
    }

    #[test]
    fn subnet_mode_rejects_different_ipv6_64() {
        let captured = ClientFingerprint::new(TenantId::new(1), v6("2001:db8::1"));
        let caller = ClientFingerprint::new(TenantId::new(1), v6("2001:db8:0:1::1"));
        assert!(!captured.matches(&caller, FingerprintMode::Subnet));
    }

    #[test]
    fn disabled_mode_ignores_ip() {
        let captured = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 5));
        let caller = ClientFingerprint::new(TenantId::new(1), v4(192, 168, 1, 1));
        assert!(captured.matches(&caller, FingerprintMode::Disabled));
    }

    #[test]
    fn mixed_address_families_never_match_under_subnet() {
        let captured = ClientFingerprint::new(TenantId::new(1), v4(10, 0, 0, 5));
        let caller = ClientFingerprint::new(TenantId::new(1), v6("2001:db8::1"));
        assert!(!captured.matches(&caller, FingerprintMode::Subnet));
        assert!(!captured.matches(&caller, FingerprintMode::Strict));
    }
}
