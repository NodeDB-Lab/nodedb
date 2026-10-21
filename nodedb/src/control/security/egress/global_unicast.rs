// SPDX-License-Identifier: BUSL-1.1

//! The global-unicast test an egress check runs on every resolved address.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` is a routable global unicast address: not loopback,
/// private, link-local, shared (CGNAT), documentation, benchmarking,
/// multicast, broadcast, unspecified or reserved. An IPv6 address that
/// embeds an IPv4 address passes only when that IPv4 address passes.
pub fn is_global_unicast(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_global_v4(v4),
        IpAddr::V6(v6) => is_global_v6(v6),
    }
}

fn is_global_v4(v4: &Ipv4Addr) -> bool {
    let o = v4.octets();
    !(v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_multicast()
        || v4.is_unspecified()
        || v4.is_documentation()
        || o[0] == 0
        || (o[0] == 100 && (o[1] & 0xC0) == 64)
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)
        || (o[0] == 198 && (o[1] & 0xFE) == 18)
        || o[0] >= 240)
}

fn is_global_v6(v6: &Ipv6Addr) -> bool {
    if let Some(v4) = embedded_v4(v6) {
        return is_global_v4(&v4);
    }
    let seg = v6.segments();
    !(v6.is_multicast()
        // Link-local fe80::/10 and site-local fec0::/10.
        || (seg[0] & 0xffc0) == 0xfe80
        || (seg[0] & 0xffc0) == 0xfec0
        // Unique local fc00::/7.
        || (seg[0] & 0xfe00) == 0xfc00
        // NAT64 local-use 64:ff9b:1::/48: the IPv4 position depends on a
        // site-chosen prefix length, and the block is not globally reachable.
        || (seg[0] == 0x0064 && seg[1] == 0xff9b && seg[2] == 0x0001)
        // Discard-only 100::/64.
        || (seg[0] == 0x0100 && seg[1] == 0 && seg[2] == 0 && seg[3] == 0)
        // Teredo 2001::/32 tunnels to an obfuscated IPv4 endpoint.
        || (seg[0] == 0x2001 && seg[1] == 0)
        // Benchmarking 2001:2::/48.
        || (seg[0] == 0x2001 && seg[1] == 0x0002 && seg[2] == 0)
        // Documentation 2001:db8::/32 and 3fff::/20.
        || (seg[0] == 0x2001 && seg[1] == 0x0db8)
        || (seg[0] == 0x3fff && (seg[1] & 0xf000) == 0))
}

/// The IPv4 address an IPv6 address carries in a form that reaches it:
/// IPv4-mapped `::ffff:0:0/96`, IPv4-compatible `::/96` (the loopback and
/// unspecified addresses among them), NAT64 `64:ff9b::/96` and 6to4
/// `2002::/16`.
fn embedded_v4(v6: &Ipv6Addr) -> Option<Ipv4Addr> {
    let seg = v6.segments();
    let low = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    if seg[..5] == [0; 5] && (seg[5] == 0 || seg[5] == 0xffff) {
        return Some(low(seg[6], seg[7]));
    }
    if seg[..6] == [0x0064, 0xff9b, 0, 0, 0, 0] {
        return Some(low(seg[6], seg[7]));
    }
    if seg[0] == 0x2002 {
        return Some(low(seg[1], seg[2]));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn global(s: &str) -> bool {
        is_global_unicast(&s.parse().expect("ip"))
    }

    #[test]
    fn private_loopback_and_link_local_are_not_global() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "::",
            "fe80::1",
            "fd00::1",
            "ff02::1",
            "2001:db8::1",
        ] {
            assert!(!global(ip), "{ip}");
        }
        assert!(global("93.184.216.34"));
        assert!(global("2606:2800:220:1:248:1893:25c8:1946"));
    }

    #[test]
    fn ipv4_mapped_takes_the_ipv4_verdict() {
        assert!(!global("::ffff:127.0.0.1"));
        assert!(!global("::ffff:169.254.169.254"));
        assert!(global("::ffff:93.184.216.34"));
    }

    #[test]
    fn ipv4_compatible_takes_the_ipv4_verdict() {
        assert!(!global("::127.0.0.1"));
        assert!(!global("::10.0.0.1"));
        assert!(global("::93.184.216.34"));
    }

    #[test]
    fn nat64_well_known_takes_the_ipv4_verdict() {
        assert!(!global("64:ff9b::127.0.0.1"));
        assert!(!global("64:ff9b::a9fe:a9fe"));
        assert!(global("64:ff9b::93.184.216.34"));
    }

    #[test]
    fn nat64_local_use_is_not_global() {
        assert!(!global("64:ff9b:1::5db8:d822"));
        assert!(!global("64:ff9b:1:ffff::1"));
    }

    #[test]
    fn six_to_four_takes_the_ipv4_verdict() {
        // 2002:WWXX:YYZZ:: embeds WW.XX.YY.ZZ.
        assert!(!global("2002:7f00:1::1"));
        assert!(!global("2002:c0a8:101::"));
        assert!(global("2002:5db8:d822::1"));
    }

    #[test]
    fn teredo_is_not_global() {
        assert!(!global("2001:0:4136:e378:8000:63bf:3fff:fdd2"));
        assert!(!global("2001::1"));
    }

    #[test]
    fn site_local_is_not_global() {
        assert!(!global("fec0::1"));
        assert!(!global("feff::1"));
    }

    #[test]
    fn discard_only_is_not_global() {
        assert!(!global("100::1"));
        assert!(!global("100::ffff:ffff:ffff:ffff"));
        assert!(global("100:0:0:1::1"));
    }

    #[test]
    fn benchmarking_and_documentation_are_not_global() {
        assert!(!global("2001:2::1"));
        assert!(!global("3fff::1"));
        assert!(!global("3fff:fff::1"));
        assert!(global("2001:4860::8888"));
    }
}
