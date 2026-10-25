// SPDX-License-Identifier: BUSL-1.1

//! A CIDR block: the one address-range type the IP blacklist, the
//! conditional-grant IP rule and the egress allow-list share.

use std::net::IpAddr;

/// An IPv4 or IPv6 address block. A block matches only addresses of its
/// own family. An IPv4-mapped IPv6 address matches as its IPv4 form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    base: u128,
    mask: u128,
    prefix_len: u8,
    is_v4: bool,
}

impl Cidr {
    /// Parse `"10.42.0.0/16"`, `"fd00::/8"`, or a single host such as
    /// `"192.168.1.100"` (a `/32` or `/128`). Returns `None` when the input
    /// is not an address with an optional prefix no wider than the family.
    pub fn parse(s: &str) -> Option<Self> {
        let (ip_str, prefix_str) = match s.split_once('/') {
            Some((ip, prefix)) => (ip, Some(prefix)),
            None => (s, None),
        };
        let ip: IpAddr = ip_str.parse().ok()?;
        let width = family_width(&ip);
        let prefix_len = match prefix_str {
            Some(p) => p.parse::<u8>().ok()?,
            None => width,
        };
        if prefix_len > width {
            return None;
        }
        let mask = mask_of(prefix_len, width);
        Some(Self {
            base: bits_of(&ip) & mask,
            mask,
            prefix_len,
            is_v4: ip.is_ipv4(),
        })
    }

    /// The prefix length: 24 for a `/24`.
    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    /// Whether `ip` lies inside the block.
    pub fn contains(&self, ip: &IpAddr) -> bool {
        let ip = ip.to_canonical();
        ip.is_ipv4() == self.is_v4 && bits_of(&ip) & self.mask == self.base
    }
}

/// The address width of `ip`'s family in bits.
fn family_width(ip: &IpAddr) -> u8 {
    if ip.is_ipv4() { 32 } else { 128 }
}

/// The address as an integer. An IPv4 address fills the low 32 bits.
fn bits_of(ip: &IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => u128::from(u32::from(*v4)),
        IpAddr::V6(v6) => u128::from(*v6),
    }
}

/// The mask of the top `prefix_len` bits of a `width`-bit address.
fn mask_of(prefix_len: u8, width: u8) -> u128 {
    if prefix_len == 0 {
        return 0;
    }
    let full = u128::MAX >> (128 - u32::from(width));
    (full >> (width - prefix_len)) << (width - prefix_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("ip")
    }

    fn cidr(s: &str) -> Cidr {
        Cidr::parse(s).expect("cidr")
    }

    #[test]
    fn ranges_contain_their_addresses() {
        assert!(cidr("10.0.0.0/8").contains(&ip("10.255.255.255")));
        assert!(!cidr("10.0.0.0/8").contains(&ip("11.0.0.1")));
        assert!(cidr("192.168.1.0/24").contains(&ip("192.168.1.254")));
        assert!(!cidr("192.168.1.0/24").contains(&ip("192.168.2.1")));
        assert!(cidr("2001:db8::/32").contains(&ip("2001:db8:ffff::1")));
        assert!(!cidr("2001:db8::/32").contains(&ip("2001:db9::1")));
        assert!(cidr("fd00::/8").contains(&ip("fd12::1")));
        assert!(!cidr("fd00::/8").contains(&ip("fe80::1")));
        assert_eq!(cidr("10.0.0.0/8").prefix_len(), 8);
    }

    #[test]
    fn a_bare_address_is_a_single_host() {
        let host = cidr("192.168.1.100");
        assert_eq!(host.prefix_len(), 32);
        assert!(host.contains(&ip("192.168.1.100")));
        assert!(!host.contains(&ip("192.168.1.101")));
        let host6 = cidr("2001:db8::1");
        assert_eq!(host6.prefix_len(), 128);
        assert!(host6.contains(&ip("2001:db8::1")));
        assert!(!host6.contains(&ip("2001:db8::2")));
    }

    #[test]
    fn a_block_matches_only_its_own_family() {
        // `::a00:1` has the same low 32 bits as `10.0.0.1`.
        assert!(!cidr("10.0.0.0/8").contains(&ip("::a00:1")));
        assert!(!cidr("0.0.0.0/0").contains(&ip("2001:db8::1")));
        assert!(!cidr("::/0").contains(&ip("10.0.0.1")));
        assert!(cidr("0.0.0.0/0").contains(&ip("10.0.0.1")));
        assert!(cidr("::/0").contains(&ip("2001:db8::1")));
    }

    #[test]
    fn a_mapped_address_matches_as_ipv4() {
        assert!(cidr("10.0.0.0/8").contains(&ip("::ffff:10.0.0.5")));
        assert!(!cidr("::ffff:0:0/96").contains(&ip("::ffff:10.0.0.5")));
    }

    #[test]
    fn malformed_input_is_refused() {
        for bad in [
            "not-an-ip",
            "10.0.0.0/33",
            "::/129",
            "10.0.0.0/",
            "10.0.0.0/x",
        ] {
            assert!(Cidr::parse(bad).is_none(), "{bad}");
        }
    }
}
