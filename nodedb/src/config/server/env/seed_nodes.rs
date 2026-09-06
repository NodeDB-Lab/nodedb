// SPDX-License-Identifier: BUSL-1.1

//! Parses `NODEDB_SEED_NODES`: a comma-separated list of `host:port`
//! addresses.

use std::net::SocketAddr;

/// Parses a comma-separated list of `SocketAddr` entries.
///
/// The `Err` text names the expected shape, not the offending entry. The
/// caller's own violation message already carries the full raw value, so
/// the bad entry survives there as a substring.
pub fn parse_seed_nodes(raw: &str) -> Result<Vec<SocketAddr>, &'static str> {
    let mut addrs = Vec::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let addr = entry
            .parse::<SocketAddr>()
            .map_err(|_| "a comma-separated list of host:port addresses")?;
        addrs.push(addr);
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multiple_addresses() {
        let addrs = parse_seed_nodes("10.0.0.1:9400,10.0.0.2:9400").expect("valid addresses");
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0].to_string(), "10.0.0.1:9400");
        assert_eq!(addrs[1].to_string(), "10.0.0.2:9400");
    }

    #[test]
    fn rejects_bad_entry() {
        assert!(parse_seed_nodes("10.0.0.1:9400,garbage").is_err());
    }
}
