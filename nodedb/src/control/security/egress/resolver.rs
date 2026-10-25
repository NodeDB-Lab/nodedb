// SPDX-License-Identifier: BUSL-1.1

//! `EgressResolver`: the DNS resolver every egress HTTP client connects
//! through. It checks each resolved address against the policy and hands
//! the client exactly the addresses it checked, so the connection goes to
//! a checked address. A name that resolves to a private address between a
//! first check and the connect (DNS rebinding) is refused at the connect,
//! and so is every redirect hop's host.

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use super::policy::EgressPolicy;

/// The future a host lookup returns.
pub type LookupFuture = Pin<Box<dyn Future<Output = std::io::Result<Vec<IpAddr>>> + Send>>;

/// Resolves a host name to addresses. Tests replace the system resolver
/// with a fixed table.
pub trait HostLookup: Send + Sync + 'static {
    /// The addresses `host` resolves to.
    fn lookup(&self, host: &str) -> LookupFuture;
}

/// The system resolver (`getaddrinfo` on Tokio's blocking pool).
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemLookup;

impl HostLookup for SystemLookup {
    fn lookup(&self, host: &str) -> LookupFuture {
        let host = host.to_string();
        Box::pin(async move {
            let addrs = tokio::net::lookup_host((host.as_str(), 0)).await?;
            Ok(addrs.map(|addr| addr.ip()).collect())
        })
    }
}

/// A DNS resolver that refuses addresses the policy refuses.
#[derive(Clone)]
pub struct EgressResolver {
    policy: Arc<EgressPolicy>,
    lookup: Arc<dyn HostLookup>,
}

impl EgressResolver {
    /// A resolver that checks `lookup`'s answers against `policy`.
    pub fn new(policy: Arc<EgressPolicy>, lookup: Arc<dyn HostLookup>) -> Self {
        Self { policy, lookup }
    }
}

impl Resolve for EgressResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let policy = Arc::clone(&self.policy);
        let lookup = Arc::clone(&self.lookup);
        Box::pin(async move {
            let ips = lookup.lookup(&host).await?;
            policy.check_resolved(&host, &ips)?;
            let addrs: Addrs = Box::new(ips.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A fixed host table in place of DNS.
    pub(crate) struct TableLookup(pub HashMap<String, Vec<IpAddr>>);

    impl HostLookup for TableLookup {
        fn lookup(&self, host: &str) -> LookupFuture {
            let answer = self.0.get(host).cloned();
            Box::pin(async move {
                answer.ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "no such host")
                })
            })
        }
    }

    pub(crate) fn table(entries: &[(&str, &str)]) -> Arc<dyn HostLookup> {
        let mut map: HashMap<String, Vec<IpAddr>> = HashMap::new();
        for (host, ip) in entries {
            let ip: IpAddr = ip.parse().expect("ip");
            map.entry((*host).to_string()).or_default().push(ip);
        }
        Arc::new(TableLookup(map))
    }

    #[tokio::test]
    async fn a_name_rebound_to_a_private_address_is_refused_at_resolve() {
        let lookup = table(&[
            ("cdn.example.com", "93.184.216.34"),
            ("rebind.example.com", "169.254.169.254"),
        ]);
        let resolver = EgressResolver::new(Arc::new(EgressPolicy::strict()), lookup);
        let name: Name = "cdn.example.com".parse().expect("name");
        let addrs: Vec<SocketAddr> = resolver.resolve(name).await.expect("public").collect();
        assert_eq!(addrs.len(), 1);
        let name: Name = "rebind.example.com".parse().expect("name");
        assert!(resolver.resolve(name).await.is_err());
    }
}
