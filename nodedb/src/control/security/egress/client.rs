// SPDX-License-Identifier: BUSL-1.1

//! The HTTP client every egress request goes through: the policy-checking
//! resolver, a redirect policy that checks every hop, no system proxy, and
//! bounded timeouts.

use std::sync::Arc;
use std::time::Duration;

use super::policy::{EgressError, EgressPolicy};
use super::resolver::{EgressResolver, HostLookup, SystemLookup};

/// Timeouts and the redirect cap of one client.
#[derive(Debug, Clone, Copy)]
pub struct EgressClientOptions {
    /// Cap on a whole request. `None` bounds only the connect and each read.
    pub timeout: Option<Duration>,
    pub connect_timeout: Duration,
    /// Cap on the time between two reads of a response.
    pub read_timeout: Duration,
    pub max_redirects: usize,
}

/// Why a redirect was not followed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedirectError {
    #[error("the request exceeded {0} redirects")]
    TooMany(usize),
    #[error("a redirect hop was refused: {0}")]
    Refused(EgressError),
}

/// A client that reaches only what `policy` allows. Each redirect hop's URL
/// is checked, and each host it resolves passes through the resolver.
pub fn egress_client(
    policy: Arc<EgressPolicy>,
    lookup: Arc<dyn HostLookup>,
    options: EgressClientOptions,
) -> Result<reqwest::Client, reqwest::Error> {
    let hop_policy = Arc::clone(&policy);
    let max = options.max_redirects;
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(options.connect_timeout)
        .read_timeout(options.read_timeout)
        .dns_resolver(EgressResolver::new(policy, lookup))
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= max {
                return attempt.error(RedirectError::TooMany(max));
            }
            match hop_policy.check_parsed(attempt.url()) {
                Ok(()) => attempt.follow(),
                Err(e) => attempt.error(RedirectError::Refused(e)),
            }
        }));
    if let Some(timeout) = options.timeout {
        builder = builder.timeout(timeout);
    }
    builder.build()
}

/// The options of the shared outbound client: each request sets its own
/// total timeout, and no redirect is followed.
pub const OUTBOUND_OPTIONS: EgressClientOptions = EgressClientOptions {
    timeout: None,
    connect_timeout: Duration::from_secs(10),
    read_timeout: Duration::from_secs(30),
    max_redirects: 0,
};

/// The client the server's own outbound requests share: alert webhooks,
/// SIEM export, OTLP export and the key-management client. It resolves
/// through the system resolver under `policy`. The caller checks each URL
/// with [`EgressPolicy::check_url`] before it sends: an IP-literal host is
/// never resolved, so only that check refuses one.
pub fn outbound_client(policy: Arc<EgressPolicy>) -> Result<reqwest::Client, reqwest::Error> {
    egress_client(policy, Arc::new(SystemLookup), OUTBOUND_OPTIONS)
}

/// What an egress request failed on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressFault {
    /// The policy refused a redirect hop or a resolved address.
    Refused(EgressError),
    /// The request exceeded its redirect cap.
    TooManyRedirects(usize),
    /// Any other error: connect, TLS, timeout, status or body read.
    Other,
}

/// The egress fault behind a failed send or body read of an egress client.
pub fn classify(error: &reqwest::Error) -> EgressFault {
    match find_cause::<RedirectError>(error) {
        Some(RedirectError::TooMany(max)) => EgressFault::TooManyRedirects(max),
        Some(RedirectError::Refused(refused)) => EgressFault::Refused(refused),
        None => match find_cause::<EgressError>(error) {
            Some(refused) => EgressFault::Refused(refused),
            None => EgressFault::Other,
        },
    }
}

/// The first `T` in the source chain of `error`. A boxed error in the
/// chain exposes its inner error as its source, so the walk reaches it.
fn find_cause<T: std::error::Error + Clone + 'static>(error: &reqwest::Error) -> Option<T> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(error);
    while let Some(e) = source {
        if let Some(found) = e.downcast_ref::<T>() {
            return Some(found.clone());
        }
        source = e.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::control::security::egress::resolver::tests::table;

    fn client(policy: EgressPolicy, max_redirects: usize) -> reqwest::Client {
        egress_client(
            Arc::new(policy),
            table(&[("mirror.test", "127.0.0.1"), ("rebind.test", "127.0.0.1")]),
            EgressClientOptions {
                timeout: Some(Duration::from_secs(5)),
                connect_timeout: Duration::from_secs(5),
                read_timeout: Duration::from_secs(5),
                max_redirects,
            },
        )
        .expect("client")
    }

    fn mirror_policy() -> EgressPolicy {
        EgressPolicy::from_parts(true, &["mirror.test".into()], &["127.0.0.0/8".into()])
            .expect("valid policy")
    }

    /// A loopback server that answers every request with a redirect to
    /// itself. Returns its port.
    async fn redirect_loop() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    let _ = socket.read(&mut request).await;
                    let answer = format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://mirror.test:{port}/next\r\n\
                         Content-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = socket.write_all(answer.as_bytes()).await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn each_fault_is_classified() {
        let port = redirect_loop().await;
        let err = client(mirror_policy(), 0)
            .get(format!("http://mirror.test:{port}/a"))
            .send()
            .await
            .expect_err("redirect cap");
        assert_eq!(classify(&err), EgressFault::TooManyRedirects(0));

        let err = client(mirror_policy(), 3)
            .get(format!("http://rebind.test:{port}/a"))
            .send()
            .await
            .expect_err("loopback refused");
        assert_eq!(
            classify(&err),
            EgressFault::Refused(EgressError::PrivateAddress)
        );

        // An allowed host with nothing listening fails at connect.
        let closed = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let closed_port = closed.local_addr().expect("addr").port();
        drop(closed);
        let err = client(mirror_policy(), 3)
            .get(format!("http://mirror.test:{closed_port}/a"))
            .send()
            .await
            .expect_err("connection refused");
        assert_eq!(classify(&err), EgressFault::Other);
    }
}
