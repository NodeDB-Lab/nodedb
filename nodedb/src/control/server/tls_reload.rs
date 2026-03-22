//! TLS certificate hot-reload.
//!
//! Watches cert/key files for changes and atomically swaps the TLS
//! configuration in all listeners without dropping connections.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::server::TlsSettings;
use crate::control::security::audit::AuditEvent;
use crate::control::state::SharedState;

/// Shared TLS acceptor that can be atomically swapped.
///
/// Listeners clone the `watch::Receiver` and get the latest acceptor
/// on each new connection.
pub type TlsAcceptorWatch = watch::Receiver<Option<Arc<tokio_rustls::rustls::ServerConfig>>>;

/// Build a rustls `ServerConfig` from PEM files.
fn load_server_config(tls: &TlsSettings) -> crate::Result<tokio_rustls::rustls::ServerConfig> {
    use std::fs::File;
    use std::io::BufReader;

    let cert_file = File::open(&tls.cert_path)?;
    let key_file = File::open(&tls.key_path)?;

    let certs: Vec<_> =
        rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<Vec<_>, _>>()?;

    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?.ok_or_else(|| {
        crate::Error::Config {
            detail: format!("no private key in {}", tls.key_path.display()),
        }
    })?;

    tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| crate::Error::Config {
            detail: format!("TLS config error: {e}"),
        })
}

/// Get file modification time, or None if the file doesn't exist.
fn file_mtime(path: &PathBuf) -> Option<SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

/// Start a background task that watches cert/key files for changes
/// and reloads the TLS config atomically.
///
/// Returns a `watch::Receiver` that listeners use to get the latest config.
pub fn start_tls_reloader(
    tls: &TlsSettings,
    check_interval: Duration,
    state: Arc<SharedState>,
) -> crate::Result<(
    TlsAcceptorWatch,
    watch::Sender<Option<Arc<tokio_rustls::rustls::ServerConfig>>>,
)> {
    // Load initial config.
    let initial_config = load_server_config(tls)?;
    let (tx, rx) = watch::channel(Some(Arc::new(initial_config)));

    let cert_path = tls.cert_path.clone();
    let key_path = tls.key_path.clone();
    let tls_settings = tls.clone();

    // Track last modification times.
    let mut last_cert_mtime = file_mtime(&cert_path);
    let mut last_key_mtime = file_mtime(&key_path);

    let tx_clone = tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(check_interval).await;

            let cert_mtime = file_mtime(&cert_path);
            let key_mtime = file_mtime(&key_path);

            // Check if either file changed.
            let changed = cert_mtime != last_cert_mtime || key_mtime != last_key_mtime;
            if !changed {
                continue;
            }

            info!("TLS cert/key file change detected, reloading");

            match load_server_config(&tls_settings) {
                Ok(new_config) => {
                    last_cert_mtime = cert_mtime;
                    last_key_mtime = key_mtime;

                    let _ = tx_clone.send(Some(Arc::new(new_config)));

                    state.audit_record(
                        AuditEvent::CertRotation,
                        None,
                        "tls_reloader",
                        "TLS certificate reloaded successfully",
                    );

                    info!("TLS certificate reloaded successfully");
                }
                Err(e) => {
                    state.audit_record(
                        AuditEvent::CertRotationFailed,
                        None,
                        "tls_reloader",
                        &format!("TLS reload failed: {e}"),
                    );

                    warn!(error = %e, "TLS certificate reload failed, keeping current config");
                }
            }
        }
    });

    Ok((rx, tx))
}

/// Build a `tokio_rustls::TlsAcceptor` from the latest config in the watch channel.
pub fn acceptor_from_watch(rx: &TlsAcceptorWatch) -> Option<tokio_rustls::TlsAcceptor> {
    rx.borrow()
        .as_ref()
        .map(|config| tokio_rustls::TlsAcceptor::from(Arc::clone(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_nonexistent_cert_fails() {
        let tls = TlsSettings {
            cert_path: "/nonexistent/cert.pem".into(),
            key_path: "/nonexistent/key.pem".into(),
            cert_reload_interval_secs: None,
        };
        assert!(load_server_config(&tls).is_err());
    }
}
