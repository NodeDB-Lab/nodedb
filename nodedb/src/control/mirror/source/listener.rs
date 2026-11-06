// SPDX-License-Identifier: BUSL-1.1

//! Start the mirror listener from the `[mirror]` config.

use std::sync::Arc;

use nodedb_cluster::TlsCredentials;
use nodedb_cluster::mirror::{MirrorAllowlist, bind_mirror_endpoint, serve_mirror_source};

use crate::config::server::{MirrorSettings, MirrorTlsPaths};
use crate::control::cluster::tls::{read_private_key, read_single_cert};
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::service::SourceService;

/// Read the listener's certificate, key and peer CA. No field of these
/// credentials signs a cluster frame, so the frame secret stays empty.
pub fn read_mirror_credentials(paths: &MirrorTlsPaths) -> crate::Result<TlsCredentials> {
    let cert = read_single_cert(&paths.cert)?;
    let spki_pin =
        nodedb_cluster::transport::spki_pin_from_cert_der(cert.as_ref()).map_err(|e| {
            crate::Error::Config {
                detail: format!("mirror certificate {}: {e}", paths.cert.display()),
            }
        })?;
    Ok(TlsCredentials {
        cert,
        key: read_private_key(&paths.key)?,
        ca_cert: read_single_cert(&paths.ca)?,
        additional_ca_certs: Vec::new(),
        crls: Vec::new(),
        cluster_secret: [0; 32],
        spki_pin,
        bootstrap_peer_spki: None,
    })
}

/// Bind and serve the mirror listener, when the config names one. A config that
/// names one and cannot bind it fails the boot: an operator asked for it.
pub fn spawn_mirror_listener(
    shared: &Arc<SharedState>,
    settings: &MirrorSettings,
) -> crate::Result<()> {
    let Some(listen) = settings.listen else {
        return Ok(());
    };
    settings.validate()?;
    let (Some(tls), Some(cluster_name)) = (&settings.tls, &settings.cluster_name) else {
        return Err(crate::Error::Config {
            detail: "[mirror] listen needs tls and cluster_name".to_string(),
        });
    };
    let credentials = read_mirror_credentials(tls)?;
    let allowlist = Arc::new(MirrorAllowlist::new(settings.allowed_clients()?));
    let endpoint =
        bind_mirror_endpoint(listen, &credentials, Arc::clone(&allowlist)).map_err(|e| {
            crate::Error::Config {
                detail: format!("[mirror] listen {listen}: {e}"),
            }
        })?;
    let service = Arc::new(SourceService::new(Arc::clone(shared), cluster_name.clone()));
    tracing::info!(%listen, clients = settings.clients.len(), "mirror listener running");
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_listener",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let (stop, stopped) = tokio::sync::watch::channel(false);
            let serving = tokio::spawn(serve_mirror_source(endpoint, allowlist, service, stopped));
            shutdown.wait_cancelled().await;
            if stop.send(true).is_err() {
                tracing::debug!("mirror listener stopped before shutdown");
            }
            if let Err(error) = serving.await {
                tracing::warn!(%error, "mirror listener task ended abnormally");
            }
        },
    );
    Ok(())
}
