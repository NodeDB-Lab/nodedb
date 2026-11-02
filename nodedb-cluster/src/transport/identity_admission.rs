// SPDX-License-Identifier: BUSL-1.1

//! Narrow enrollment exception for a CA-verified peer not yet in topology.

use crate::rpc_codec::RaftRpc;
use crate::transport::peer_identity_store::PeerIdentityStore;
use crate::transport::peer_identity_verifier::{
    enrollment_identity_from_cert_der, spiffe_id_from_cert_der, spki_pin_from_cert_der,
};

/// Whether an unknown peer's request is a self-consistent enrollment request.
///
/// The HMAC-authenticated envelope node id, request node id, advertised SPKI,
/// and presented leaf certificate must agree. No other RPC is admitted before
/// topology commit makes the identity a normal pinned peer.
pub(crate) fn enrollment_matches<S: PeerIdentityStore + ?Sized>(
    request: &RaftRpc,
    from_node_id: u64,
    cert_der: &[u8],
    store: &S,
) -> bool {
    let RaftRpc::JoinRequest(join) = request else {
        return false;
    };
    if join.node_id != from_node_id {
        return false;
    }
    let Some((enrollment_node_id, expires_at_ms)) = enrollment_identity_from_cert_der(cert_der)
    else {
        return false;
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(u64::MAX);
    if enrollment_node_id != join.node_id || expires_at_ms <= now_ms {
        return false;
    }
    let Ok(cert_spki) = spki_pin_from_cert_der(cert_der) else {
        return false;
    };
    if !store.is_preauthorized(&cert_spki)
        || store
            .find_by_spki(&cert_spki)
            .is_some_and(|owner| owner.node_id != join.node_id)
        || join.spki_pin.as_deref() != Some(cert_spki.as_slice())
    {
        return false;
    }
    match &join.spiffe_id {
        Some(advertised) => spiffe_id_from_cert_der(cert_der).as_deref() == Some(advertised),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, KeyPair};

    use super::*;
    use crate::rpc_codec::{JoinRequest, PingRequest};

    fn cert() -> (Vec<u8>, [u8; 32]) {
        let key = KeyPair::generate().unwrap();
        let params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        let cert = params.self_signed(&key).unwrap().der().to_vec();
        let pin = spki_pin_from_cert_der(&cert).unwrap();
        (cert, pin)
    }

    fn enrollment_cert(node_id: u64) -> (Vec<u8>, [u8; 32]) {
        let key = KeyPair::generate().unwrap();
        let expires_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let params = CertificateParams::new(vec![
            "localhost".into(),
            format!("nodedb-enrollment-{node_id}-{expires_at_ms}"),
        ])
        .unwrap();
        let cert = params.self_signed(&key).unwrap().der().to_vec();
        let pin = spki_pin_from_cert_der(&cert).unwrap();
        (cert, pin)
    }

    fn join(node_id: u64, pin: [u8; 32]) -> RaftRpc {
        RaftRpc::JoinRequest(JoinRequest {
            node_id,
            listen_addr: "127.0.0.1:9400".into(),
            wire_version: 1,
            build_id: nodedb_types::wire_version::WIRE_BUILD_ID.to_owned(),
            spiffe_id: None,
            spki_pin: Some(pin.to_vec()),
            swim_addr: None,
        })
    }

    #[test]
    fn enrollment_marker_is_bound_to_requested_node() {
        let (cert, pin) = enrollment_cert(7);
        let store = crate::transport::topology_identity_store::TopologyIdentityStore::new();
        assert!(store.preauthorize(
            pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(enrollment_matches(&join(7, pin), 7, &cert, &store));
        assert!(!enrollment_matches(&join(8, pin), 8, &cert, &store));
    }

    #[test]
    fn matching_join_requires_marker_and_live_preauthorization() {
        let (plain_cert, plain_pin) = cert();
        let (enrollment_cert, enrollment_pin) = enrollment_cert(7);
        let store = crate::transport::topology_identity_store::TopologyIdentityStore::new();
        assert!(!enrollment_matches(
            &join(7, plain_pin),
            7,
            &plain_cert,
            &store
        ));
        assert!(!enrollment_matches(
            &join(7, enrollment_pin),
            7,
            &enrollment_cert,
            &store
        ));
        assert!(store.preauthorize(
            enrollment_pin,
            std::time::Instant::now() + std::time::Duration::from_secs(60)
        ));
        assert!(enrollment_matches(
            &join(7, enrollment_pin),
            7,
            &enrollment_cert,
            &store
        ));
        assert!(!enrollment_matches(
            &RaftRpc::Ping(PingRequest {
                sender_id: 7,
                topology_version: 1,
                oldest_snapshot: crate::snapshot_board::NO_SNAPSHOT_REPORT,
            }),
            7,
            &enrollment_cert,
            &store
        ));
    }
}
