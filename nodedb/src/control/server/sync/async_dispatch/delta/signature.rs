// SPDX-License-Identifier: BUSL-1.1

//! Delta signature verification against the session's handshake-bound identity.

use super::super::super::wire::DeltaPushMsg;

pub(super) fn delta_signature_valid(
    delta_msg: &DeltaPushMsg,
    user_id: u64,
    session_signing_key: Option<&[u8; 32]>,
    session_producer_id: u64,
    session_epoch: u64,
    signing_required: bool,
) -> bool {
    let signed = delta_msg.delta_signature != [0; 32];
    if !signed {
        return !signing_required;
    }
    if session_producer_id == 0 || delta_msg.device_id != session_producer_id {
        return false;
    }
    session_signing_key.is_some_and(|key| {
        let mut verifier = nodedb_crdt::DeltaSigner::new();
        verifier.register_key(user_id, *key);
        verifier
            .verify_sync_delta(
                user_id,
                session_producer_id,
                session_epoch,
                delta_msg.seq,
                &delta_msg.collection,
                &delta_msg.document_id,
                &delta_msg.delta,
                &delta_msg.delta_signature,
            )
            .is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta() -> DeltaPushMsg {
        DeltaPushMsg {
            collection: "orders".into(),
            document_id: "order-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 42,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::new(1_000, 0),
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[test]
    fn required_signing_rejects_unsigned_delta() {
        assert!(!delta_signature_valid(
            &delta(),
            7,
            Some(&[0x42; 32]),
            9,
            3,
            true,
        ));
        assert!(delta_signature_valid(&delta(), 7, None, 0, 0, false));
    }

    #[test]
    fn signature_binds_payload_user_device_and_sequence() {
        let key = [0x42; 32];
        let mut signer = nodedb_crdt::DeltaSigner::new();
        signer.register_key(7, key);
        let mut msg = delta();
        msg.delta = b"exact delta".to_vec();
        msg.device_id = 9;
        msg.seq = 11;
        msg.delta_signature = signer
            .sign_sync_delta(
                7,
                msg.device_id,
                3,
                msg.seq,
                &msg.collection,
                &msg.document_id,
                &msg.delta,
            )
            .expect("sign");
        assert!(delta_signature_valid(&msg, 7, Some(&key), 9, 3, true));

        let mut tampered = msg.clone();
        tampered.delta.push(0);
        assert!(!delta_signature_valid(&tampered, 7, Some(&key), 9, 3, true));
        assert!(!delta_signature_valid(&msg, 8, Some(&key), 9, 3, true));
        assert!(!delta_signature_valid(&msg, 7, Some(&key), 10, 3, true));
        assert!(!delta_signature_valid(&msg, 7, Some(&key), 9, 4, true));
    }
}
