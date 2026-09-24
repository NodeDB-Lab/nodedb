// SPDX-License-Identifier: BUSL-1.1

//! Versioned, self-describing WAL payloads for CRDT delta records.

use nodedb_types::sync::wire::SyncProvenance;

const CRDT_DELTA_WAL_FORMAT_V2: u8 = 2;
const CRDT_DELTA_WAL_FORMAT_V3: u8 = 3;
const CRDT_DELTA_WAL_FORMAT_V4: u8 = 4;

/// Normalized CRDT WAL payload used by replay.
///
/// Current writers encode V4 for signed sync deltas and V3 otherwise.
/// Decoding also accepts the previous fenced V2 and exact legacy shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CrdtDeltaSigning {
    pub auth_user_id: u64,
    pub auth_device_id: u64,
    pub auth_seq_no: u64,
    pub delta_signature: [u8; 32],
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrdtDeltaWalPayload {
    pub bytes: Vec<u8>,
    pub collection: Option<String>,
    pub provenance: Option<SyncProvenance>,
    pub expected_frontier_digest: Option<[u8; 32]>,
    pub document_id: Option<String>,
    pub surrogate: Option<u32>,
    pub signing: Option<CrdtDeltaSigning>,
    /// The peer that produced the delta. Replay validates under it, so a
    /// dead-letter entry replay stores names the same peer the live apply did.
    pub peer_id: u64,
}

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct CrdtDeltaWalPayloadV4 {
    format: u8,
    bytes: Vec<u8>,
    collection: Option<String>,
    provenance: Option<SyncProvenance>,
    expected_frontier_digest: Option<[u8; 32]>,
    document_id: Option<String>,
    surrogate: Option<u32>,
    peer_id: u64,
    auth_user_id: u64,
    auth_device_id: u64,
    auth_seq_no: u64,
    delta_signature: [u8; 32],
    signing_required: bool,
}

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct CrdtDeltaWalPayloadV3 {
    format: u8,
    bytes: Vec<u8>,
    collection: Option<String>,
    provenance: Option<SyncProvenance>,
    expected_frontier_digest: Option<[u8; 32]>,
    document_id: Option<String>,
    surrogate: Option<u32>,
    peer_id: u64,
}

/// Exact fenced wire shape emitted before sparse replay identity was retained.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct CrdtDeltaWalPayloadV2 {
    format: u8,
    bytes: Vec<u8>,
    collection: Option<String>,
    provenance: Option<SyncProvenance>,
    expected_frontier_digest: Option<[u8; 32]>,
}

/// Exact wire shape emitted by pre-fence binaries.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct LegacyCrdtDeltaWalPayload {
    bytes: Vec<u8>,
    collection: Option<String>,
    provenance: Option<SyncProvenance>,
}

impl CrdtDeltaWalPayload {
    pub(crate) fn new(
        bytes: Vec<u8>,
        collection: Option<String>,
        provenance: Option<SyncProvenance>,
        expected_frontier_digest: Option<[u8; 32]>,
        document_id: Option<String>,
        surrogate: Option<u32>,
    ) -> Self {
        Self {
            bytes,
            collection,
            provenance,
            expected_frontier_digest,
            document_id,
            surrogate,
            signing: None,
            peer_id: 0,
        }
    }

    pub(crate) fn with_signing(mut self, signing: CrdtDeltaSigning) -> Self {
        self.signing = Some(signing);
        self
    }

    pub(crate) fn with_peer_id(mut self, peer_id: u64) -> Self {
        self.peer_id = peer_id;
        self
    }

    /// Encode the current explicit wire format.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, zerompk::Error> {
        if let Some(signing) = self.signing {
            return zerompk::to_msgpack_vec(&CrdtDeltaWalPayloadV4 {
                format: CRDT_DELTA_WAL_FORMAT_V4,
                bytes: self.bytes.clone(),
                collection: self.collection.clone(),
                provenance: self.provenance.clone(),
                expected_frontier_digest: self.expected_frontier_digest,
                document_id: self.document_id.clone(),
                surrogate: self.surrogate,
                peer_id: self.peer_id,
                auth_user_id: signing.auth_user_id,
                auth_device_id: signing.auth_device_id,
                auth_seq_no: signing.auth_seq_no,
                delta_signature: signing.delta_signature,
                signing_required: signing.required,
            });
        }
        zerompk::to_msgpack_vec(&CrdtDeltaWalPayloadV3 {
            format: CRDT_DELTA_WAL_FORMAT_V3,
            bytes: self.bytes.clone(),
            collection: self.collection.clone(),
            provenance: self.provenance.clone(),
            expected_frontier_digest: self.expected_frontier_digest,
            document_id: self.document_id.clone(),
            surrogate: self.surrogate,
            peer_id: self.peer_id,
        })
    }

    /// Decode V3, the previous exact V2 form, or the exact legacy three-field
    /// representation. No missing-field defaults or arity widening are used.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, zerompk::Error> {
        if let Ok(v4) = zerompk::from_msgpack::<CrdtDeltaWalPayloadV4>(bytes)
            && v4.format == CRDT_DELTA_WAL_FORMAT_V4
        {
            return Ok(Self::new(
                v4.bytes,
                v4.collection,
                v4.provenance,
                v4.expected_frontier_digest,
                v4.document_id,
                v4.surrogate,
            )
            .with_signing(CrdtDeltaSigning {
                auth_user_id: v4.auth_user_id,
                auth_device_id: v4.auth_device_id,
                auth_seq_no: v4.auth_seq_no,
                delta_signature: v4.delta_signature,
                required: v4.signing_required,
            })
            .with_peer_id(v4.peer_id));
        }
        if let Ok(v3) = zerompk::from_msgpack::<CrdtDeltaWalPayloadV3>(bytes)
            && v3.format == CRDT_DELTA_WAL_FORMAT_V3
        {
            return Ok(Self::new(
                v3.bytes,
                v3.collection,
                v3.provenance,
                v3.expected_frontier_digest,
                v3.document_id,
                v3.surrogate,
            )
            .with_peer_id(v3.peer_id));
        }
        if let Ok(v2) = zerompk::from_msgpack::<CrdtDeltaWalPayloadV2>(bytes)
            && v2.format == CRDT_DELTA_WAL_FORMAT_V2
        {
            return Ok(Self::new(
                v2.bytes,
                v2.collection,
                v2.provenance,
                v2.expected_frontier_digest,
                None,
                None,
            ));
        }
        zerompk::from_msgpack::<LegacyCrdtDeltaWalPayload>(bytes).map(|legacy| {
            Self::new(
                legacy.bytes,
                legacy.collection,
                legacy.provenance,
                None,
                None,
                None,
            )
        })
    }

    #[cfg(test)]
    fn encode_v2_for_test(&self) -> Result<Vec<u8>, zerompk::Error> {
        zerompk::to_msgpack_vec(&CrdtDeltaWalPayloadV2 {
            format: CRDT_DELTA_WAL_FORMAT_V2,
            bytes: self.bytes.clone(),
            collection: self.collection.clone(),
            provenance: self.provenance.clone(),
            expected_frontier_digest: self.expected_frontier_digest,
        })
    }

    #[cfg(test)]
    fn encode_legacy_for_test(&self) -> Result<Vec<u8>, zerompk::Error> {
        zerompk::to_msgpack_vec(&LegacyCrdtDeltaWalPayload {
            bytes: self.bytes.clone(),
            collection: self.collection.clone(),
            provenance: self.provenance.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_exact_legacy_three_field_payload() {
        let legacy =
            CrdtDeltaWalPayload::new(vec![1, 2], Some("docs".into()), None, None, None, None);
        let decoded =
            CrdtDeltaWalPayload::decode(&legacy.encode_legacy_for_test().expect("encode"))
                .expect("decode legacy");
        assert_eq!(decoded, legacy);
    }

    #[test]
    fn decodes_previous_fenced_v2_without_projection_identity() {
        let v2 = CrdtDeltaWalPayload::new(
            vec![3, 4],
            Some("docs".into()),
            None,
            Some([0xa5; 32]),
            None,
            None,
        );
        let decoded = CrdtDeltaWalPayload::decode(&v2.encode_v2_for_test().expect("encode"))
            .expect("decode v2");
        assert_eq!(decoded, v2);
    }

    #[test]
    fn v4_round_trips_authenticated_signing_admission() {
        let payload = CrdtDeltaWalPayload::new(
            vec![8, 9],
            Some("docs".into()),
            None,
            Some([0x11; 32]),
            Some("doc-2".into()),
            Some(43),
        )
        .with_signing(CrdtDeltaSigning {
            auth_user_id: 7,
            auth_device_id: 9,
            auth_seq_no: 11,
            delta_signature: [0x22; 32],
            required: true,
        });
        let decoded =
            CrdtDeltaWalPayload::decode(&payload.encode().expect("encode")).expect("decode v4");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn v3_round_trips_fenced_projection_identity() {
        let payload = CrdtDeltaWalPayload::new(
            vec![3, 4],
            Some("docs".into()),
            None,
            Some([0xa5; 32]),
            Some("doc-1".into()),
            Some(42),
        );
        let decoded =
            CrdtDeltaWalPayload::decode(&payload.encode().expect("encode")).expect("decode v3");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn the_producing_peer_round_trips_in_both_current_formats() {
        let unsigned =
            CrdtDeltaWalPayload::new(vec![1], Some("docs".into()), None, None, None, None)
                .with_peer_id(0xFEED);
        let decoded =
            CrdtDeltaWalPayload::decode(&unsigned.encode().expect("encode")).expect("decode v3");
        assert_eq!(decoded.peer_id, 0xFEED);

        let signed = unsigned.clone().with_signing(CrdtDeltaSigning {
            auth_user_id: 1,
            auth_device_id: 2,
            auth_seq_no: 3,
            delta_signature: [4; 32],
            required: true,
        });
        let decoded =
            CrdtDeltaWalPayload::decode(&signed.encode().expect("encode")).expect("decode v4");
        assert_eq!(decoded.peer_id, 0xFEED);
    }
}
