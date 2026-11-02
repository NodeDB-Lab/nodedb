// SPDX-License-Identifier: BUSL-1.1

//! Admission helpers for CRDT WAL replay: signed-record metadata check and replay signing admission.

/// Check the admission metadata of a signed record against what its writer
/// stamps. The writer copies the session's producer id and the frame's seq
/// into both the provenance and the signing fields, so a signed record
/// without provenance, or with fields that disagree, is not one it wrote.
/// Such a record at `lsn` is a corrupt WAL record.
pub(super) fn signed_record_metadata(
    payload: &crate::wal::CrdtDeltaWalPayload,
    lsn: u64,
) -> crate::Result<()> {
    let corrupt =
        |detail: String| crate::Error::Wal(nodedb_wal::WalError::CorruptRecord { lsn, detail });
    let Some(signing) = payload.signing else {
        return Ok(());
    };
    let Some(provenance) = payload.provenance.as_ref() else {
        return Err(corrupt(format!(
            "signed delta for {} carries no provenance",
            payload.collection
        )));
    };
    if provenance.producer_id != signing.auth_device_id || provenance.seq != signing.auth_seq_no {
        return Err(corrupt(format!(
            "signed delta for {} names producer {} seq {} in its provenance and device {} seq {} \
             in its signing fields",
            payload.collection,
            provenance.producer_id,
            provenance.seq,
            signing.auth_device_id,
            signing.auth_seq_no
        )));
    }
    Ok(())
}

/// The signing admission a committed record replays under: the one its live
/// apply ran under.
///
/// A signed record carries the signing fields the session admitted, already
/// verified, so it replays preverified. An absent required signature still
/// refuses it, as it did live. An unsigned record replays the live local
/// apply's admission: no signature, checked against the collection's signing
/// policy.
pub(super) fn replayed_admission(
    payload: &crate::wal::CrdtDeltaWalPayload,
) -> crate::engine::crdt::tenant_state::DeltaSigningAdmission {
    match payload.signing {
        Some(signing) => crate::engine::crdt::tenant_state::DeltaSigningAdmission {
            auth: nodedb_crdt::CrdtAuthContext {
                user_id: signing.auth_user_id,
                device_id: signing.auth_device_id,
                seq_no: signing.auth_seq_no,
                delta_signature: signing.delta_signature,
                ..nodedb_crdt::CrdtAuthContext::default()
            },
            required: signing.required,
            preverified: true,
        },
        None => crate::engine::crdt::tenant_state::DeltaSigningAdmission {
            auth: nodedb_crdt::CrdtAuthContext::default(),
            required: false,
            preverified: false,
        },
    }
}
