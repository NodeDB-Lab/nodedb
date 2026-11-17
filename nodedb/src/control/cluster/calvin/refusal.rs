// SPDX-License-Identifier: BUSL-1.1

//! The bytes a participant's typed stage refusal crosses the sequencer log in.
//!
//! A participant whose stage refuses its slice votes abort with the refusal's
//! Data-Plane [`ErrorCode`]. A refused `RETURNING` evaluation adds the
//! `nextval` and `setval` results it drew: they stand, as in PostgreSQL, so
//! the writer's session takes them though the statement fails. The vote
//! carries both as the rkyv bytes of [`RefusalWire`], opaque to the
//! sequencer. The coordinator decodes the verdict's refusal back into the
//! code, so the client sees the SQLSTATE a direct write renders.

use nodedb_cluster::rpc_codec::DataPlaneErrorCode;
use nodedb_types::{DatabaseId, TenantId};

use crate::Error;
use crate::bridge::envelope::ErrorCode;
use crate::control::wal_replication::types::FrozenSequenceEffect;

/// A participant's refusal of its slice.
#[derive(Debug, Clone, PartialEq)]
pub struct StageRefusal {
    pub code: ErrorCode,
    /// The sequence results of a refused `RETURNING` evaluation. `None` for
    /// any other refusal.
    pub sequences: Option<RefusedSequences>,
}

/// The `nextval` and `setval` results a refused `RETURNING` evaluation
/// drew, in invocation order, and the scope its sequences live in: the
/// txn's database and tenant.
#[derive(Debug, Clone, PartialEq)]
pub struct RefusedSequences {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub effects: Vec<FrozenSequenceEffect>,
}

/// The wire form of a [`StageRefusal`].
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct RefusalWire {
    code: DataPlaneErrorCode,
    sequences: Option<SequencesWire>,
}

/// The wire form of a [`RefusedSequences`].
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct SequencesWire {
    database_id: u64,
    tenant_id: u64,
    effects: Vec<EffectWire>,
}

/// The wire form of a [`FrozenSequenceEffect`].
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum EffectWire {
    Nextval { name: String, value: i64 },
    Setval { name: String, value: i64 },
}

impl From<&RefusedSequences> for SequencesWire {
    fn from(sequences: &RefusedSequences) -> Self {
        Self {
            database_id: sequences.database_id.as_u64(),
            tenant_id: sequences.tenant_id.as_u64(),
            effects: sequences.effects.iter().map(EffectWire::from).collect(),
        }
    }
}

impl From<SequencesWire> for RefusedSequences {
    fn from(wire: SequencesWire) -> Self {
        Self {
            database_id: DatabaseId::new(wire.database_id),
            tenant_id: TenantId::new(wire.tenant_id),
            effects: wire
                .effects
                .into_iter()
                .map(FrozenSequenceEffect::from)
                .collect(),
        }
    }
}

impl From<&FrozenSequenceEffect> for EffectWire {
    fn from(effect: &FrozenSequenceEffect) -> Self {
        match effect {
            FrozenSequenceEffect::Nextval { name, value } => Self::Nextval {
                name: name.clone(),
                value: *value,
            },
            FrozenSequenceEffect::Setval { name, value } => Self::Setval {
                name: name.clone(),
                value: *value,
            },
        }
    }
}

impl From<EffectWire> for FrozenSequenceEffect {
    fn from(effect: EffectWire) -> Self {
        match effect {
            EffectWire::Nextval { name, value } => Self::Nextval { name, value },
            EffectWire::Setval { name, value } => Self::Setval { name, value },
        }
    }
}

/// Encode `code` for an abort vote, with no sequence results.
pub fn encode_refusal(code: &ErrorCode) -> Result<Vec<u8>, Error> {
    encode_refusal_with_sequences(code, None)
}

/// Encode `code` and the `sequences` a refused `RETURNING` evaluation
/// drew, for an abort vote.
pub fn encode_refusal_with_sequences(
    code: &ErrorCode,
    sequences: Option<&RefusedSequences>,
) -> Result<Vec<u8>, Error> {
    let wire = RefusalWire {
        code: DataPlaneErrorCode::from(code.clone()),
        sequences: sequences.map(SequencesWire::from),
    };
    rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
        .map(|bytes| bytes.to_vec())
        .map_err(|e| Error::Internal {
            detail: format!("encode a Calvin stage refusal {code:?}: {e}"),
        })
}

/// `bytes` copied to the alignment rkyv reads at.
fn aligned(bytes: &[u8]) -> rkyv::util::AlignedVec<16> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    aligned
}

/// Decode the refusal an abort verdict carries.
pub fn decode_refusal(bytes: &[u8]) -> Result<StageRefusal, Error> {
    rkyv::from_bytes::<RefusalWire, rkyv::rancor::Error>(&aligned(bytes))
        .map(|wire| StageRefusal {
            code: ErrorCode::from(wire.code),
            sequences: wire.sequences.map(RefusedSequences::from),
        })
        .map_err(|e| Error::Internal {
            detail: format!(
                "decode the stage refusal of a Calvin abort verdict ({} bytes): {e}",
                bytes.len()
            ),
        })
}

/// Encode `sequences` alone, for a forwarded submit's reply.
pub fn encode_refused_sequences(sequences: &RefusedSequences) -> Result<Vec<u8>, Error> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&SequencesWire::from(sequences))
        .map(|bytes| bytes.to_vec())
        .map_err(|e| Error::Internal {
            detail: format!("encode the sequence results of a Calvin refusal: {e}"),
        })
}

/// Decode the sequence results a forwarded submit's reply carries.
pub fn decode_refused_sequences(bytes: &[u8]) -> Result<RefusedSequences, Error> {
    rkyv::from_bytes::<SequencesWire, rkyv::rancor::Error>(&aligned(bytes))
        .map(RefusedSequences::from)
        .map_err(|e| Error::Internal {
            detail: format!(
                "decode the sequence results of a Calvin refusal ({} bytes): {e}",
                bytes.len()
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_survives_the_round_trip() {
        let codes = [
            ErrorCode::RejectedConstraint {
                constraint: "primary_key_immutable".into(),
                detail: "UPDATE cannot change primary key 'id'".into(),
            },
            ErrorCode::RejectedConstraint {
                constraint: "not_null".into(),
                detail: "column 'v' is NOT NULL".into(),
            },
            ErrorCode::BalanceViolation {
                collection: "ledger".into(),
                detail: "debits exceed credits".into(),
            },
        ];
        for code in codes {
            let bytes = encode_refusal(&code).expect("encode");
            assert_eq!(
                decode_refusal(&bytes).expect("decode"),
                StageRefusal {
                    code,
                    sequences: None,
                }
            );
        }
    }

    fn sequences() -> RefusedSequences {
        RefusedSequences {
            database_id: DatabaseId::new(3),
            tenant_id: TenantId::new(7),
            effects: vec![
                FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: 7,
                },
                FrozenSequenceEffect::Setval {
                    name: "t".into(),
                    value: 40,
                },
                FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: 8,
                },
            ],
        }
    }

    #[test]
    fn a_refusal_keeps_its_sequence_results_in_order() {
        let bytes = encode_refusal_with_sequences(&ErrorCode::DivisionByZero, Some(&sequences()))
            .expect("encode");
        assert_eq!(
            decode_refusal(&bytes).expect("decode"),
            StageRefusal {
                code: ErrorCode::DivisionByZero,
                sequences: Some(sequences()),
            }
        );
    }

    #[test]
    fn sequence_results_survive_their_own_round_trip() {
        let bytes = encode_refused_sequences(&sequences()).expect("encode");
        assert_eq!(
            decode_refused_sequences(&bytes).expect("decode"),
            sequences()
        );
        assert!(matches!(
            decode_refused_sequences(&[0xde, 0xad]),
            Err(Error::Internal { .. })
        ));
    }

    #[test]
    fn undecodable_bytes_are_an_internal_error() {
        assert!(matches!(
            decode_refusal(&[0xde, 0xad]),
            Err(Error::Internal { .. })
        ));
    }
}
