// SPDX-License-Identifier: BUSL-1.1

//! The stored form of a boot verdict in a core's sparse store.
//!
//! Boot reads every core's verdicts before any core opens its store, so a
//! verdict decides its record whichever core stored it.

use std::collections::BTreeMap;

use super::never_durable::BootVerdictOutcome;

/// One verdict as the sparse store holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredVerdict {
    /// The LSN of the judged `TransactionRedo` record.
    pub lsn: u64,
    pub outcome: BootVerdictOutcome,
}

impl StoredVerdict {
    pub fn to_bytes(self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("boot verdict on the redo at lsn {} encode: {e}", self.lsn),
        })
    }

    /// Decode the verdict stored under `lsn`.
    pub fn from_bytes(lsn: u64, bytes: &[u8]) -> crate::Result<Self> {
        let verdict: Self = zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("the boot verdict stored at lsn {lsn} does not decode: {e}"),
        })?;
        if verdict.lsn != lsn {
            return Err(crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!(
                    "the boot verdict stored at lsn {lsn} names lsn {}",
                    verdict.lsn
                ),
            });
        }
        Ok(verdict)
    }
}

/// Decode every stored verdict, by LSN.
pub fn decode_stored_verdicts(
    stored: Vec<(u64, Vec<u8>)>,
) -> crate::Result<BTreeMap<u64, BootVerdictOutcome>> {
    stored
        .into_iter()
        .map(|(lsn, bytes)| StoredVerdict::from_bytes(lsn, &bytes).map(|v| (lsn, v.outcome)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_verdict_round_trips_and_names_its_lsn() {
        for outcome in [
            BootVerdictOutcome::Passed,
            BootVerdictOutcome::Refused {
                final_refusal: true,
            },
        ] {
            let verdict = StoredVerdict { lsn: 12, outcome };
            let bytes = verdict.to_bytes().expect("encode");
            assert_eq!(
                StoredVerdict::from_bytes(12, &bytes).expect("decode"),
                verdict
            );
            assert!(StoredVerdict::from_bytes(13, &bytes).is_err());
        }
    }
}
