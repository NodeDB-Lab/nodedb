// SPDX-License-Identifier: BUSL-1.1

//! The catalog row of one credential. The secret is sealed before the row
//! is proposed, so the metadata log, the catalog file and every snapshot
//! hold ciphertext only.

use serde::{Deserialize, Serialize};

/// The credential type, readable without opening the secret.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CredentialKind {
    S3,
    Bearer,
    Sas,
}

impl CredentialKind {
    /// The `TYPE` keyword.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Bearer => "bearer",
            Self::Sas => "sas",
        }
    }
}

/// One `CREATE CREDENTIAL` row.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct StoredCredential {
    pub tenant_id: u64,
    pub name: String,
    pub kind: CredentialKind,
    /// The sealed `CredentialSecret`.
    pub sealed: Vec<u8>,
    /// The user that created it.
    pub owner: String,
    pub created_at_ms: u64,
}
