// SPDX-License-Identifier: BUSL-1.1

//! Seal and open credential secrets: AES-256-GCM under a key derived from
//! the credential key with HKDF-SHA256.
//!
//! A sealed secret is `nonce (12 bytes) || ciphertext || tag`. The
//! additional data binds it to its tenant, name and type, so a sealed blob
//! copied onto another credential row fails to open.

use aes_gcm::aead::{Aead as _, KeyInit as _, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use super::kek::CredentialKek;
use super::secret::CredentialSecret;

/// HKDF salt: the sealing scheme and its version.
const HKDF_SALT: &[u8] = b"nodedb-credential-v1";
/// HKDF info: what the derived key encrypts.
const HKDF_INFO: &[u8] = b"credential-secret";
/// Bytes of an AES-GCM nonce.
const NONCE_LEN: usize = 12;

/// Why a secret did not seal or open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SealError {
    #[error("credential key derivation failed")]
    Derive,
    #[error("the OS random source failed")]
    Random,
    #[error("credential secret encoding failed")]
    Encode,
    #[error("credential secret encryption failed")]
    Encrypt,
    #[error(
        "the sealed credential does not open with this node's credential key; every node needs \
         the same [credential_encryption] key file, or the credential must be re-created"
    )]
    Open,
}

/// The additional data of one credential row.
fn aad(tenant_id: u64, name: &str, kind: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + name.len() + kind.len());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    out.extend_from_slice(&(name.len() as u64).to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(kind.as_bytes());
    out
}

fn cipher(kek: &CredentialKek) -> Result<Aes256Gcm, SealError> {
    let mut key = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(HKDF_SALT), kek.bytes())
        .expand(HKDF_INFO, &mut *key)
        .map_err(|_| SealError::Derive)?;
    Aes256Gcm::new_from_slice(&*key).map_err(|_| SealError::Derive)
}

/// Seal `secret` for credential `name` of `tenant_id`.
pub fn seal_secret(
    kek: &CredentialKek,
    tenant_id: u64,
    name: &str,
    secret: &CredentialSecret,
) -> Result<Vec<u8>, SealError> {
    let plaintext = Zeroizing::new(zerompk::to_msgpack_vec(secret).map_err(|_| SealError::Encode)?);
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|_| SealError::Random)?;
    let ciphertext = cipher(kek)?
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: &plaintext,
                aad: &aad(tenant_id, name, secret.type_name()),
            },
        )
        .map_err(|_| SealError::Encrypt)?;
    let mut sealed = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    sealed.extend_from_slice(&nonce);
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

/// Open the sealed secret of credential `name` of `tenant_id`, whose type
/// is `kind`.
pub fn open_secret(
    kek: &CredentialKek,
    tenant_id: u64,
    name: &str,
    kind: &str,
    sealed: &[u8],
) -> Result<CredentialSecret, SealError> {
    let (nonce, ciphertext) = sealed.split_at_checked(NONCE_LEN).ok_or(SealError::Open)?;
    let nonce: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| SealError::Open)?;
    let plaintext = Zeroizing::new(
        cipher(kek)?
            .decrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: ciphertext,
                    aad: &aad(tenant_id, name, kind),
                },
            )
            .map_err(|_| SealError::Open)?,
    );
    let secret: CredentialSecret =
        zerompk::from_msgpack(&plaintext).map_err(|_| SealError::Open)?;
    if secret.type_name() != kind {
        return Err(SealError::Open);
    }
    Ok(secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bearer() -> CredentialSecret {
        CredentialSecret::Bearer {
            token: "eyJ-token".into(),
        }
    }

    #[test]
    fn a_sealed_secret_opens_only_on_its_own_row_and_key() {
        let kek = CredentialKek::from_bytes([7; 32]);
        let sealed = seal_secret(&kek, 1, "earthdata", &bearer()).expect("seals");
        assert!(!sealed.windows(9).any(|w| w == b"eyJ-token"));
        let opened = open_secret(&kek, 1, "earthdata", "bearer", &sealed).expect("opens");
        assert_eq!(opened, bearer());
        let other_key = CredentialKek::from_bytes([8; 32]);
        assert_eq!(
            open_secret(&other_key, 1, "earthdata", "bearer", &sealed),
            Err(SealError::Open)
        );
        assert_eq!(
            open_secret(&kek, 2, "earthdata", "bearer", &sealed),
            Err(SealError::Open)
        );
        assert_eq!(
            open_secret(&kek, 1, "other", "bearer", &sealed),
            Err(SealError::Open)
        );
        assert_eq!(
            open_secret(&kek, 1, "earthdata", "sas", &sealed),
            Err(SealError::Open)
        );
        assert_eq!(
            open_secret(&kek, 1, "earthdata", "bearer", &sealed[..5]),
            Err(SealError::Open)
        );
    }

    #[test]
    fn debug_never_prints_a_secret() {
        let text = format!("{:?} {:?}", bearer(), CredentialKek::from_bytes([7; 32]));
        assert!(!text.contains("eyJ"), "{text}");
    }
}
