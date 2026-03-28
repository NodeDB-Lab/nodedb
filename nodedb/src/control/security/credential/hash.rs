use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};

pub(super) fn generate_scram_salt() -> Vec<u8> {
    use argon2::password_hash::rand_core::RngCore;
    let mut salt = vec![0u8; 16];
    OsRng.fill_bytes(&mut salt);
    salt
}

pub(super) fn compute_scram_salted_password(password: &str, salt: &[u8]) -> Vec<u8> {
    pgwire::api::auth::sasl::scram::gen_salted_password(password, salt, 4096)
}

pub(super) fn hash_password_argon2(password: &str) -> crate::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| crate::Error::Internal {
            detail: format!("argon2 hashing failed: {e}"),
        })?;
    Ok(hash.to_string())
}

/// Compute PostgreSQL-compatible MD5 hash: `md5(password + username)`.
///
/// Returns the hex-encoded MD5 digest (32 chars). Stored alongside
/// the Argon2 hash to support pgwire MD5 authentication.
pub(super) fn compute_md5_hash(username: &str, password: &str) -> String {
    let input = format!("{password}{username}");
    let digest = md5::compute(input.as_bytes());
    format!("{digest:x}")
}

pub(super) fn verify_argon2(stored_hash: &str, password: &str) -> bool {
    let parsed = match PasswordHash::new(stored_hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}
