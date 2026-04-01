//! WASM module blob store: per-tenant, content-addressed by SHA-256.
//!
//! Stores raw `.wasm` binaries in the system catalog (redb) under
//! `wasm_module:{sha256_hex}`. Metadata (function name → hash mapping)
//! is stored separately in the function catalog.

use super::runtime::sha256_hex;

/// Store a WASM binary in the system catalog, returning its content hash.
///
/// Content-addressed: if the same binary is uploaded twice, it's stored once.
/// The hash is used as the key for deduplication and cache lookup.
pub fn store_wasm_binary(
    catalog: &crate::control::security::catalog::types::SystemCatalog,
    wasm_bytes: &[u8],
    max_size: usize,
) -> crate::Result<String> {
    if wasm_bytes.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "WASM binary is empty".into(),
        });
    }
    if wasm_bytes.len() > max_size {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "WASM binary exceeds maximum size ({} bytes > {max_size} bytes)",
                wasm_bytes.len()
            ),
        });
    }

    // Validate WASM magic number: \0asm
    if wasm_bytes.len() < 4 || &wasm_bytes[..4] != b"\0asm" {
        return Err(crate::Error::BadRequest {
            detail: "invalid WASM binary: missing \\0asm magic header".into(),
        });
    }

    let hash = sha256_hex(wasm_bytes);
    let key = format!("wasm_module:{hash}");

    catalog
        .put_raw(key.as_bytes(), wasm_bytes)
        .map_err(|e| crate::Error::Internal {
            detail: format!("failed to store WASM binary: {e}"),
        })?;

    Ok(hash)
}

/// Load a WASM binary from the system catalog by its content hash.
pub fn load_wasm_binary(
    catalog: &crate::control::security::catalog::types::SystemCatalog,
    hash: &str,
) -> crate::Result<Vec<u8>> {
    let key = format!("wasm_module:{hash}");
    catalog
        .get_raw(key.as_bytes())
        .map_err(|e| crate::Error::Internal {
            detail: format!("failed to load WASM binary: {e}"),
        })?
        .ok_or_else(|| crate::Error::BadRequest {
            detail: format!("WASM module with hash '{hash}' not found"),
        })
}

/// Delete a WASM binary from the catalog by its content hash.
pub fn delete_wasm_binary(
    catalog: &crate::control::security::catalog::types::SystemCatalog,
    hash: &str,
) -> crate::Result<()> {
    let key = format!("wasm_module:{hash}");
    catalog
        .delete_raw(key.as_bytes())
        .map_err(|e| crate::Error::Internal {
            detail: format!("failed to delete WASM binary: {e}"),
        })
}
