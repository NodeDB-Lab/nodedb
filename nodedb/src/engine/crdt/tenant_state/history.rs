// SPDX-License-Identifier: BUSL-1.1

//! Version-history operations: read at version, export delta, restore, compact.

use super::core::TenantCrdtEngine;

/// Envelope version embedded in every serialized `VersionVector` JSON string.
///
/// Format: `{"v": <LORO_VV_FORMAT_VERSION>, "vv": {"<peer_hex>": <counter>, …}}`
///
/// A string of any other version is refused. Every call site that produces or
/// consumes a version-vector string goes through [`version_vector_json`] /
/// [`json_to_vv`], so the version check is centralised here.
const LORO_VV_FORMAT_VERSION: u8 = 1;

impl TenantCrdtEngine {
    /// Get the current version vector for a collection as a versioned JSON
    /// string.
    ///
    /// The returned string has the shape:
    /// `{"v": <LORO_VV_FORMAT_VERSION>, "vv": {"<peer_hex>": <counter>, …}}`
    ///
    /// A collection with no local state yields an empty version vector (the
    /// peer has observed nothing for it yet).
    ///
    /// Pass this string to any method that accepts a `version_json` parameter.
    pub fn version_vector_json(&self, collection: &str) -> crate::Result<String> {
        let vv = match self.collections.get(collection) {
            Some(state) => state.oplog_version_vector(),
            None => loro::VersionVector::default(),
        };
        let inner = vv_to_json_map(&vv);
        let envelope = VvEnvelope {
            v: LORO_VV_FORMAT_VERSION,
            vv: inner,
        };
        sonic_rs::to_string(&envelope).map_err(|e| crate::Error::Internal {
            detail: format!("version vector serialization: {e}"),
        })
    }

    /// Read a document at a historical version, returning JSON bytes.
    pub fn read_at_version_json(
        &self,
        collection: &str,
        document_id: &str,
        version_json: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        let vv = json_to_vv(version_json)?;
        let Some(state) = self.collections.get(collection) else {
            return Ok(None);
        };
        match state.read_at_version(collection, document_id, &vv) {
            Ok(Some(val)) => {
                let json = crate::engine::document::crdt_store::loro_value_to_json(&val);
                sonic_rs::to_vec(&json)
                    .map(Some)
                    .map_err(|e| crate::Error::Internal {
                        detail: format!("JSON serialization: {e}"),
                    })
            }
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Crdt(e)),
        }
    }

    /// Export delta for a collection from a version to current, returning raw
    /// Loro bytes. A collection with no local state has no updates to export.
    pub fn export_delta(
        &self,
        collection: &str,
        from_version_json: &str,
    ) -> crate::Result<Vec<u8>> {
        let vv = json_to_vv(from_version_json)?;
        match self.collections.get(collection) {
            Some(state) => state.export_updates_since(&vv).map_err(crate::Error::Crdt),
            None => Ok(Vec::new()),
        }
    }

    /// Generate a forward restore delta without changing tenant CRDT state.
    pub fn preview_restore_to_version(
        &self,
        collection: &str,
        document_id: &str,
        target_version_json: &str,
    ) -> crate::Result<Vec<u8>> {
        let vv = json_to_vv(target_version_json)?;
        let state = self.collections.get(collection).ok_or_else(|| {
            crate::Error::Crdt(nodedb_crdt::CrdtError::RowAbsentAtVersion {
                collection: collection.to_string(),
                row_id: document_id.to_string(),
            })
        })?;
        state
            .preview_restore_to_version(collection, document_id, &vv)
            .map_err(crate::Error::Crdt)
    }

    /// Restore a document to a historical version (forward mutation).
    pub fn restore_to_version(
        &self,
        collection: &str,
        document_id: &str,
        target_version_json: &str,
    ) -> crate::Result<Vec<u8>> {
        let vv = json_to_vv(target_version_json)?;
        let state = self.collections.get(collection).ok_or_else(|| {
            crate::Error::Crdt(nodedb_crdt::CrdtError::RowAbsentAtVersion {
                collection: collection.to_string(),
                row_id: document_id.to_string(),
            })
        })?;
        state
            .restore_to_version(collection, document_id, &vv)
            .map_err(crate::Error::Crdt)
    }

    /// Compact history at a specific version for a collection. A collection
    /// with no local state is a no-op.
    pub fn compact_at_version(
        &mut self,
        collection: &str,
        target_version_json: &str,
    ) -> crate::Result<()> {
        let vv = json_to_vv(target_version_json)?;
        match self.collections.get_mut(collection) {
            Some(state) => state.compact_at_version(&vv).map_err(crate::Error::Crdt),
            None => Ok(()),
        }
    }
}

/// Serialization envelope for a Loro `VersionVector`.
///
/// Produced by [`TenantCrdtEngine::version_vector_json`] and consumed by
/// [`json_to_vv`]. The `v` field lets readers detect format-version mismatches
/// before attempting to interpret the `vv` payload.
#[derive(serde::Serialize, serde::Deserialize)]
struct VvEnvelope {
    v: u8,
    vv: std::collections::HashMap<String, i64>,
}

/// Convert a Loro VersionVector to a JSON-friendly map: `{peer_id_hex: counter}`.
fn vv_to_json_map(vv: &loro::VersionVector) -> std::collections::HashMap<String, i64> {
    let mut map = std::collections::HashMap::new();
    for (peer, counter) in vv.iter() {
        map.insert(format!("{peer:016x}"), *counter as i64);
    }
    map
}

/// Parse a versioned JSON version-vector string into a Loro VersionVector.
///
/// The string must have been produced by [`TenantCrdtEngine::version_vector_json`].
/// Returns [`crate::Error::VersionCompat`] if the envelope version does not
/// match [`LORO_VV_FORMAT_VERSION`].
fn json_to_vv(json: &str) -> crate::Result<loro::VersionVector> {
    let envelope: VvEnvelope = sonic_rs::from_str(json).map_err(|e| crate::Error::BadRequest {
        detail: format!("invalid version vector JSON: {e}"),
    })?;
    if envelope.v != LORO_VV_FORMAT_VERSION {
        return Err(crate::Error::VersionCompat {
            detail: format!(
                "version vector format version mismatch: expected {LORO_VV_FORMAT_VERSION}, got {}",
                envelope.v
            ),
        });
    }
    let mut vv = loro::VersionVector::default();
    for (peer_hex, counter) in &envelope.vv {
        let peer = u64::from_str_radix(peer_hex.trim_start_matches("0x"), 16).map_err(|e| {
            crate::Error::BadRequest {
                detail: format!("invalid peer_id hex '{peer_hex}': {e}"),
            }
        })?;
        // Loro's VersionVector counter is `i32`. The envelope carries `i64`
        // (and version JSON is client-supplied), so a value past `i32::MAX`
        // cannot be represented — reject it instead of silently truncating.
        let counter = i32::try_from(*counter).map_err(|_| crate::Error::BadRequest {
            detail: format!(
                "version vector counter {counter} for peer '{peer_hex}' exceeds the \
                 representable range (Loro counters are 32-bit; max {})",
                i32::MAX
            ),
        })?;
        vv.insert(peer, counter);
    }
    Ok(vv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vv_json(v: u8, peers: &[(&str, i64)]) -> String {
        let inner: std::collections::HashMap<String, i64> =
            peers.iter().map(|(k, c)| (k.to_string(), *c)).collect();
        let envelope = VvEnvelope { v, vv: inner };
        sonic_rs::to_string(&envelope).unwrap()
    }

    #[test]
    fn vv_roundtrip_through_json() {
        let json = make_vv_json(LORO_VV_FORMAT_VERSION, &[("000000000000001a", 42)]);
        let vv = json_to_vv(&json).unwrap();
        // Verify the peer 0x1a is present with counter 42.
        let found = vv
            .iter()
            .find(|&(peer, _)| *peer == 0x1a_u64)
            .map(|(_, c)| *c);
        assert_eq!(found, Some(42_i32));
    }

    #[test]
    fn vv_json_rejects_wrong_version() {
        let json = make_vv_json(
            LORO_VV_FORMAT_VERSION.wrapping_add(1),
            &[("000000000000001a", 1)],
        );
        let err = json_to_vv(&json).unwrap_err();
        assert!(
            matches!(err, crate::Error::VersionCompat { .. }),
            "expected VersionCompat, got: {err}"
        );
    }

    #[test]
    fn vv_json_rejects_version_zero() {
        let json = make_vv_json(0, &[]);
        let err = json_to_vv(&json).unwrap_err();
        assert!(
            matches!(err, crate::Error::VersionCompat { .. }),
            "expected VersionCompat, got: {err}"
        );
    }

    #[test]
    fn vv_json_rejects_malformed_input() {
        let err = json_to_vv("not json at all").unwrap_err();
        assert!(
            matches!(err, crate::Error::BadRequest { .. }),
            "expected BadRequest, got: {err}"
        );
    }

    #[test]
    fn vv_json_rejects_invalid_peer_hex() {
        let json = make_vv_json(LORO_VV_FORMAT_VERSION, &[("not-hex-at-all!!", 1)]);
        let err = json_to_vv(&json).unwrap_err();
        assert!(
            matches!(err, crate::Error::BadRequest { .. }),
            "expected BadRequest, got: {err}"
        );
    }

    #[test]
    fn vv_json_accepts_i32_max_counter() {
        let json = make_vv_json(
            LORO_VV_FORMAT_VERSION,
            &[("000000000000001a", i32::MAX as i64)],
        );
        let vv = json_to_vv(&json).unwrap();
        let found = vv
            .iter()
            .find(|&(peer, _)| *peer == 0x1a_u64)
            .map(|(_, c)| *c);
        assert_eq!(found, Some(i32::MAX));
    }

    #[test]
    fn vv_json_rejects_counter_exceeding_i32_max() {
        // A counter past i32::MAX cannot be represented by Loro's 32-bit
        // counter — it must be rejected, not silently truncated.
        let json = make_vv_json(
            LORO_VV_FORMAT_VERSION,
            &[("000000000000001a", i32::MAX as i64 + 1)],
        );
        let err = json_to_vv(&json).unwrap_err();
        assert!(
            matches!(err, crate::Error::BadRequest { .. }),
            "expected BadRequest, got: {err}"
        );
    }
}
