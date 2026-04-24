//! Version-history operations: read at version, export delta, restore, compact.

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Get the current version vector as a JSON string.
    pub fn version_vector_json(&self) -> crate::Result<String> {
        let vv = self.state.oplog_version_vector();
        let map = vv_to_json_map(&vv);
        sonic_rs::to_string(&map).map_err(|e| crate::Error::Internal {
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
        match self.state.read_at_version(collection, document_id, &vv) {
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

    /// Export delta from a version to current, returning raw Loro bytes.
    pub fn export_delta(&self, from_version_json: &str) -> crate::Result<Vec<u8>> {
        let vv = json_to_vv(from_version_json)?;
        self.state
            .export_updates_since(&vv)
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
        self.state
            .restore_to_version(collection, document_id, &vv)
            .map_err(crate::Error::Crdt)
    }

    /// Compact history at a specific version.
    pub fn compact_at_version(&mut self, target_version_json: &str) -> crate::Result<()> {
        let vv = json_to_vv(target_version_json)?;
        self.state
            .compact_at_version(&vv)
            .map_err(crate::Error::Crdt)
    }
}

/// Convert a Loro VersionVector to a JSON-friendly map: `{peer_id_hex: counter}`.
fn vv_to_json_map(vv: &loro::VersionVector) -> std::collections::HashMap<String, i64> {
    let mut map = std::collections::HashMap::new();
    for (peer, counter) in vv.iter() {
        map.insert(format!("{peer:016x}"), *counter as i64);
    }
    map
}

/// Parse a JSON version vector string into a Loro VersionVector.
fn json_to_vv(json: &str) -> crate::Result<loro::VersionVector> {
    let map: std::collections::HashMap<String, i64> =
        sonic_rs::from_str(json).map_err(|e| crate::Error::BadRequest {
            detail: format!("invalid version vector JSON: {e}"),
        })?;
    let mut vv = loro::VersionVector::default();
    for (peer_hex, counter) in &map {
        let peer = u64::from_str_radix(peer_hex.trim_start_matches("0x"), 16).map_err(|e| {
            crate::Error::BadRequest {
                detail: format!("invalid peer_id hex '{peer_hex}': {e}"),
            }
        })?;
        vv.insert(peer, *counter as i32);
    }
    Ok(vv)
}
