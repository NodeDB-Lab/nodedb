use nodedb_types::TenantId;
use redb::ReadableTable;

use super::store::{EDGES, EdgeStore, redb_err};

impl EdgeStore {
    /// Export all forward edges as `(tenant, composite_key, properties)`
    /// triples for snapshot transfer. Reverse index is rebuilt on
    /// restore from the forward records — not shipped separately.
    pub fn export_edges(&self) -> crate::Result<Vec<(TenantId, String, Vec<u8>)>> {
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let table = txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut pairs = Vec::new();
        for entry in table.iter().map_err(|e| redb_err("iter edges", e))? {
            let (k, v) = entry.map_err(|e| redb_err("read edge", e))?;
            let (tid, composite) = k.value();
            pairs.push((
                TenantId::new(tid),
                composite.to_string(),
                v.value().to_vec(),
            ));
        }
        Ok(pairs)
    }

    /// Import edges from a snapshot. Each record is inserted via
    /// [`EdgeStore::put_edge_raw`], which maintains the reverse index
    /// atomically.
    pub fn import_edges(&self, edges: &[(TenantId, String, Vec<u8>)]) -> crate::Result<()> {
        for (tid, key, value) in edges {
            self.put_edge_raw(*tid, key, value)?;
        }
        Ok(())
    }
}
