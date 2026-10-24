// SPDX-License-Identifier: BUSL-1.1

//! Node-label versions in snapshots: export a tenant's versions.

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::node_labels::{NODE_LABEL_VERSIONS, decode_labels, parse_version_key};
use super::store::{EdgeStore, redb_err};

/// One exported label version: `(node, ordinal, labels)`.
pub type NodeLabelVersionRecord = (String, i64, Vec<String>);

impl EdgeStore {
    /// Every label version of `(db, tid)`, by node and then ascending
    /// ordinal.
    pub fn scan_node_label_versions(
        &self,
        db: DatabaseId,
        tid: TenantId,
    ) -> crate::Result<Vec<NodeLabelVersionRecord>> {
        let d = db.as_u64();
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(NODE_LABEL_VERSIONS)
            .map_err(|e| redb_err("open node_label_versions", e))?;
        let range = table
            .range((d, t, "")..(d, t + 1, ""))
            .map_err(|e| redb_err("node label version range", e))?;
        let mut out = Vec::new();
        for entry in range {
            let (key, value) = entry.map_err(|e| redb_err("node label version entry", e))?;
            let Some((node, ordinal)) = parse_version_key(key.value().2) else {
                return Err(crate::Error::Storage {
                    engine: "graph".into(),
                    detail: format!(
                        "node label version key of database {d}, tenant {t} does not parse"
                    ),
                });
            };
            out.push((node.to_string(), ordinal, decode_labels(value.value())?));
        }
        Ok(out)
    }
}
