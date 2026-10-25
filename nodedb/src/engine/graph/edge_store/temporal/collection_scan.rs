// SPDX-License-Identifier: BUSL-1.1

//! One collection's edges at a bitemporal cut, read by a range scan over
//! that collection's key prefix.

use redb::{ReadableDatabase, ReadableTable};

use nodedb_types::{Surrogate, TenantId};

use super::keys::{is_gdpr_erasure, is_tombstone, parse_versioned_edge_key};
use super::payload::EdgeValuePayload;
use super::visibility::read_visibility;
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, redb_err};

/// One edge of a collection at a cut: the version it resolves to there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutEdge {
    pub src: String,
    pub label: String,
    pub dst: String,
    pub properties: Vec<u8>,
    /// The source endpoint's identity the version recorded.
    pub src_surrogate: Option<Surrogate>,
    /// The destination endpoint's identity the version recorded.
    pub dst_surrogate: Option<Surrogate>,
}

/// The version a base edge resolves to while its versions stream past in
/// ascending system time.
struct BaseResolution {
    src: String,
    label: String,
    dst: String,
    /// The newest qualifying payload, or `None` when the newest qualifying
    /// version is a sentinel or none qualified yet.
    payload: Option<EdgeValuePayload>,
}

impl EdgeStore {
    /// The edges of `collection` live at `cutoff_ordinal`, optionally one
    /// label only, in key order. Each carries the endpoint identities its
    /// resolved version recorded.
    ///
    /// Each base edge resolves as [`EdgeStore::ceiling_resolve_edge`] does:
    /// the newest version at or below the cutoff that no TRUNCATE hides. A
    /// tombstone or GDPR erasure leaves the edge out. With `valid_at_ms`, a
    /// payload whose valid time does not contain it is passed over for the
    /// next older version.
    pub fn scan_collection_as_of(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        label: Option<&str>,
        cutoff_ordinal: i64,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Vec<CutEdge>> {
        if cutoff_ordinal < 0 {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "scan_collection_as_of: negative system-time cutoff {cutoff_ordinal}"
                ),
            });
        }
        let t = tid.as_u64();
        let prefix = format!("{collection}\x00");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        let mut out = Vec::new();
        let mut current: Option<BaseResolution> = None;
        let range = table
            .range((db, t, prefix.as_str())..)
            .map_err(|e| redb_err("collection edge range", e))?;
        for entry in range {
            let (key, value) = entry.map_err(|e| redb_err("collection edge entry", e))?;
            let (kd, kt, composite) = key.value();
            if kd != db || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let Some((coll, src, edge_label, dst, sys)) = parse_versioned_edge_key(composite)
            else {
                continue;
            };
            if label.is_some_and(|wanted| wanted != edge_label) {
                continue;
            }
            let same_base = current
                .as_ref()
                .is_some_and(|base| base.src == src && base.label == edge_label && base.dst == dst);
            if !same_base {
                if let Some(done) = current.take() {
                    push_resolved(&mut out, done);
                }
                current = Some(BaseResolution {
                    src: src.to_string(),
                    label: edge_label.to_string(),
                    dst: dst.to_string(),
                    payload: None,
                });
            }
            if !visibility.visible_at(db, t, coll, composite, sys, cutoff_ordinal)? {
                continue;
            }
            let bytes = value.value();
            let Some(base) = current.as_mut() else {
                continue;
            };
            if is_tombstone(bytes) || is_gdpr_erasure(bytes) {
                base.payload = None;
                continue;
            }
            let payload = EdgeValuePayload::decode(bytes)?;
            let valid = valid_at_ms
                .is_none_or(|vt| payload.valid_from_ms <= vt && vt < payload.valid_until_ms);
            if valid {
                base.payload = Some(payload);
            }
        }
        if let Some(done) = current.take() {
            push_resolved(&mut out, done);
        }
        Ok(out)
    }
}

fn push_resolved(out: &mut Vec<CutEdge>, base: BaseResolution) {
    if let Some(payload) = base.payload {
        out.push(CutEdge {
            src_surrogate: payload.src(),
            dst_surrogate: payload.dst(),
            src: base.src,
            label: base.label,
            dst: base.dst,
            properties: payload.properties,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::super::keys::EdgeRef;
    use super::*;
    use nodedb_types::DatabaseId;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const D: u64 = 0;

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open store");
        (store, dir)
    }

    fn put(store: &EdgeStore, coll: &str, src: &str, dst: &str, ord: i64, valid: (i64, i64)) {
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, coll, src, "L", dst),
                b"p",
                ord,
                valid.0,
                valid.1,
            )
            .expect("put edge");
    }

    fn pairs(records: &[CutEdge]) -> Vec<(String, String)> {
        records
            .iter()
            .map(|r| (r.src.clone(), r.dst.clone()))
            .collect()
    }

    #[test]
    fn a_cutoff_reads_the_versions_live_at_it() {
        let (store, _dir) = make_store();
        put(&store, "g", "a", "b", 100, (0, i64::MAX));
        put(&store, "g", "a", "c", 200, (0, i64::MAX));
        store
            .soft_delete_edge(EdgeRef::new(DB, T, "g", "a", "L", "b"), 300)
            .expect("delete");
        let at = |cut| {
            pairs(
                &store
                    .scan_collection_as_of(D, T, "g", None, cut, None)
                    .expect("scan"),
            )
        };
        assert_eq!(at(150), vec![("a".into(), "b".into())]);
        assert_eq!(
            at(250),
            vec![("a".into(), "b".into()), ("a".into(), "c".into())]
        );
        assert_eq!(at(350), vec![("a".into(), "c".into())]);
    }

    #[test]
    fn only_the_named_collection_and_tenant_are_read() {
        let (store, _dir) = make_store();
        put(&store, "g", "a", "b", 100, (0, i64::MAX));
        put(&store, "g2", "x", "y", 100, (0, i64::MAX));
        put(&store, "h", "p", "q", 100, (0, i64::MAX));
        store
            .put_edge_versioned(
                EdgeRef::new(DB, TenantId::new(2), "g", "m", "L", "n"),
                b"p",
                100,
                0,
                i64::MAX,
            )
            .expect("put other tenant");
        let records = store
            .scan_collection_as_of(D, T, "g", None, i64::MAX, None)
            .expect("scan");
        assert_eq!(pairs(&records), vec![("a".into(), "b".into())]);
    }

    #[test]
    fn a_cut_carries_the_identities_its_version_recorded() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, "g", "a", "L", "b")
                    .with_surrogates(Surrogate::new(10), Surrogate::new(20)),
                b"p",
                100,
                0,
                i64::MAX,
            )
            .expect("put edge");
        store
            .soft_delete_edge(EdgeRef::new(DB, T, "g", "a", "L", "b"), 200)
            .expect("delete");
        let records = store
            .scan_collection_as_of(D, T, "g", None, 150, None)
            .expect("scan");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].src_surrogate, Some(Surrogate::new(10)));
        assert_eq!(records[0].dst_surrogate, Some(Surrogate::new(20)));
        assert!(
            store
                .scan_collection_as_of(D, T, "g", None, 250, None)
                .expect("scan")
                .is_empty()
        );
    }

    #[test]
    fn valid_time_passes_over_versions_that_do_not_contain_it() {
        let (store, _dir) = make_store();
        put(&store, "g", "a", "b", 100, (10, 20));
        put(&store, "g", "a", "b", 200, (30, 40));
        put(&store, "g", "a", "c", 100, (10, 20));
        let at = |vt| {
            pairs(
                &store
                    .scan_collection_as_of(D, T, "g", None, i64::MAX, Some(vt))
                    .expect("scan"),
            )
        };
        assert_eq!(
            at(15),
            vec![("a".into(), "b".into()), ("a".into(), "c".into())]
        );
        assert_eq!(at(35), vec![("a".into(), "b".into())]);
        assert!(at(25).is_empty());
    }

    #[test]
    fn a_label_filter_keeps_only_that_label() {
        let (store, _dir) = make_store();
        put(&store, "g", "a", "b", 100, (0, i64::MAX));
        store
            .put_edge_versioned(
                EdgeRef::new(DB, T, "g", "a", "M", "c"),
                b"p",
                100,
                0,
                i64::MAX,
            )
            .expect("put");
        let records = store
            .scan_collection_as_of(D, T, "g", Some("M"), i64::MAX, None)
            .expect("scan");
        assert_eq!(pairs(&records), vec![("a".into(), "c".into())]);
    }

    #[test]
    fn a_negative_cutoff_is_refused() {
        let (store, _dir) = make_store();
        assert!(
            store
                .scan_collection_as_of(D, T, "g", None, -1, None)
                .is_err()
        );
    }
}
