// SPDX-License-Identifier: BUSL-1.1

//! Relationship properties: `r.price` for a bound edge variable `r`.
//!
//! The core that traverses an edge holds it, so the property object is
//! captured there, at bind time, into a hidden key of the row
//! ([`super::row::edge_properties_key`]). A row resumed on another core or
//! node carries it along, so a `WHERE` or `RETURN` on any later core reads
//! the exact typed values the edge stores.
//!
//! The object is the transaction's staged put of the edge when it staged
//! one, else the stored version at the MATCH's cut. The crossed edge names
//! its own collection (the CSR interns it per edge), so the read needs no
//! `IN '<collection>'`.

use std::collections::{HashMap, HashSet};

use nodedb_types::{DatabaseId, TenantId, Value};

use super::predicates::PropertyLookup;
use super::row::edge_properties_key;
use super::types::BindingRow;
use crate::engine::graph::edge_store::{EdgeRef, EdgeStore};

/// A transaction's staged edge puts: `(collection, src, label, dst)` → the
/// staged property image.
pub type StagedEdgeProperties = HashMap<(String, String, String, String), Vec<u8>>;

/// One physical edge a pattern crossed. `collection` is the collection the
/// edge is stored under, as the CSR or the store holds it, or `None` for an
/// edge only the transaction's staged overlay holds.
#[derive(Debug, Clone, Copy)]
pub(super) struct CrossedEdge<'e> {
    pub collection: Option<&'e str>,
    pub src: &'e str,
    pub label: &'e str,
    pub dst: &'e str,
}

/// Which edge variables one execution captures properties for, and where it
/// reads them.
#[derive(Clone, Copy)]
pub(super) struct EdgeCapture<'a> {
    pub vars: &'a HashSet<String>,
    pub edge_store: &'a EdgeStore,
    pub props: &'a PropertyLookup<'a>,
}

impl<'a> EdgeCapture<'a> {
    /// The capture for a query reading the properties of `vars`, or `None`
    /// when it reads no relationship property.
    pub(super) fn for_query(
        vars: &'a HashSet<String>,
        edge_store: &'a EdgeStore,
        props: &'a PropertyLookup<'a>,
    ) -> Option<Self> {
        (!vars.is_empty()).then_some(Self {
            vars,
            edge_store,
            props,
        })
    }

    /// Record `edge`'s property object on `row` when the query reads
    /// properties of `var`.
    pub(super) fn capture(
        &self,
        row: &mut BindingRow,
        var: &str,
        edge: CrossedEdge<'_>,
    ) -> crate::Result<()> {
        if !self.vars.contains(var) {
            return Ok(());
        }
        let fields = fetch_edge_properties(self.edge_store, self.props, edge)?;
        row.insert(edge_properties_key(var), Value::Object(fields));
        Ok(())
    }
}

/// The property object of the physical `edge`. An edge with no properties,
/// or no version at the cut, is `{}`.
pub(super) fn fetch_edge_properties(
    edge_store: &EdgeStore,
    props: &PropertyLookup<'_>,
    edge: CrossedEdge<'_>,
) -> crate::Result<HashMap<String, Value>> {
    let CrossedEdge {
        collection,
        src,
        label,
        dst,
    } = edge;
    let stored;
    let image: &[u8] = match staged_image(props, edge)? {
        Some(image) => image,
        None => {
            let collection = collection.ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "MATCH: edge ({src})-[{label}]->({dst}) is neither stored under a collection \
                     nor staged"
                ),
            })?;
            stored = read_stored(edge_store, props, collection, src, label, dst)?;
            stored.as_slice()
        }
    };
    nodedb_graph::decode_edge_properties(image).map_err(|e| crate::Error::Codec {
        detail: format!("MATCH: edge ({src})-[{label}]->({dst}) properties do not decode: {e}"),
    })
}

/// The transaction's staged image of `edge`. An edge whose collection is
/// unknown (staged only) matches the staged puts of its endpoints and label in
/// any collection, and more than one is ambiguous: the row binds one edge.
fn staged_image<'s>(
    props: &PropertyLookup<'s>,
    edge: CrossedEdge<'_>,
) -> crate::Result<Option<&'s [u8]>> {
    let Some(staged) = props.staged_edges else {
        return Ok(None);
    };
    let CrossedEdge {
        collection,
        src,
        label,
        dst,
    } = edge;
    if let Some(collection) = collection {
        let key = (
            collection.to_string(),
            src.to_string(),
            label.to_string(),
            dst.to_string(),
        );
        return Ok(staged.get(&key).map(Vec::as_slice));
    }
    let mut matches = staged
        .iter()
        .filter(|((_, s, l, d), _)| s == src && l == label && d == dst);
    let first = matches.next();
    if let Some(((other, _, _, _), _)) = matches.next() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "MATCH: the transaction staged edge ({src})-[{label}]->({dst}) in more than one \
                 collection, including '{other}'; name one with IN '<collection>'"
            ),
        });
    }
    Ok(first.map(|(_, image)| image.as_slice()))
}

/// The stored image of the edge at the MATCH's cut, empty when the edge has
/// no version there.
fn read_stored(
    edge_store: &EdgeStore,
    props: &PropertyLookup<'_>,
    collection: &str,
    src: &str,
    label: &str,
    dst: &str,
) -> crate::Result<Vec<u8>> {
    let scope = props.edge_cut;
    let cutoff = scope
        .cutoff_ordinal()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: nodedb_physical::physical_plan::ALL_VERSIONS_REFUSED.into(),
        })?;
    let edge = EdgeRef::new(
        DatabaseId::new(props.database_id),
        TenantId::new(props.tenant_id),
        collection,
        src,
        label,
        dst,
    );
    Ok(edge_store
        .ceiling_resolve_edge(edge, cutoff, scope.valid_at_ms)?
        .unwrap_or_default())
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::HashMap;

    use nodedb_types::{DatabaseId, NdbDateTime, TenantId, Value};

    use super::super::core::triple::tests::{make_sparse, props_for, s};
    use super::super::core::{MatchExecCtx, execute};
    use super::super::expansion::VarLenCaps;
    use super::super::predicates::PropertyLookup;
    use super::super::types::BindingRow;
    use super::StagedEdgeProperties;
    use crate::engine::graph::csr::CsrIndex;
    use crate::engine::graph::edge_store::{EdgeRef, EdgeStore};
    use crate::engine::graph::pattern::compiler::parse;

    fn price(text: &str) -> Value {
        Value::Decimal(text.parse().expect("decimal"))
    }

    /// The typed properties of `alice -KNOWS-> bob`.
    fn typed() -> HashMap<String, Value> {
        HashMap::from([
            ("price".to_string(), price("12.50")),
            (
                "id".to_string(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (
                "at".to_string(),
                Value::DateTime(NdbDateTime::from_micros(1_700_000_000_000_000)),
            ),
            ("blob".to_string(), Value::Bytes(vec![0, 1, 255])),
        ])
    }

    /// `alice -KNOWS-> bob` with [`typed`] properties and
    /// `alice -KNOWS-> carol` with `{price: 3}`, in collection `col`.
    fn graph() -> (CsrIndex, EdgeStore, tempfile::TempDir) {
        graph_with(vec![
            ("bob", typed()),
            (
                "carol",
                HashMap::from([("price".to_string(), Value::Integer(3))]),
            ),
        ])
    }

    /// One core's graph: `alice -KNOWS-> dst` for each `(dst, properties)`,
    /// in collection `col`.
    pub(in crate::engine::graph::pattern::executor) fn graph_with(
        edges: Vec<(&str, HashMap<String, Value>)>,
    ) -> (CsrIndex, EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open edge store");
        for (ord, (dst, fields)) in (1i64..).zip(edges) {
            let image = nodedb_graph::encode_edge_properties(fields).expect("encode");
            store
                .put_edge_versioned(
                    EdgeRef::new(
                        DatabaseId::DEFAULT,
                        TenantId::new(1),
                        "col",
                        "alice",
                        "KNOWS",
                        dst,
                    ),
                    &image,
                    ord,
                    ord,
                    i64::MAX,
                )
                .expect("put edge");
        }
        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&store).expect("csr");
        (csr, store, dir)
    }

    pub(in crate::engine::graph::pattern::executor) fn run(
        text: &str,
        csr: &CsrIndex,
        store: &EdgeStore,
        props: &PropertyLookup<'_>,
    ) -> Vec<BindingRow> {
        let query = parse(text).expect("parse");
        execute(
            &query,
            MatchExecCtx {
                csr,
                edge_store: store,
                frontier_bitmap: None,
                is_remote_node: None,
                varlen_caps: VarLenCaps::default(),
                props,
                overlay: None,
            },
        )
        .expect("execute")
        .rows
    }

    fn by_b(mut rows: Vec<BindingRow>) -> Vec<BindingRow> {
        rows.sort_by(|x, y| x["b"].as_str().cmp(&y["b"].as_str()));
        rows
    }

    /// `RETURN r.<field>` projects the relationship's exact stored values: a
    /// Decimal keeps its scale, a UUID, an instant and bytes keep their kinds,
    /// and a missing field is SQL NULL.
    #[test]
    fn relationship_properties_project_exactly_typed() {
        let (csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let rows = by_b(run(
            "MATCH (a)-[r:KNOWS]->(b) IN 'col' WHERE a = 'alice' \
             RETURN b, r.price, r.id, r.at, r.blob, r.missing",
            &csr,
            &store,
            &props,
        ));
        assert_eq!(rows.len(), 2);
        let expected = typed();
        assert_eq!(rows[0]["b"], s("bob"));
        for field in ["price", "id", "at", "blob"] {
            assert_eq!(rows[0][&format!("r.{field}")], expected[field], "r.{field}");
        }
        let Value::Decimal(d) = &rows[0]["r.price"] else {
            panic!("r.price is a Decimal");
        };
        assert_eq!(d.to_string(), "12.50", "the scale survives");
        assert_eq!(rows[0]["r.missing"], Value::Null);
        assert_eq!(rows[1]["r.price"], Value::Integer(3));
    }

    /// `WHERE r.price > 12` compares the typed Decimal: only `bob` passes, and
    /// `= 12.50` matches by value.
    #[test]
    fn a_where_on_a_relationship_property_compares_typed_values() {
        let (csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        for predicate in ["r.price > 12", "r.price = 12.50", "r.price >= 12.5"] {
            let rows = run(
                &format!("MATCH (a)-[r:KNOWS]->(b) IN 'col' WHERE {predicate} RETURN b"),
                &csr,
                &store,
                &props,
            );
            assert_eq!(rows.len(), 1, "{predicate}");
            assert_eq!(rows[0]["b"], s("bob"), "{predicate}");
        }
        let none = run(
            "MATCH (a)-[r:KNOWS]->(b) IN 'col' WHERE r.price > 13 RETURN b",
            &csr,
            &store,
            &props,
        );
        assert!(none.is_empty());
    }

    /// An incoming crossing reads the physical edge `alice -> bob`.
    #[test]
    fn an_incoming_crossing_reads_the_physical_edge() {
        let (csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        for pattern in ["(b)<-[r:KNOWS]-(a)", "(b)-[r:KNOWS]-(a)"] {
            let rows = run(
                &format!("MATCH {pattern} IN 'col' WHERE b = 'bob' RETURN a, r.price"),
                &csr,
                &store,
                &props,
            );
            assert_eq!(rows.len(), 1, "{pattern}");
            assert_eq!(rows[0]["a"], s("alice"), "{pattern}");
            assert_eq!(rows[0]["r.price"], price("12.50"), "{pattern}");
        }
    }

    /// A row with no RETURN list carries its bindings and no hidden
    /// relationship-property key.
    #[test]
    fn an_unprojected_row_carries_no_captured_properties() {
        let (csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        let props = props_for(&sparse, &csr);
        let rows = run(
            "MATCH (a)-[r:KNOWS]->(b) IN 'col' WHERE r.price > 12",
            &csr,
            &store,
            &props,
        );
        assert_eq!(rows.len(), 1);
        let mut keys: Vec<&str> = rows[0].keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["a", "b", "r"]);
        assert_eq!(rows[0]["r"], s("alice|KNOWS|bob"));
    }

    /// The transaction's staged put of an edge replaces its stored properties.
    #[test]
    fn a_staged_put_replaces_the_stored_properties() {
        let (csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        let staged: StagedEdgeProperties = HashMap::from([(
            (
                "col".to_string(),
                "alice".to_string(),
                "KNOWS".to_string(),
                "bob".to_string(),
            ),
            nodedb_graph::encode_edge_properties(HashMap::from([(
                "price".to_string(),
                price("99.00"),
            )]))
            .expect("encode"),
        )]);
        let props = PropertyLookup {
            staged_edges: Some(&staged),
            ..props_for(&sparse, &csr)
        };
        let rows = run(
            "MATCH (a)-[r:KNOWS]->(b) IN 'col' WHERE b = 'bob' RETURN r.price",
            &csr,
            &store,
            &props,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["r.price"], price("99.00"));
    }

    /// Without `IN '<collection>'` the crossed edge names its own collection,
    /// so `r.price` reads the edge's stored properties.
    #[test]
    fn a_relationship_property_reads_without_a_collection_clause() {
        let (csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        let props = PropertyLookup {
            collection: None,
            ..props_for(&sparse, &csr)
        };
        let rows = by_b(run(
            "MATCH (a)-[r:KNOWS]->(b) WHERE r.price > 1 RETURN b, r.price",
            &csr,
            &store,
            &props,
        ));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["r.price"], price("12.50"));
        assert_eq!(rows[1]["r.price"], Value::Integer(3));
    }

    /// A node property projects its exact stored value from a native body.
    #[test]
    fn node_properties_project_exactly_typed() {
        let (mut csr, store, _dir) = graph();
        let (sparse, _sdir) = make_sparse();
        csr.set_node_surrogate("alice", nodedb_types::Surrogate::new(1));
        let body = zerompk::to_msgpack_vec(&Value::Object(typed())).expect("encode body");
        sparse
            .put(
                0,
                1,
                "col",
                &nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(1)),
                &body,
                1,
            )
            .expect("put document");
        let props = props_for(&sparse, &csr);
        let rows = run(
            "MATCH (a)-[:KNOWS]->(b) IN 'col' WHERE a.price > 12 RETURN a.price, a.id, a.blob",
            &csr,
            &store,
            &props,
        );
        assert_eq!(rows.len(), 2, "alice reaches bob and carol");
        let expected = typed();
        for row in &rows {
            assert_eq!(row["a.price"], expected["price"]);
            assert_eq!(row["a.id"], expected["id"]);
            assert_eq!(row["a.blob"], expected["blob"]);
        }
    }
}
