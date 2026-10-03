// SPDX-License-Identifier: BUSL-1.1

//! Edge-property work for one traversal hop: the `EDGE WHERE` predicate and
//! the property object a `GRAPH TRAVERSE` row carries.
//!
//! Both read the crossed edge's current property map on the core that holds
//! the edge, through one read transaction per hop. A dual-homed edge is
//! stored in full on both homes, so either direction reads it locally. An
//! edge the hop's transaction staged a put of uses the staged map instead.

use std::borrow::Cow;

use nodedb_types::filter::MetadataFilter;
use nodedb_types::graph::Direction;
use nodedb_types::{DatabaseId, TenantId, Value};

use crate::engine::graph::edge_store::{EdgePropertyReader, EdgeRef, EdgeStore};

/// What one hop does with each crossed edge's properties, and the read
/// transaction it reads them under. The read transaction opens on the first
/// crossed edge whose properties come from the store.
pub(in crate::data::executor) struct HopEdgeProperties<'a> {
    filters: &'a [MetadataFilter],
    with_properties: bool,
    store: &'a EdgeStore,
    reader: Option<EdgePropertyReader>,
    database: DatabaseId,
    tenant: TenantId,
    collection: &'a str,
}

/// The verdict on one crossed edge.
pub(in crate::data::executor) enum EdgeCrossing {
    /// The predicate rejects the edge: the hop drops its row.
    Rejected,
    /// The row stays. Carries the edge's property object when the hop
    /// returns properties.
    Admitted(Option<Value>),
}

/// The scope a hop reads edge properties in.
pub(in crate::data::executor) struct HopPropertyScope<'a> {
    pub database: DatabaseId,
    pub tenant: TenantId,
    /// Collection that keys each edge's properties, or `None` for a
    /// label-only hop.
    pub collection: Option<&'a str>,
    /// AND-ed predicate. Empty admits every edge.
    pub filters: &'a [MetadataFilter],
    /// Each admitted row carries the edge's property object.
    pub with_properties: bool,
}

impl<'a> HopEdgeProperties<'a> {
    /// `Ok(None)` when the hop neither filters nor returns properties.
    /// Either needs the collection that keys each edge's properties.
    pub(in crate::data::executor) fn open(
        store: &'a EdgeStore,
        scope: HopPropertyScope<'a>,
    ) -> crate::Result<Option<Self>> {
        let HopPropertyScope {
            database,
            tenant,
            collection,
            filters,
            with_properties,
        } = scope;
        if filters.is_empty() && !with_properties {
            return Ok(None);
        }
        let Some(collection) = collection else {
            return Err(crate::Error::BadRequest {
                detail: "an edge property predicate or edge properties need a collection \
                         scope: name the collection"
                    .into(),
            });
        };
        Ok(Some(Self {
            filters,
            with_properties,
            store,
            reader: None,
            database,
            tenant,
            collection,
        }))
    }

    /// Decide the physical edge `(src, label, dst)`. `staged` is the property
    /// map of a put the hop's transaction staged for this edge, and is used
    /// in place of the stored map. An edge with no live version or no
    /// properties evaluates as an empty object.
    pub(in crate::data::executor) fn cross(
        &mut self,
        src: &str,
        label: &str,
        dst: &str,
        staged: Option<&[u8]>,
    ) -> crate::Result<EdgeCrossing> {
        let properties: Cow<'_, [u8]> = match staged {
            Some(bytes) => Cow::Borrowed(bytes),
            None => Cow::Owned(self.stored(src, label, dst)?),
        };
        let admitted = nodedb_query::metadata_filter::matches_all_msgpack(&properties, self.filters)
            .map_err(|e| crate::Error::Codec {
                detail: format!("edge ({src})-[{label}]->({dst}) properties do not decode: {e}"),
            })?;
        if !admitted {
            return Ok(EdgeCrossing::Rejected);
        }
        if !self.with_properties {
            return Ok(EdgeCrossing::Admitted(None));
        }
        Ok(EdgeCrossing::Admitted(Some(property_object(
            &properties,
            src,
            label,
            dst,
        )?)))
    }

    /// The stored property map of `(src, label, dst)`, empty when the edge
    /// has no live version. Opens the hop's read transaction on first use.
    fn stored(&mut self, src: &str, label: &str, dst: &str) -> crate::Result<Vec<u8>> {
        let reader = match &mut self.reader {
            Some(reader) => reader,
            empty => empty.insert(self.store.property_reader()?),
        };
        Ok(reader
            .current(EdgeRef::new(
                self.database,
                self.tenant,
                self.collection,
                src,
                label,
                dst,
            ))?
            .unwrap_or_default())
    }
}

/// The stored property map as a `Value::Object`. Empty bytes are `{}`.
fn property_object(bytes: &[u8], src: &str, label: &str, dst: &str) -> crate::Result<Value> {
    if bytes.is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    match nodedb_types::json_msgpack::value_from_msgpack(bytes) {
        Ok(object @ Value::Object(_)) => Ok(object),
        Ok(other) => Err(crate::Error::Codec {
            detail: format!(
                "edge ({src})-[{label}]->({dst}) stores properties that are not a map: {other:?}"
            ),
        }),
        Err(e) => Err(crate::Error::Codec {
            detail: format!("edge ({src})-[{label}]->({dst}) properties do not decode: {e}"),
        }),
    }
}

/// One oriented pass of a hop: the edges leaving, or entering, the frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum Orientation {
    Out,
    In,
}

impl Orientation {
    /// The CSR direction this pass reads.
    pub(in crate::data::executor) fn direction(self) -> Direction {
        match self {
            Self::Out => Direction::Out,
            Self::In => Direction::In,
        }
    }

    /// The physical `(src, dst)` of a row `(frontier, neighbour)` crossed in
    /// this pass.
    pub(in crate::data::executor) fn endpoints<'s>(
        self,
        frontier: &'s str,
        neighbour: &'s str,
    ) -> (&'s str, &'s str) {
        match self {
            Self::Out => (frontier, neighbour),
            Self::In => (neighbour, frontier),
        }
    }
}

/// The passes one hop runs.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::data::executor) enum HopPasses {
    /// One `Both` read. No crossed edge resolves to its physical orientation.
    Unoriented,
    /// One read per orientation. Each crossed edge resolves to its physical
    /// `(src, dst)`.
    Oriented(&'static [Orientation]),
}

/// The passes one hop runs. Reading an edge's properties, or merging a
/// transaction's staged writes, needs each edge's orientation, so `Both`
/// then runs outgoing then incoming.
pub(in crate::data::executor) fn hop_passes(direction: Direction, oriented: bool) -> HopPasses {
    match (direction, oriented) {
        (Direction::Both, true) => HopPasses::Oriented(&[Orientation::Out, Orientation::In]),
        (Direction::Both, false) => HopPasses::Unoriented,
        (Direction::Out, _) => HopPasses::Oriented(&[Orientation::Out]),
        (Direction::In, _) => HopPasses::Oriented(&[Orientation::In]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_splits_only_when_oriented() {
        assert_eq!(
            hop_passes(Direction::Both, true),
            HopPasses::Oriented(&[Orientation::Out, Orientation::In])
        );
        assert_eq!(hop_passes(Direction::Both, false), HopPasses::Unoriented);
        assert_eq!(
            hop_passes(Direction::In, true),
            HopPasses::Oriented(&[Orientation::In])
        );
        assert_eq!(
            hop_passes(Direction::Out, false),
            HopPasses::Oriented(&[Orientation::Out])
        );
    }

    #[test]
    fn an_incoming_row_flips_to_its_physical_orientation() {
        assert_eq!(Orientation::In.endpoints("b", "a"), ("a", "b"));
        assert_eq!(Orientation::Out.endpoints("a", "b"), ("a", "b"));
    }

    #[test]
    fn property_object_reads_a_map_and_refuses_anything_else() {
        assert_eq!(
            property_object(&[], "a", "L", "b").unwrap(),
            Value::Object(Default::default())
        );
        let map = nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"w": 2}))
            .expect("encode");
        let Value::Object(fields) = property_object(&map, "a", "L", "b").unwrap() else {
            panic!("a map decodes as an object");
        };
        assert_eq!(fields.get("w"), Some(&Value::Integer(2)));
        let list =
            nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!([1])).expect("encode");
        assert!(property_object(&list, "a", "L", "b").is_err());
    }

    fn store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open edge store");
        (store, dir)
    }

    fn scope<'a>(
        collection: Option<&'a str>,
        filters: &'a [MetadataFilter],
        with_properties: bool,
    ) -> HopPropertyScope<'a> {
        HopPropertyScope {
            database: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection,
            filters,
            with_properties,
        }
    }

    #[test]
    fn nothing_to_do_opens_nothing_and_a_predicate_needs_a_collection() {
        let (store, _dir) = store();
        assert!(
            HopEdgeProperties::open(&store, scope(None, &[], false))
                .unwrap()
                .is_none()
        );
        let filters = [MetadataFilter::eq("k", 1i64)];
        assert!(matches!(
            HopEdgeProperties::open(&store, scope(None, &filters, false)),
            Err(crate::Error::BadRequest { .. })
        ));
        assert!(matches!(
            HopEdgeProperties::open(&store, scope(None, &[], true)),
            Err(crate::Error::BadRequest { .. })
        ));
    }

    #[test]
    fn cross_filters_on_stored_properties_and_returns_them() {
        let (store, _dir) = store();
        let props = nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"score": 9}))
            .expect("encode");
        let edge = EdgeRef::new(DatabaseId::DEFAULT, TenantId::new(1), "g", "a", "L", "b");
        store
            .put_edge_versioned(edge, &props, 10, 10, i64::MAX)
            .expect("put edge");
        let filters = [MetadataFilter::Gt {
            field: "score".into(),
            value: Value::Integer(5),
        }];
        let mut hop = HopEdgeProperties::open(&store, scope(Some("g"), &filters, true))
            .unwrap()
            .expect("a predicate opens a reader");
        match hop.cross("a", "L", "b", None).unwrap() {
            EdgeCrossing::Admitted(Some(Value::Object(fields))) => {
                assert_eq!(fields.get("score"), Some(&Value::Integer(9)));
            }
            _ => panic!("score 9 > 5 admits the edge with its properties"),
        }
        // An edge with no live version evaluates as `{}`: `score > 5` fails.
        assert!(matches!(
            hop.cross("a", "L", "zz", None).unwrap(),
            EdgeCrossing::Rejected
        ));
    }

    /// A staged map replaces the stored one for the predicate and the result.
    #[test]
    fn staged_properties_replace_the_stored_map() {
        let (store, _dir) = store();
        let stored = nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"score": 9}))
            .expect("encode");
        let edge = EdgeRef::new(DatabaseId::DEFAULT, TenantId::new(1), "g", "a", "L", "b");
        store
            .put_edge_versioned(edge, &stored, 10, 10, i64::MAX)
            .expect("put edge");
        let staged = nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"score": 1}))
            .expect("encode");
        let filters = [MetadataFilter::Gt {
            field: "score".into(),
            value: Value::Integer(5),
        }];
        let mut hop = HopEdgeProperties::open(&store, scope(Some("g"), &filters, true))
            .unwrap()
            .expect("a predicate opens a reader");
        assert!(matches!(
            hop.cross("a", "L", "b", Some(&staged)).unwrap(),
            EdgeCrossing::Rejected
        ));
        assert!(hop.reader.is_none(), "a staged map reads nothing from the store");
        let fresh = nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"score": 7}))
            .expect("encode");
        match hop.cross("a", "L", "new", Some(&fresh)).unwrap() {
            EdgeCrossing::Admitted(Some(Value::Object(fields))) => {
                assert_eq!(fields.get("score"), Some(&Value::Integer(7)));
            }
            _ => panic!("a staged edge with score 7 is admitted with its staged map"),
        }
    }

    /// A predicate-only hop refuses an edge whose properties do not decode,
    /// as a hop that returns properties does.
    #[test]
    fn undecodable_properties_fail_a_predicate_only_hop() {
        let (store, _dir) = store();
        let mut corrupt =
            nodedb_types::json_msgpack::json_to_msgpack(&serde_json::json!({"score": 9}))
                .expect("encode");
        let last = corrupt.len() - 1;
        corrupt[last] = 0xc1;
        let filters = [MetadataFilter::Gt {
            field: "score".into(),
            value: Value::Integer(5),
        }];
        for with_properties in [false, true] {
            let mut hop =
                HopEdgeProperties::open(&store, scope(Some("g"), &filters, with_properties))
                    .unwrap()
                    .expect("a predicate opens a reader");
            assert!(matches!(
                hop.cross("a", "L", "b", Some(&corrupt)),
                Err(crate::Error::Codec { .. })
            ));
        }
    }
}
