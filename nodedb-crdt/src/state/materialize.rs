// SPDX-License-Identifier: Apache-2.0

//! Native materialization of row values and nested containers.

use loro::LoroValue;

use super::core::CrdtState;

impl CrdtState {
    /// Read a row with nested containers materialized by Loro.
    ///
    /// Returns `None` for an absent row. Unlike [`Self::read_row`], this
    /// recursively reads the row's containers and preserves native scalar types.
    pub fn read_row_deep(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        self.doc
            .get_map(collection)
            .get(row_id)
            .map(|row| row.get_deep_value())
    }
}

#[cfg(test)]
mod tests {
    use loro::{
        Container, LoroCounter, LoroList, LoroMap, LoroText, LoroTree, LoroValue, ValueOrContainer,
    };

    use super::CrdtState;

    fn map(fields: &[(&str, LoroValue)]) -> LoroValue {
        LoroValue::Map(
            fields
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    fn nested_row(state: &CrdtState) -> LoroMap {
        let row = state
            .doc
            .get_map("pages")
            .insert_container("p1", LoroMap::new())
            .unwrap();
        row.insert("title", "Draft").unwrap();
        let metadata = row.insert_container("metadata", LoroMap::new()).unwrap();
        metadata.insert("integer", i64::MAX).unwrap();
        metadata.insert("double", 1.000_000_000_000_000_2).unwrap();
        metadata
            .insert("binary", LoroValue::Binary(vec![0, 128, 255].into()))
            .unwrap();
        metadata.insert("enabled", true).unwrap();
        metadata.insert("empty", LoroValue::Null).unwrap();
        let tags = metadata.insert_container("tags", LoroList::new()).unwrap();
        tags.insert(0, "first").unwrap();
        let tag = tags.insert_container(1, LoroMap::new()).unwrap();
        tag.insert("priority", 7_i64).unwrap();
        let text = row.insert_container("body", LoroText::new()).unwrap();
        text.insert(0, "Native text 🦀").unwrap();
        let counter = row.insert_container("count", LoroCounter::new()).unwrap();
        counter.increment(2.5).unwrap();
        let tree = row.insert_container("tree", LoroTree::new()).unwrap();
        let root = tree.create(None).unwrap();
        tree.get_meta(root)
            .unwrap()
            .insert("label", "root")
            .unwrap();
        state.doc.commit();
        state
            .list_insert_fields(
                "pages",
                "p1",
                "blocks",
                0,
                &[("id".into(), LoroValue::String("b1".into()))],
            )
            .unwrap();
        row
    }

    #[test]
    fn deep_read_materializes_native_containers_and_typed_scalars() {
        let state = CrdtState::new(1).unwrap();
        let row = nested_row(&state);
        let Some(ValueOrContainer::Container(Container::Tree(tree))) = row.get("tree") else {
            panic!("expected a tree container");
        };
        let roots = tree.roots();
        assert_eq!(roots.len(), 1);
        let tree = LoroValue::List(
            vec![map(&[
                ("id", roots[0].to_string().into()),
                ("parent", LoroValue::Null),
                ("index", LoroValue::I64(0)),
                ("fractional_index", "80".into()),
                ("children", LoroValue::List(Vec::new().into())),
                ("meta", map(&[("label", "root".into())])),
            ])]
            .into(),
        );
        let expected = map(&[
            ("title", "Draft".into()),
            (
                "metadata",
                map(&[
                    ("integer", LoroValue::I64(i64::MAX)),
                    ("double", LoroValue::Double(1.000_000_000_000_000_2)),
                    ("binary", LoroValue::Binary(vec![0, 128, 255].into())),
                    ("enabled", LoroValue::Bool(true)),
                    ("empty", LoroValue::Null),
                    (
                        "tags",
                        LoroValue::List(
                            vec!["first".into(), map(&[("priority", LoroValue::I64(7))])].into(),
                        ),
                    ),
                ]),
            ),
            ("body", "Native text 🦀".into()),
            ("count", LoroValue::Double(2.5)),
            ("tree", tree),
            (
                "blocks",
                LoroValue::List(vec![map(&[("id", "b1".into())])].into()),
            ),
        ]);
        assert_eq!(state.read_row_deep("pages", "p1"), Some(expected));
        assert_eq!(
            state.read_row_deep("pages", "p1"),
            Some(row.get_deep_value())
        );

        let LoroValue::Map(shallow) = state.read_row("pages", "p1").unwrap() else {
            panic!("expected a shallow row map");
        };
        for field in ["metadata", "body", "count", "tree", "blocks"] {
            assert!(matches!(shallow.get(field), Some(LoroValue::Container(_))));
        }
    }

    #[test]
    fn deep_read_returns_none_for_absent_rows() {
        let state = CrdtState::new(1).unwrap();
        assert_eq!(state.read_row_deep("absent", "p1"), None);
        nested_row(&state);
        assert_eq!(state.read_row_deep("pages", "absent"), None);
        state.delete("pages", "p1").unwrap();
        assert_eq!(state.read_row_deep("pages", "p1"), None);
    }

    #[test]
    fn deep_read_preserves_scalar_rows() {
        let state = CrdtState::new(1).unwrap();
        for (row_id, value) in [
            ("integer", LoroValue::I64(i64::MAX)),
            ("binary", LoroValue::Binary(vec![0, 255].into())),
            ("null", LoroValue::Null),
        ] {
            state
                .doc
                .get_map("values")
                .insert(row_id, value.clone())
                .unwrap();
            state.doc.commit();
            assert_eq!(state.read_row_deep("values", row_id), Some(value));
        }
    }

    #[test]
    fn deep_read_reflects_later_nested_changes() {
        let state = CrdtState::new(1).unwrap();
        let row = nested_row(&state);
        let before = state.read_row_deep("pages", "p1");
        state
            .list_insert_fields("pages", "p1", "blocks", 1, &[("id".into(), "b2".into())])
            .unwrap();
        let after = state.read_row_deep("pages", "p1");
        assert_ne!(before, after);
        assert_eq!(after, Some(row.get_deep_value()));
    }

    #[test]
    fn deep_read_preserves_operations_deltas_and_frontiers() {
        let state = CrdtState::new(1).unwrap();
        nested_row(&state);
        let version = state.oplog_version_vector();
        let frontier = state.doc.state_frontiers();
        let oplog_frontier = state.doc.oplog_frontiers();
        let operations = state.local_op_counter();
        let pending = state.doc.get_pending_txn_len();
        let delta = state.export_updates_since(&version).unwrap();

        for _ in 0..3 {
            assert!(state.read_row_deep("pages", "p1").is_some());
            assert!(state.read_row_deep("pages", "absent").is_none());
            assert!(state.read_row_deep("absent", "p1").is_none());
        }

        assert_eq!(state.oplog_version_vector(), version);
        assert_eq!(state.doc.state_frontiers(), frontier);
        assert_eq!(state.doc.oplog_frontiers(), oplog_frontier);
        assert_eq!(state.local_op_counter(), operations);
        assert_eq!(state.doc.get_pending_txn_len(), pending);
        assert_eq!(state.export_updates_since(&version).unwrap(), delta);
    }
}
