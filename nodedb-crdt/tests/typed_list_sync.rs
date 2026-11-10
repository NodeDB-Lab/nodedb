// SPDX-License-Identifier: Apache-2.0

//! A block-list field keeps its exact type through a CRDT sync.
//!
//! One peer inserts a block of typed fields, encoded through
//! `nodedb_crdt::loro_value`. A second peer imports the first peer's
//! snapshot, as a sync peer does, and decodes the block back to exactly the
//! values written: a Decimal with its scale, a UUID, an instant and bytes.

use std::collections::HashMap;

use loro::LoroValue;

use nodedb_crdt::loro_value::{fields_to_loro_map, loro_to_value};
use nodedb_crdt::state::CrdtState;
use nodedb_types::{NdbDateTime, Value};

fn typed_block() -> HashMap<String, Value> {
    let at = NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant");
    HashMap::from([
        (
            "price".to_string(),
            Value::Decimal("12.50".parse().expect("decimal literal")),
        ),
        (
            "uid".to_string(),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
        ),
        ("at".to_string(), Value::DateTime(at)),
        ("local".to_string(), Value::NaiveDateTime(at)),
        ("blob".to_string(), Value::Bytes(vec![0x01, 0x02, 0xff])),
        ("title".to_string(), Value::String("hello".into())),
    ])
}

#[test]
fn a_synced_block_keeps_every_type() {
    let origin = CrdtState::new(1).expect("origin state");
    origin
        .upsert(
            "pages",
            "doc-1",
            &[("title", LoroValue::String("p".into()))],
        )
        .expect("row");
    let block = typed_block();
    let fields: Vec<(String, LoroValue)> = fields_to_loro_map(&block)
        .expect("block encodes")
        .into_iter()
        .collect();
    origin
        .list_insert_fields("pages", "doc-1", "blocks", 0, &fields)
        .expect("list insert");

    let peer = CrdtState::new(2).expect("peer state");
    peer.import(&origin.export_snapshot().expect("snapshot"))
        .expect("peer imports the snapshot");

    let synced = peer
        .list_get("pages", "doc-1", "blocks", 0)
        .expect("list get")
        .expect("the block synced");
    let Value::Object(got) = loro_to_value(&synced).expect("block decodes") else {
        panic!("a block decodes as an object");
    };
    assert_eq!(got, block);
    let Some(Value::Decimal(price)) = got.get("price") else {
        panic!("price stays a Decimal");
    };
    assert_eq!(price.to_string(), "12.50", "the scale survives the sync");

    // The whole row reads back typed too, the block list included.
    let row = peer.read_row("pages", "doc-1").expect("row synced");
    let Value::Object(row) = loro_to_value(&row).expect("row decodes") else {
        panic!("a row decodes as an object");
    };
    assert_eq!(row.get("title"), Some(&Value::String("p".into())));
}
