//! Binding-row serialization for SPSC transport.

use crate::engine::graph::pattern::executor::types::BindingRow;

/// Serialize binding rows for SPSC and cross-node transport: a msgpack array
/// of native tagged objects, the native row transport
/// (`response_codec::encode_native_rows`). Every cell keeps its exact type,
/// and a public boundary renders the rows through `RowContext::Flat`.
///
/// The array shape lets broadcast merge (`extract_msgpack_elements`) split
/// and re-merge rows from multiple cores. MATCH bindings carry user-visible
/// node ids: the CSR partition that produced them is tenant-scoped by
/// construction, so there is no `<tid>:` prefix to strip.
pub fn rows_to_msgpack(rows: &[BindingRow]) -> Result<Vec<u8>, crate::Error> {
    let objects: Vec<nodedb_types::Value> = rows
        .iter()
        .map(|row| nodedb_types::Value::Object(row.clone()))
        .collect();
    crate::data::executor::response_codec::encode_native_rows(&objects)
}
