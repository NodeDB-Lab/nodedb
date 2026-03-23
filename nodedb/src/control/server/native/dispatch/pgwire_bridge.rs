//! Convert pgwire DDL results to native protocol responses.

use nodedb_types::protocol::NativeResponse;
use nodedb_types::value::Value;

/// Convert a pgwire DDL result to a NativeResponse.
///
/// The DDL router returns pgwire `Response` types. We consume query
/// streams to extract row data, parsing pgwire `DataRow` raw bytes
/// into native `Value` columns.
pub(super) async fn pgwire_result_to_native(
    seq: u64,
    result: pgwire::error::PgWireResult<Vec<pgwire::api::results::Response>>,
) -> NativeResponse {
    use futures::StreamExt;
    use pgwire::api::results::Response as PgResponse;

    match result {
        Ok(responses) => {
            for resp in responses {
                match resp {
                    PgResponse::Execution(tag) => {
                        return NativeResponse::status_row(seq, format!("{tag:?}"));
                    }
                    PgResponse::Query(mut query_resp) => {
                        let schema = query_resp.row_schema();
                        let columns: Vec<String> =
                            schema.iter().map(|f| f.name().to_string()).collect();
                        let ncols = columns.len();

                        let row_stream = query_resp.data_rows();
                        let mut rows: Vec<Vec<Value>> = Vec::new();
                        while let Some(row_result) = row_stream.next().await {
                            match row_result {
                                Ok(data_row) => {
                                    let row_vals = parse_data_row_fields(&data_row.data, ncols);
                                    rows.push(row_vals);
                                }
                                Err(e) => {
                                    return NativeResponse::error(
                                        seq,
                                        "XX000",
                                        format!("row stream error: {e}"),
                                    );
                                }
                            }
                        }

                        return NativeResponse {
                            seq,
                            status: nodedb_types::protocol::ResponseStatus::Ok,
                            columns: Some(columns),
                            rows: Some(rows),
                            rows_affected: None,
                            watermark_lsn: 0,
                            error: None,
                            auth: None,
                        };
                    }
                    PgResponse::EmptyQuery => {
                        return NativeResponse::ok(seq);
                    }
                    _ => {}
                }
            }
            NativeResponse::ok(seq)
        }
        Err(e) => NativeResponse::error(seq, "XX000", format!("{e}")),
    }
}

/// Parse raw pgwire DataRow field bytes into Value vec.
///
/// Each field is encoded as: `i32 len` (-1 = NULL), then `len` bytes of
/// text-encoded data. This matches pgwire text format encoding.
fn parse_data_row_fields(data: &[u8], expected_fields: usize) -> Vec<Value> {
    let mut values = Vec::with_capacity(expected_fields);
    let mut offset = 0;

    for _ in 0..expected_fields {
        if offset + 4 > data.len() {
            values.push(Value::Null);
            continue;
        }
        let len = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        if len < 0 {
            values.push(Value::Null);
        } else {
            let len = len as usize;
            if offset + len > data.len() {
                values.push(Value::Null);
                break;
            }
            let text = String::from_utf8_lossy(&data[offset..offset + len]).into_owned();
            values.push(Value::String(text));
            offset += len;
        }
    }

    values
}
