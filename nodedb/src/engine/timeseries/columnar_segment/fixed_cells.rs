// SPDX-License-Identifier: BUSL-1.1

//! The on-disk cell layout of the decimal, UUID and boolean columns.
//!
//! Each cell is fixed-width and carries its own validity byte, so a NULL
//! cell needs no separate bitmap:
//!
//! - DECIMAL: `[valid][16 bytes of rust_decimal::Decimal::serialize]`.
//! - UUID: `[valid][16 bytes]`.
//! - BOOLEAN: one byte, `0` false, `1` true, `2` NULL.
//!
//! A NULL decimal or UUID cell writes zero payload bytes. The raw cells then
//! pass through the codec's bytes pipeline.

use super::error::SegmentError;

/// Bytes one DECIMAL or UUID cell takes: the validity byte and 16 bytes.
const WIDE_CELL: usize = 17;

const BOOL_FALSE: u8 = 0;
const BOOL_TRUE: u8 = 1;
const BOOL_NULL: u8 = 2;

/// The raw cells of a 16-byte column.
pub(super) fn wide_cells_to_bytes(cells: &[Option<[u8; 16]>]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(cells.len() * WIDE_CELL);
    for cell in cells {
        match cell {
            Some(bytes) => {
                raw.push(1);
                raw.extend_from_slice(bytes);
            }
            None => {
                raw.push(0);
                raw.extend_from_slice(&[0; 16]);
            }
        }
    }
    raw
}

/// Read the raw cells of a 16-byte column.
pub(super) fn wide_cells_from_bytes(raw: &[u8]) -> Result<Vec<Option<[u8; 16]>>, SegmentError> {
    if !raw.len().is_multiple_of(WIDE_CELL) {
        return Err(SegmentError::Corrupt(format!(
            "16-byte cell column of {} bytes is not a whole number of {WIDE_CELL}-byte cells",
            raw.len()
        )));
    }
    raw.as_chunks::<WIDE_CELL>()
        .0
        .iter()
        .map(|cell| match cell[0] {
            0 => Ok(None),
            1 => {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&cell[1..]);
                Ok(Some(bytes))
            }
            other => Err(SegmentError::Corrupt(format!(
                "16-byte cell has validity byte {other}, expected 0 or 1"
            ))),
        })
        .collect()
}

/// The raw cells of a DECIMAL column.
pub(super) fn decimal_cells_to_bytes(cells: &[Option<rust_decimal::Decimal>]) -> Vec<u8> {
    let wide: Vec<Option<[u8; 16]>> = cells.iter().map(|d| d.map(|d| d.serialize())).collect();
    wide_cells_to_bytes(&wide)
}

/// Read the raw cells of a DECIMAL column. A cell whose scale is past
/// `rust_decimal`'s maximum is corrupt.
pub(super) fn decimal_cells_from_bytes(
    raw: &[u8],
) -> Result<Vec<Option<rust_decimal::Decimal>>, SegmentError> {
    wide_cells_from_bytes(raw)?
        .into_iter()
        .map(|cell| {
            cell.map(|bytes| {
                // Byte 2 of the serialized form is the scale.
                if u32::from(bytes[2]) > rust_decimal::Decimal::MAX_SCALE {
                    return Err(SegmentError::Corrupt(format!(
                        "decimal cell has scale {}, past the maximum {}",
                        bytes[2],
                        rust_decimal::Decimal::MAX_SCALE
                    )));
                }
                Ok(rust_decimal::Decimal::deserialize(bytes))
            })
            .transpose()
        })
        .collect()
}

/// The raw cells of a BOOLEAN column.
pub(super) fn bool_cells_to_bytes(cells: &[Option<bool>]) -> Vec<u8> {
    cells
        .iter()
        .map(|cell| match cell {
            Some(false) => BOOL_FALSE,
            Some(true) => BOOL_TRUE,
            None => BOOL_NULL,
        })
        .collect()
}

/// Read the raw cells of a BOOLEAN column.
pub(super) fn bool_cells_from_bytes(raw: &[u8]) -> Result<Vec<Option<bool>>, SegmentError> {
    raw.iter()
        .map(|byte| match *byte {
            BOOL_FALSE => Ok(Some(false)),
            BOOL_TRUE => Ok(Some(true)),
            BOOL_NULL => Ok(None),
            other => Err(SegmentError::Corrupt(format!(
                "boolean cell byte {other}, expected 0, 1 or 2"
            ))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_precision_decimals_round_trip_exactly() {
        let cells = vec![
            Some("0.1000000000000000001".parse().expect("decimal")),
            None,
            Some("12345678901234567.89".parse().expect("decimal")),
            Some("-79228162514264337593543950335".parse().expect("decimal")),
        ];
        let raw = decimal_cells_to_bytes(&cells);
        assert_eq!(decimal_cells_from_bytes(&raw).expect("decode"), cells);
    }

    #[test]
    fn uuid_and_bool_cells_round_trip() {
        let uuids = vec![Some([7u8; 16]), None, Some([0u8; 16])];
        let raw = wide_cells_to_bytes(&uuids);
        assert_eq!(wide_cells_from_bytes(&raw).expect("decode"), uuids);
        let bools = vec![Some(true), None, Some(false)];
        let raw = bool_cells_to_bytes(&bools);
        assert_eq!(bool_cells_from_bytes(&raw).expect("decode"), bools);
    }

    #[test]
    fn malformed_cells_are_corrupt() {
        assert!(matches!(
            wide_cells_from_bytes(&[1, 2, 3]),
            Err(SegmentError::Corrupt(_))
        ));
        let mut bad_validity = wide_cells_to_bytes(&[Some([1; 16])]);
        bad_validity[0] = 9;
        assert!(matches!(
            wide_cells_from_bytes(&bad_validity),
            Err(SegmentError::Corrupt(_))
        ));
        assert!(matches!(
            bool_cells_from_bytes(&[3]),
            Err(SegmentError::Corrupt(_))
        ));
        let mut bad_scale = [0u8; 16];
        bad_scale[2] = 29;
        let raw = wide_cells_to_bytes(&[Some(bad_scale)]);
        assert!(matches!(
            decimal_cells_from_bytes(&raw),
            Err(SegmentError::Corrupt(_))
        ));
    }

    /// A memtable flush stores DECIMAL, UUID and BOOLEAN cells exactly: the
    /// flushed partition reads back every high-precision decimal, UUID,
    /// boolean and NULL as ingested, and its schema keeps the types.
    #[test]
    fn a_flush_round_trips_typed_cells_exactly() {
        use super::super::super::columnar_memtable::{
            ColumnData, ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig,
            ColumnarSchema, TimeKind,
        };
        use super::super::{ColumnarSegmentReader, ColumnarSegmentWriter};

        let decimals: Vec<rust_decimal::Decimal> = [
            "0.1000000000000000001",
            "12345678901234567.89",
            "-0.0000000000000000000000000001",
        ]
        .iter()
        .map(|d| d.parse().expect("decimal"))
        .collect();
        let uuid = *uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
            .expect("uuid")
            .as_bytes();
        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("price".into(), ColumnType::Decimal(None)),
                ("device".into(), ColumnType::Uuid),
                ("ok".into(), ColumnType::Bool),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        for (i, d) in decimals.iter().enumerate() {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(1_000 + i as i64),
                    ColumnValue::decimal(*d),
                    ColumnValue::Uuid(uuid),
                    ColumnValue::Bool(i % 2 == 0),
                ],
            )
            .expect("ingest");
        }
        mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(2_000),
                ColumnValue::Null,
                ColumnValue::Null,
                ColumnValue::Null,
            ],
        )
        .expect("ingest NULLs");
        assert_eq!(
            mt.column(1).as_decimals()[..3],
            decimals.iter().copied().map(Some).collect::<Vec<_>>()[..]
        );

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let writer = ColumnarSegmentWriter::new(tmp.path());
        writer
            .write_partition("ts-typed", &mt.flush_view(), 86_400_000, 1, None)
            .expect("flush");
        let dir = tmp.path().join("ts-typed");
        let read_schema = ColumnarSegmentReader::read_schema(&dir, None).expect("schema");
        assert_eq!(read_schema.columns, mt.schema().columns);

        let price =
            ColumnarSegmentReader::read_column(&dir, "price", ColumnType::Decimal(None), None)
                .expect("price");
        let mut expected: Vec<Option<rust_decimal::Decimal>> =
            decimals.iter().copied().map(Some).collect();
        expected.push(None);
        assert!(matches!(&price, ColumnData::Decimal(cells) if *cells == expected));
        // The scale survives too: the stored decimal is the one ingested.
        assert_eq!(
            price.as_decimals()[1].map(|d| d.to_string()),
            Some("12345678901234567.89".to_string())
        );
        let device = ColumnarSegmentReader::read_column(&dir, "device", ColumnType::Uuid, None)
            .expect("device");
        assert_eq!(
            device.as_uuids(),
            &[Some(uuid), Some(uuid), Some(uuid), None]
        );
        let ok =
            ColumnarSegmentReader::read_column(&dir, "ok", ColumnType::Bool, None).expect("ok");
        assert_eq!(ok.as_bools(), &[Some(true), Some(false), Some(true), None]);
    }
}
