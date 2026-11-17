// SPDX-License-Identifier: BUSL-1.1

//! Integer key parts for DECIMAL and UUID GROUP BY columns.
//!
//! A grouping strategy packs each GROUP BY cell into one `u64` part. A
//! decimal or a UUID is 128 bits, so one source interns each column first:
//! every distinct value gets a dense code, and the code is the part. Equal
//! decimals of different scales (`1.5`, `1.50`) share one code. The source
//! keeps one representative value per code, which the key resolves back to
//! ([`super::strategies::keys::key_cells`]).
//!
//! A code is local to its source. The typed key it resolves to is what the
//! merge across sources compares, as for a symbol id.

use std::collections::HashMap;

use super::super::columnar_memtable::{ColumnData, ColumnType};
use super::types::ResolvedSchema;

/// The code a NULL cell packs as. No value has it.
pub(super) const NULL_CODE: i64 = -1;

/// The per-row codes of one interned GROUP BY column.
pub(super) struct ExactKeyCodes {
    /// The GROUP BY position the codes key.
    position: usize,
    /// One code per row, as `ColumnData::Int64`. NULL is [`NULL_CODE`].
    codes: ColumnData,
}

/// Intern every DECIMAL and UUID GROUP BY column of one source.
///
/// Stores each column's representatives in `resolved.group_reps` and returns
/// the codes, which [`point_group_columns_at_codes`] then makes the column the
/// strategies read. A column the source holds no data for is left as it is:
/// every row of it is NULL.
pub(super) fn intern_exact_group_columns(
    resolved: &mut ResolvedSchema,
    columns: &[Option<&ColumnData>],
) -> Vec<ExactKeyCodes> {
    let mut out = Vec::new();
    for (position, &(col_idx, ty)) in resolved.group_cols.iter().enumerate() {
        if !matches!(ty, ColumnType::Decimal(_) | ColumnType::Uuid) {
            continue;
        }
        let Some(data) = columns.get(col_idx).copied().flatten() else {
            continue;
        };
        let (codes, reps) = match data {
            ColumnData::Decimal(cells) => {
                let (codes, reps) = intern(cells.iter().map(|cell| cell.map(decimal_identity)));
                (codes, ColumnData::Decimal(reps))
            }
            ColumnData::Uuid(cells) => {
                let (codes, reps) = intern(cells.iter().map(|cell| cell.map(|b| (b, b))));
                (codes, ColumnData::Uuid(reps))
            }
            _ => continue,
        };
        resolved.group_reps.insert(position, reps);
        out.push(ExactKeyCodes {
            position,
            codes: ColumnData::Int64(codes),
        });
    }
    out
}

/// Point each interned GROUP BY column at its codes: the codes join
/// `columns` after the source's own columns, and the group column reads
/// them. Aggregates and filters keep reading the stored column.
pub(super) fn point_group_columns_at_codes<'a>(
    resolved: &mut ResolvedSchema,
    columns: &mut Vec<Option<&'a ColumnData>>,
    codes: &'a [ExactKeyCodes],
) {
    for interned in codes {
        resolved.group_cols[interned.position].0 = columns.len();
        columns.push(Some(&interned.codes));
    }
}

/// A decimal's identity, its normalized form, and its representative, the
/// decimal itself: a group keeps the scale of its first-seen key.
fn decimal_identity(d: rust_decimal::Decimal) -> ([u8; 16], rust_decimal::Decimal) {
    (d.normalize().serialize(), d)
}

/// Dense codes for `cells`, each `(identity, representative)`, and the
/// representative of each code. A `None` cell is [`NULL_CODE`].
fn intern<R: Copy>(
    cells: impl Iterator<Item = Option<([u8; 16], R)>>,
) -> (Vec<i64>, Vec<Option<R>>) {
    let mut by_identity: HashMap<[u8; 16], i64> = HashMap::new();
    let mut reps: Vec<Option<R>> = Vec::new();
    let codes = cells
        .map(|cell| match cell {
            None => NULL_CODE,
            Some((identity, rep)) => *by_identity.entry(identity).or_insert_with(|| {
                reps.push(Some(rep));
                (reps.len() - 1) as i64
            }),
        })
        .collect();
    (codes, reps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_decimals_of_any_scale_share_a_code() {
        let cells: Vec<Option<rust_decimal::Decimal>> = vec![
            Some("1.50".parse().expect("decimal")),
            None,
            Some("1.5".parse().expect("decimal")),
            Some("0.1000000000000000001".parse().expect("decimal")),
        ];
        let (codes, reps) = intern(cells.iter().map(|cell| cell.map(decimal_identity)));
        assert_eq!(codes, vec![0, NULL_CODE, 0, 1]);
        assert_eq!(reps.len(), 2);
        // The representative is the first-seen key, with its scale.
        let first = reps[0].expect("representative");
        assert_eq!(first.to_string(), "1.50");
        assert_eq!(
            reps[1],
            Some("0.1000000000000000001".parse().expect("decimal"))
        );
    }

    /// A grouped aggregate over DECIMAL cells is exact, and a UUID or
    /// DECIMAL GROUP BY column groups by value: SUM and AVG keep every
    /// digit, MIN and MAX return the stored decimals, and `1.5` and `1.50`
    /// are one group.
    #[test]
    fn grouped_decimal_aggregates_are_exact() {
        use super::super::super::columnar_memtable::{
            ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
        };
        use super::super::{GroupCell, aggregate_memtable};
        use nodedb_types::Value;

        let dec = |s: &str| -> rust_decimal::Decimal { s.parse().expect("decimal") };
        let a = *uuid::Uuid::from_u128(1).as_bytes();
        let b = *uuid::Uuid::from_u128(2).as_bytes();
        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("device".into(), ColumnType::Uuid),
                ("price".into(), ColumnType::Decimal(None)),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        for (i, (device, price)) in [
            (a, "0.1000000000000000001"),
            (a, "0.2000000000000000002"),
            (b, "1.5"),
            (b, "1.50"),
        ]
        .iter()
        .enumerate()
        {
            mt.ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(1_000 + i as i64),
                    ColumnValue::Uuid(*device),
                    ColumnValue::decimal(dec(price)),
                ],
            )
            .expect("ingest");
        }
        let aggregates = [
            ("sum".to_string(), "price".to_string()),
            ("avg".to_string(), "price".to_string()),
            ("min".to_string(), "price".to_string()),
            ("max".to_string(), "price".to_string()),
        ];
        let by_device = aggregate_memtable(
            &mt,
            &["device".to_string()],
            &aggregates,
            &[],
            (0, i64::MAX),
            0,
        )
        .expect("aggregate")
        .expect("columns resolve");
        assert_eq!(by_device.groups.len(), 2);
        let group = |id: [u8; 16]| {
            by_device
                .groups
                .iter()
                .find(|(key, _)| key.cells == [GroupCell::Uuid(id)])
                .map(|(_, accums)| accums)
                .expect("group")
        };
        let first = group(a);
        assert_eq!(
            first[0].sum_value().expect("sum"),
            Value::Decimal(dec("0.3000000000000000003"))
        );
        assert_eq!(
            first[1].avg_value().expect("avg"),
            Value::Decimal(dec("0.15000000000000000015"))
        );
        assert_eq!(
            first[2].min(),
            Some(&Value::Decimal(dec("0.1000000000000000001")))
        );
        assert_eq!(
            first[3].max(),
            Some(&Value::Decimal(dec("0.2000000000000000002")))
        );
        assert_eq!(
            group(b)[0].sum_value().expect("sum"),
            Value::Decimal(dec("3.00"))
        );

        let by_price = aggregate_memtable(
            &mt,
            &["price".to_string()],
            &[("count".to_string(), "*".to_string())],
            &[],
            (0, i64::MAX),
            0,
        )
        .expect("aggregate")
        .expect("columns resolve");
        assert_eq!(by_price.groups.len(), 3, "1.5 and 1.50 are one group");
        let one_and_a_half = by_price
            .groups
            .iter()
            .find(|(key, _)| key.cells == [GroupCell::decimal(dec("1.5"))])
            .map(|(_, accums)| accums[0].count)
            .expect("1.5 group");
        assert_eq!(one_and_a_half, 2);
    }
}
