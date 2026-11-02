// SPDX-License-Identifier: BUSL-1.1

//! The system time an index search reads at.
//!
//! A current vector search ranks the live nodes. A search at a cut (`AS OF SYSTEM
//! TIME`, a statement's pinned cut, a transaction block's snapshot) ranks the
//! node of each row visible at the cut and reads each candidate's body at the
//! same cut. A transaction snapshot also merges the transaction's staged
//! rows on top. The payload bitmap indexes of a vector-primary collection
//! describe live nodes only, so a search at a cut evaluates its payload
//! predicates against the bodies at the cut instead.

use nodedb_query::scan_filter::{FilterOp, ScanFilter};
use nodedb_types::{PayloadAtom, SystemTimeScope, Value};

use crate::bridge::envelope::ErrorCode;

/// The cut of one index search: vector, full-text, or spatial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) struct SearchCut {
    /// The ordinal the search reads at. `None` reads the current state.
    pub at: Option<i64>,
    /// Whether the transaction's staged rows merge on top.
    pub merge_overlay: bool,
}

impl SearchCut {
    /// The cut a search at `scope` reads. `AS OF SYSTEM TIME NULL` is no
    /// search input: a search ranks one state of the collection.
    pub(in crate::data::executor) fn of(scope: SystemTimeScope) -> crate::Result<Self> {
        if scope.is_all_versions() {
            return Err(crate::Error::BadRequest {
                detail: "AS OF SYSTEM TIME NULL is not supported on an index search; \
                         name a system time"
                    .into(),
            });
        }
        Ok(Self {
            at: scope.as_of_ordinal(),
            merge_overlay: scope.merges_txn_overlay(),
        })
    }
}

/// The payload predicates of a vector-primary search as row filters, so a
/// search at a cut evaluates them against the bodies at the cut. An atom
/// shape this build does not know fails with `Unsupported`.
pub(in crate::data::executor) fn payload_atoms_as_filters(
    atoms: &[PayloadAtom],
) -> Result<Vec<ScanFilter>, ErrorCode> {
    let filter = |field: &str, op: FilterOp, value: Value| ScanFilter {
        field: field.to_string(),
        op,
        value,
        clauses: Vec::new(),
        expr: None,
    };
    let mut filters = Vec::with_capacity(atoms.len());
    for atom in atoms {
        match atom {
            PayloadAtom::Eq(field, value) => {
                filters.push(filter(field, FilterOp::Eq, value.clone()))
            }
            PayloadAtom::In(field, values) => {
                filters.push(filter(field, FilterOp::In, Value::Array(values.clone())));
            }
            PayloadAtom::Range {
                field,
                low,
                low_inclusive,
                high,
                high_inclusive,
            } => {
                if let Some(low) = low {
                    let op = if *low_inclusive {
                        FilterOp::Gte
                    } else {
                        FilterOp::Gt
                    };
                    filters.push(filter(field, op, low.clone()));
                }
                if let Some(high) = high {
                    let op = if *high_inclusive {
                        FilterOp::Lte
                    } else {
                        FilterOp::Lt
                    };
                    filters.push(filter(field, op, high.clone()));
                }
            }
            other => {
                return Err(ErrorCode::Unsupported {
                    detail: format!(
                        "payload predicate on '{}' cannot be read at a system-time cut",
                        other.field()
                    ),
                });
            }
        }
    }
    Ok(filters)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_reads_its_ordinal_and_a_snapshot_merges_the_overlay() {
        assert_eq!(
            SearchCut::of(SystemTimeScope::Current).unwrap(),
            SearchCut {
                at: None,
                merge_overlay: true
            }
        );
        assert_eq!(
            SearchCut::of(SystemTimeScope::Pinned {
                ordinal: 9,
                cut_marker: 0
            })
            .unwrap(),
            SearchCut {
                at: Some(9),
                merge_overlay: false
            }
        );
        assert_eq!(
            SearchCut::of(SystemTimeScope::TxnSnapshot {
                ordinal: 9,
                cut_marker: 0
            })
            .unwrap(),
            SearchCut {
                at: Some(9),
                merge_overlay: true
            }
        );
        assert!(SearchCut::of(SystemTimeScope::AllVersions).is_err());
    }

    #[test]
    fn payload_atoms_become_row_filters() {
        let filters = payload_atoms_as_filters(&[
            PayloadAtom::Eq("owner".into(), Value::String("a".into())),
            PayloadAtom::Range {
                field: "score".into(),
                low: Some(Value::Integer(1)),
                low_inclusive: true,
                high: Some(Value::Integer(5)),
                high_inclusive: false,
            },
        ])
        .expect("known atoms");
        let ops: Vec<FilterOp> = filters.iter().map(|f| f.op).collect();
        assert_eq!(ops, vec![FilterOp::Eq, FilterOp::Gte, FilterOp::Lt]);
    }
}
