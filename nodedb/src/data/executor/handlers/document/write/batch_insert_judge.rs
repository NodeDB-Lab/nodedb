// SPDX-License-Identifier: BUSL-1.1

//! The per-row primary-key judgment of a `BatchInsert` page.
//!
//! A page row is an INSERT, judged as `PointInsert` judges one: a key already
//! stored is a `unique` violation (23505), and so is a key an earlier row of
//! the same page names. Under `if_absent` (`ON CONFLICT DO NOTHING`) such a
//! row is skipped instead. The probe runs inside the page's write
//! transaction, so no other writer lands a key between the probe and the
//! commit.

use std::collections::HashSet;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::StorageKey;

/// A key the page refuses: stored already, or named twice by the page.
fn duplicate_key(collection: &str, document_id: &str) -> crate::Error {
    crate::Error::RejectedConstraint {
        collection: collection.to_string(),
        constraint: "unique".to_string(),
        detail: format!(
            "duplicate key value '{document_id}' violates primary-key uniqueness on \
             '{collection}'"
        ),
    }
}

/// How the judgment reads one page row.
pub(in crate::data::executor) struct RowProbe<S, D> {
    /// Whether row `i`'s key is already stored.
    pub stored: S,
    /// The key value row `i` names, for the refusal message.
    pub describe: D,
}

/// Which page rows land, in page order, given which keys are stored and
/// whether a duplicate skips. A row's identity is its surrogate, the one
/// `PointInsert` probes: two rows with one primary-key value share it.
pub(in crate::data::executor) fn landing_rows<S, D>(
    collection: &str,
    surrogates: &[nodedb_types::Surrogate],
    if_absent: bool,
    probe: RowProbe<S, D>,
) -> crate::Result<Vec<usize>>
where
    S: FnMut(usize) -> crate::Result<bool>,
    D: Fn(usize) -> String,
{
    let RowProbe {
        mut stored,
        describe,
    } = probe;
    let mut seen: HashSet<u32> = HashSet::with_capacity(surrogates.len());
    let mut landing = Vec::with_capacity(surrogates.len());
    for (i, surrogate) in surrogates.iter().enumerate() {
        let duplicate = !seen.insert(surrogate.as_u32()) || stored(i)?;
        match (duplicate, if_absent) {
            (false, _) => landing.push(i),
            (true, true) => {}
            (true, false) => return Err(duplicate_key(collection, &describe(i))),
        }
    }
    Ok(landing)
}

/// The key value a page row names: its identity column's value in the body
/// (the declared primary key), else the row's document id.
fn row_key_text(identity_column: &str, document_id: &str, body: &[u8]) -> String {
    match nodedb_types::value_from_msgpack(body) {
        Ok(nodedb_types::Value::Object(map)) => match map.get(identity_column) {
            Some(nodedb_types::Value::String(text)) => text.clone(),
            Some(value) if !value.is_null() => value.to_string(),
            Some(_) | None => document_id.to_string(),
        },
        _ => document_id.to_string(),
    }
}

/// One `BatchInsert` page, as its judgment reads it.
pub(in crate::data::executor) struct BatchPage<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub documents: &'a [(String, Vec<u8>)],
    pub surrogates: &'a [nodedb_types::Surrogate],
    pub if_absent: bool,
}

impl CoreLoop {
    /// The rows of a `BatchInsert` page that land, probing each key inside
    /// `txn`. See [`landing_rows`].
    pub(in crate::data::executor) fn batch_insert_landing_rows(
        &self,
        txn: &redb::WriteTransaction,
        page: BatchPage<'_>,
    ) -> crate::Result<Vec<usize>> {
        let BatchPage {
            database_id,
            tid,
            collection,
            documents,
            surrogates,
            if_absent,
        } = page;
        let identity_column = self.identity_column(database_id, tid, collection);
        landing_rows(
            collection,
            surrogates,
            if_absent,
            RowProbe {
                stored: |i: usize| {
                    let key = StorageKey::for_surrogate(surrogates[i]);
                    self.sparse
                        .exists_in_txn(txn, database_id, tid, collection, &key)
                },
                describe: |i: usize| {
                    let (document_id, body) = &documents[i];
                    row_key_text(&identity_column, document_id, body)
                },
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Surrogate;

    fn page(ids: &[(&str, u32)]) -> (Vec<(String, Vec<u8>)>, Vec<Surrogate>) {
        (
            ids.iter()
                .map(|(id, _)| (id.to_string(), Vec::new()))
                .collect(),
            ids.iter().map(|(_, s)| Surrogate::new(*s)).collect(),
        )
    }

    fn refused(result: crate::Result<Vec<usize>>, key: &str) {
        match result {
            Err(crate::Error::RejectedConstraint {
                constraint, detail, ..
            }) => {
                assert_eq!(constraint, "unique");
                assert!(detail.contains(&format!("'{key}'")), "{detail}");
            }
            other => panic!("expected a unique violation, got {other:?}"),
        }
    }

    fn judge(
        docs: &[(String, Vec<u8>)],
        surrogates: &[Surrogate],
        if_absent: bool,
        stored_row: Option<usize>,
    ) -> crate::Result<Vec<usize>> {
        landing_rows(
            "t",
            surrogates,
            if_absent,
            RowProbe {
                stored: |i: usize| Ok(Some(i) == stored_row),
                describe: |i: usize| docs[i].0.clone(),
            },
        )
    }

    #[test]
    fn a_key_already_stored_is_refused() {
        let (docs, surrogates) = page(&[("a", 1), ("b", 2)]);
        refused(judge(&docs, &surrogates, false, Some(1)), "b");
    }

    #[test]
    fn a_key_the_page_names_twice_is_refused() {
        let (docs, surrogates) = page(&[("a", 1), ("b", 2), ("a", 1)]);
        refused(judge(&docs, &surrogates, false, None), "a");
    }

    #[test]
    fn skip_mode_skips_stored_and_repeated_keys() {
        let (docs, surrogates) = page(&[("a", 1), ("b", 2), ("a", 1), ("c", 3)]);
        let landing = judge(&docs, &surrogates, true, Some(1)).expect("skip mode");
        assert_eq!(landing, vec![0, 3]);
    }

    #[test]
    fn new_keys_all_land() {
        let (docs, surrogates) = page(&[("a", 1), ("b", 2)]);
        let landing = judge(&docs, &surrogates, false, None).expect("lands");
        assert_eq!(landing, vec![0, 1]);
    }

    /// A refusal names the row's declared key value, not its storage key:
    /// an `INSERT ... SELECT` row's document id is the surrogate hex.
    #[test]
    fn the_refusal_names_the_declared_key_value() {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), nodedb_types::Value::String(String::new()));
        let body =
            nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map)).expect("encode");
        assert_eq!(row_key_text("id", "00000003", &body), "");
        assert_eq!(row_key_text("pk", "00000003", &body), "00000003");
    }
}
