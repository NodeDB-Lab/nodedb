// SPDX-License-Identifier: BUSL-1.1

//! The [`FoldedStore`] of a streamed validate: the target rows one source
//! collection's folds left, in the post-state's spilled `folds` map.
//!
//! The whole-record check keeps one in-memory store per source collection.
//! Here each key leads with the source collection, so each source's folds
//! read only the rows its own earlier folds left.
//!
//! A read or write of the map that fails is a failure of this node, not a
//! verdict on the record. The store records it, so the caller can refuse
//! the record as retryable instead of with the fold check's error.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeSet;

use crate::data::executor::enforcement::unique::claimed_values;
use crate::engine::document::store::IndexPath;

use super::super::folded_store::{FoldedRow, FoldedStore};
use super::post_state::{FoldKey, KeyPrefix, PostState};

/// The folded target rows of one source collection.
pub(super) struct SpilledFolds<'a> {
    source: &'a str,
    post: &'a mut PostState,
    /// The first failure of the map, as text.
    local: RefCell<Option<String>>,
}

fn row_codec(detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "folded target row".into(),
        detail,
    }
}

impl<'a> SpilledFolds<'a> {
    pub(super) fn new(source: &'a str, post: &'a mut PostState) -> Self {
        Self {
            source,
            post,
            local: RefCell::new(None),
        }
    }

    /// The failure of the map an operation hit, if any.
    pub(super) fn take_local(&self) -> Option<String> {
        self.local.borrow_mut().take()
    }

    /// Record the failure `result` holds, and hand it on.
    fn local<T>(&self, result: crate::Result<T>) -> crate::Result<T> {
        if let Err(error) = &result {
            let mut local = self.local.borrow_mut();
            if local.is_none() {
                *local = Some(error.to_string());
            }
        }
        result
    }

    fn key(&self, collection: &str, surrogate: u32) -> FoldKey {
        (self.source.to_string(), collection.to_string(), surrogate)
    }

    fn decode_doc(&self, bytes: &[u8]) -> crate::Result<serde_json::Value> {
        self.local(
            nodedb_types::json_from_msgpack(bytes)
                .map_err(|error| row_codec(format!("decode a folded document: {error}"))),
        )
    }
}

impl FoldedStore for SpilledFolds<'_> {
    fn folded(
        &self,
        collection: &str,
        surrogate: u32,
    ) -> crate::Result<Option<Cow<'_, FoldedRow>>> {
        let found = self.local(
            self.post
                .folds
                .get(&self.key(collection, surrogate), self.post.reader()),
        )?;
        let Some(row) = found else {
            return Ok(None);
        };
        let (doc, stored) = row.into_owned();
        Ok(Some(Cow::Owned(FoldedRow {
            doc: self.decode_doc(&doc)?,
            stored,
        })))
    }

    fn put(&mut self, collection: &str, surrogate: u32, row: FoldedRow) -> crate::Result<()> {
        let doc = self.local(
            nodedb_types::json_to_msgpack(&row.doc)
                .map_err(|error| row_codec(format!("encode a folded document: {error}"))),
        )?;
        let key = self.key(collection, surrogate);
        self.post.put_fold(key, (doc, row.stored));
        Ok(())
    }

    fn holds(&self, collection: &str, surrogate: u32) -> crate::Result<bool> {
        self.local(
            self.post
                .folds
                .contains(&self.key(collection, surrogate), self.post.reader()),
        )
    }

    fn first_claimed_by_other(
        &self,
        collection: &str,
        surrogate: u32,
        path: &IndexPath,
        needles: &BTreeSet<String>,
        crs: &nodedb_crs::CrsContext<'_>,
    ) -> crate::Result<Option<String>> {
        let prefix = self.local(
            KeyPrefix::tuple(3)
                .then(&self.source)
                .and_then(|prefix| prefix.then(&collection)),
        )?;
        let mut first: Option<String> = None;
        for item in self.post.folds.scan(prefix.bounds(), self.post.reader()) {
            let ((_, _, held), row) = self.local(item)?;
            if held == surrogate {
                continue;
            }
            let doc = self.decode_doc(&row.0)?;
            for value in claimed_values(&doc, path, crs) {
                if needles.contains(&value) && first.as_ref().is_none_or(|kept| value < *kept) {
                    first = Some(value);
                }
            }
        }
        Ok(first)
    }
}
