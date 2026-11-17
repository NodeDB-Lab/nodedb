// SPDX-License-Identifier: BUSL-1.1

//! A collection's full-text analysis on this core: its analyzer and its
//! fuzzy default.
//!
//! The catalog's full-text index records own both values. A `Register`
//! carries them in the collection's `TextFieldSet`, and the core installs
//! them in its inverted-index metadata. Every tokenization of the
//! collection's text reads them there: forward indexing, the staged-write
//! overlay, search, and the rebuild's scratch index, which copies them.

use nodedb_physical::physical_plan::TextFieldSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};

/// The analyzer an unset analyzer tokenizes with.
const STANDARD_ANALYZER: &str = "standard";

/// The analyzer and fuzzy default of one collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) struct TextAnalysis<'a> {
    /// `None` means the standard analyzer.
    pub analyzer: Option<&'a str>,
    pub fuzzy_default: bool,
}

impl<'a> TextAnalysis<'a> {
    /// The analysis `fields` carries.
    pub(in crate::data::executor) fn of(fields: &'a TextFieldSet) -> Self {
        Self {
            analyzer: fields.analyzer(),
            fuzzy_default: fields.fuzzy_default(),
        }
    }
}

impl CoreLoop {
    /// Refuse `TextOp::SetTextConfig`. The analyzer and fuzzy default of a
    /// collection travel on its full-text index records, so a binding set on
    /// one core alone diverges from every other core and node.
    pub(in crate::data::executor) fn execute_set_text_config(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        self.response_error(
            task,
            ErrorCode::Unsupported {
                detail: format!(
                    "binding an analyzer on '{collection}' outside its full-text index is not \
                     supported; name it in CREATE SEARCH INDEX ... ANALYZER '<name>' FUZZY <bool>"
                ),
            },
        )
    }

    /// Install `fields`'s analyzer and fuzzy default for `collection` in this
    /// core's inverted-index metadata, unless the core holds them already.
    ///
    /// An unnamed analyzer installs as the standard analyzer, the one an
    /// unset analyzer tokenizes with. A collection that never had a
    /// full-text index holds the defaults, so its register writes nothing.
    pub(in crate::data::executor) fn install_text_analysis(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        fields: &TextFieldSet,
    ) -> crate::Result<()> {
        self.install_analysis(database_id, tid, collection, TextAnalysis::of(fields))
            .map(|_| ())
    }

    /// Install `analysis` for `collection`, unless the core holds it.
    /// Returns whether the held analyzer changed: postings built under the
    /// old one no longer match a query.
    pub(in crate::data::executor) fn install_analysis(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        analysis: TextAnalysis<'_>,
    ) -> crate::Result<bool> {
        let db = database_id.as_u64();
        let (held_analyzer, held_fuzzy) = self.inverted.collection_analysis(db, tid, collection)?;
        let wanted = analysis.analyzer.unwrap_or(STANDARD_ANALYZER);
        let analyzer_changed = held_analyzer.as_deref().unwrap_or(STANDARD_ANALYZER) != wanted;
        if analyzer_changed {
            self.inverted
                .set_collection_analyzer(db, tid, collection, wanted)?;
        }
        if held_fuzzy != analysis.fuzzy_default {
            self.inverted
                .set_collection_fuzzy(db, tid, collection, analysis.fuzzy_default)?;
        }
        Ok(analyzer_changed)
    }

    /// Install the analysis of `collection`'s registered config again, after
    /// a clear that keeps the registration dropped its inverted-index
    /// metadata. A collection with no registered config installs nothing.
    pub(in crate::data::executor) fn reinstall_text_analysis(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<()> {
        let key = (database_id, tid, collection.to_string());
        match self.doc_configs.get(&key) {
            Some(config) => {
                self.install_text_analysis(database_id, tid, collection, &config.text_fields)
            }
            None => Ok(()),
        }
    }
}
