// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for the install fence of a Data-Plane core.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A collection name the install fence keys by does not parse as a stored
/// collection name.
pub(in crate::diag) struct StoredCollectionNameMalformed<'a> {
    /// The site that met the name, and what it does instead.
    pub site: &'static str,
    pub name: &'a str,
}

impl DomainContext for StoredCollectionNameMalformed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.stored_collection_name_malformed"
    }

    fn grouping_key(&self) -> String {
        // The site names the code that met the name. The name is the
        // occurrence.
        format!("site={}", self.site)
    }

    fn to_json(&self) -> Value {
        json!({
            "site": self.site,
            "name": self.name,
            "effect": "a read of the collection waits behind an install of its whole \
                       database, or an install with the name runs to its end without \
                       giving the core back",
            "operator_action": "report this: the code that built the plan names a \
                                collection in neither '<name>' nor '<database_id>/<name>' \
                                form",
        })
    }
}

/// A stream install's walk found fence keys its marker did not declare.
pub(in crate::diag) struct StreamKeyUndeclared<'a> {
    /// The site that found the keys, and what it does instead.
    pub site: &'static str,
    /// The LSN of the marker.
    pub lsn: u64,
    /// The keys the marker did not declare.
    pub keys: &'a str,
}

impl DomainContext for StreamKeyUndeclared<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.stream_key_undeclared"
    }

    fn grouping_key(&self) -> String {
        // The site names the code that found the keys. The marker and its
        // keys are the occurrence.
        format!("site={}", self.site)
    }

    fn to_json(&self) -> Value {
        json!({
            "site": self.site,
            "lsn": self.lsn,
            "keys": self.keys,
            "effect": "the marker's footprint and its first fence missed these keys, so a task \
                       that touches them did not wait behind the marker. The install arms them \
                       as it walks them and keeps parking",
            "operator_action": "report this: the proposer's key walk of the stream content \
                                missed state the Data Plane's validate walk keys",
        })
    }
}
