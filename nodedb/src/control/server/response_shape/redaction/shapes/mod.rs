// SPDX-License-Identifier: BUSL-1.1

//! Shape-preserving redaction hooks, one per wire shape a client-facing path
//! can deliver.
//!
//! Every hook here rewrites a delivered result IN PLACE and leaves its wire
//! shape byte-for-byte intact — only a ruled cell's value changes. They all
//! route the actual matching through [`RedactionStore::apply_flat_row`], so
//! the mask / hash / null semantics stay defined exactly once
//! (`security::redaction::apply`), no matter which transport delivered the row.
//!
//! These exist because several client-facing paths never reach the
//! named-projection shaping core: they decode a Data-Plane payload and write
//! the answer straight to the client. The shapes those payloads arrive in are:
//!
//! - a scan envelope or flat row map — [`redact_envelope_row`]
//! - a positional `RETURNING` payload — [`redact_rows_payload`]
//! - stored bytes handed back verbatim — [`redact_stored_value_bytes`]
//! - one stored document row's MessagePack bytes — [`redact_document_row_bytes`]
//!
//! [`redact_decoded_value`] dispatches over the first two for callers holding
//! a decoded payload of unknown shape.

mod decoded;
mod stored;
#[cfg(test)]
mod test_support;

pub use decoded::{redact_decoded_value, redact_envelope_row, redact_rows_payload};
pub use stored::{redact_document_row_bytes, redact_stored_value_bytes};
