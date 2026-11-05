// SPDX-License-Identifier: BUSL-1.1

//! Shared target-identity helpers for statement drivers that write into a
//! target document collection on behalf of another operation (`MERGE`,
//! `INSERT ... SELECT`, `UPDATE ... FROM`): primary-key classification,
//! surrogate assignment, and `document_id` derivation. Factored here so these
//! drivers cannot diverge on identity derivation.

pub(crate) mod document_id;
pub(crate) mod pk;
pub(crate) mod surrogate;

pub(crate) use document_id::{derive_document_id, require_surrogate};
pub(crate) use pk::{TargetPk, resolve_target_pk};
pub(crate) use surrogate::assign_target_surrogates;
