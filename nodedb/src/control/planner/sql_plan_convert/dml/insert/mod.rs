// SPDX-License-Identifier: BUSL-1.1

mod convert;
mod identity;
mod schema;

pub(super) use schema::build_schema_bytes;
pub(crate) use schema::{DEFAULT_IDENTITY_COLUMN, build_columnar_schema};

pub(in super::super) use identity::declared_primary_key_name;
pub(super) use identity::{
    assign_for_pk, columnar_row_surrogates, is_auto_rowid_pk, resolve_doc_identity_with_declared,
};

pub(in super::super) use convert::{ConvertInsertArgs, convert_insert};
