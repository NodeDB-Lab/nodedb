// SPDX-License-Identifier: BUSL-1.1

mod balanced_gate;
mod crdt_gate;
mod insert;
mod kv_insert;
mod merge;
mod update_delete;
mod upsert;
mod vector_primary;

pub(super) use insert::{ConvertInsertArgs, convert_insert, declared_primary_key_name};
pub(crate) use insert::{DEFAULT_IDENTITY_COLUMN, build_columnar_schema};
pub(super) use kv_insert::convert_kv_insert;
pub(super) use merge::{ConvertMergeArgs, convert_merge};
pub(super) use update_delete::{
    UpdateFromParams, UpdateParams, convert_delete, convert_update, convert_update_from,
};
pub(super) use upsert::{ConvertUpsertArgs, convert_upsert};
pub(super) use vector_primary::{
    VectorPrimaryCfg, VectorPrimaryInsertArgs, VectorPrimaryUpdateArgs,
    convert_vector_primary_delete, convert_vector_primary_insert, convert_vector_primary_truncate,
    convert_vector_primary_update,
};
