// SPDX-License-Identifier: BUSL-1.1

//! The stored bytes a Calvin install writes for a document or CRDT row,
//! derived when the plan stages.
//!
//! The install hands each row's redo value to `apply_point_put`, which
//! derives the stored body with `build_stored_body`: strict defaults,
//! typeguard defaults, generated columns, `_rowid`, declared-type coercion,
//! the hash-chain identity of a schemaless row, and the strict Binary Tuple.
//! The stage runs the same function on the same value, so the image and the
//! installed row are equal by construction.
//!
//! - A document row's redo value is its staged body, a strict tuple decoded
//!   back to MessagePack as the resolve emits it.
//! - A CRDT row's value is the Loro row its install predicts, encoded as the
//!   install encodes it (`crdt_row_body`).
//!
//! Two stored fields exist only at the install, and neither is in an image:
//! - the version stamp of a bitemporal row, which a read never shows;
//! - the hash-chain link (`_chain_hash`, `_chain_seq`) of a `HASH_CHAIN`
//!   row, which follows the chain head the install finds. A write of
//!   another transaction to the collection can move that head between this
//!   stage and the install.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::Surrogate;

use super::crdt_writes::{StagedCrdtWrite, predicted_upsert_row};
use super::images::{ReturningImageRow, RowLocation};
use super::target::RowEngine;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::handlers::control::convert::crdt_row_body;
use crate::data::executor::handlers::point::apply_put::stored_body::StoredBodyInput;
use crate::data::executor::handlers::transaction::overlay::TouchedSlot;
use crate::data::executor::handlers::transaction::resolve::redo_put_value;
use crate::types::{DatabaseId, TenantId};

/// One row a plan wrote: its overlay slot and its staged body.
pub(super) type WrittenRow<'s, 'o> = (&'s TouchedSlot<'o>, &'o [u8]);

impl CoreLoop {
    /// The installed image of every row in `written`, which `plan`, a
    /// document or CRDT write, staged. `earlier` holds the CRDT row writes
    /// of the plans staged before `plan`.
    pub(super) fn calvin_installed_rows(
        &self,
        at: &RowLocation<'_>,
        plan: &PhysicalPlan,
        earlier: &[StagedCrdtWrite],
        written: &[WrittenRow<'_, '_>],
    ) -> Result<Vec<ReturningImageRow>, ErrorCode> {
        match at.engine {
            RowEngine::Document => {
                let strict_schema =
                    self.resolve_strict_schema(at.database_id, at.tid, at.collection);
                written
                    .iter()
                    .map(|(slot, body)| {
                        let value = redo_put_value(body, strict_schema.as_ref(), slot.doc_id)
                            .map_err(ErrorCode::from)?;
                        // A committed redo install stamps every document row
                        // with the stamp its record carries.
                        self.calvin_installed_row(at, slot, &value, true)
                    })
                    .collect()
            }
            RowEngine::Crdt => {
                let this = StagedCrdtWrite::of_plan(plan).ok_or_else(|| ErrorCode::Internal {
                    detail: format!(
                        "calvin RETURNING: the CRDT plan on '{}' writes no row",
                        at.collection
                    ),
                })?;
                let engine = self
                    .crdt_engines
                    .get(&(DatabaseId::new(at.database_id), TenantId::new(at.tid)));
                let row = predicted_upsert_row(engine, earlier, &this)?;
                let value = crdt_row_body(&row).map_err(ErrorCode::from)?;
                let bitemporal = self.is_bitemporal(at.database_id, at.tid, at.collection);
                written
                    .iter()
                    .map(|(slot, _)| self.calvin_installed_row(at, slot, &value, bitemporal))
                    .collect()
            }
            RowEngine::Kv | RowEngine::Vector | RowEngine::Columnar | RowEngine::Timeseries => {
                Err(ErrorCode::Internal {
                    detail: format!(
                        "calvin RETURNING: {:?} rows of '{}' store their staged bytes",
                        at.engine, at.collection
                    ),
                })
            }
        }
    }

    /// The row the install stores for `slot` from the redo value `value`.
    /// `bitemporal` is whether the install stamps the row's version into a
    /// bitemporal strict tuple.
    fn calvin_installed_row(
        &self,
        at: &RowLocation<'_>,
        slot: &TouchedSlot<'_>,
        value: &[u8],
        bitemporal: bool,
    ) -> Result<ReturningImageRow, ErrorCode> {
        let surrogate = Surrogate::new(slot.surrogate);
        let config_key = (
            DatabaseId::new(at.database_id),
            TenantId::new(at.tid),
            at.collection.to_string(),
        );
        // A read never shows the version stamp, so the stage's ordinal
        // stands in for the one the install pins.
        let body = self
            .build_stored_body(StoredBodyInput {
                config_key: &config_key,
                surrogate,
                value,
                bitemporal,
                sys_from: self.bitemporal_ordinal(),
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                // The install injects defaults again. The stage judged the
                // guards.
                guards: GuardScope::InjectOnly,
            })
            .map_err(ErrorCode::from)?;
        Ok(ReturningImageRow {
            identity: slot.doc_id.clone(),
            surrogate,
            bytes: body.stored,
            vector: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        CrdtOp, CrdtWriteVerb, DocumentOp, GeneratedColumnSpec, PhysicalPlan, ReturningColumns,
        ReturningSpec, StorageMode, TimeseriesOp,
    };
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    use nodedb_types::{RlsWriteCheck, Surrogate, TypeGuardFieldDef, Value};

    use super::super::fixtures::{TID, coll, object, resolved_ingest, text};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::enforcement::hash_chain::is_chain_field;
    use crate::data::executor::enforcement::typeguard::CompiledTypeGuard;
    use crate::data::executor::response_codec::{RowsPayload, StagedReturningImages};
    use crate::engine::document::store::CollectionConfig;
    use crate::types::{DatabaseId, TenantId};

    fn star() -> Option<ReturningSpec> {
        Some(ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        })
    }

    fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    fn generated(name: &str, expression: &str) -> GeneratedColumnSpec {
        let (expr, depends_on) =
            nodedb_query::expr_parse::parse_generated_expr(expression).expect("parse");
        GeneratedColumnSpec {
            name: name.into(),
            expr,
            depends_on,
        }
    }

    fn insert(
        collection: &str,
        id: &str,
        surrogate: u32,
        fields: &[(&str, Value)],
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: coll(collection),
            document_id: id.to_string(),
            value: object(fields),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: star(),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn crdt_upsert(fields: &[(&str, Value)], partial: bool, ret: bool) -> PhysicalPlan {
        let fields = fields
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect();
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: coll("tasks"),
            document_id: "t1".to_string(),
            fields: nodedb_physical::physical_plan::encode_fields_body(&fields)
                .expect("fields body encodes"),
            surrogate: Surrogate::new(71),
            partial,
            verb: CrdtWriteVerb::Insert,
            returning: if ret { star() } else { None },
            rls_filters: Vec::new(),
        })
    }

    /// A resolved ingest of `line` with `RETURNING *`.
    fn ingest_star(line: &str) -> PhysicalPlan {
        let mut plan = resolved_ingest(line);
        if let PhysicalPlan::Timeseries(TimeseriesOp::Ingest { returning, .. }) = &mut plan {
            *returning = star();
        }
        plan
    }

    /// Commit `plans` as the Calvin slice at `epoch` and return the images
    /// the last `RETURNING` plan staged beside the `RETURNING *` rows the
    /// install answered, each as one object.
    fn staged_and_installed(
        core: &mut CoreLoop,
        plans: &[PhysicalPlan],
        epoch: u64,
    ) -> (Vec<Value>, Vec<Value>) {
        let (staged, installed) = core.calvin_commit_staged_for_test(
            &make_default_task(),
            TID,
            plans,
            (epoch, 0),
            epoch * 100,
        );
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
        let installed = installed.expect("the stage admitted the slice");
        assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
        assert!(installed.error_code.is_none(), "{:?}", installed.error_code);
        let images: StagedReturningImages =
            zerompk::from_msgpack(staged.payload.as_bytes()).expect("stage images");
        let last = images.plans.last().expect("a RETURNING plan staged images");
        let staged_rows = last.rows.iter().map(|row| row.fields.clone()).collect();
        let payload: RowsPayload =
            zerompk::from_msgpack(installed.payload.as_bytes()).expect("RETURNING rows");
        let installed_rows = payload
            .rows
            .iter()
            .map(|row| {
                Value::Object(
                    payload
                        .columns
                        .iter()
                        .cloned()
                        .zip(row.iter().map(|cell| cell.0.clone()))
                        .collect(),
                )
            })
            .collect();
        (staged_rows, installed_rows)
    }

    /// Commit `plans` as the Calvin slice at `epoch` and require it installs.
    fn commit(core: &mut CoreLoop, plans: &[PhysicalPlan], epoch: u64) {
        let response =
            core.calvin_commit_for_test(&make_default_task(), TID, plans, epoch, epoch * 100);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    }

    fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
        row.get(name)
    }

    /// The image carries the guard DEFAULT and the generated column the
    /// install stores.
    #[test]
    fn a_document_image_is_the_row_its_install_stores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut config = CollectionConfig::new("orders");
        config.enforcement.generated_columns = vec![generated("doubled", "n * 2")];
        config.type_guards = vec![CompiledTypeGuard::compile(
            "orders",
            &TypeGuardFieldDef {
                field: "status".into(),
                type_expr: "STRING".into(),
                required: false,
                check_expr: None,
                default_expr: Some("'ready'".into()),
                value_expr: None,
            },
        )];
        core.doc_configs.insert(config_key("orders"), config);

        let plan = insert("orders", "o1", 7, &[("n", Value::Integer(21))]);
        let (staged, installed) = staged_and_installed(&mut core, &[plan], 1);

        assert_eq!(staged, installed);
        assert_eq!(staged.len(), 1);
        assert_eq!(field(&staged[0], "status"), Some(&text("ready")));
        assert!(field(&staged[0], "doubled").is_some(), "{staged:?}");
    }

    /// A strict row's image is its installed tuple: `_rowid` from the
    /// surrogate, and every column its schema declares.
    #[test]
    fn a_strict_document_image_is_the_tuple_its_install_stores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let schema = StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable("name", ColumnType::String),
            ColumnDef::nullable("status", ColumnType::String).with_default("'new'"),
        ])
        .expect("schema");
        let config =
            CollectionConfig::new("people").with_storage_mode(StorageMode::Strict { schema });
        core.doc_configs.insert(config_key("people"), config);

        let plan = insert("people", "p1", 9, &[("name", text("ada"))]);
        let (staged, installed) = staged_and_installed(&mut core, &[plan], 1);

        assert_eq!(staged, installed);
        assert_eq!(staged.len(), 1);
        assert_eq!(field(&staged[0], "_rowid"), Some(&Value::Integer(9)));
    }

    /// A hash-chained row's image holds the identity the install stamps.
    /// Its chain link follows the head the install finds, so only the
    /// installed row carries it.
    #[test]
    fn a_chained_document_image_is_its_installed_row_without_the_link() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut config = CollectionConfig::new("ledger");
        config.enforcement.append_only = true;
        config.enforcement.hash_chain = true;
        core.doc_configs.insert(config_key("ledger"), config);

        let plan = insert("ledger", "l1", 5, &[("amount", Value::Integer(21))]);
        let (staged, installed) = staged_and_installed(&mut core, &[plan], 1);

        let unlinked: Vec<Value> = installed
            .iter()
            .map(|row| match row {
                Value::Object(fields) => Value::Object(
                    fields
                        .iter()
                        .filter(|(name, _)| !is_chain_field(name))
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect(),
                ),
                other => other.clone(),
            })
            .collect();
        assert_eq!(staged, unlinked);
        assert_ne!(staged, installed, "the installed row carries its link");
    }

    /// A merge over a row an earlier transaction installed answers the
    /// merged row with its generated column, as the install stores it.
    #[test]
    fn a_crdt_merge_image_is_the_merged_row_its_install_stores() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut config = CollectionConfig::new("tasks");
        config.crdt_enabled = true;
        config.enforcement.generated_columns = vec![generated("doubled", "n * 2")];
        core.doc_configs.insert(config_key("tasks"), config);
        commit(
            &mut core,
            &[crdt_upsert(
                &[("title", text("kept")), ("n", Value::Integer(1))],
                false,
                false,
            )],
            1,
        );

        let merge = crdt_upsert(&[("n", Value::Integer(2))], true, true);
        let (staged, installed) = staged_and_installed(&mut core, &[merge], 2);

        assert_eq!(staged, installed);
        assert_eq!(staged.len(), 1);
        assert_eq!(field(&staged[0], "title"), Some(&text("kept")));
        assert!(field(&staged[0], "doubled").is_some(), "{staged:?}");
    }

    /// A merge after a replace of the same row in the same slice folds the
    /// replace, as the install replays both in plan order.
    #[test]
    fn a_crdt_merge_after_a_replace_in_the_slice_is_the_installed_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        commit(
            &mut core,
            &[crdt_upsert(&[("old", text("gone"))], false, false)],
            1,
        );

        let plans = [
            crdt_upsert(&[("title", text("kept"))], false, false),
            crdt_upsert(&[("n", Value::Integer(2))], true, true),
        ];
        let (staged, installed) = staged_and_installed(&mut core, &plans, 2);

        assert_eq!(staged, installed);
        assert_eq!(staged.len(), 1);
        assert_eq!(field(&staged[0], "old"), None);
        assert_eq!(field(&staged[0], "title"), Some(&text("kept")));
    }

    /// A resolved ingest that fits the live schema lands the rows its
    /// resolve imaged.
    #[test]
    fn a_resolved_ingest_image_is_the_row_its_install_lands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let plan = ingest_star("cpu value=7 1000000000");
        let (staged, installed) = staged_and_installed(&mut core, &[plan], 1);

        assert_eq!(staged, installed);
        assert_eq!(staged.len(), 1);
        assert_eq!(field(&staged[0], "value"), Some(&Value::Float(7.0)));
    }

    /// A resolved ingest whose schema drifted from the live one lands by
    /// column name, and its image is the row the live schema stores.
    #[test]
    fn a_by_name_resolved_ingest_image_is_the_row_its_install_lands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        commit(
            &mut core,
            &[resolved_ingest("cpu value=1,extra=2 500000000")],
            1,
        );

        let plan = ingest_star("cpu value=7 1000000000");
        let (staged, installed) = staged_and_installed(&mut core, &[plan], 2);

        assert_eq!(staged, installed);
        assert_eq!(staged.len(), 1);
        assert_eq!(field(&staged[0], "value"), Some(&Value::Float(7.0)));
    }
}
