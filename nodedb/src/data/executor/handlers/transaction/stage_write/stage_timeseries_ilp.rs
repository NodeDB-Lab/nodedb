// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the timeseries ingests the row-keyed paths in
//! `stage_timeseries` do not take:
//!
//! - A raw line-protocol payload (`format = "ilp"`) that carries one
//!   surrogate per line is rewritten into the canonical line list and staged
//!   row by row through the canonical path, so a same-transaction read
//!   observes its rows.
//! - A payload in any format that carries no surrogates has no overlay key
//!   for its rows, as a native bulk ingest sends it. It is decided the way
//!   the canonical path decides a batch: normalized into line protocol,
//!   parsed, matched to its routed collection, admitted by the write policy
//!   and prevalidated against the memtable. It stages no row. COMMIT resolve
//!   serializes the ingest from the plan node, so the install still writes
//!   every line.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::StampedIngest;
use crate::types::TenantId;

use super::stage_timeseries::CanonicalIlpStage;

impl CoreLoop {
    /// Stage a raw line-protocol ingest that carries surrogates. Answers the
    /// number of lines.
    pub(super) fn stage_raw_ilp_rows(&mut self, args: CanonicalIlpStage<'_>) -> Response {
        let task = args.task;
        let text = match std::str::from_utf8(args.payload) {
            Ok(text) => text,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("line protocol is not UTF-8: {error}"),
                    },
                );
            }
        };
        let lines: Vec<String> = text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();
        if lines.len() != args.surrogates.len() {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "line-protocol payload carries {} lines but {} surrogates",
                        lines.len(),
                        args.surrogates.len()
                    ),
                },
            );
        }
        let canonical = match zerompk::to_msgpack_vec(&lines) {
            Ok(bytes) => bytes,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("canonical ILP payload encode failed: {error}"),
                    },
                );
            }
        };
        self.stage_canonical_ilp_rows(CanonicalIlpStage {
            payload: &canonical,
            ..args
        })
    }

    /// Decide an ingest that carries no surrogates, in `format`. Stages no
    /// row and answers the number of lines it holds.
    pub(super) fn stage_unkeyed_timeseries(
        &mut self,
        args: CanonicalIlpStage<'_>,
        format: &str,
    ) -> Response {
        match self.decide_unkeyed_timeseries(&args, format) {
            Ok((lines, now_ms)) => {
                // COMMIT resolve stamps the untimed rows with the instant the
                // statement read, not its own.
                let coll_key = (
                    args.task.request.database_id,
                    TenantId::new(args.tid),
                    args.collection.to_string(),
                );
                self.txn_overlay_mut(args.txn_id)
                    .note_unkeyed_ingest_now(&coll_key, now_ms);
                self.stage_count_response(args.task, lines)
            }
            Err(error) => self.response_error(args.task, error),
        }
    }

    /// Normalize, parse, route-check, admit and prevalidate an unkeyed
    /// ingest. Mutates nothing. Returns the number of lines and the instant
    /// the ingest read as its default row timestamp.
    fn decide_unkeyed_timeseries(
        &self,
        args: &CanonicalIlpStage<'_>,
        format: &str,
    ) -> Result<(usize, i64), ErrorCode> {
        let tenant = TenantId::new(args.tid);
        let now_ms = self.ingest_now_ms();
        let lines = self.stamped_ingest_lines(StampedIngest {
            database_id: args.task.request.database_id,
            tid: tenant,
            collection: args.collection,
            payload: args.payload,
            format,
            now_ms,
        })?;
        let source = lines.join("\n");
        let parsed = match crate::engine::timeseries::ilp::parse_batch(&source) {
            Ok(parsed) if parsed.lines().len() == lines.len() => parsed,
            _ => {
                return Err(ErrorCode::RejectedPrevalidation {
                    reason: "invalid line-protocol row".into(),
                });
            }
        };
        let measurement = args
            .collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(args.collection);
        if parsed
            .lines()
            .iter()
            .any(|row| row.measurement.as_ref() != measurement)
        {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: "line-protocol measurement does not match routed collection".into(),
            });
        }
        crate::data::executor::handlers::timeseries::admit_ilp_lines(
            args.rls_write_check,
            parsed.lines(),
            self.declared_ts_time_key(args.task.request.database_id, tenant, args.collection),
            now_ms,
            args.tid,
            args.collection,
        )?;
        self.prevalidate_deferred_ilp_ingest(args.task, tenant, args.collection, parsed.lines())?;
        Ok((lines.len(), now_ms))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::task::ExecutionTask;
    use crate::types::TxnId;

    const TID: u64 = 1;

    fn unkeyed_ingest() -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics,host=a value=1".to_vec(),
            format: "ilp".to_owned(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// Stage the unkeyed ingest at `stage_ms` and resolve it at
    /// `resolve_ms`. Returns the resolved redo record.
    fn stage_then_resolve(stage_ms: i64, resolve_ms: i64) -> Vec<u8> {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let txn_id = TxnId::new(5);
        let mut request = make_default_task().request;
        request.txn_id = Some(txn_id);
        let task = ExecutionTask::new(request);
        let plan = unkeyed_ingest();

        core.epoch_system_ms = Some(stage_ms);
        let staged = core.execute_stage_write(&task, TID, &plan);
        assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);

        core.epoch_system_ms = Some(resolve_ms);
        let resolved = core.execute_resolve_txn(&task, TID, txn_id, std::slice::from_ref(&plan));
        assert_eq!(resolved.status, Status::Ok, "{:?}", resolved.error_code);
        resolved.payload.as_bytes().to_vec()
    }

    #[test]
    fn an_unkeyed_ingest_resolves_with_the_instant_its_statement_read() {
        let staged_early = stage_then_resolve(1_000_000, 9_000_000);
        assert_eq!(
            staged_early,
            stage_then_resolve(1_000_000, 1_000_000),
            "the untimed row carries the stage instant"
        );
        assert_ne!(
            staged_early,
            stage_then_resolve(9_000_000, 9_000_000),
            "the commit instant does not reach the row"
        );
    }
}
