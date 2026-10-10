// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::HomeVersions`: this core's current version of each probed home.
//!
//! The Control Plane hands a core only the probes whose vShard the core owns.
//! A probe with a collection answers the collection's version on the probe's
//! vShard. A probe without one answers the vShard's latest version. Nothing
//! is read beyond those two values, and nothing is written.

use nodedb_physical::physical_plan::{HomeAnswer, HomeVersion, HomeVersionProbe};
use nodedb_types::WriteVersion;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::VShardId;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_home_versions(
        &self,
        task: &ExecutionTask,
        probes: &[HomeVersionProbe],
    ) -> Response {
        let mut answers: Vec<HomeVersion> = Vec::with_capacity(probes.len());
        for probe in probes {
            let Some(version) = self.home_version(task, probe) else {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "home versions: probe names vShard {}, past the vShard space",
                            probe.vshard
                        ),
                    },
                );
            };
            answers.push(HomeVersion {
                probe: probe.clone(),
                answer: HomeAnswer::Version(version),
            });
        }
        match zerompk::to_msgpack_vec(&answers) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("home versions: encoding the answer failed: {e}"),
                },
            ),
        }
    }

    /// The probe's current version, `None` for a vShard id past the vShard
    /// space.
    fn home_version(&self, task: &ExecutionTask, probe: &HomeVersionProbe) -> Option<WriteVersion> {
        if probe.vshard >= VShardId::COUNT {
            return None;
        }
        let vshard = VShardId::new(probe.vshard);
        Some(match probe.collection.as_deref() {
            Some(collection) => self.write_index.collection_current(
                task.request.database_id,
                task.request.tenant_id,
                vshard,
                collection,
            ),
            None => self.write_index.latest(vshard),
        })
    }
}
