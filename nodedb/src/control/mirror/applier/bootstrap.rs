// SPDX-License-Identifier: BUSL-1.1

//! Install a mirror's first capture.
//!
//! 1. Fetch the capture into a file the receiver checks, then read it back.
//! 2. Propose one `MirrorCut` for [`BOOTSTRAP_CUT`]: it installs the catalog and
//!    records where each source group resumes, in one index.
//! 3. Install each tenant's conversion records, re-issue its rows, and verify
//!    them against the capture.
//! 4. Report the cut applied at the capture's cap, then move the status.
//!
//! A re-issued write carries no mirror stamp, so no cut holds it and none can
//! name cut 0. It passes the mirror's gates as a follower's write, and the leader
//! gate admits an unstamped one only while the mirror bootstraps. A key the
//! capture does not bind reaches the assigner's mirror refusal, and stops the
//! bootstrap: nothing here mints a surrogate.
//!
//! A source numbers each cut `(term << 32) | count`, and a term starts at 1. So
//! [`BOOTSTRAP_CUT`] sits below every cut the tail can show.
//!
//! The installed cut with no applied report marks a stopped re-issue. A partial
//! re-issue plus a newer capture leaves rows the source deleted, so a new leader
//! stops there, and only an operator's rebuild clears it.
//!
//! A re-issue with `restore_id = 0` has no retry proof: nothing dedupes its
//! writes against a second run, and the rows of a stopped run stay. So any
//! failure during it stays fatal, and `REBOOTSTRAP` removes everything first.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::mirror::{
    BootstrapChunkOutcome, CrossClusterSnapshotEnvelope, MAX_BOOTSTRAP_BYTES, MAX_BOOTSTRAP_CHUNK,
    MirrorBootstrapReceiver, MirrorError, MirrorRefusal, ProgressCallback,
};
use nodedb_types::backup_envelope::VerificationPhase;
use nodedb_types::{
    DatabaseId, MirrorCutAdvance, MirrorCutState, MirrorFailure, MirrorStatus, MirrorStatusStep,
};

use crate::control::backup::restore::reissue_mirror_capture;
use crate::control::backup::verify::destination::verify_destination;
use crate::control::backup::verify::expect::expect_capture;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::cluster::metadata_applier::WedgeReport;
use crate::control::metadata_proposer::{propose_catalog_entry_async, propose_mirror_cut};
use crate::control::mirror::bootstrap_image::MirrorBootstrapImage;
use crate::types::TenantDataSnapshot;

use super::conversions::install_conversions;
use super::driver::{MirrorStop, SessionEnd, follows};
use super::rewrite::{CatalogCheck, DescriptorRule};
use super::session::{FIRST_RETRY_BACKOFF, MAX_RETRY_BACKOFF, Session};

/// The cut a capture installs as. It is never 0, the number of a mirror that
/// holds no cut.
pub(super) const BOOTSTRAP_CUT: u64 = 1;

/// How often the mirror asks whether the source finished its capture.
const CAPTURE_POLL: Duration = Duration::from_millis(200);

/// How many times one metadata proposal of a bootstrap can fail. Each attempt
/// waits up to the statement deadline.
const MAX_LAND_ATTEMPTS: u32 = 8;

/// The fail point that stops a bootstrap after a tenant's re-issue. A test arms
/// it on the node that runs the applier.
pub const REISSUE_FAIL_POINT: &str = "mirror_bootstrap::after_reissue";

/// How far a former leader took the bootstrap.
#[derive(Debug, PartialEq, Eq)]
enum BootstrapPhase {
    /// No capture installed.
    Fresh,
    /// The catalog installed, and the re-issue did not report its end.
    Interrupted,
    /// The rows installed. Only the status move remains.
    Installed,
}

fn phase_of(state: &MirrorCutState) -> BootstrapPhase {
    if state.ready_cut == 0 {
        BootstrapPhase::Fresh
    } else if state.applied_cut == 0 {
        BootstrapPhase::Interrupted
    } else {
        BootstrapPhase::Installed
    }
}

/// Why a metadata proposal stops retrying, or `None` while another attempt can
/// land it. A halted metadata apply never moves on its own, and no entry applies
/// past it.
fn unlanded_cause(wedge: Option<&WedgeReport>, attempt: u32, last: &str) -> Option<String> {
    if let Some(wedge) = wedge {
        return Some(format!(
            "this node's metadata apply halted at index {} on {}: {}",
            wedge.raft_index, wedge.entry_kind, wedge.error
        ));
    }
    (attempt >= MAX_LAND_ATTEMPTS).then(|| format!("{attempt} attempts failed; last: {last}"))
}

fn failed(detail: impl std::fmt::Display) -> MirrorFailure {
    MirrorFailure::BootstrapFailed {
        detail: detail.to_string(),
    }
}

/// Refuse a chunk of another database, or one past the transfer's bounds.
fn check_chunk(
    chunk: &CrossClusterSnapshotEnvelope,
    database: DatabaseId,
) -> Result<(), MirrorFailure> {
    if chunk.source_database_id != database.as_u64().to_string() {
        return Err(failed(format!(
            "the source sent a capture of database {}, and this mirror holds {}",
            chunk.source_database_id,
            database.as_u64()
        )));
    }
    if chunk.total_bytes > MAX_BOOTSTRAP_BYTES {
        return Err(failed(format!(
            "the capture holds {} bytes, past the bound of {MAX_BOOTSTRAP_BYTES}",
            chunk.total_bytes
        )));
    }
    if chunk.data.len() > MAX_BOOTSTRAP_CHUNK {
        return Err(failed(format!(
            "a chunk holds {} bytes, past the bound of {MAX_BOOTSTRAP_CHUNK}",
            chunk.data.len()
        )));
    }
    Ok(())
}

impl Session<'_> {
    /// Take this mirror's first capture, and answer the state to resume from.
    pub(super) async fn bootstrap(&self) -> Result<MirrorCutState, SessionEnd> {
        let held = self.cut_state().map_err(|error| {
            tracing::warn!(%error, "mirror bootstrap: a cut state read failed");
            SessionEnd::LostLeadership
        })?;
        match phase_of(&held) {
            BootstrapPhase::Fresh => {
                let image = self.receive_capture().await?;
                self.install(image).await?;
            }
            BootstrapPhase::Interrupted => {
                return Err(failed(
                    "a former leader stopped during the re-issue; the mirror needs a rebuild",
                )
                .into());
            }
            BootstrapPhase::Installed => {}
        }
        self.move_status(MirrorStatusStep::Installed).await?;
        let state = self.cut_state().map_err(|error| {
            tracing::warn!(%error, "mirror bootstrap: a cut state read failed");
            SessionEnd::LostLeadership
        })?;
        self.ack(BOOTSTRAP_CUT, state.positions.clone()).await?;
        Ok(state)
    }

    /// Stop when this node no longer leads or the mirror left the bootstrap.
    fn still_bootstraps(&self) -> Result<(), SessionEnd> {
        if !self.still_leads() {
            return Err(SessionEnd::LostLeadership);
        }
        match follows(self.state, self.database) {
            Ok(Some(false)) => Ok(()),
            Ok(Some(true)) => Err(SessionEnd::LostLeadership),
            Ok(None) => Err(SessionEnd::Stop(MirrorStop::Ended)),
            Err(error) => {
                tracing::warn!(%error, "mirror bootstrap: a catalog read failed");
                Ok(())
            }
        }
    }

    /// Fetch the capture from the source. A replica that lost it, a failed
    /// capture and a failed transfer all start over at offset 0.
    async fn receive_capture(&self) -> Result<MirrorBootstrapImage, SessionEnd> {
        let progress = Arc::new(Mutex::new(None::<MirrorStatus>));
        let report = Arc::clone(&progress);
        let on_progress: ProgressCallback = Arc::new(move |status| {
            *report.lock().unwrap_or_else(|p| p.into_inner()) = Some(status);
        });
        let receiver = MirrorBootstrapReceiver::new(self.state.data_dir.clone(), on_progress);
        let mut offset = 0;
        let mut backoff = FIRST_RETRY_BACKOFF;
        loop {
            self.still_bootstraps()?;
            let error = match self.client.bootstrap_chunk(self.link_id, offset).await {
                Ok(None) => {
                    tokio::time::sleep(CAPTURE_POLL).await;
                    continue;
                }
                Ok(Some(chunk)) => {
                    check_chunk(&chunk, self.database)?;
                    match receiver.handle_chunk(chunk).await {
                        Ok(BootstrapChunkOutcome::Pending { bytes_done }) => {
                            offset = bytes_done;
                            backoff = FIRST_RETRY_BACKOFF;
                            let due = progress.lock().unwrap_or_else(|p| p.into_inner()).take();
                            if let Some(status) = due {
                                self.report_progress(status).await;
                            }
                            continue;
                        }
                        Ok(BootstrapChunkOutcome::Committed { snapshot_path, .. }) => {
                            return self.read_capture(&snapshot_path).await;
                        }
                        Err(error) => error,
                    }
                }
                Err(MirrorError::Refused(refusal)) if !refusal.is_transient() => {
                    return Err(SessionEnd::Stop(MirrorStop::Refused(refusal)));
                }
                Err(error) => error,
            };
            tracing::warn!(
                database = self.database.as_u64(),
                offset,
                %error,
                "mirror bootstrap: the transfer starts over"
            );
            if !matches!(
                error,
                MirrorError::Refused(
                    MirrorRefusal::BootstrapFailed { .. } | MirrorRefusal::BootstrapRestart { .. }
                )
            ) {
                self.client.next_seed().await;
            }
            receiver.abort().await;
            offset = 0;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
        }
    }

    /// Read the checked file back. A transfer holds on disk only until here.
    async fn read_capture(
        &self,
        path: &std::path::Path,
    ) -> Result<MirrorBootstrapImage, SessionEnd> {
        let bytes = tokio::fs::read(path).await.map_err(failed)?;
        let image = MirrorBootstrapImage::decode(&bytes).map_err(failed)?;
        if let Err(error) = tokio::fs::remove_file(path).await {
            tracing::warn!(path = %path.display(), %error, "mirror bootstrap: a capture file stays");
        }
        if image.database != self.database.as_u64() {
            return Err(failed(format!(
                "the capture names database {}, and this mirror holds {}",
                image.database,
                self.database.as_u64()
            ))
            .into());
        }
        Ok(image)
    }

    /// Install the catalog, the rows, and the report that they applied.
    async fn install(&self, image: MirrorBootstrapImage) -> Result<(), SessionEnd> {
        {
            let mut check = CatalogCheck::new(
                self.state.credentials.catalog(),
                self.database,
                DescriptorRule::Image,
            );
            for payload in &image.catalog {
                check.check(payload, 0)?;
            }
        }
        self.raise(
            MirrorCutAdvance {
                cut: BOOTSTRAP_CUT,
                epoch: self.epoch,
                positions: image.positions.clone(),
                source_timeline: Some(self.source_timeline),
                ..MirrorCutAdvance::default()
            },
            image.catalog,
            |state| state.ready_cut >= BOOTSTRAP_CUT,
        )
        .await?;
        for (tenant, body) in image.tenants {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            self.install_tenant(tenant, &body).await?;
        }
        self.raise(
            MirrorCutAdvance {
                cut: BOOTSTRAP_CUT,
                epoch: self.epoch,
                applied_cut: BOOTSTRAP_CUT,
                applied_cut_hlc_ns: image.cut_hlc_ns,
                cap_ordinal: image.cap_ordinal,
                positions: Vec::new(),
                source_timeline: None,
            },
            Vec::new(),
            |state| state.applied_cut >= BOOTSTRAP_CUT,
        )
        .await
    }

    /// Re-issue one tenant's rows and check them against its capture.
    async fn install_tenant(&self, tenant: u64, body: &[u8]) -> Result<(), MirrorFailure> {
        let database = self.database;
        let snap: TenantDataSnapshot = zerompk::from_msgpack(body).map_err(failed)?;
        let collections = self
            .state
            .credentials
            .catalog()
            .load_all_collections(database)
            .map_err(failed)?;
        let expected =
            expect_capture(self.state, tenant, database, &collections, &snap).map_err(failed)?;
        // Before any row: each row re-issues in the encoding its record names.
        install_conversions(
            self.state,
            tenant,
            database,
            &snap.storage_conversions,
            &collections,
        )
        .await
        .map_err(failed)?;
        let stats = reissue_mirror_capture(self.state, tenant, database, snap)
            .await
            .map_err(failed)?;
        // Stops a bootstrap with rows in place and no applied report.
        crate::fail_point_err!(
            crate::fail_point::FailScope::Node(self.state.node_id),
            REISSUE_FAIL_POINT,
            |detail: String| failed(detail)
        );
        tracing::info!(
            database = database.as_u64(),
            tenant,
            ?stats,
            "mirror bootstrap: re-issued a tenant"
        );
        verify_destination(
            self.state,
            tenant,
            expected,
            |_| Ok((database, i64::MAX)),
            VerificationPhase::Move,
        )
        .await
        .map_err(failed)?;
        Ok(())
    }

    /// Propose `advance` until the state shows `landed`. An advance the row
    /// already holds changes nothing, so a repeat is safe. A later epoch took
    /// the mirror when the state holds it.
    async fn raise(
        &self,
        advance: MirrorCutAdvance,
        catalog: Vec<Vec<u8>>,
        landed: impl Fn(&MirrorCutState) -> bool,
    ) -> Result<(), SessionEnd> {
        let what = format!("cut {} at epoch {}", advance.cut, advance.epoch);
        let mut backoff = FIRST_RETRY_BACKOFF;
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            let proposed = propose_mirror_cut(
                self.state,
                self.database.as_u64(),
                advance.clone(),
                catalog.clone(),
            )
            .await;
            let last = match proposed.and_then(|_| self.cut_state()) {
                Ok(state) if state.fence_epoch > self.epoch => {
                    return Err(SessionEnd::LostLeadership);
                }
                Ok(state) if landed(&state) => return Ok(()),
                Ok(_) => "it applied and the state does not show it".to_string(),
                Err(error) => error.to_string(),
            };
            self.note_unlanded(&what, attempt, &last)?;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
        }
        Err(failed(format!("{what} did not land")).into())
    }

    /// Log one failed attempt at the metadata log, and stop on a cause no
    /// further attempt clears.
    fn note_unlanded(&self, what: &str, attempt: u32, last: &str) -> Result<(), SessionEnd> {
        tracing::warn!(
            database = self.database.as_u64(),
            group = METADATA_GROUP_ID,
            attempt,
            error = %last,
            "mirror bootstrap: {what} did not land; the wait was for this node's metadata apply"
        );
        let wedge = self.state.metadata_apply_wedge.report();
        match unlanded_cause(wedge.as_ref(), attempt, last) {
            Some(cause) => {
                tracing::error!(
                    database = self.database.as_u64(),
                    group = METADATA_GROUP_ID,
                    %cause,
                    "mirror bootstrap: {what} stops the bootstrap"
                );
                Err(failed(format!("{what} did not land: {cause}")).into())
            }
            None => Ok(()),
        }
    }

    /// Propose `step`. It acts only while the mirror still bootstraps.
    async fn write_status(&self, step: MirrorStatusStep) -> crate::Result<()> {
        propose_catalog_entry_async(
            self.state,
            &CatalogEntry::StepMirrorStatus {
                database_id: self.database.as_u64(),
                step,
            },
        )
        .await
        .map(drop)
    }

    /// Show how far the transfer got. The next report repeats the same field.
    async fn report_progress(&self, status: MirrorStatus) {
        let MirrorStatus::Bootstrapping {
            bytes_done,
            bytes_total,
        } = status
        else {
            return;
        };
        let step = MirrorStatusStep::Progress {
            bytes_done,
            bytes_total,
        };
        if let Err(error) = self.write_status(step).await {
            tracing::warn!(%error, "mirror bootstrap: a progress report did not land");
        }
    }

    /// Move the status, while this node leads. A later leader finds the rows
    /// installed and moves it again.
    async fn move_status(&self, step: MirrorStatusStep) -> Result<(), SessionEnd> {
        let mut backoff = FIRST_RETRY_BACKOFF;
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            if !self.still_leads() {
                return Err(SessionEnd::LostLeadership);
            }
            match self.write_status(step.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    self.note_unlanded("the status move", attempt, &error.to_string())?;
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
        }
        Err(failed("the status move did not land").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::new(1800);

    fn chunk(database: &str, total_bytes: u64, data: Vec<u8>) -> CrossClusterSnapshotEnvelope {
        CrossClusterSnapshotEnvelope {
            source_cluster_id: "source".to_string(),
            source_database_id: database.to_string(),
            snapshot_lsn: 1,
            total_bytes,
            total_crc32c: 0,
            offset: 0,
            data,
            done: false,
        }
    }

    /// A bootstrap never installs as cut 0: a stamp of cut 0 skips on every
    /// replica, because no `MirrorCut` raised it.
    #[test]
    fn the_bootstrap_cut_is_not_zero_and_sits_below_every_source_cut() {
        assert_ne!(BOOTSTRAP_CUT, 0);
        const { assert!(BOOTSTRAP_CUT < 1u64 << 32) };
    }

    #[test]
    fn a_halted_metadata_apply_stops_the_bootstrap_at_once() {
        let wedge = WedgeReport {
            raft_index: 20,
            last_applied_watermark: 19,
            entry_kind: "MirrorCut".to_string(),
            error: "descriptor version anomaly".to_string(),
        };
        let cause = unlanded_cause(Some(&wedge), 1, "deadline").expect("no attempt clears it");
        assert!(cause.contains("index 20"), "{cause}");
        assert!(cause.contains("descriptor version anomaly"), "{cause}");
    }

    #[test]
    fn a_failed_attempt_retries_up_to_the_bound() {
        assert_eq!(unlanded_cause(None, 1, "deadline"), None);
        assert_eq!(
            unlanded_cause(None, MAX_LAND_ATTEMPTS - 1, "deadline"),
            None
        );
        let cause = unlanded_cause(None, MAX_LAND_ATTEMPTS, "deadline").expect("the bound");
        assert!(cause.contains("deadline"), "{cause}");
    }

    #[test]
    fn a_mirror_with_no_cut_starts_fresh() {
        assert_eq!(phase_of(&MirrorCutState::default()), BootstrapPhase::Fresh);
    }

    /// A partial re-issue plus a newer capture leaves rows the source deleted.
    #[test]
    fn an_installed_cut_with_no_applied_report_is_interrupted() {
        let state = MirrorCutState {
            ready_cut: BOOTSTRAP_CUT,
            ..MirrorCutState::default()
        };
        assert_eq!(phase_of(&state), BootstrapPhase::Interrupted);
    }

    #[test]
    fn an_applied_report_leaves_only_the_status_move() {
        let state = MirrorCutState {
            ready_cut: BOOTSTRAP_CUT,
            applied_cut: BOOTSTRAP_CUT,
            ..MirrorCutState::default()
        };
        assert_eq!(phase_of(&state), BootstrapPhase::Installed);
    }

    #[test]
    fn a_chunk_within_its_bounds_passes() {
        assert!(check_chunk(&chunk("1800", 3, vec![1, 2, 3]), DB).is_ok());
    }

    #[test]
    fn a_chunk_of_another_database_is_refused() {
        assert!(matches!(
            check_chunk(&chunk("1801", 3, vec![1, 2, 3]), DB),
            Err(MirrorFailure::BootstrapFailed { .. })
        ));
    }

    #[test]
    fn a_capture_past_the_bound_is_refused() {
        assert!(matches!(
            check_chunk(&chunk("1800", MAX_BOOTSTRAP_BYTES + 1, Vec::new()), DB),
            Err(MirrorFailure::BootstrapFailed { .. })
        ));
    }

    #[test]
    fn a_chunk_past_the_bound_is_refused() {
        assert!(matches!(
            check_chunk(&chunk("1800", 3, vec![0; MAX_BOOTSTRAP_CHUNK + 1]), DB),
            Err(MirrorFailure::BootstrapFailed { .. })
        ));
    }
}
