//! Transaction lifecycle methods on SessionStore.

use std::net::SocketAddr;

use crate::control::planner::physical::PhysicalTask;
use crate::types::Lsn;

use super::state::TransactionState;
use super::store::SessionStore;

impl SessionStore {
    /// Get transaction state for a connection.
    pub fn transaction_state(&self, addr: &SocketAddr) -> TransactionState {
        self.read_session(addr, |s| s.tx_state)
            .unwrap_or(TransactionState::Idle)
    }

    /// BEGIN — enter transaction block with snapshot isolation.
    ///
    /// Captures the current WAL LSN as the snapshot point. All reads
    /// within this transaction see data as of this LSN.
    pub fn begin(&self, addr: &SocketAddr, current_lsn: Lsn) -> Result<(), &'static str> {
        self.write_session(addr, |session| match session.tx_state {
            TransactionState::Idle => {
                session.tx_state = TransactionState::InBlock;
                session.tx_snapshot_lsn = Some(current_lsn);
                session.tx_read_set.clear();
                Ok(())
            }
            TransactionState::InBlock => {
                // PostgreSQL issues a WARNING here, not an error.
                Ok(())
            }
            TransactionState::Failed => Err(
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
        })
        .unwrap_or(Ok(()))
    }

    /// Record a read for write conflict detection.
    pub fn record_read(
        &self,
        addr: &SocketAddr,
        collection: String,
        document_id: String,
        read_lsn: Lsn,
    ) {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session
                    .tx_read_set
                    .push((collection, document_id, read_lsn));
            }
        });
    }

    /// Get the snapshot LSN for the current transaction.
    pub fn snapshot_lsn(&self, addr: &SocketAddr) -> Option<Lsn> {
        self.read_session(addr, |s| s.tx_snapshot_lsn)?
    }

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: &SocketAddr) -> Vec<(String, String, Lsn)> {
        self.write_session(addr, |session| std::mem::take(&mut session.tx_read_set))
            .unwrap_or_default()
    }

    /// COMMIT — drain the write buffer and pending offset commits, return to idle.
    ///
    /// Returns the buffered write tasks for atomic dispatch.
    pub fn commit(&self, addr: &SocketAddr) -> Result<Vec<PhysicalTask>, &'static str> {
        self.write_session(addr, |session| {
            let buffer = std::mem::take(&mut session.tx_buffer);
            session.tx_state = TransactionState::Idle;
            session.tx_snapshot_lsn = None;
            session.savepoints.clear();
            // Note: pending_sequence_reservations are taken separately via
            // take_pending_reservations() so the caller can finalize them
            // with the GAP_FREE manager (which requires Arc<SequenceRegistry>).
            Ok(buffer)
        })
        .unwrap_or(Ok(Vec::new()))
    }

    /// Take pending GAP_FREE sequence reservations (called after successful COMMIT).
    pub fn take_pending_reservations(
        &self,
        addr: &SocketAddr,
    ) -> Vec<crate::control::sequence::gap_free::ReservationHandle> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_sequence_reservations)
        })
        .unwrap_or_default()
    }

    /// Take pending offset commits (called after successful COMMIT dispatch).
    pub fn take_pending_offsets(&self, addr: &SocketAddr) -> Vec<(u32, String, String, u16, u64)> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_offset_commits)
        })
        .unwrap_or_default()
    }

    /// Defer an offset commit until the current transaction commits.
    ///
    /// Returns `true` if deferred (in transaction), `false` if not (commit immediately).
    pub fn defer_offset_commit(
        &self,
        addr: &SocketAddr,
        tenant_id: u32,
        stream: String,
        group: String,
        partition_id: u16,
        lsn: u64,
    ) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session
                    .pending_offset_commits
                    .push((tenant_id, stream, group, partition_id, lsn));
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// Buffer a write task during a transaction block.
    ///
    /// Returns `true` if buffered (in transaction), `false` if not (dispatch immediately).
    pub fn buffer_write(&self, addr: &SocketAddr, task: PhysicalTask) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_buffer.push(task);
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// ROLLBACK — discard the write buffer and return to idle.
    /// Returns any pending GAP_FREE reservations that need to be rolled back.
    pub fn rollback(
        &self,
        addr: &SocketAddr,
    ) -> Result<Vec<crate::control::sequence::gap_free::ReservationHandle>, &'static str> {
        let reservations = self
            .write_session(addr, |session| {
                session.tx_buffer.clear();
                session.tx_state = TransactionState::Idle;
                session.tx_snapshot_lsn = None;
                session.tx_read_set.clear();
                session.savepoints.clear();
                session.pending_offset_commits.clear();
                std::mem::take(&mut session.pending_sequence_reservations)
            })
            .unwrap_or_default();
        Ok(reservations)
    }

    /// Mark the current transaction as failed (after a query error inside BEGIN).
    pub fn fail_transaction(&self, addr: &SocketAddr) {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_state = TransactionState::Failed;
            }
        });
    }

    /// Create a savepoint at the current tx_buffer position.
    pub fn create_savepoint(&self, addr: &SocketAddr, name: String) {
        self.write_session(addr, |session| {
            let pos = session.tx_buffer.len();
            session.savepoints.push((name, pos));
        });
    }

    /// Release a savepoint (remove from stack, keep buffered operations).
    pub fn release_savepoint(&self, addr: &SocketAddr, name: &str) {
        self.write_session(addr, |session| {
            session.savepoints.retain(|(n, _)| n != name);
        });
    }

    /// Rollback to a savepoint: truncate tx_buffer to the saved position.
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(&self, addr: &SocketAddr, name: &str) -> crate::Result<()> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|(n, _)| n == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            let buffer_pos = session.savepoints[pos].1;
            session.tx_buffer.truncate(buffer_pos);
            session.savepoints.truncate(pos + 1);
            Ok(())
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }
}
