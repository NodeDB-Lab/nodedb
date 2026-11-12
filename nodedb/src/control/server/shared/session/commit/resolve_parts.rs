// SPDX-License-Identifier: BUSL-1.1

//! A single-shard COMMIT's redo, read part by part from the owning core and
//! proposed as it arrives.
//!
//! Part `0` resolves the transaction on its owning core and opens a resolve
//! cursor there. It reports the byte length of every part, so the commit
//! knows the length of the framed content before it proposes anything. A
//! content that fits one entry travels inline. A larger one travels as a
//! chunk stream: the commit pulls a part, proposes every full chunk, and
//! pulls the next part. It holds at most one chunk and one part.

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, ResolvedTxnOpen, ResolvedTxnPart};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::envelope::Status;
use crate::control::state::SharedState;
use crate::control::wal_replication::AppliedOutput;
use crate::control::wal_replication::encode::{fits_inline, redo_header_frames};
use crate::control::wal_replication::transaction_redo::{
    RedoStreamProposal, RedoTarget, TransactionRedoPayload, propose_inline_redo,
};
use crate::control::wal_replication::types::RedoContent;
use crate::types::{DatabaseId, TenantId, TxnId, VShardId};

use super::super::outcome::{AbortReason, TxnDataPlane};

/// The transaction a resolve reads, and where its overlay is.
#[derive(Debug, Clone, Copy)]
pub(super) struct ResolveScope {
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub txn_id: TxnId,
}

/// The parts of one transaction's resolve, read in order.
pub(super) struct ResolveParts<'a, D: TxnDataPlane> {
    dp: &'a D,
    scope: ResolveScope,
    max_part_bytes: u64,
    /// The part the next read asks for.
    next: u32,
    /// Whether the last part was read.
    done: bool,
}

/// Why the owning core did not answer a part.
enum PartRefusal {
    /// The core answered an error status with this code.
    Rejected(Option<crate::bridge::envelope::ErrorCode>),
    /// The dispatch or the answer's decode failed.
    Failed(crate::Error),
}

impl PartRefusal {
    /// The reason the commit aborts with.
    fn into_abort(self) -> AbortReason {
        match self {
            PartRefusal::Rejected(code) => AbortReason::BatchRejected { code },
            PartRefusal::Failed(e) => AbortReason::Dispatch(e),
        }
    }
}

/// What part `0` of a resolve answers.
pub(super) struct OpenedResolve {
    pub opened: ResolvedTxnOpen,
    pub first: Vec<u8>,
}

impl<'a, D: TxnDataPlane> ResolveParts<'a, D> {
    pub(super) fn new(dp: &'a D, scope: ResolveScope, max_part_bytes: u64) -> Self {
        Self {
            dp,
            scope,
            max_part_bytes,
            next: 0,
            done: false,
        }
    }

    /// Ask the owning core for part `part`. A refusal answers its error code.
    async fn request(
        &self,
        part: u32,
        plans: Vec<PhysicalPlan>,
    ) -> Result<ResolvedTxnPart, PartRefusal> {
        let task = PhysicalTask {
            tenant_id: self.scope.tenant_id,
            vshard_id: self.scope.vshard_id,
            database_id: self.scope.database_id,
            plan: PhysicalPlan::Meta(MetaOp::ResolveTxnPart {
                txn_id: self.scope.txn_id,
                plans,
                part,
                max_part_bytes: self.max_part_bytes,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let response = match self.dp.dispatch_no_wal(task).await {
            Ok(r) if r.status == Status::Ok => r,
            Ok(r) => return Err(PartRefusal::Rejected(r.error_code.as_deref().cloned())),
            Err(e) => return Err(PartRefusal::Failed(e)),
        };
        let answer: ResolvedTxnPart =
            zerompk::from_msgpack(response.payload.as_bytes()).map_err(|e| {
                PartRefusal::Failed(crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "{} resolve part {part} does not decode: {e}",
                        self.scope.txn_id
                    ),
                })
            })?;
        if answer.index != part {
            return Err(PartRefusal::Failed(crate::Error::Internal {
                detail: format!(
                    "{} asked for resolve part {part} and got part {}",
                    self.scope.txn_id, answer.index
                ),
            }));
        }
        Ok(answer)
    }

    /// Resolve the transaction from `plans` and read part `0`. Every check
    /// of the resolve runs on the owning core before it answers.
    pub(super) async fn open(
        &mut self,
        plans: Vec<PhysicalPlan>,
    ) -> Result<OpenedResolve, AbortReason> {
        let answer = self
            .request(0, plans)
            .await
            .map_err(PartRefusal::into_abort)?;
        let Some(opened) = answer.opened else {
            return Err(AbortReason::Dispatch(crate::Error::Internal {
                detail: format!(
                    "{} resolve part 0 reports no resolve summary",
                    self.scope.txn_id
                ),
            }));
        };
        self.next = 1;
        self.done = answer.done;
        Ok(OpenedResolve {
            opened,
            first: answer.bytes,
        })
    }

    /// Read the next part. `None` once the last part was read.
    pub(super) async fn next_part(&mut self) -> crate::Result<Option<Vec<u8>>> {
        if self.done {
            return Ok(None);
        }
        let answer = match self.request(self.next, Vec::new()).await {
            Ok(answer) => answer,
            Err(PartRefusal::Rejected(Some(code))) => return Err(crate::Error::DataPlane(code)),
            Err(PartRefusal::Rejected(None)) => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "{} resolve part {} refused with no error code",
                        self.scope.txn_id, self.next
                    ),
                });
            }
            Err(PartRefusal::Failed(e)) => return Err(e),
        };
        self.next += 1;
        self.done = answer.done;
        Ok(Some(answer.bytes))
    }
}

/// Propose a resolved redo whose header fields `payload` carries and whose
/// resolved lists `parts` holds, and wait until it applied on this node.
///
/// A framed content that fits one entry travels inline. A larger one
/// travels as a chunk stream, and a stream that stops before its final
/// entry applies is abandoned.
pub(super) async fn propose_resolved<D: TxnDataPlane>(
    state: &SharedState,
    target: RedoTarget,
    mut payload: TransactionRedoPayload,
    resolved: OpenedResolve,
    parts: &mut ResolveParts<'_, D>,
    deadline: tokio::time::Instant,
) -> crate::Result<AppliedOutput> {
    let header = redo_header_frames(&payload)?;
    let len = (header.len() as u64).saturating_add(resolved.opened.len);
    if fits_inline(&payload, len, state.redo_chunks.limits().max_entry_bytes) {
        let mut framed = header;
        framed.extend_from_slice(&resolved.first);
        while let Some(part) = parts.next_part().await? {
            framed.extend_from_slice(&part);
        }
        payload.redo = RedoContent::from_framed(&framed)?.redo;
        return propose_inline_redo(state, target, &payload, deadline).await;
    }
    let mut stream = RedoStreamProposal::open(state, target, &payload, len, deadline)?;
    match push_parts(&mut stream, &header, resolved.first, parts).await {
        Ok(()) => stream.finish().await,
        Err(error) => {
            stream.abandon().await;
            Err(error)
        }
    }
}

/// Push the header frames, then every part, to `stream`.
async fn push_parts<D: TxnDataPlane>(
    stream: &mut RedoStreamProposal<'_>,
    header: &[u8],
    first: Vec<u8>,
    parts: &mut ResolveParts<'_, D>,
) -> crate::Result<()> {
    stream.push(header).await?;
    stream.push(&first).await?;
    drop(first);
    while let Some(part) = parts.next_part().await? {
        stream.push(&part).await?;
    }
    Ok(())
}
