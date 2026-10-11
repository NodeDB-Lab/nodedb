// SPDX-License-Identifier: BUSL-1.1

//! Where the response phase reads a dispatched write's answer.

use std::time::Instant;

use crate::control::ResponseReceiver;
use crate::control::local_dispatch::{DispatchCollectError, collect_bounded_response};
use crate::control::server::dispatch_utils::minted::{OwnedReport, OwnedResponse};

/// A dispatched write's answer, before anyone waits for it.
pub(super) enum Answer {
    /// The write carries no records. The waiter reads the channel, up to
    /// `deadline` and under the byte budget. The waiter drops the receiver
    /// once it stops reading, which removes the request's tracker entry.
    Unminted {
        rx: ResponseReceiver,
        deadline: Instant,
        max_result_bytes: usize,
    },
    /// A spawned task owns the write's records, closes them from the final
    /// response, and reports the answer.
    Owned(OwnedReport),
}

impl Answer {
    pub(super) async fn wait(self) -> crate::Result<OwnedResponse> {
        match self {
            Self::Owned(report) => report.recv().await,
            Self::Unminted {
                mut rx,
                deadline,
                max_result_bytes,
            } => {
                let collected = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    collect_bounded_response(&mut rx, max_result_bytes),
                )
                .await;
                Ok(match collected {
                    Ok(Ok(response)) => OwnedResponse::Answered {
                        response: Box::new(response),
                        closed: Ok(()),
                    },
                    Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
                        OwnedResponse::OverBudget { bytes }
                    }
                    Ok(Err(DispatchCollectError::ChannelClosed)) => OwnedResponse::ChannelClosed,
                    Err(_) => OwnedResponse::DeadlineExceeded,
                })
            }
        }
    }
}
