// SPDX-License-Identifier: BUSL-1.1

//! The mirror link's wire: one request and one response on each bidi stream.
//!
//! A frame is `[len u32 BE][MessagePack payload]`. The reader checks `len`
//! against its bound before it allocates, so a peer on either side of this
//! trust boundary cannot force a large allocation. [`MirrorRequest::validate`]
//! bounds what a decoded request carries.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use nodedb_types::MirrorGroupPosition;

use super::bootstrap::CrossClusterSnapshotEnvelope;
use super::error::MirrorError;
use super::handshake::{MirrorHello, MirrorHelloAck};

/// The largest request frame a source reads.
pub const MAX_REQUEST_FRAME: usize = 64 * 1024;
/// The largest response frame a mirror reads, and a source sends. It leaves
/// room for a full batch plus one oversized entry.
pub const MAX_RESPONSE_FRAME: usize = 64 * 1024 * 1024;
/// The most log bytes one fetch scans. A larger request reads as this.
pub const MAX_TAIL_BYTES: u64 = 16 * 1024 * 1024;
/// The longest one fetch long-polls. A larger request reads as this.
pub const MAX_TAIL_WAIT_MS: u64 = 30_000;
/// The most positions one ack carries.
pub const MAX_ACK_POSITIONS: usize = 4_096;
/// The longest cluster or database name a hello carries.
pub const MAX_NAME_BYTES: usize = 256;
/// The most capture bytes one bootstrap response carries.
pub const MAX_BOOTSTRAP_CHUNK: usize = 16 * 1024 * 1024;
/// The most bytes one bootstrap capture holds. The source refuses to serve a
/// larger one, and the mirror refuses to receive it.
pub const MAX_BOOTSTRAP_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Ask for the bootstrap capture of a link, from `offset`.
///
/// Offset 0 starts a capture when the source holds none for the link. A source
/// that holds none at a later offset refuses, and the mirror starts over.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorBootstrapRequest {
    /// The link the capture is for. Its pin names the database.
    pub link_id: u64,
    pub offset: u64,
}

/// One fetch of a group's committed log.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorTailRequest {
    /// The link the fetch reads for. Its pin names the database.
    pub link_id: u64,
    pub group: u64,
    /// The first log index the mirror wants.
    pub from_index: u64,
    /// The most log bytes the source scans for this fetch.
    pub max_bytes: u64,
    /// How long the source waits for `from_index` to commit, in milliseconds.
    pub wait_ms: u64,
}

/// One log entry that passed the source's filter. A metadata entry yields one
/// of these per catalog entry of the database, all at its log index.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorTailEntry {
    pub index: u64,
    pub data: Vec<u8>,
}

/// What one fetch read. `next_index` is past every entry the source scanned,
/// including the ones its filter dropped.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorTailBatch {
    pub entries: Vec<MirrorTailEntry>,
    pub next_index: u64,
    /// The replica's commit index.
    pub commit_index: u64,
    /// The replica's HLC wall time, in nanoseconds.
    pub source_hlc_ns: u64,
    /// The lowest index the replica still holds.
    pub first_available: u64,
}

/// A mirror's progress: every entry at or below `positions` belongs to `cut`,
/// which its groups committed.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorAck {
    pub link_id: u64,
    pub cut: u64,
    pub positions: Vec<MirrorGroupPosition>,
}

/// Why a source answered no. Each variant names what the mirror acts on.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    thiserror::Error,
)]
pub enum MirrorRefusal {
    #[error("the source speaks mirror protocol {supported}, not {requested}")]
    BadVersion { supported: u16, requested: u16 },
    #[error("this cluster is {actual:?}, not the {declared:?} the mirror named")]
    WrongCluster { declared: String, actual: String },
    #[error("a cluster cannot mirror its own database")]
    SameCluster,
    #[error("the client's allowlist entry does not name database {database:?}")]
    DatabaseNotAllowed { database: String },
    #[error("the allowed database {database:?} does not exist on this cluster")]
    DatabaseUnknown { database: String },
    #[error("the default database cannot be mirrored")]
    DefaultDatabase,
    #[error("database {database:?} is itself a mirror, so this cluster is not its source")]
    SourceIsMirror { database: String },
    #[error("link {link_id} belongs to another client or another database")]
    LinkNotOwned { link_id: u64 },
    #[error("link {link_id} has no pin on this cluster; send a hello first")]
    LinkUnknown { link_id: u64 },
    #[error("the pin of link {link_id} expired; the mirror must bootstrap again")]
    PinExpired { link_id: u64 },
    #[error("the source database of link {link_id} dropped")]
    SourceDropped { link_id: u64 },
    #[error("group {group} is not a group a mirror tails")]
    GroupNotTailed { group: u64 },
    #[error("this node hosts no replica of group {group}")]
    NotHosted { group: u64 },
    #[error("group {group} starts at index {first_available} on this replica")]
    LogTruncated { group: u64, first_available: u64 },
    #[error("a frame of {len} bytes passes the bound of {max}")]
    FrameTooLarge { len: u64, max: u64 },
    #[error("the request is malformed: {detail}")]
    Malformed { detail: String },
    #[error("the source cannot answer yet: {detail}")]
    Unavailable { detail: String },
    #[error("database {database:?} is an unmaterialized clone; materialize it first")]
    UnmaterializedClone { database: String },
    #[error("the capture of link {link_id} failed on the source: {detail}")]
    BootstrapFailed { link_id: u64, detail: String },
    #[error("this replica holds no capture of link {link_id} at offset {offset}")]
    BootstrapRestart { link_id: u64, offset: u64 },
}

impl MirrorRefusal {
    /// A refusal the mirror's next attempt can clear with no operator.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Unavailable { .. }
                | Self::NotHosted { .. }
                | Self::BootstrapFailed { .. }
                | Self::BootstrapRestart { .. }
        )
    }
}

/// One request of a mirror link.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MirrorRequest {
    Hello(MirrorHello),
    Tail(MirrorTailRequest),
    Ack(MirrorAck),
    Bootstrap(MirrorBootstrapRequest),
}

impl MirrorRequest {
    /// Refuse what a well-formed frame cannot carry.
    pub fn validate(&self) -> Result<(), MirrorRefusal> {
        let malformed = |detail: String| Err(MirrorRefusal::Malformed { detail });
        match self {
            Self::Hello(hello) => {
                for name in [&hello.source_cluster, &hello.source_database] {
                    if name.is_empty() || name.len() > MAX_NAME_BYTES {
                        return malformed(format!(
                            "a hello name of {} bytes is outside 1 to {MAX_NAME_BYTES}",
                            name.len()
                        ));
                    }
                }
                Ok(())
            }
            Self::Tail(tail) if tail.from_index == 0 => {
                malformed(format!("group {} fetch from index 0", tail.group))
            }
            Self::Tail(_) => Ok(()),
            Self::Ack(ack) if ack.positions.len() > MAX_ACK_POSITIONS => malformed(format!(
                "an ack of {} positions passes the bound of {MAX_ACK_POSITIONS}",
                ack.positions.len()
            )),
            Self::Ack(_) | Self::Bootstrap(_) => Ok(()),
        }
    }
}

/// One response of a mirror link.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MirrorResponse {
    Hello(MirrorHelloAck),
    Tail(MirrorTailBatch),
    Ack,
    /// A chunk of the capture, or `None` while the source still takes it.
    Bootstrap(Option<Box<CrossClusterSnapshotEnvelope>>),
    Refused(MirrorRefusal),
}

/// Encode `message`, and refuse an encoding past `max`.
pub fn encode_frame<T: zerompk::ToMessagePack>(
    message: &T,
    max: usize,
) -> Result<Vec<u8>, MirrorError> {
    let payload = zerompk::to_msgpack_vec(message).map_err(|e| MirrorError::HandshakeCodec {
        detail: format!("encode a mirror frame: {e}"),
    })?;
    if payload.len() > max {
        return Err(MirrorError::Refused(MirrorRefusal::FrameTooLarge {
            len: payload.len() as u64,
            max: max as u64,
        }));
    }
    Ok(payload)
}

/// Write one frame of `payload`.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), MirrorError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        MirrorError::Refused(MirrorRefusal::FrameTooLarge {
            len: payload.len() as u64,
            max: u64::from(u32::MAX),
        })
    })?;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .map_err(transport("write a mirror frame's length"))?;
    writer
        .write_all(payload)
        .await
        .map_err(transport("write a mirror frame"))
}

/// Read one frame, refusing a declared length past `max` before it allocates.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> Result<Vec<u8>, MirrorError> {
    let mut header = [0u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(transport("read a mirror frame's length"))?;
    let len = u32::from_be_bytes(header) as usize;
    if len > max {
        return Err(MirrorError::Refused(MirrorRefusal::FrameTooLarge {
            len: len as u64,
            max: max as u64,
        }));
    }
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(transport("read a mirror frame"))?;
    Ok(payload)
}

fn transport<E: std::fmt::Display>(step: &'static str) -> impl FnOnce(E) -> MirrorError {
    move |e| MirrorError::Transport {
        detail: format!("{step}: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello(database: &str) -> MirrorRequest {
        MirrorRequest::Hello(MirrorHello {
            source_cluster: "prod".to_string(),
            source_database: database.to_string(),
            mirror_cluster_id: 2,
            link_id: 7,
            protocol_version: 1,
        })
    }

    #[test]
    fn every_message_round_trips() {
        let response = MirrorResponse::Tail(MirrorTailBatch {
            entries: vec![MirrorTailEntry {
                index: 4,
                data: vec![1, 2],
            }],
            next_index: 5,
            commit_index: 9,
            source_hlc_ns: 11,
            first_available: 1,
        });
        let bytes = encode_frame(&response, MAX_RESPONSE_FRAME).expect("encode");
        assert_eq!(
            zerompk::from_msgpack::<MirrorResponse>(&bytes).expect("decode"),
            response
        );
        let refused = MirrorResponse::Refused(MirrorRefusal::LogTruncated {
            group: 1,
            first_available: 9,
        });
        let bytes = encode_frame(&refused, MAX_RESPONSE_FRAME).expect("encode");
        assert_eq!(
            zerompk::from_msgpack::<MirrorResponse>(&bytes).expect("decode"),
            refused
        );
        let bytes = encode_frame(&hello("orders"), MAX_REQUEST_FRAME).expect("encode");
        assert_eq!(
            zerompk::from_msgpack::<MirrorRequest>(&bytes).expect("decode"),
            hello("orders")
        );
    }

    /// The length bound fires before the reader allocates: the frame declares
    /// 4 GiB and holds nothing.
    #[tokio::test]
    async fn a_declared_length_past_the_bound_is_refused() {
        let header = u32::MAX.to_be_bytes();
        let error = read_frame(&mut header.as_slice(), MAX_REQUEST_FRAME)
            .await
            .expect_err("refused");
        assert!(matches!(
            error,
            MirrorError::Refused(MirrorRefusal::FrameTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn a_truncated_frame_is_an_error() {
        let mut bytes = 8u32.to_be_bytes().to_vec();
        bytes.push(1);
        assert!(matches!(
            read_frame(&mut bytes.as_slice(), MAX_REQUEST_FRAME).await,
            Err(MirrorError::Transport { .. })
        ));
    }

    #[tokio::test]
    async fn a_frame_round_trips() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, b"abc").await.expect("write");
        assert_eq!(
            read_frame(&mut bytes.as_slice(), 3).await.expect("read"),
            b"abc"
        );
    }

    #[test]
    fn an_encoding_past_the_bound_is_refused() {
        let error = encode_frame(&hello("orders"), 4).expect_err("refused");
        assert!(matches!(
            error,
            MirrorError::Refused(MirrorRefusal::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn a_hello_name_is_bounded() {
        assert!(hello("orders").validate().is_ok());
        assert!(hello("").validate().is_err());
        assert!(hello(&"x".repeat(MAX_NAME_BYTES + 1)).validate().is_err());
    }

    #[test]
    fn an_ack_is_bounded() {
        let ack = |count| {
            MirrorRequest::Ack(MirrorAck {
                link_id: 1,
                cut: 1,
                positions: vec![
                    MirrorGroupPosition {
                        group_id: 1,
                        log_index: 1,
                    };
                    count
                ],
            })
        };
        assert!(ack(MAX_ACK_POSITIONS).validate().is_ok());
        assert!(ack(MAX_ACK_POSITIONS + 1).validate().is_err());
    }

    #[test]
    fn a_fetch_from_index_zero_is_refused() {
        let tail = |from_index| {
            MirrorRequest::Tail(MirrorTailRequest {
                link_id: 1,
                group: 1,
                from_index,
                max_bytes: 1,
                wait_ms: 0,
            })
        };
        assert!(tail(0).validate().is_err());
        assert!(tail(1).validate().is_ok());
    }
}
