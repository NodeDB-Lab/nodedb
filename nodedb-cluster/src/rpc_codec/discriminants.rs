// SPDX-License-Identifier: BUSL-1.1

//! RPC type discriminant constants.
//!
//! All constants MUST remain stable across versions — they appear on the
//! wire. Adding new constants is fine; changing existing ones breaks
//! binary compatibility.

pub const RPC_APPEND_ENTRIES_REQ: u8 = 1;
pub const RPC_APPEND_ENTRIES_RESP: u8 = 2;
pub const RPC_REQUEST_VOTE_REQ: u8 = 3;
pub const RPC_REQUEST_VOTE_RESP: u8 = 4;
pub const RPC_INSTALL_SNAPSHOT_REQ: u8 = 5;
pub const RPC_INSTALL_SNAPSHOT_RESP: u8 = 6;
pub const RPC_JOIN_REQ: u8 = 7;
pub const RPC_JOIN_RESP: u8 = 8;
pub const RPC_PING: u8 = 9;
pub const RPC_PONG: u8 = 10;
pub const RPC_TOPOLOGY_UPDATE: u8 = 11;
pub const RPC_TOPOLOGY_ACK: u8 = 12;
/// Retired in Phase C-δ.6: reserved, do not reuse — was ForwardRequest/Response
/// (SQL-string forwarding path replaced by gateway.execute / ExecuteRequest).
#[allow(dead_code)]
pub const RPC_FORWARD_REQ: u8 = 13;
/// Retired in Phase C-δ.6: reserved, do not reuse — was ForwardRequest/Response
/// (SQL-string forwarding path replaced by gateway.execute / ExecuteRequest).
#[allow(dead_code)]
pub const RPC_FORWARD_RESP: u8 = 14;
pub const RPC_VSHARD_ENVELOPE: u8 = 15;
pub const RPC_METADATA_PROPOSE_REQ: u8 = 16;
pub const RPC_METADATA_PROPOSE_RESP: u8 = 17;
pub const RPC_EXECUTE_REQ: u8 = 18;
pub const RPC_EXECUTE_RESP: u8 = 19;
/// Data-group (non-metadata) proposal forwarding — vshard_id + payload.
pub const RPC_DATA_PROPOSE_REQ: u8 = 20;
pub const RPC_DATA_PROPOSE_RESP: u8 = 21;
/// Streaming physical-plan execution (L4). The request reuses the existing
/// [`ExecuteRequest`](super::execute::ExecuteRequest) body; the response is a
/// multi-frame sequence of `RPC_EXECUTE_STREAM_CHUNK` envelopes terminated by
/// exactly one `RPC_EXECUTE_STREAM_END` envelope on the same QUIC stream.
pub const RPC_EXECUTE_STREAM_REQ: u8 = 22;
pub const RPC_EXECUTE_STREAM_CHUNK: u8 = 23;
pub const RPC_EXECUTE_STREAM_END: u8 = 24;
/// Cross-node streaming shuffle (E1). A producer opens one bidi stream per
/// target partition and writes a `RPC_SHUFFLE_PUSH_REQ` envelope, then a
/// sequence of `RPC_SHUFFLE_PUSH_CHUNK` envelopes terminated by exactly one
/// `RPC_SHUFFLE_PUSH_END` envelope on the same QUIC stream. Direction is
/// producer → receiver: chunks travel on the send half, not the response half.
pub const RPC_SHUFFLE_PUSH_REQ: u8 = 25;
pub const RPC_SHUFFLE_PUSH_CHUNK: u8 = 26;
pub const RPC_SHUFFLE_PUSH_END: u8 = 27;
/// Cross-node shuffle PRODUCER trigger (E4a). A coordinator sends a
/// `RPC_SHUFFLE_PRODUCE_REQ` to a producer node; that node executes a local
/// scan fragment, hash-partitions each output row, and fans the rows out to the
/// per-part owners as `RPC_SHUFFLE_PUSH_*` streams (looping back into its own
/// receiver registry for parts it owns). The producer replies with exactly one
/// `RPC_SHUFFLE_PRODUCE_RESP` carrying terminal success or a typed error — it
/// does NOT stream the scanned rows back to the coordinator.
pub const RPC_SHUFFLE_PRODUCE_REQ: u8 = 28;
pub const RPC_SHUFFLE_PRODUCE_RESP: u8 = 29;
/// Cross-node shuffle CONSUMER trigger (E4b). A coordinator sends a
/// `RPC_SHUFFLE_CONSUME_REQ` to a part-owner node; that node waits for both
/// staged sides of its `(shuffle_id, part)` to finalize, runs the node-local
/// grace-hash join over them, and replies with exactly one
/// `RPC_SHUFFLE_CONSUME_RESP` carrying the join-result rows (or a typed error).
/// One-shot request/response — no streaming.
pub const RPC_SHUFFLE_CONSUME_REQ: u8 = 30;
pub const RPC_SHUFFLE_CONSUME_RESP: u8 = 31;
/// Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b). A coordinator
/// sends a `RPC_SHUFFLE_AGG_CONSUME_REQ` to a part-owner node; that node waits
/// for its part's single staged producer side (side 0) to finalize, merges the
/// staged partial `GroupState`s, finalizes / HAVING-filters / sorts / LIMITs, and
/// replies with exactly one `RPC_SHUFFLE_AGG_CONSUME_RESP` carrying the result
/// rows (or a typed error). One-shot request/response — no streaming. The
/// single-sided aggregate sibling of `RPC_SHUFFLE_CONSUME_*`.
pub const RPC_SHUFFLE_AGG_CONSUME_REQ: u8 = 32;
pub const RPC_SHUFFLE_AGG_CONSUME_RESP: u8 = 33;
/// Routed-surrogate-exchange (F1b). A coordinator planning a cross-shard graph
/// edge sends a `RPC_ASSIGN_SURROGATE_REQ` to the LEADER of the endpoint key's
/// home vShard; that leader assign-or-returns the AUTHORITATIVE global surrogate
/// for `(collection, pk)` (a LOCAL assign on the leader yields the authoritative
/// value) and replies with exactly one `RPC_ASSIGN_SURROGATE_RESP` carrying the
/// surrogate (or a typed error). One-shot request/response — no streaming.
pub const RPC_ASSIGN_SURROGATE_REQ: u8 = 34;
pub const RPC_ASSIGN_SURROGATE_RESP: u8 = 35;

/// Routed Calvin-submit (Cv1). A coordinator that is NOT the sequencer-group
/// leader sends a `RPC_SUBMIT_CALVIN_TXN_REQ` carrying a msgpack-encoded
/// `TxClass` to the leader; the leader submits it to its local Calvin sequencer
/// inbox and awaits assignment + completion, replying with exactly one
/// `RPC_SUBMIT_CALVIN_TXN_RESP` carrying success or a typed error. One-shot
/// request/response — no streaming.
pub const RPC_SUBMIT_CALVIN_TXN_REQ: u8 = 36;
pub const RPC_SUBMIT_CALVIN_TXN_RESP: u8 = 37;

/// Routed Calvin-INBOX submit (Cv1). The OLLP dependent sibling of
/// `RPC_SUBMIT_CALVIN_TXN_*`: a coordinator that is NOT the sequencer-group
/// leader sends a `RPC_SUBMIT_CALVIN_INBOX_REQ` carrying a msgpack-encoded
/// dependent `TxClass` to the leader; the leader submits it to its local Calvin
/// sequencer inbox and awaits only the ASSIGNMENT (NOT completion), replying
/// with exactly one `RPC_SUBMIT_CALVIN_INBOX_RESP` carrying the assignment
/// (`inbox_seq` / `epoch` / `position` / `participants`) or a typed error.
/// One-shot request/response — no streaming. Discriminants 38/39 are
/// permanently assigned to these variants.
pub const RPC_SUBMIT_CALVIN_INBOX_REQ: u8 = 38;
pub const RPC_SUBMIT_CALVIN_INBOX_RESP: u8 = 39;

/// TimeoutNow (leadership transfer). One-way — sender does not await a reply.
/// The receiver immediately starts an election for the addressed Raft group.
pub const RPC_TIMEOUT_NOW_REQ: u8 = 40;

/// Routed reserve-read (Calvin OLLP). A coordinator sends a
/// `RPC_RESERVE_READ_REQ` carrying a msgpack-encoded `LockKeyWire` to the
/// sequencer-group leader; the leader assign-only reserves the read lock and
/// replies with exactly one `RPC_RESERVE_READ_RESP` carrying the minted owner
/// (`TxnIdWire`, msgpack-encoded) or a typed error. One-shot request/response
/// — no streaming.
pub const RPC_RESERVE_READ_REQ: u8 = 41;
pub const RPC_RESERVE_READ_RESP: u8 = 42;

/// Routed release-reservation (Calvin OLLP). The ack-only sibling of
/// `RPC_RESERVE_READ_*`: a coordinator sends a `RPC_RELEASE_RESERVATION_REQ`
/// carrying the msgpack-encoded owner (`TxnIdWire`) and release reason
/// (`ReleaseReason`) to the sequencer-group leader; the leader releases the
/// reservation and replies with exactly one `RPC_RELEASE_RESERVATION_RESP`
/// carrying success or a typed error. One-shot request/response — no
/// streaming.
pub const RPC_RELEASE_RESERVATION_REQ: u8 = 43;
pub const RPC_RELEASE_RESERVATION_RESP: u8 = 44;

/// PreVote (Raft pre-vote extension). Request/response, like RequestVote —
/// a candidate probes whether peers WOULD grant a vote at a hypothetical
/// term before bumping its own term and running a real election.
pub const RPC_PRE_VOTE_REQ: u8 = 45;
pub const RPC_PRE_VOTE_RESP: u8 = 46;

/// Routed read index. A node that does not lead a Raft group sends a
/// `RPC_READ_INDEX_REQ` to the group leader; the leader confirms its
/// leadership against a quorum and replies with exactly one
/// `RPC_READ_INDEX_RESP` carrying its read index, a leader hint, or a
/// timeout. One-shot request/response — no streaming.
pub const RPC_READ_INDEX_REQ: u8 = 47;
pub const RPC_READ_INDEX_RESP: u8 = 48;
/// Authorization lease renewal: a node reports its authorization coverage to
/// the metadata group leader in `RPC_AUTH_LEASE_RENEW_REQ` and receives one
/// `RPC_AUTH_LEASE_RENEW_RESP` granting or withholding its lease.
pub const RPC_AUTH_LEASE_RENEW_REQ: u8 = 49;
pub const RPC_AUTH_LEASE_RENEW_RESP: u8 = 50;
/// Authorization barrier: a writer holds its acknowledgement until the
/// metadata group leader answers `RPC_AUTH_BARRIER_REQ` with one
/// `RPC_AUTH_BARRIER_RESP`.
pub const RPC_AUTH_BARRIER_REQ: u8 = 51;
pub const RPC_AUTH_BARRIER_RESP: u8 = 52;

// VShardMessageType discriminants for distributed array ops (u16, range 80-89).
// These mirror `crate::wire::VShardMessageType` repr values and are declared
// here so external code can reference them without importing the full enum.
pub const VSHARD_ARRAY_SHARD_SLICE_REQ: u16 = 80;
pub const VSHARD_ARRAY_SHARD_SLICE_RESP: u16 = 81;
pub const VSHARD_ARRAY_SHARD_AGG_REQ: u16 = 82;
pub const VSHARD_ARRAY_SHARD_AGG_RESP: u16 = 83;
pub const VSHARD_ARRAY_SHARD_PUT_REQ: u16 = 84;
pub const VSHARD_ARRAY_SHARD_PUT_RESP: u16 = 85;
pub const VSHARD_ARRAY_SHARD_DELETE_REQ: u16 = 86;
pub const VSHARD_ARRAY_SHARD_DELETE_RESP: u16 = 87;
pub const VSHARD_ARRAY_SHARD_SURROGATE_BITMAP_REQ: u16 = 88;
pub const VSHARD_ARRAY_SHARD_SURROGATE_BITMAP_RESP: u16 = 89;
