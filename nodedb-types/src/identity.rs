// SPDX-License-Identifier: Apache-2.0

//! Plane-neutral row identity.
//!
//! [`KeyRepr`] is the single representation of a row's identity within a
//! collection, shared across planes and crates: the Data-Plane write notes
//! key committed writes by it, the Control-Plane transaction read-set keys
//! observed reads by it, and the replicated Calvin
//! `TxClass` carries it in its versioned read-set. Because read keys and write
//! keys must compare in one namespace — and because that namespace now travels
//! over the sequencer Raft log — the type lives in the shared `nodedb-types`
//! crate: a pure `Send + Sync` value type with no plane affiliation.

use serde::{Deserialize, Serialize};

/// Identity of a row within a collection.
///
/// The engine that owns the row chooses the representation:
/// - `Surrogate` for the cross-engine `u32` surrogate (schemaless + strict
///   document rows, and vector-by-document upserts keyed on the owning doc).
/// - `KvKey` for the raw Key-Value engine key bytes.
/// - `Edge` for a graph edge, whose identity is the `(src, label, dst)` tuple
///   rather than a surrogate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum KeyRepr {
    /// Cross-engine `u32` surrogate identity.
    Surrogate(u32),
    /// Raw Key-Value engine key bytes.
    KvKey(Box<[u8]>),
    /// Graph edge identity: `(source node, edge label, destination node)`.
    Edge {
        src: Box<str>,
        label: Box<str>,
        dst: Box<str>,
    },
}

// Manual MessagePack impls: `KeyRepr` carries `Box<[u8]>` / `Box<str>` fields
// which the derive cannot handle (zerompk has no impl for unsized-boxed
// slices/strings), and keeping the boxed representation is a deliberate
// footprint choice for the Data-Plane write index. The wire form is a
// self-consistent `[tag, payload]` array mirroring how the derive encodes a
// data enum.
const KEY_REPR_TAG_SURROGATE: u32 = 0;
const KEY_REPR_TAG_KV: u32 = 1;
const KEY_REPR_TAG_EDGE: u32 = 2;

impl zerompk::ToMessagePack for KeyRepr {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        match self {
            KeyRepr::Surrogate(v) => {
                writer.write_u32(KEY_REPR_TAG_SURROGATE)?;
                writer.write_u32(*v)?;
            }
            KeyRepr::KvKey(bytes) => {
                writer.write_u32(KEY_REPR_TAG_KV)?;
                writer.write_binary(bytes)?;
            }
            KeyRepr::Edge { src, label, dst } => {
                writer.write_u32(KEY_REPR_TAG_EDGE)?;
                writer.write_array_len(3)?;
                writer.write_string(src)?;
                writer.write_string(label)?;
                writer.write_string(dst)?;
            }
        }
        Ok(())
    }
}

impl<'de> zerompk::FromMessagePack<'de> for KeyRepr {
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        let outer = reader.read_array_len()?;
        if outer != 2 {
            return Err(zerompk::Error::InvalidMarker(0));
        }
        let tag = reader.read_u32()?;
        match tag {
            KEY_REPR_TAG_SURROGATE => Ok(KeyRepr::Surrogate(reader.read_u32()?)),
            KEY_REPR_TAG_KV => {
                let bytes = reader.read_binary()?;
                Ok(KeyRepr::KvKey(Box::from(bytes.as_ref())))
            }
            KEY_REPR_TAG_EDGE => {
                let inner = reader.read_array_len()?;
                if inner != 3 {
                    return Err(zerompk::Error::InvalidMarker(0));
                }
                let src = reader.read_string()?;
                let label = reader.read_string()?;
                let dst = reader.read_string()?;
                Ok(KeyRepr::Edge {
                    src: Box::from(src.as_ref()),
                    label: Box::from(label.as_ref()),
                    dst: Box::from(dst.as_ref()),
                })
            }
            _ => Err(zerompk::Error::InvalidMarker(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(k: &KeyRepr) -> KeyRepr {
        let bytes = zerompk::to_msgpack_vec(k).expect("encode KeyRepr");
        zerompk::from_msgpack(&bytes).expect("decode KeyRepr")
    }

    #[test]
    fn surrogate_roundtrips() {
        let k = KeyRepr::Surrogate(0xDEAD_BEEF);
        assert_eq!(roundtrip(&k), k);
    }

    #[test]
    fn kv_key_roundtrips() {
        let k = KeyRepr::KvKey(Box::from(&b"\x00\x01\xffhello"[..]));
        assert_eq!(roundtrip(&k), k);
    }

    #[test]
    fn edge_roundtrips() {
        let k = KeyRepr::Edge {
            src: Box::from("alice"),
            label: Box::from("FOLLOWS"),
            dst: Box::from("bob"),
        };
        assert_eq!(roundtrip(&k), k);
    }
}
