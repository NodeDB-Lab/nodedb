// SPDX-License-Identifier: BUSL-1.1

//! The spooled form of a [`WriteOp`], and the helpers every spooled record
//! shares.

use crate::event::WriteOp;

/// Bytes an owned byte vector costs beside its heap bytes.
pub(in crate::data::executor) const HEADER: usize = std::mem::size_of::<Vec<u8>>();

/// The weight of an optional byte vector.
pub(in crate::data::executor) fn bytes_weight(bytes: Option<&[u8]>) -> usize {
    HEADER + bytes.map_or(0, <[u8]>::len)
}

/// A spool record that does not decode to what it encoded.
pub(in crate::data::executor) fn undecodable(
    what: &str,
    detail: impl std::fmt::Display,
) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("spooled {what}: {detail}"),
    }
}

/// A [`WriteOp`] as a tag and its row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct SpooledOp {
    tag: u8,
    count: u32,
}

impl SpooledOp {
    pub(in crate::data::executor) fn of(op: WriteOp) -> Self {
        let (tag, count) = match op {
            WriteOp::Insert => (0, 0),
            WriteOp::Update => (1, 0),
            WriteOp::Delete => (2, 0),
            WriteOp::BulkInsert { count } => (3, count),
            WriteOp::BulkDelete { count } => (4, count),
            WriteOp::Heartbeat => (5, 0),
            WriteOp::Publish => (6, 0),
        };
        Self { tag, count }
    }

    pub(in crate::data::executor) fn op(self) -> crate::Result<WriteOp> {
        Ok(match self.tag {
            0 => WriteOp::Insert,
            1 => WriteOp::Update,
            2 => WriteOp::Delete,
            3 => WriteOp::BulkInsert { count: self.count },
            4 => WriteOp::BulkDelete { count: self.count },
            5 => WriteOp::Heartbeat,
            6 => WriteOp::Publish,
            tag => return Err(undecodable("write op", format_args!("tag {tag}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T) -> T
    where
        T: zerompk::ToMessagePack + for<'de> zerompk::FromMessagePack<'de>,
    {
        let bytes = zerompk::to_msgpack_vec(value).expect("encode");
        zerompk::from_msgpack(&bytes).expect("decode")
    }

    #[test]
    fn every_write_op_round_trips() {
        for op in [
            WriteOp::Insert,
            WriteOp::Update,
            WriteOp::Delete,
            WriteOp::BulkInsert { count: 7 },
            WriteOp::BulkDelete { count: 9 },
            WriteOp::Heartbeat,
            WriteOp::Publish,
        ] {
            let spooled = round_trip(&SpooledOp::of(op));
            assert_eq!(spooled.op().expect("op"), op);
        }
        assert!(SpooledOp { tag: 99, count: 0 }.op().is_err());
    }
}
