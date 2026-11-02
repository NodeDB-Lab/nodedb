// SPDX-License-Identifier: BUSL-1.1

//! The result of one fan-out/gather cycle, before any merge.
//!
//! A gather keeps each answering core's or shard's payload as that core
//! produced it. Each consumer builds the shape it reads on demand:
//!
//! - [`GatherOutcome::merged_array`]: one msgpack array of every element,
//!   for a consumer whose producers answer only row arrays. Every payload
//!   must be a msgpack array.
//! - [`GatherOutcome::merged_rows`]: one msgpack array of every relational
//!   row. A payload is a row array, or one point-read row as a bare map.
//! - [`GatherOutcome::payloads`]: the per-core payloads, for a consumer that
//!   reads one structured envelope per core (an array slice).
//! - [`GatherOutcome::single_core_shape`]: the payload a single core
//!   produces, for a node-level result.
//!
//! A gather parses no payload. A consumer that reads no rows, such as a
//! purge barrier, parses none.

use crate::control::server::payload_merge::{
    encode_msgpack_array, merge_msgpack_arrays, merge_payload_rows,
};
use crate::types::{Lsn, ReadShards, VShardId};

use super::gather::finalize_aggregate;

/// Outcomes of a full fan-out/gather cycle across all Data-Plane cores.
pub struct GatherOutcome {
    /// Every non-empty payload, one per answering core or shard, in answer
    /// order, each in the shape its producer wrote.
    pub(super) payloads: Vec<Vec<u8>>,
    /// Maximum watermark LSN seen across all responding cores. Retained as the
    /// scalar fence value for Strong-consistency callers that need one LSN.
    pub watermark_lsn: Lsn,
    /// The vShards every responding core read. The transaction read-set
    /// homes an entry on each. Distinct from `watermark_lsn`.
    pub read_vshards: ReadShards,
    /// Per-shard watermark LSNs — one `(vshard, watermark_lsn)` per responding
    /// core, NOT collapsed to the max. The transaction read-set records one
    /// entry per participating shard from this, so a predicate read fanned over
    /// N cores is validated against each core's own version rather than a
    /// single global max.
    pub shard_watermarks: Vec<(VShardId, Lsn)>,
}

impl GatherOutcome {
    /// An outcome of `payloads` that carries no watermark and no read
    /// vShard. Empty payloads are dropped.
    pub(super) fn without_reads(payloads: Vec<Vec<u8>>) -> Self {
        Self {
            payloads: payloads.into_iter().filter(|p| !p.is_empty()).collect(),
            watermark_lsn: Lsn::ZERO,
            read_vshards: ReadShards::new(),
            shard_watermarks: Vec::new(),
        }
    }

    /// Every non-empty payload, one per answering core or shard.
    pub fn payloads(&self) -> &[Vec<u8>] {
        &self.payloads
    }

    /// One msgpack array of every element of every payload, in answer order.
    ///
    /// Every payload must be one msgpack array. Any other payload is a
    /// `Codec` error, never an element.
    pub fn merged_array(&self) -> crate::Result<Vec<u8>> {
        merge_msgpack_arrays(&self.payloads)
    }

    /// One msgpack array of every relational row of every payload, in answer
    /// order, passed through [`finalize_aggregate`] when the gather is a
    /// `Gather{as_aggregate: true}`.
    ///
    /// A payload is a row array, or a point read's one row as a bare map
    /// (see `payload_merge::payload_rows`). Any other payload is a `Codec`
    /// error.
    pub fn merged_rows(&self, as_aggregate: bool) -> crate::Result<Vec<u8>> {
        let merged = merge_payload_rows(&self.payloads)?;
        Ok(if as_aggregate {
            finalize_aggregate(&merged)
        } else {
            merged
        })
    }

    /// The payload in the shape a single core produces.
    ///
    /// No payload is an empty msgpack array. One payload returns verbatim,
    /// whatever its shape. Two or more merge as row arrays through
    /// [`Self::merged_array`].
    pub fn single_core_shape(&self) -> crate::Result<Vec<u8>> {
        match self.payloads.as_slice() {
            [] => Ok(encode_msgpack_array(&[])),
            [one] => Ok(one.clone()),
            _ => self.merged_array(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::payload_merge::extract_msgpack_elements;

    /// One flat row, the plain msgpack map `{"k": value}`.
    fn row(value: u8) -> Vec<u8> {
        assert!(value < 0x80, "a positive fixint holds 0..=127");
        vec![0x81, 0xa1, b'k', value]
    }

    #[test]
    fn row_arrays_merge_in_answer_order() {
        let outcome = GatherOutcome::without_reads(vec![
            encode_msgpack_array(&[row(1), row(2)]),
            Vec::new(),
            encode_msgpack_array(&[row(3)]),
        ]);
        assert_eq!(outcome.payloads().len(), 2, "empty payloads are dropped");
        let merged = outcome.merged_array().expect("merge");
        assert_eq!(
            extract_msgpack_elements(&merged).expect("extract"),
            vec![row(1), row(2), row(3)]
        );
    }

    /// A per-core map envelope is not an array element: the array merge
    /// refuses it.
    #[test]
    fn a_non_array_payload_is_not_merged_as_an_element() {
        let outcome = GatherOutcome::without_reads(vec![encode_msgpack_array(&[row(1)]), row(2)]);
        assert!(matches!(
            outcome.merged_array(),
            Err(crate::Error::Codec { .. })
        ));
    }

    /// A routed point read answers its one row as a bare map.
    #[test]
    fn a_point_read_row_merges_as_one_relational_row() {
        let outcome = GatherOutcome::without_reads(vec![encode_msgpack_array(&[row(1)]), row(2)]);
        let merged = outcome.merged_rows(false).expect("rows");
        assert_eq!(
            extract_msgpack_elements(&merged).expect("extract"),
            vec![row(1), row(2)]
        );
    }

    /// A payload in no row shape fails the relational merge.
    #[test]
    fn a_scalar_payload_is_not_a_relational_row() {
        let outcome = GatherOutcome::without_reads(vec![vec![0x2a]]);
        assert!(matches!(
            outcome.merged_rows(true),
            Err(crate::Error::Codec { .. })
        ));
    }

    /// The per-core payloads stay as their cores wrote them.
    #[test]
    fn payloads_keep_each_core_shape() {
        let outcome = GatherOutcome::without_reads(vec![row(1), row(2)]);
        assert_eq!(outcome.payloads(), &[row(1), row(2)]);
    }

    #[test]
    fn no_payload_is_an_empty_array() {
        let outcome = GatherOutcome::without_reads(vec![Vec::new()]);
        let shape = outcome.single_core_shape().expect("shape");
        assert!(
            extract_msgpack_elements(&shape)
                .expect("extract")
                .is_empty()
        );
        assert!(
            extract_msgpack_elements(&outcome.merged_array().expect("merge"))
                .expect("extract")
                .is_empty()
        );
    }

    /// One core's payload is the single-core shape as is, a map included.
    #[test]
    fn one_payload_returns_verbatim() {
        let outcome = GatherOutcome::without_reads(vec![row(7)]);
        assert_eq!(outcome.single_core_shape().expect("shape"), row(7));
    }

    #[test]
    fn several_row_arrays_merge_into_one() {
        let outcome = GatherOutcome::without_reads(vec![
            encode_msgpack_array(&[row(1)]),
            encode_msgpack_array(&[row(2)]),
        ]);
        let shape = outcome.single_core_shape().expect("shape");
        assert_eq!(
            extract_msgpack_elements(&shape).expect("extract"),
            vec![row(1), row(2)]
        );
    }

    /// Two cores that each answer a map have no single-core shape.
    #[test]
    fn several_non_array_payloads_are_refused() {
        let outcome = GatherOutcome::without_reads(vec![row(1), row(2)]);
        assert!(matches!(
            outcome.single_core_shape(),
            Err(crate::Error::Codec { .. })
        ));
    }
}
