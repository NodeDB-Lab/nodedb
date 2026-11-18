// SPDX-License-Identifier: BUSL-1.1

//! The post-state a streamed validate folds a record's sub-records into,
//! held in tiered maps that spill to disk.
//!
//! The whole-record checks hold every sub-record in memory. A stream sees
//! each sub-record once, so the checks that judge the record as a whole
//! fold what they need into these indexes:
//!
//! - `balanced`: each collection's debit and credit sums per group key;
//! - `claims`: each UNIQUE value a put claims, per collection and path;
//! - `written`: the last op that wrote each row of a UNIQUE collection;
//! - `folds`: the target rows the materialized-sum folds left.
//!
//! Once the hot tiers weigh more than the threshold, every map writes its
//! hot tier as a sorted run in the spill directory. A spill never changes a
//! lookup or a scan, so the threshold never changes a verdict. The
//! directory goes when the post-state drops, and the boot sweep removes
//! one a crash left behind.

use std::path::PathBuf;

use rust_decimal::Decimal;

use crate::data::executor::enforcement::balanced::BalancedEntry;
use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_dir};
use crate::data::executor::handlers::spill::run::BlockReader;
use crate::data::executor::handlers::spill::tiered::map::encode_key;
use crate::data::executor::handlers::spill::tiered::{ScanBounds, SpillSpace, TieredMap};
use nodedb_physical::physical_plan::BalancedDef;

/// `(collection, group key)`.
pub(super) type BalancedKey = (String, String);
/// The debit sum and the credit sum, each as the two halves of its
/// serialized [`Decimal`].
pub(super) type BalancedSums = (u64, u64, u64, u64);
/// `(collection, unique path position, value, op)`.
pub(super) type ClaimKey = (String, u32, String, u64);
/// `(collection, surrogate)`.
pub(super) type WrittenKey = (String, u32);
/// `(source collection, target collection, surrogate)`.
pub(super) type FoldKey = (String, String, u32);
/// A folded target row: its document as MessagePack, and its stored bytes.
pub(super) type FoldRowBytes = (Vec<u8>, Vec<u8>);

/// The spilled indexes of one streamed validate.
pub(super) struct PostState {
    space: SpillSpace,
    dir: PathBuf,
    pub(super) balanced: TieredMap<BalancedKey, BalancedSums>,
    pub(super) claims: TieredMap<ClaimKey, u32>,
    pub(super) written: TieredMap<WrittenKey, u64>,
    pub(super) folds: TieredMap<FoldKey, FoldRowBytes>,
    /// The position the next map write takes.
    pub(super) seq: u64,
}

impl PostState {
    /// An empty post-state that spills into `dir` past `threshold` hot
    /// bytes. The directory is created on the first spill.
    pub(super) fn new(dir: PathBuf, threshold: usize) -> Self {
        Self {
            space: SpillSpace::new(dir.clone(), "redo", threshold),
            dir,
            balanced: TieredMap::default(),
            claims: TieredMap::default(),
            written: TieredMap::default(),
            folds: TieredMap::default(),
            seq: 0,
        }
    }

    pub(super) fn reader(&self) -> &BlockReader {
        self.space.reader()
    }

    fn next_seq(&mut self) -> u64 {
        let seq = self.seq;
        self.seq += 1;
        seq
    }

    /// The weight of every hot tier.
    pub(super) fn hot_bytes(&self) -> usize {
        self.balanced.hot_bytes()
            + self.claims.hot_bytes()
            + self.written.hot_bytes()
            + self.folds.hot_bytes()
    }

    /// Spill every hot tier once their weight passed the threshold.
    pub(super) fn spill_past_threshold(&mut self) -> crate::Result<()> {
        if self.hot_bytes() <= self.space.threshold() {
            return Ok(());
        }
        self.balanced.spill(&mut self.space, "balanced")?;
        self.claims.spill(&mut self.space, "claims")?;
        self.written.spill(&mut self.space, "written")?;
        self.folds.spill(&mut self.space, "folds")
    }

    /// Add `entries` of `collection` to its group sums under `def`. An
    /// entry whose type is neither the debit nor the credit value adds
    /// nothing, as `balanced::check_balanced` ignores it.
    pub(super) fn add_balanced(
        &mut self,
        collection: &str,
        def: &BalancedDef,
        entries: &[BalancedEntry],
    ) -> crate::Result<()> {
        for entry in entries {
            let is_debit = entry.entry_type == def.debit_value;
            if !is_debit && entry.entry_type != def.credit_value {
                continue;
            }
            let key = (collection.to_string(), entry.group_key.clone());
            let (mut debit, mut credit) = match self.balanced.get(&key, self.space.reader())? {
                Some(sums) => balanced_sums(&sums),
                None => (Decimal::default(), Decimal::default()),
            };
            if is_debit {
                debit += entry.amount;
            } else {
                credit += entry.amount;
            }
            let seq = self.next_seq();
            self.balanced.insert(key, encode_sums(debit, credit), seq);
        }
        Ok(())
    }

    /// Record op `op` as the last write of row `surrogate` of `collection`.
    pub(super) fn note_written(&mut self, collection: &str, surrogate: u32, op: u64) {
        let seq = self.next_seq();
        self.written
            .insert((collection.to_string(), surrogate), op, seq);
    }

    /// Record that op `op` claims `value` for row `surrogate` under the
    /// unique path at `path` of `collection`.
    pub(super) fn claim(
        &mut self,
        collection: &str,
        path: u32,
        value: String,
        op: u64,
        surrogate: u32,
    ) {
        let seq = self.next_seq();
        self.claims
            .insert((collection.to_string(), path, value, op), surrogate, seq);
    }

    /// Record a folded target row under `key`.
    pub(super) fn put_fold(&mut self, key: FoldKey, row: FoldRowBytes) {
        let seq = self.next_seq();
        self.folds.insert(key, row, seq);
    }
}

impl Drop for PostState {
    fn drop(&mut self) {
        // A removal that fails files its own faultbox report, and the boot
        // sweep removes the directory at the next open.
        let _removed = remove_spill_dir(SpillKind::Txn, &self.dir);
    }
}

/// The debit and credit sums `sums` holds.
pub(super) fn balanced_sums(sums: &BalancedSums) -> (Decimal, Decimal) {
    let (debit_low, debit_high, credit_low, credit_high) = *sums;
    (
        decimal_of(debit_low, debit_high),
        decimal_of(credit_low, credit_high),
    )
}

fn encode_sums(debit: Decimal, credit: Decimal) -> BalancedSums {
    let (debit_low, debit_high) = decimal_halves(debit);
    let (credit_low, credit_high) = decimal_halves(credit);
    (debit_low, debit_high, credit_low, credit_high)
}

/// The serialized form of `value` as two halves. The form keeps the
/// mantissa, the scale and the sign, so a sum reads back exactly.
fn decimal_halves(value: Decimal) -> (u64, u64) {
    let bits = u128::from_le_bytes(value.serialize());
    (bits as u64, (bits >> 64) as u64)
}

fn decimal_of(low: u64, high: u64) -> Decimal {
    Decimal::deserialize(((u128::from(high) << 64) | u128::from(low)).to_le_bytes())
}

/// The key bytes every key of a tuple map shares when its leading fields
/// are the ones pushed. A tuple key encodes as a MessagePack array.
pub(super) struct KeyPrefix(Vec<u8>);

impl KeyPrefix {
    /// The prefix of a key tuple of `arity` fields, before any field.
    /// `arity` must be below 16: the array header is one byte.
    pub(super) fn tuple(arity: u8) -> Self {
        Self(vec![0x90 | (arity & 0x0f)])
    }

    /// Push the next leading field.
    pub(super) fn then<T: zerompk::ToMessagePack>(mut self, field: &T) -> crate::Result<Self> {
        self.0.extend(encode_key(field)?);
        Ok(self)
    }

    /// The scan bounds of every key with this prefix.
    pub(super) fn bounds(self) -> ScanBounds {
        ScanBounds {
            after: None,
            prefix: Some(self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn def() -> BalancedDef {
        BalancedDef {
            group_key_column: "j".into(),
            entry_type_column: "t".into(),
            debit_value: "D".into(),
            credit_value: "C".into(),
            amount_column: "a".into(),
        }
    }

    fn entry(group: &str, kind: &str, amount: &str) -> BalancedEntry {
        BalancedEntry {
            group_key: group.into(),
            entry_type: kind.into(),
            amount: Decimal::from_str(amount).expect("decimal"),
        }
    }

    #[test]
    fn a_decimal_reads_back_with_its_scale_and_sign() {
        for text in ["100.00", "-0.5", "0", "79228162514264337593543950335"] {
            let value = Decimal::from_str(text).expect("decimal");
            let (low, high) = decimal_halves(value);
            let back = decimal_of(low, high);
            assert_eq!(back, value);
            assert_eq!(back.to_string(), value.to_string());
        }
    }

    #[test]
    fn sums_read_back_after_a_spill() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut post = PostState::new(dir.path().join("post"), 0);
        post.add_balanced("ledger", &def(), &[entry("g1", "D", "10.50")])
            .expect("add");
        post.spill_past_threshold().expect("spill");
        post.add_balanced(
            "ledger",
            &def(),
            &[entry("g1", "D", "1"), entry("g1", "C", "11.5")],
        )
        .expect("add");
        let sums = post
            .balanced
            .get(&("ledger".to_string(), "g1".to_string()), post.reader())
            .expect("get")
            .expect("group");
        let (debit, credit) = balanced_sums(&sums);
        assert_eq!(debit.to_string(), "11.50");
        assert_eq!(credit.to_string(), "11.5");
    }

    #[test]
    fn the_directory_goes_when_the_post_state_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("post");
        {
            let mut post = PostState::new(path.clone(), 0);
            post.note_written("c", 1, 0);
            post.spill_past_threshold().expect("spill");
            assert!(path.exists());
        }
        assert!(!path.exists());
    }
}
