// SPDX-License-Identifier: BUSL-1.1

//! BALANCED of one collection, judged on the group sums a streamed validate
//! folded.
//!
//! `balanced::check_balanced` names the first unbalanced group in
//! group-key byte order. The sums sit in a tiered map whose scan order is
//! the keys' MessagePack order, so the judge scans every group of the
//! collection and keeps the smallest unbalanced key. The verdict is the same
//! whatever the scan order.

use rust_decimal::Decimal;

use crate::data::executor::enforcement::balanced::group_violation;

use super::post_state::{KeyPrefix, PostState, balanced_sums};

impl PostState {
    /// The violation of `collection`'s first unbalanced group, in group-key
    /// byte order. `Err` when a spilled run does not read: a failure of
    /// this node.
    pub(super) fn balanced_violation(
        &self,
        collection: &str,
    ) -> crate::Result<Option<crate::Error>> {
        let bounds = KeyPrefix::tuple(2).then(&collection)?.bounds();
        let mut first: Option<(String, Decimal, Decimal)> = None;
        for item in self.balanced.scan(bounds, self.reader()) {
            let ((_, group), sums) = item?;
            let (debit, credit) = balanced_sums(&sums);
            if debit == credit {
                continue;
            }
            if first.as_ref().is_none_or(|(kept, ..)| group < *kept) {
                first = Some((group, debit, credit));
            }
        }
        Ok(first
            .and_then(|(group, debit, credit)| group_violation(collection, &group, debit, credit)))
    }
}
