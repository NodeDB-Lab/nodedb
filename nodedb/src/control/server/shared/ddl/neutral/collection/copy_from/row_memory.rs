// SPDX-License-Identifier: BUSL-1.1

//! The reservation one row of a row-format `COPY` holds while it parses and
//! inserts.
//!
//! A load holds one parsed row at a time, so the file's bytes and one row's
//! reservation bound its memory. The bound comes from the row's text: its
//! bytes copy a fixed number of times, and each value costs a fixed size.

use nodedb_mem::{MemError, ReservationToken, ScopedMemory};

use crate::control::server::shared::ddl::result::DdlError;

/// How many times a row's bytes can exist at once: the JSON string copy, the
/// `Value` string copy, and the INSERT text, where escaping can double a byte.
const TEXT_COPIES: usize = 4;

/// Most heap one parsed value takes outside its text: a CSV cell's `String`,
/// and an entry in the `serde_json` map and in the `nodedb_types::Value` map.
/// Each map entry counts twice, since a map holds spare capacity.
const VALUE_BYTES: usize = size_of::<String>()
    + 2 * size_of::<(String, serde_json::Value)>()
    + 2 * size_of::<(String, nodedb_types::Value)>();

/// The most bytes the parsed form of `row` takes. Every value but the first
/// follows a `,`, a `:`, an opening bracket, or the CSV `delimiter`.
fn row_bound(row: &str, delimiter: Option<char>) -> usize {
    let separators = row
        .bytes()
        .filter(|byte| matches!(byte, b',' | b':' | b'{' | b'['))
        .count();
    let delimiters = delimiter.map_or(0, |d| row.matches(d).count());
    let values = separators.saturating_add(delimiters).saturating_add(1);
    row.len()
        .saturating_mul(TEXT_COPIES)
        .saturating_add(values.saturating_mul(VALUE_BYTES))
}

/// Reserve the parsed form of row `row_no` of `path`. The governor's refusal
/// is `53200`, naming the row's size, the limit and the next action.
pub(super) fn reserve_row(
    memory: &ScopedMemory,
    path: &str,
    row_no: usize,
    row: &str,
    delimiter: Option<char>,
) -> Result<ReservationToken, DdlError> {
    let bound = row_bound(row, delimiter);
    memory
        .reserve(bound)
        .map_err(|refusal| over_memory(path, row_no, row.len(), bound, &refusal))
}

fn over_memory(
    path: &str,
    row_no: usize,
    len: usize,
    bound: usize,
    refusal: &MemError,
) -> DdlError {
    DdlError::new(
        "53200",
        format!(
            "COPY: row {row_no} of '{path}' is {len} bytes and takes up to {bound} bytes \
             parsed: {refusal}; split the row, or raise the memory limit"
        ),
    )
}

#[cfg(test)]
pub(super) mod test_support {
    use std::sync::Arc;

    use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
    use nodedb_types::{DatabaseId, TenantId};

    /// A governor that gives every engine `budget` bytes.
    pub(in super::super) fn governor_of(budget: usize) -> Arc<MemoryGovernor> {
        let config = GovernorConfig {
            global_ceiling: budget * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(budget),
        };
        Arc::new(MemoryGovernor::new(config).expect("the ceiling covers every engine"))
    }

    /// A query budget of `budget` bytes.
    pub(in super::super) fn memory_of(budget: usize) -> ScopedMemory {
        ScopedMemory::new(
            governor_of(budget),
            DatabaseId::DEFAULT,
            TenantId::new(1),
            EngineId::Query,
        )
    }
}

#[cfg(test)]
mod tests {
    use nodedb_mem::EngineId;

    use super::test_support::memory_of;
    use super::*;

    #[test]
    fn a_dense_row_costs_more_than_a_blob_of_its_size() {
        let dense = "[0,0,0,0,0,0,0,0]";
        let blob = "\"0000000000000000\"";
        assert!(row_bound(dense, None) > row_bound(blob, None));
    }

    #[test]
    fn a_csv_delimiter_counts_as_a_separator() {
        assert_eq!(
            row_bound("a;b", None) + VALUE_BYTES,
            row_bound("a;b", Some(';'))
        );
    }

    #[test]
    fn a_row_over_the_budget_names_its_size_the_limit_and_the_next_action() {
        let memory = memory_of(4096);
        let row = "x".repeat(8192);
        let error = reserve_row(&memory, "/d/x.csv", 7, &row, None).expect_err("over the budget");
        assert_eq!(error.sqlstate, "53200");
        assert!(error.message.contains("row 7"), "{}", error.message);
        assert!(error.message.contains("8192 bytes"), "{}", error.message);
        assert!(
            error.message.contains("limit: 4096 bytes"),
            "{}",
            error.message
        );
        assert!(error.message.contains("split the row"), "{}", error.message);
    }

    #[test]
    fn a_row_releases_its_reservation_when_the_token_drops() {
        let memory = memory_of(1 << 20);
        let token = reserve_row(&memory, "/d/x.csv", 1, "a,b", None).expect("under the budget");
        let budget = memory.governor().budget(EngineId::Query);
        assert_eq!(budget.allocated(), token.size());
        drop(token);
        assert_eq!(budget.allocated(), 0);
    }
}
