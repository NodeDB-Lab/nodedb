// SPDX-License-Identifier: Apache-2.0

//! SELECT query routing and search lowering.

#[cfg(test)]
mod derived_join;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod having_alias;
mod index_prefilter;
#[cfg(test)]
mod join_columns;
#[cfg(test)]
mod lateral_cte;
#[cfg(test)]
mod lateral_temporal;
mod payload;
mod pk_prefilter;
mod query;
mod search;
#[cfg(test)]
mod search_over_relation;

pub use query::{plan_query, plan_statement_query};
