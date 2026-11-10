// SPDX-License-Identifier: BUSL-1.1

//! `SELECT CONVERT_CURRENCY_LOOKUP(amount, from, to, rate_table, as_of, key_column, rate_column, time_column, precision)`
//!
//! Convenience overload: performs TEMPORAL_LOOKUP internally to find the rate,
//! then applies the arithmetic conversion.

use nodedb_sql::parser::query_function::ConvertCurrencyLookupArgs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{err, scan_rows, single_result, value_text, value_to_decimal};

/// Convenience currency conversion with rate table lookup.
///
/// Performs a temporal lookup on `rate_table` to find the latest rate for the
/// currency pair where `time_column <= as_of`, then returns `ROUND(amount * rate, precision)`.
pub async fn convert_currency_lookup(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    args: &ConvertCurrencyLookupArgs,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    let amount_str = args.amount.clone();
    let from_ccy = args.from.clone();
    let to_ccy = args.to.clone();
    let rate_table = args.rate_table.clone();
    let as_of = args.as_of.clone();
    let key_column = args.key_column.clone();
    let rate_column = args.rate_column.clone();
    let time_column = args.time_column.clone();
    let precision: u32 = args
        .precision
        .parse()
        .map_err(|_| err("22023", "precision must be an integer"))?;
    if precision > 38 {
        return Err(err(
            "22023",
            &format!("precision must be <= 38, got {precision}"),
        ));
    }

    let amount: rust_decimal::Decimal = amount_str
        .parse()
        .map_err(|_| err("22023", &format!("cannot parse amount '{amount_str}'")))?;

    // `rate_table` is a caller argument, so the read it names is authorized and
    // row-filtered here. The converted amount is arithmetic over `rate_column`,
    // so a redaction rule on that column is refused rather than answered with a
    // figure derived from a value the caller may not see.
    let gate = CollectionReadGate::open(state, identity, database_id, &rate_table)?;
    gate.require_document_engine(&rate_table, "CONVERT_CURRENCY")?;
    gate.refuse_if_field_redacted(&rate_table, &rate_column, "the converted amount")?;

    // Build the composite key: "{from}/{to}" for the rate table lookup.
    let key_value = format!("{from_ccy}/{to_ccy}");

    // Scan rate table to find latest rate where key_column == key_value AND time_column <= as_of.
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &rate_table).vshard();
    let mut scan_plan = PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(database_id, &rate_table),
        limit: usize::MAX,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });
    gate.inject_rls(&mut scan_plan)?;

    let scan_resp = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        scan_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("rate table scan failed", &e))?;

    // `scan_rows` unwraps the `{"id", "data"}` scan envelope so matching
    // reads the stored fields, typed, not the wire wrapper.
    let docs = scan_rows(&scan_resp.payload)?;

    // Find latest row where key matches and time <= as_of.
    let mut best_rate: Option<rust_decimal::Decimal> = None;
    let mut best_time = String::new();

    for obj in &docs {
        let key_val = obj
            .get(&key_column)
            .and_then(value_text)
            .unwrap_or_default();
        if key_val != key_value {
            continue;
        }
        let time_val = obj
            .get(&time_column)
            .and_then(value_text)
            .unwrap_or_default();
        if time_val.is_empty() || time_val.as_str() > as_of.as_str() {
            continue;
        }
        if time_val > best_time {
            best_time = time_val;
            best_rate = obj.get(&rate_column).and_then(value_to_decimal);
        }
    }

    let Some(rate) = best_rate else {
        return Err(err(
            "02000",
            &format!("no exchange rate found for {from_ccy}/{to_ccy} as of {as_of}"),
        ));
    };

    // Compute: ROUND(amount * rate, precision, HALF_EVEN).
    let converted = amount * rate;
    let rounded = converted.round_dp_with_strategy(
        precision,
        rust_decimal::RoundingStrategy::MidpointNearestEven,
    );

    Ok(single_result(&rounded.to_string()))
}
