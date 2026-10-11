// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral atomic transfer SQL functions: TRANSFER (fungible) and
//! TRANSFER_ITEM (non-fungible).
//!
//! `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
//!   — Atomically: source.field -= amount, dest.field += amount.
//!   — Fails with INSUFFICIENT_BALANCE if source.field < amount.
//!   — A `DECIMAL` field takes an exact INT or DECIMAL amount and moves by
//!     exact decimal arithmetic. Every other field moves by float arithmetic.
//!   — Returns: `{ source_key, dest_key, field, amount, source_balance, dest_balance }`.
//!
//! `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
//!   — Atomically: remove item from source owner, add to dest owner.
//!   — Fails with NOT_FOUND if source doesn't own the item.
//!   — Returns: `{ item_key, dest_key, source_collection, dest_collection }`.
//!
//! Both dispatch to the Data Plane as dedicated KvOp variants. The entire
//! read-validate-write executes in a single TPC core pass — no TOCTOU race.
//! A `TRANSFER_ITEM` between collections on two vShards runs as one
//! read-dependent Calvin transaction instead (see `transfer_cross_shard`).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan, TransferAmount};
use rust_decimal::Decimal;

use super::super::result::{DdlError, DdlResult};
use super::kv_atomic::{dispatch_and_respond, parse_function_args, unquote};

/// Handle `SELECT TRANSFER(collection, source_key, dest_key, field, amount)`
pub async fn transfer(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "TRANSFER")?;
    if args.len() < 5 {
        return Err(ddl_err(
            "42601",
            "TRANSFER requires 5 arguments: (collection, source_key, dest_key, field, amount)",
        ));
    }

    // A string literal is data: the collection name resolves exactly as
    // written, with no case folding.
    let collection = unquote(&args[0]);
    let source_key = unquote(&args[1]);
    let dest_key = unquote(&args[2]);
    let field = unquote(&args[3]);
    let decimal_field = transfer_field_is_decimal(state, identity, &collection, &field)?;
    let amount = parse_amount(args[4].trim(), &field, decimal_field)?;
    if !amount.is_positive() {
        return Err(ddl_err("42601", "TRANSFER: amount must be positive"));
    }

    let vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &collection).vshard();

    // Dispatch to Data Plane — entire read+validate+write is atomic (single TPC
    // core). Routed through the protocol-neutral in-transaction staging gate
    // (`dispatch_and_respond`, shared with `KV_INCR` et al.): outside a
    // transaction it dispatches immediately, byte-identical to before;
    // inside a `BEGIN..COMMIT` block `KvOp::Transfer` is staged into the
    // per-transaction overlay so a same-transaction read observes both
    // updated balances and COMMIT durably replays the same op.
    // Content-addressed cross-engine identity per key: the debited (source)
    // row and the credited (dest) row each keep the surrogate their original
    // insert assigned. Distinct keys → distinct surrogates, so the two rows
    // never collapse onto one identity.
    let source_bytes = source_key.into_bytes();
    let dest_bytes = dest_key.into_bytes();
    // Both identities resolve in one batch at the collection's home.
    let bound = crate::control::server::surrogate_exchange::assign_surrogates_routed(
        state,
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &collection),
        identity.tenant_id,
        &[source_bytes.as_slice(), dest_bytes.as_slice()],
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;
    let [debit_surrogate, credit_surrogate] = bound[..] else {
        return Err(DdlError::internal(format!(
            "TRANSFER: the home answered {} surrogates for 2 keys",
            bound.len()
        )));
    };
    let plan = PhysicalPlan::Kv(KvOp::Transfer {
        collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, &collection),
        source_key: source_bytes,
        dest_key: dest_bytes,
        field,
        amount,
        debit_surrogate,
        credit_surrogate,
        // Filled by `dispatch_and_respond`, which runs the same RLS injection
        // pass the planner-driven path runs.
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    });

    dispatch_and_respond(
        state,
        identity,
        vshard,
        plan,
        "TRANSFER",
        &[collection.as_str()],
        txn_ctx,
    )
    .await
}

/// Handle `SELECT TRANSFER_ITEM(source_collection, dest_collection, item_id, source_owner, dest_owner)`
pub async fn transfer_item(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql, "TRANSFER_ITEM")?;
    if args.len() < 5 {
        return Err(ddl_err(
            "42601",
            "TRANSFER_ITEM requires 5 arguments: (source_collection, dest_collection, item_id, source_owner, dest_owner)",
        ));
    }

    // A string literal is data: each collection name resolves exactly as
    // written, with no case folding.
    let source_collection = unquote(&args[0]);
    let dest_collection = unquote(&args[1]);
    let item_id = unquote(&args[2]);
    let source_owner = unquote(&args[3]);
    let dest_owner = unquote(&args[4]);

    // A move between collections on two vShards runs as one read-dependent
    // Calvin transaction. A move within one vShard runs on its core.
    let vshard_src =
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &source_collection).vshard();
    let vshard_dst =
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &dest_collection).vshard();
    let cross_shard = vshard_src != vshard_dst;

    let item_key = format!("{source_owner}:{item_id}");
    let dest_key = format!("{dest_owner}:{item_id}");

    // The moved row's identity is content-addressed at its DESTINATION
    // `(dest_collection, dest_key)`, matching the engine write-back.
    let dest_bytes = dest_key.into_bytes();
    let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &dest_collection),
        identity.tenant_id,
        &dest_bytes,
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    // Dispatch to Data Plane — verify + delete + insert is atomic. Routed
    // through the same in-transaction staging gate as `TRANSFER` (see above).
    let plan = PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::DEFAULT,
            &source_collection,
        ),
        dest_collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::DEFAULT,
            &dest_collection,
        ),
        item_key: item_key.into_bytes(),
        dest_key: dest_bytes,
        surrogate,
        // One predicate per side, both filled by `dispatch_and_respond`: the
        // two collections carry independent policies.
        source_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        dest_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    });

    if cross_shard {
        return super::transfer_cross_shard::transfer_item_across_shards(
            state,
            identity,
            txn_ctx,
            plan,
            [source_collection.as_str(), dest_collection.as_str()],
            vshard_src,
        )
        .await;
    }

    dispatch_and_respond(
        state,
        identity,
        vshard_src,
        plan,
        "TRANSFER_ITEM",
        &[source_collection.as_str(), dest_collection.as_str()],
        txn_ctx,
    )
    .await
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Whether the catalog declares `field` of `collection` as `DECIMAL`.
///
/// The caller's grants are checked first, the pair `dispatch_and_respond`
/// checks: a caller refused the collection learns nothing of its columns.
fn transfer_field_is_decimal(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    field: &str,
) -> Result<bool, DdlError> {
    let gate =
        super::read_gate::CollectionReadGate::for_request(state, identity, DatabaseId::DEFAULT);
    gate.authorize(collection)?;
    gate.authorize_permission(
        collection,
        crate::control::security::identity::Permission::Write,
    )?;
    crate::control::planner::sql_plan_convert::kv_transfer_field::kv_transfer_field_is_decimal(
        state,
        identity.tenant_id,
        DatabaseId::DEFAULT,
        collection,
        field,
    )
    .map_err(|error| DdlError::from_error(&error))
}

/// The `TRANSFER` amount literal, typed by the field it moves.
///
/// A `DECIMAL` field takes an exact INT or DECIMAL literal. A number no
/// `Decimal` holds is out of range for it (`22003`). Every other field takes
/// a finite float. Text that is no number is refused with `42601`.
fn parse_amount(text: &str, field: &str, decimal_field: bool) -> Result<TransferAmount, DdlError> {
    let not_a_number = || {
        ddl_err(
            "42601",
            format!("TRANSFER: amount must be a number, got '{text}'"),
        )
    };
    let float = text
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .ok_or_else(not_a_number)?;
    if !decimal_field {
        return Ok(TransferAmount::Float(float));
    }
    let exact = Decimal::from_str_exact(text)
        .or_else(|_| Decimal::from_scientific(text))
        .map_err(|_| {
            ddl_err(
                "22003",
                format!("TRANSFER: amount {text} is out of range for DECIMAL field '{field}'"),
            )
        })?;
    Ok(TransferAmount::Decimal(exact))
}

fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        text.parse().expect("test decimal parses")
    }

    #[test]
    fn a_decimal_field_takes_an_exact_amount() {
        for (text, expected) in [
            ("30", "30"),
            ("0.1", "0.1"),
            ("12.345", "12.345"),
            ("1.5e2", "150"),
        ] {
            assert_eq!(
                parse_amount(text, "balance", true).expect(text),
                TransferAmount::Decimal(dec(expected)),
                "{text}"
            );
        }
    }

    #[test]
    fn a_decimal_field_refuses_an_amount_no_decimal_holds() {
        let err = parse_amount("1e40", "balance", true).expect_err("past the Decimal range");
        assert_eq!(err.sqlstate, "22003");
    }

    #[test]
    fn other_fields_take_a_float_amount() {
        assert_eq!(
            parse_amount("2.5", "balance", false).expect("float"),
            TransferAmount::Float(2.5)
        );
    }

    #[test]
    fn text_that_is_no_finite_number_is_refused() {
        for text in ["abc", "NaN", "inf", ""] {
            for decimal_field in [true, false] {
                let err = parse_amount(text, "balance", decimal_field).expect_err(text);
                assert_eq!(err.sqlstate, "42601", "{text}");
            }
        }
    }
}
