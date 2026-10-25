// SPDX-License-Identifier: BUSL-1.1

//! Turning an incoming document body into the bytes storage will hold. Its
//! own concern because two callers need the same answer:
//! [`CoreLoop::apply_point_put`](super::core), which writes it, and the
//! governed-write RESOLVE pass, which reports what a write WOULD store —
//! deriving the image twice is how the two disagree. Pure: no store, no
//! index, no transaction.

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::chain_guard::ChainIntent;
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::handlers::write_body;
use crate::data::executor::{doc_format, strict_format};
use crate::types::{DatabaseId, TenantId};

/// Inputs to [`CoreLoop::build_stored_body`].
pub(in crate::data::executor) struct StoredBodyInput<'a> {
    pub config_key: &'a (DatabaseId, TenantId, String),
    pub surrogate: Surrogate,
    /// The incoming MessagePack body, for both storage modes.
    pub value: &'a [u8],
    pub bitemporal: bool,
    /// The version's system-time ordinal, stamped into a bitemporal strict
    /// tuple.
    pub sys_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    /// Which fields this write puts in scope for typeguard validation.
    pub guards: GuardScope<'a>,
}

/// The two forms of one row this write lands.
pub(in crate::data::executor) struct StoredBody {
    /// Canonical MessagePack body — generated columns evaluated and `_rowid`
    /// injected. Downstream indexing reads THIS, so it sees the injected
    /// `_rowid` alongside the user's fields.
    pub value: Vec<u8>,
    /// The exact bytes handed to storage: a Binary Tuple on a strict
    /// collection, the canonical MessagePack otherwise.
    pub stored: Vec<u8>,
}

impl CoreLoop {
    /// Build the canonical body and the stored image for one incoming write.
    /// A body with no readable fields skips generated-column evaluation and
    /// stores as supplied; the strict encode below still rejects it.
    pub(in crate::data::executor) fn build_stored_body(
        &self,
        input: StoredBodyInput<'_>,
    ) -> crate::Result<StoredBody> {
        let StoredBodyInput {
            config_key,
            surrogate,
            value,
            bitemporal,
            sys_from,
            valid_from_ms,
            valid_until_ms,
            guards,
        } = input;

        // A HASH_CHAIN row a write re-applies keeps its durable bytes, so no
        // expression is evaluated a second time.
        let intent = self
            .chain
            .intents
            .get(&(config_key.clone(), surrogate.as_u32()));
        if let Some(ChainIntent::Keep { stored }) = intent {
            return self.kept_stored_body(config_key, stored);
        }
        let link_after = match intent {
            Some(ChainIntent::Link { head }) => Some(head),
            _ => None,
        };

        // Apply typeguards and generated columns before encoding. A refused
        // write keeps its own error code: a typeguard violation, or the
        // evaluation error of a generated expression.
        let value = if let Some(config) = self.doc_configs.get(config_key)
            && write_body::rewrites_body(config)
        {
            if let Ok(mut doc) = doc_format::decode_document(value) {
                write_body::materialize_write_body(config, &mut doc, guards, &self.crs_context())
                    .map_err(crate::Error::DataPlane)?;
                doc_format::encode_to_msgpack(&doc)
            } else {
                value.to_vec()
            }
        } else {
            doc_format::canonicalize_document_for_storage(value)
        };

        // A declared numeric column holds the value its type stores. Runs
        // after the generated columns, so a generated value is re-typed too.
        let value = strict_format::coerce_declared_body(value, self.declared_columns(config_key))?;

        // Strict (Binary Tuple) pipeline: inject an auto-generated `_rowid`
        // from the surrogate if the schema declares one and the client
        // payload lacks it, then encode into Binary Tuple.
        let Some(config) = self.doc_configs.get(config_key) else {
            return Ok(StoredBody {
                stored: value.clone(),
                value,
            });
        };
        let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
            config.storage_mode
        else {
            let value = match link_after {
                Some(head) => {
                    // A chained row carries its identity inside the linked
                    // contents: a minted row gets its identity column from its
                    // surrogate here, as a strict row gets `_rowid` below. A
                    // copy under a new surrogate then keeps both its identity
                    // and its link. A declared-key row holds its key and gains
                    // no `id`. A body that is not an object is left for the
                    // link to refuse.
                    let value = if nodedb_query::msgpack_scan::map_header(&value, 0).is_some() {
                        nodedb_query::msgpack_scan::inject_str_field(
                            &value,
                            config
                                .declared_key
                                .as_deref()
                                .unwrap_or(nodedb_types::DEFAULT_IDENTITY_COLUMN),
                            &surrogate.as_u32().to_string(),
                        )
                    } else {
                        value
                    };
                    let encode = |body: &[u8]| -> crate::Result<Vec<u8>> { Ok(body.to_vec()) };
                    self.link_value(config, &value, head, &encode)?
                }
                None => value,
            };
            return Ok(StoredBody {
                stored: value.clone(),
                value,
            });
        };

        let value_with_rowid: Option<Vec<u8>> = if schema
            .columns
            .first()
            .is_some_and(|c| c.name == "_rowid" && !c.nullable)
            && let Ok(mut decoded) = nodedb_types::json_from_msgpack(&value)
            && let serde_json::Value::Object(ref mut obj) = decoded
            && !obj.contains_key("_rowid")
        {
            obj.insert(
                "_rowid".to_string(),
                serde_json::Value::Number((surrogate.0 as i64).into()),
            );
            Some(nodedb_types::json_to_msgpack(&decoded).unwrap_or_else(|_| value.clone()))
        } else {
            None
        };
        let value = value_with_rowid.unwrap_or(value);

        let collection = &config_key.2;
        let encode = |body: &[u8]| {
            if bitemporal && schema.bitemporal {
                strict_format::bytes_to_binary_tuple_bitemporal(
                    body,
                    schema,
                    sys_from,
                    valid_from_ms,
                    valid_until_ms,
                    collection,
                )
            } else {
                strict_format::bytes_to_binary_tuple(body, schema, collection)
            }
        };
        let value = match link_after {
            Some(head) => self.link_value(config, &value, head, &encode)?,
            None => value,
        };
        let stored = encode(&value)?;

        Ok(StoredBody { value, stored })
    }
}
