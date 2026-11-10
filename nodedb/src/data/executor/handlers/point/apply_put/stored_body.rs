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
use crate::data::executor::strict_format;
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
            let mut body = self.kept_stored_body(config_key, stored)?;
            let schema = self.capture_storage_schema(config_key)?;
            body.value = super::installed_schema::canonical_stored(
                &body.stored,
                &schema,
                Some(crate::bridge::envelope::RowVersion {
                    sys_from,
                    valid_from_ms,
                    valid_until_ms,
                }),
            )?;
            return Ok(body);
        }
        let frozen = self.freeze_document_body(super::frozen_body::FreezeDocumentBodyInput {
            config_key,
            surrogate,
            value,
            guards,
        })?;
        self.complete_frozen_body(&frozen, bitemporal, sys_from, valid_from_ms, valid_until_ms)
    }

    pub(super) fn complete_frozen_body(
        &self,
        frozen: &super::frozen_body::FrozenDocumentBody,
        bitemporal: bool,
        sys_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> crate::Result<StoredBody> {
        let config_key = &frozen.config_key;
        frozen.check(self, config_key, frozen.surrogate)?;
        let value = frozen.value.clone();
        let Some(config) = self.doc_configs.get(config_key) else {
            return Ok(StoredBody {
                stored: value.clone(),
                value,
            });
        };
        let link_after = match self
            .chain
            .intents
            .get(&(config_key.clone(), frozen.surrogate.as_u32()))
        {
            Some(ChainIntent::Link { head }) => {
                let current = self
                    .chain
                    .heads
                    .get(config_key)
                    .cloned()
                    .unwrap_or_else(crate::types::hash_chain::ChainHead::genesis);
                if *head != current {
                    return Err(crate::Error::DataPlane(
                        crate::bridge::envelope::ErrorCode::ConflictRetry,
                    ));
                }
                Some(head)
            }
            Some(ChainIntent::Keep { .. }) => {
                return Err(crate::Error::DataPlane(
                    crate::bridge::envelope::ErrorCode::ConflictRetry,
                ));
            }
            None => None,
        };
        let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
            config.storage_mode
        else {
            let value = match link_after {
                Some(head) => self.link_value(config, &value, head, &|body| Ok(body.to_vec()))?,
                None => value,
            };
            return Ok(StoredBody {
                stored: value.clone(),
                value,
            });
        };

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

        let schema = self.capture_storage_schema(config_key)?;
        let value = super::installed_schema::canonical_stored(
            &stored,
            &schema,
            Some(crate::bridge::envelope::RowVersion {
                sys_from,
                valid_from_ms,
                valid_until_ms,
            }),
        )?;
        Ok(StoredBody { value, stored })
    }
}
