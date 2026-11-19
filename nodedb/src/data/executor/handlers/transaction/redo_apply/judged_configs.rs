// SPDX-License-Identifier: BUSL-1.1

//! The configs a pinned record's validate judges under.
//!
//! A pin's raw bytes must hash to its digest before the view trusts the
//! pin. Every replica refuses a malformed pin, whatever config it holds.
//! Each pin hashes at most once per record.
//!
//! A pin whose digest equals the core's current config digest then reads
//! the current config and decodes nothing. Any other pin decodes once, on
//! its first lookup, and the view keeps the decoded config for the record.
//! A digest of 0 never matches, so a config built outside a `Register`
//! always decodes the pin.
//!
//! A decoded config judges by the pinned rules but reads stored rows in
//! the core's current storage mode: the stored bytes are in that mode.
//!
//! Row bodies derive under a second decoded config, the install config, in
//! the validate and the install alike. It takes the pin's storage mode when
//! its kind is the core's current kind.
//! Otherwise it takes the current mode: a CONVERT applies in data-log order.

use std::cell::OnceCell;
use std::mem::size_of;
use std::sync::Arc;

use nodedb_crs::CustomCrsSet;
use nodedb_physical::physical_plan::{ConfigPin, CrsPin, RedoPins, StorageMode, pin_digest};

use crate::data::executor::core_loop::doc_config_key::DocConfigRef;
use crate::data::executor::core_loop::state_types::DocConfigs;
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, TenantId};

use super::config_pins::{
    ConfigPinError, check_config_pin_hash, check_crs_pin_hash, judged_config_from_pin,
};

/// The judged view of one pinned record.
pub(in crate::data::executor) struct JudgedConfigs {
    pins: RedoPins,
    /// The decoded config of each pin in `pins.configs`, at its index.
    decoded: Vec<OnceCell<CollectionConfig>>,
    /// The decoded config each pin's install derives row bodies under, at
    /// its index.
    installed: Vec<OnceCell<CollectionConfig>>,
    /// The hash of each pin's raw wire bytes, at its index, once a lookup
    /// read it.
    hashed: Vec<OnceCell<u64>>,
    /// The CRS set the record judges under: the core's current set when
    /// the pin matches it, else the pin's decoded set.
    crs: Arc<CustomCrsSet>,
    /// Whether `crs` is the pin's decoded set.
    crs_decoded: bool,
    /// The first pin that did not decode. The validate refuses with it.
    failed: OnceCell<ConfigPinError>,
}

impl JudgedConfigs {
    /// The view of a record pinned with `pins`. The core's CRS set is
    /// `current_crs`, with digest `current_crs_digest`. `None` for a record
    /// with no pins. `Err` when the CRS pin does not decode.
    pub(in crate::data::executor) fn open(
        pins: &RedoPins,
        current_crs: &Arc<CustomCrsSet>,
        current_crs_digest: u64,
    ) -> Result<Option<Self>, ConfigPinError> {
        let Some(crs_pin) = pins.crs.as_ref() else {
            return Ok(None);
        };
        check_crs_pin_hash(crs_pin, pin_digest(crs_pin.bytes.as_slice()))?;
        let (crs, crs_decoded) = if current_crs_digest != 0 && current_crs_digest == crs_pin.digest
        {
            (Arc::clone(current_crs), false)
        } else {
            (Arc::new(decode_crs(crs_pin)?), true)
        };
        Ok(Some(Self {
            pins: pins.clone(),
            decoded: pins.configs.iter().map(|_| OnceCell::new()).collect(),
            installed: pins.configs.iter().map(|_| OnceCell::new()).collect(),
            hashed: pins.configs.iter().map(|_| OnceCell::new()).collect(),
            crs,
            crs_decoded,
            failed: OnceCell::new(),
        }))
    }

    /// The config the validate reads for `collection`, of `database_id` and
    /// `tenant_id`. A collection with no pin reads its current config.
    pub(in crate::data::executor) fn config<'a>(
        &'a self,
        doc_configs: &'a DocConfigs,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Option<&'a CollectionConfig> {
        let current = doc_configs.get(&DocConfigRef {
            database_id,
            tenant_id,
            collection,
        });
        let Some(index) = self.pins.position_named(database_id, tenant_id, collection) else {
            return current;
        };
        if let Some(decoded) = self.decoded[index].get() {
            return Some(decoded);
        }
        let pin = &self.pins.configs[index];
        let derived = *self.hashed[index].get_or_init(|| pin_digest(pin.wire.as_slice()));
        if let Err(error) = check_config_pin_hash(pin, derived) {
            self.refuse(error);
            return current;
        }
        if let Some(config) = current
            && config.digest != 0
            && config.digest == pin.digest
        {
            return Some(config);
        }
        match judged_for_validate(current, pin, derived) {
            Ok(config) => Some(self.decoded[index].get_or_init(|| config)),
            Err(error) => {
                self.refuse(error);
                current
            }
        }
    }

    /// The install config: the one `collection`'s row bodies derive under. A
    /// collection with no pin reads its current config. `Err` when its pin
    /// does not decode.
    pub(in crate::data::executor) fn install_config<'a>(
        &'a self,
        doc_configs: &'a DocConfigs,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Result<Option<&'a CollectionConfig>, ConfigPinError> {
        let current = doc_configs.get(&DocConfigRef {
            database_id,
            tenant_id,
            collection,
        });
        match self.pins.position_named(database_id, tenant_id, collection) {
            Some(index) => self.install_config_at(index, current),
            None => Ok(current),
        }
    }

    /// The install config of the pin at `index`, over `current`, the
    /// core's config of the pin's key.
    fn install_config_at<'a>(
        &'a self,
        index: usize,
        current: Option<&'a CollectionConfig>,
    ) -> Result<Option<&'a CollectionConfig>, ConfigPinError> {
        if let Some(installed) = self.installed[index].get() {
            return Ok(Some(installed));
        }
        let pin = &self.pins.configs[index];
        let derived = *self.hashed[index].get_or_init(|| pin_digest(pin.wire.as_slice()));
        check_config_pin_hash(pin, derived)?;
        if let Some(config) = current
            && config.digest != 0
            && config.digest == pin.digest
        {
            return Ok(Some(config));
        }
        let config = judged_for_install(current, pin, derived)?;
        Ok(Some(self.installed[index].get_or_init(|| config)))
    }

    /// Derive the install config of every pin. A pin that does not decode
    /// is kept as the refusal, so the validate refuses before any write.
    pub(in crate::data::executor) fn prime_install(&self, doc_configs: &DocConfigs) {
        for (index, pin) in self.pins.configs.iter().enumerate() {
            let current = doc_configs.get(&DocConfigRef {
                database_id: pin.key.database_id,
                tenant_id: pin.key.tenant_id,
                collection: &pin.key.collection,
            });
            if let Err(error) = self.install_config_at(index, current) {
                self.refuse(error);
            }
        }
    }

    /// Keep `error` as the record's refusal, unless one is kept already.
    /// The validate refuses at its end. Until then its checks read the
    /// current config, and their verdict is discarded.
    fn refuse(&self, error: ConfigPinError) {
        self.failed.get_or_init(|| error);
    }

    /// The CRS set the record judges under.
    pub(in crate::data::executor) fn crs(&self) -> &Arc<CustomCrsSet> {
        &self.crs
    }

    /// The first pin that did not decode, if one did not.
    pub(in crate::data::executor) fn take_failure(&mut self) -> Option<ConfigPinError> {
        self.failed.take()
    }

    /// The RAM the view holds: each pin once, as its wire bytes, plus the
    /// estimate of each config and CRS set it decoded.
    pub(in crate::data::executor) fn resident_bytes(&self) -> usize {
        let cell_bytes = |cell: &OnceCell<CollectionConfig>, wire_len: usize| {
            cell.get()
                .map_or(0, |config| decoded_config_bytes(config, wire_len))
        };
        let configs: usize = self
            .pins
            .configs
            .iter()
            .zip(self.decoded.iter().zip(&self.installed))
            .map(|(pin, (decoded, installed))| {
                size_of::<Arc<ConfigPin>>()
                    + size_of::<ConfigPin>()
                    + pin.key.collection.len()
                    + pin.wire.len()
                    + 2 * size_of::<OnceCell<CollectionConfig>>()
                    + size_of::<OnceCell<u64>>()
                    + cell_bytes(decoded, pin.wire.len())
                    + cell_bytes(installed, pin.wire.len())
            })
            .sum();
        let crs = self.pins.crs.as_ref().map_or(0, |pin| {
            let decoded = if self.crs_decoded {
                size_of::<CustomCrsSet>() + pin.bytes.len()
            } else {
                0
            };
            size_of::<CrsPin>() + pin.bytes.len() + decoded
        });
        size_of::<Self>() + configs + crs
    }

    /// How many configs the view decoded.
    #[cfg(test)]
    pub(in crate::data::executor) fn decoded_configs(&self) -> usize {
        self.decoded
            .iter()
            .filter(|cell| cell.get().is_some())
            .count()
    }

    /// How many install configs the view decoded.
    #[cfg(test)]
    pub(in crate::data::executor) fn installed_configs(&self) -> usize {
        self.installed
            .iter()
            .filter(|cell| cell.get().is_some())
            .count()
    }
}

/// The estimate of a decoded config whose pin carried `wire_len` bytes.
/// It sums the config's size, the wire bytes its strings came from, and
/// the heap slots of every list it holds.
fn decoded_config_bytes(config: &CollectionConfig, wire_len: usize) -> usize {
    fn slots<T>(list: &[T], capacity: usize) -> usize {
        capacity.max(list.len()) * size_of::<T>()
    }
    let enforcement = &config.enforcement;
    let guards: usize = config
        .type_guards
        .iter()
        .map(|guard| guard.collection.len() + guard.field.len() + guard.type_text.len())
        .sum();
    let index_names: usize = config
        .index_paths
        .iter()
        .map(|path| path.name.len() + path.path.len())
        .sum();
    size_of::<CollectionConfig>()
        + wire_len
        + config.name.len()
        + slots(&config.type_guards, config.type_guards.capacity())
        + guards
        + slots(&config.index_paths, config.index_paths.capacity())
        + index_names
        + slots(&config.declared_columns, config.declared_columns.capacity())
        + slots(&config.vector_fields, config.vector_fields.capacity())
        + slots(
            &enforcement.state_constraints,
            enforcement.state_constraints.capacity(),
        )
        + slots(
            &enforcement.transition_checks,
            enforcement.transition_checks.capacity(),
        )
        + slots(
            &enforcement.materialized_sum_sources,
            enforcement.materialized_sum_sources.capacity(),
        )
        + slots(
            &enforcement.generated_columns,
            enforcement.generated_columns.capacity(),
        )
        + slots(&enforcement.type_guards, enforcement.type_guards.capacity())
}

/// The config `pin` fixes over `current`, reading stored rows in the
/// current storage mode.
fn judged_for_validate(
    current: Option<&CollectionConfig>,
    pin: &ConfigPin,
    derived: u64,
) -> Result<CollectionConfig, ConfigPinError> {
    let mut judged = judged_config_from_pin(current, pin, derived)?;
    if let Some(current) = current {
        judged.storage_mode = current.storage_mode.clone();
    }
    Ok(judged)
}

/// The config an install derives row bodies under: `pin` over `current`.
/// It keeps the pin's storage mode only when its kind is the current kind.
/// A core with no current config holds schemaless rows.
fn judged_for_install(
    current: Option<&CollectionConfig>,
    pin: &ConfigPin,
    derived: u64,
) -> Result<CollectionConfig, ConfigPinError> {
    let mut judged = judged_config_from_pin(current, pin, derived)?;
    let current_mode = current.map(|config| &config.storage_mode);
    let pinned_kind = std::mem::discriminant(&judged.storage_mode);
    let same_kind = match current_mode {
        Some(mode) => std::mem::discriminant(mode) == pinned_kind,
        None => matches!(judged.storage_mode, StorageMode::Schemaless),
    };
    if !same_kind {
        judged.storage_mode = current_mode.cloned().unwrap_or_default();
    }
    Ok(judged)
}

/// The CRS set `pin` carries. Its bytes passed [`check_crs_pin_hash`].
fn decode_crs(pin: &CrsPin) -> Result<CustomCrsSet, ConfigPinError> {
    zerompk::from_msgpack(pin.bytes.as_slice()).map_err(|e| ConfigPinError::CrsUndecodable {
        detail: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        ConfigPinKey, DocConfigWire, EnforcementOptions, StorageMode,
    };

    use super::*;

    const COLLECTION: &str = "orders";

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(1),
            COLLECTION.to_string(),
        )
    }

    fn pin() -> ConfigPin {
        pin_with(StorageMode::Schemaless)
    }

    fn pin_with(storage_mode: StorageMode) -> ConfigPin {
        let wire = DocConfigWire {
            bitemporal: true,
            crdt_enabled: false,
            enforcement: EnforcementOptions::default(),
            storage_mode,
            index_paths: Vec::new(),
            declared_key: None,
            vector_primary: None,
            declared_columns: Vec::new(),
            vector_fields: Vec::new(),
        };
        let (bytes, digest) = wire.pin_parts().expect("pin parts");
        ConfigPin {
            key: ConfigPinKey {
                database_id: DatabaseId::DEFAULT,
                tenant_id: TenantId::new(1),
                collection: COLLECTION.into(),
            },
            digest,
            wire: bytes.into(),
        }
    }

    fn view(pin: ConfigPin) -> JudgedConfigs {
        let set = Arc::new(CustomCrsSet::new());
        let bytes = zerompk::to_msgpack_vec(&*set).expect("encode set");
        let digest = pin_digest(&bytes);
        let pins = RedoPins {
            configs: vec![Arc::new(pin)],
            crs: Some(Arc::new(CrsPin {
                digest,
                bytes: bytes.into(),
            })),
        };
        JudgedConfigs::open(&pins, &set, digest)
            .expect("open")
            .expect("a pinned record opens a view")
    }

    fn lookup<'a>(
        view: &'a JudgedConfigs,
        configs: &'a DocConfigs,
    ) -> Option<&'a CollectionConfig> {
        view.config(configs, DatabaseId::DEFAULT, TenantId::new(1), COLLECTION)
    }

    /// A current config whose digest equals the pin's is the judged
    /// config: nothing decodes.
    #[test]
    fn a_matching_digest_decodes_nothing() {
        let pin = pin();
        let mut current = CollectionConfig::new(COLLECTION).with_bitemporal(true);
        current.digest = pin.digest;
        let mut configs = DocConfigs::new();
        configs.insert(key(), current.clone());
        let view = view(pin);
        assert_eq!(lookup(&view, &configs), Some(&current));
        assert_eq!(view.decoded_configs(), 0);
        assert!(!view.crs_decoded, "the matching CRS set decodes nothing");
    }

    /// A 0 digest never takes the fast path, even when the pin's digest is
    /// 0: the pin decodes, once.
    #[test]
    fn a_zero_digest_always_decodes() {
        let mut pin = pin();
        let mut configs = DocConfigs::new();
        configs.insert(key(), CollectionConfig::new(COLLECTION));
        let judged = view(pin.clone());
        let config = lookup(&judged, &configs).expect("judged config");
        assert!(config.bitemporal, "the pinned flag");
        assert_eq!(judged.decoded_configs(), 1);
        lookup(&judged, &configs).expect("judged config");
        assert_eq!(judged.decoded_configs(), 1, "the decode is kept");

        // A pin that claims digest 0 against a current digest of 0 still
        // decodes, and its digest refuses.
        pin.digest = 0;
        let mut zero = view(pin);
        assert!(!lookup(&zero, &configs).is_some_and(|config| config.bitemporal));
        assert!(matches!(
            zero.take_failure(),
            Some(ConfigPinError::DigestMismatch { .. })
        ));
    }

    /// A core with no config of the pinned collection decodes the pin.
    #[test]
    fn a_missing_current_config_decodes_the_pin() {
        let configs = DocConfigs::new();
        let judged = view(pin());
        assert!(lookup(&judged, &configs).is_some_and(|config| config.bitemporal));
        assert_eq!(judged.decoded_configs(), 1);
    }

    /// A decoded config reads stored rows in the core's current storage
    /// mode.
    #[test]
    fn a_decoded_config_keeps_the_current_storage_mode() {
        let schema = nodedb_types::columnar::StrictSchema::new(vec![
            nodedb_types::columnar::ColumnDef::required(
                "id",
                nodedb_types::columnar::ColumnType::String,
            ),
        ])
        .expect("schema");
        let current =
            CollectionConfig::new(COLLECTION).with_storage_mode(StorageMode::Strict { schema });
        let mut configs = DocConfigs::new();
        configs.insert(key(), current.clone());
        let judged = view(pin());
        let config = lookup(&judged, &configs).expect("judged config");
        assert!(config.bitemporal);
        assert_eq!(config.storage_mode, current.storage_mode);
    }

    fn strict(columns: &[&str]) -> StorageMode {
        let columns = columns
            .iter()
            .map(|name| {
                nodedb_types::columnar::ColumnDef::nullable(
                    *name,
                    nodedb_types::columnar::ColumnType::String,
                )
            })
            .collect();
        StorageMode::Strict {
            schema: nodedb_types::columnar::StrictSchema::new(columns).expect("schema"),
        }
    }

    fn install_lookup<'a>(
        view: &'a JudgedConfigs,
        configs: &'a DocConfigs,
    ) -> Option<&'a CollectionConfig> {
        view.install_config(configs, DatabaseId::DEFAULT, TenantId::new(1), COLLECTION)
            .expect("install config")
    }

    /// The install config keeps the pin's storage mode only when its kind
    /// is the core's current kind. The validate config keeps the current
    /// mode always.
    #[test]
    fn the_install_config_takes_the_pinned_mode_of_the_current_kind() {
        let current_mode = strict(&["a", "b"]);
        let mut configs = DocConfigs::new();
        configs.insert(
            key(),
            CollectionConfig::new(COLLECTION).with_storage_mode(current_mode.clone()),
        );

        let pinned_mode = strict(&["a"]);
        let same_kind = view(pin_with(pinned_mode.clone()));
        let installed = install_lookup(&same_kind, &configs).expect("install config");
        assert_eq!(installed.storage_mode, pinned_mode);
        assert!(installed.bitemporal, "the pinned flag");
        let judged = lookup(&same_kind, &configs).expect("judged config");
        assert_eq!(judged.storage_mode, current_mode);
        assert_eq!(same_kind.installed_configs(), 1);

        let other_kind = view(pin());
        let installed = install_lookup(&other_kind, &configs).expect("install config");
        assert_eq!(installed.storage_mode, current_mode);

        let unregistered = view(pin_with(pinned_mode));
        let none = DocConfigs::new();
        let installed = install_lookup(&unregistered, &none).expect("install config");
        assert_eq!(installed.storage_mode, StorageMode::Schemaless);
    }

    /// Priming decodes every install config, and a malformed pin is kept
    /// as the refusal.
    #[test]
    fn priming_refuses_a_malformed_pin() {
        let configs = DocConfigs::new();
        let primed = view(pin());
        primed.prime_install(&configs);
        assert_eq!(primed.installed_configs(), 1);

        let mut bad = pin();
        bad.wire = vec![0xc1].into();
        let mut refused = view(bad);
        refused.prime_install(&configs);
        assert!(matches!(
            refused.take_failure(),
            Some(ConfigPinError::DigestMismatch { .. })
        ));
    }

    /// A CRS pin that does not hash to its digest refuses the view.
    #[test]
    fn a_crs_pin_that_does_not_match_its_bytes_is_refused() {
        let set = Arc::new(CustomCrsSet::new());
        let pins = RedoPins {
            configs: Vec::new(),
            crs: Some(Arc::new(CrsPin {
                digest: 7,
                bytes: vec![0x90].into(),
            })),
        };
        assert!(matches!(
            JudgedConfigs::open(&pins, &set, 0),
            Err(ConfigPinError::CrsDigestMismatch { .. })
        ));
        assert!(
            JudgedConfigs::open(&RedoPins::default(), &set, 0)
                .expect("open")
                .is_none()
        );
    }

    /// A pin whose bytes do not hash to its digest is refused, even when
    /// its digest equals the core's current digest. Nothing decodes, and
    /// the bytes hash once.
    #[test]
    fn a_malformed_pin_matching_the_current_digest_is_refused() {
        let mut pin = pin();
        pin.wire = vec![0xc1].into();
        let mut current = CollectionConfig::new(COLLECTION);
        current.digest = pin.digest;
        let mut configs = DocConfigs::new();
        configs.insert(key(), current);
        let mut judged = view(pin);
        lookup(&judged, &configs).expect("current config");
        lookup(&judged, &configs).expect("current config");
        assert_eq!(judged.decoded_configs(), 0);
        assert_eq!(
            judged
                .hashed
                .iter()
                .filter(|cell| cell.get().is_some())
                .count(),
            1
        );
        let refused = judged.take_failure().expect("refused");
        assert!(matches!(refused, ConfigPinError::DigestMismatch { .. }));
        assert!(matches!(
            crate::bridge::envelope::ErrorCode::from(refused),
            crate::bridge::envelope::ErrorCode::RejectedPrevalidation { .. }
        ));
    }

    /// A CRS pin whose bytes do not hash to its digest is refused, even
    /// when its digest equals the core's current set digest.
    #[test]
    fn a_malformed_crs_pin_matching_the_current_digest_is_refused() {
        let set = Arc::new(CustomCrsSet::new());
        let pins = RedoPins {
            configs: Vec::new(),
            crs: Some(Arc::new(CrsPin {
                digest: 7,
                bytes: vec![0x90].into(),
            })),
        };
        assert!(matches!(
            JudgedConfigs::open(&pins, &set, 7),
            Err(ConfigPinError::CrsDigestMismatch { .. })
        ));
    }

    /// The pin's canonical bytes with the outer header widened: a fixed
    /// array or map header becomes its 16-bit form. The bytes decode to the
    /// same wire form, whose canonical digest is the pin's.
    fn non_canonical(pin: &ConfigPin) -> Vec<u8> {
        let bytes = pin.wire.as_slice();
        let (marker, count) = match bytes.first().copied() {
            Some(byte @ 0x90..=0x9f) => (0xdc, byte & 0x0f),
            Some(byte @ 0x80..=0x8f) => (0xde, byte & 0x0f),
            other => panic!("the wire form opens with a fixed array or map: {other:?}"),
        };
        let mut widened = vec![marker, 0, count];
        widened.extend_from_slice(&bytes[1..]);
        widened
    }

    /// Bytes that are not canonical refuse on the decode path and on the
    /// fast path alike, though they decode to a config with the pin's
    /// canonical digest.
    #[test]
    fn non_canonical_bytes_refuse_on_every_path() {
        let canonical = pin();
        let widened = non_canonical(&canonical);
        let decoded: DocConfigWire = zerompk::from_msgpack(&widened).expect("decode");
        assert_eq!(decoded.digest().expect("digest"), canonical.digest);
        let mut pin = canonical;
        pin.wire = widened.into();

        // The decode path: the core holds no config of the collection.
        let configs = DocConfigs::new();
        let mut judged = view(pin.clone());
        lookup(&judged, &configs);
        assert_eq!(judged.decoded_configs(), 0, "nothing decodes");
        assert!(matches!(
            judged.take_failure(),
            Some(ConfigPinError::DigestMismatch { .. })
        ));

        // The fast path: the core's current digest is the pin's.
        let mut current = CollectionConfig::new(COLLECTION);
        current.digest = pin.digest;
        let mut configs = DocConfigs::new();
        configs.insert(key(), current);
        let mut judged = view(pin);
        lookup(&judged, &configs);
        assert!(matches!(
            judged.take_failure(),
            Some(ConfigPinError::DigestMismatch { .. })
        ));
    }
}
