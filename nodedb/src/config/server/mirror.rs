// SPDX-License-Identifier: BUSL-1.1

//! `[mirror]`: this cluster as a mirror's source, and as a mirror of others.
//!
//! Example TOML:
//! ```toml
//! [mirror]
//! cluster_name = "prod-us"
//! listen = "0.0.0.0:9500"
//!
//! [mirror.tls]
//! cert = "/etc/nodedb/mirror/node.pem"
//! key = "/etc/nodedb/mirror/node.key"
//! ca = "/etc/nodedb/mirror/clients-ca.pem"
//!
//! [mirror.clients.dr-eu]
//! spki = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
//! databases = ["orders"]
//!
//! [mirror.sources.prod-us]
//! seeds = ["10.0.0.1:9500", "10.0.0.2:9500"]
//! server_name = "nodedb"
//! tls = { cert = "/etc/nodedb/mirror/client.pem", key = "/etc/nodedb/mirror/client.key", ca = "/etc/nodedb/mirror/source-ca.pem" }
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use nodedb_cluster::mirror::MirrorClient;
use serde::{Deserialize, Serialize};

/// The mirror link's certificate, key and peer CA. This CA is the peer
/// cluster's alone, pinned per link.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorTlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

/// One client the source lets follow its databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorClientSettings {
    /// Hex SHA-256 of the client certificate's SubjectPublicKeyInfo.
    pub spki: String,
    /// The source databases the client can follow, by name.
    pub databases: Vec<String>,
}

/// One source this cluster can mirror from.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSourceSettings {
    /// The source's mirror listeners. List every replica's: a fetch moves to
    /// the next one before it reports the log truncated.
    pub seeds: Vec<SocketAddr>,
    /// The name every source certificate carries.
    pub server_name: String,
    pub tls: MirrorTlsPaths,
}

/// The `[mirror]` section.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSettings {
    /// The name mirrors know this cluster by. A hello naming another is refused.
    #[serde(default)]
    pub cluster_name: Option<String>,
    /// The mirror listener's bind address. `None` serves no mirror.
    #[serde(default)]
    pub listen: Option<SocketAddr>,
    /// The listener's identity and the CA of its clients. This listener has no
    /// insecure mode: its allowlist names each client by certificate key.
    #[serde(default)]
    pub tls: Option<MirrorTlsPaths>,
    /// Every client the listener admits, by name.
    #[serde(default)]
    pub clients: BTreeMap<String, MirrorClientSettings>,
    /// Every source this cluster can mirror, by name.
    #[serde(default)]
    pub sources: BTreeMap<String, MirrorSourceSettings>,
}

fn invalid(detail: String) -> crate::Error {
    crate::Error::Config { detail }
}

fn parse_spki(name: &str, spki: &str) -> crate::Result<[u8; 32]> {
    let digits = spki.as_bytes();
    if digits.len() != 64 {
        return Err(invalid(format!(
            "[mirror.clients.{name}] spki holds {} characters; it must hold 64 hex digits",
            digits.len()
        )));
    }
    let mut pin = [0u8; 32];
    let (pairs, _) = digits.as_chunks::<2>();
    for (byte, [high, low]) in pin.iter_mut().zip(pairs) {
        let (Some(high), Some(low)) = (
            char::from(*high).to_digit(16),
            char::from(*low).to_digit(16),
        ) else {
            return Err(invalid(format!(
                "[mirror.clients.{name}] spki holds a character that is not a hex digit"
            )));
        };
        *byte = (high * 16 + low) as u8;
    }
    Ok(pin)
}

impl MirrorSettings {
    /// The allowlist this section names. Two names of one key refuse: the key
    /// must name one client.
    pub fn allowed_clients(&self) -> crate::Result<Vec<MirrorClient>> {
        let mut clients = Vec::with_capacity(self.clients.len());
        for (name, settings) in &self.clients {
            let spki = parse_spki(name, &settings.spki)?;
            if settings.databases.is_empty() {
                return Err(invalid(format!(
                    "[mirror.clients.{name}] databases names nothing; list what it can follow"
                )));
            }
            if clients.iter().any(|held: &MirrorClient| held.spki == spki) {
                return Err(invalid(format!(
                    "[mirror.clients.{name}] spki is already another client's"
                )));
            }
            clients.push(MirrorClient {
                name: name.clone(),
                spki,
                databases: settings.databases.clone(),
            });
        }
        Ok(clients)
    }

    /// Refuse a section this node cannot serve safely.
    pub fn validate(&self) -> crate::Result<()> {
        if self.listen.is_some() {
            if self.tls.is_none() {
                return Err(invalid(
                    "[mirror] listen needs [mirror.tls]; this listener has no insecure mode"
                        .to_string(),
                ));
            }
            if self.cluster_name.as_deref().is_none_or(str::is_empty) {
                return Err(invalid(
                    "[mirror] listen needs cluster_name, the name mirrors know this cluster by"
                        .to_string(),
                ));
            }
            if self.clients.is_empty() {
                return Err(invalid(
                    "[mirror] listen needs at least one [mirror.clients] entry".to_string(),
                ));
            }
        }
        self.allowed_clients()?;
        for (name, source) in &self.sources {
            if source.seeds.is_empty() {
                return Err(invalid(format!(
                    "[mirror.sources.{name}] seeds names nothing to dial"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIN: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn tls() -> MirrorTlsPaths {
        MirrorTlsPaths {
            cert: "c".into(),
            key: "k".into(),
            ca: "a".into(),
        }
    }

    fn client(spki: &str, databases: &[&str]) -> MirrorClientSettings {
        MirrorClientSettings {
            spki: spki.to_string(),
            databases: databases.iter().map(ToString::to_string).collect(),
        }
    }

    fn listening() -> MirrorSettings {
        let mut clients = BTreeMap::new();
        clients.insert("dr".to_string(), client(PIN, &["orders"]));
        MirrorSettings {
            cluster_name: Some("prod".to_string()),
            listen: Some(SocketAddr::from(([127, 0, 0, 1], 9500))),
            tls: Some(tls()),
            clients,
            sources: BTreeMap::new(),
        }
    }

    #[test]
    fn an_empty_section_serves_nothing_and_is_valid() {
        assert!(MirrorSettings::default().validate().is_ok());
    }

    #[test]
    fn a_complete_listener_is_valid_and_parses_its_pin() {
        let settings = listening();
        settings.validate().expect("valid");
        let clients = settings.allowed_clients().expect("clients");
        assert_eq!(clients[0].name, "dr");
        assert_eq!(clients[0].spki[0], 0x9f);
        assert_eq!(clients[0].spki[31], 0x08);
    }

    #[test]
    fn a_listener_with_no_tls_is_refused() {
        let settings = MirrorSettings {
            tls: None,
            ..listening()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn a_listener_with_no_cluster_name_is_refused() {
        let settings = MirrorSettings {
            cluster_name: None,
            ..listening()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn a_listener_with_no_client_is_refused() {
        let settings = MirrorSettings {
            clients: BTreeMap::new(),
            ..listening()
        };
        assert!(settings.validate().is_err());
    }

    #[test]
    fn a_malformed_pin_is_refused() {
        let bad_pins = [
            String::new(),
            "abc".to_string(),
            "zz".repeat(32),
            "é".repeat(32),
        ];
        for bad in &bad_pins {
            let mut settings = listening();
            settings
                .clients
                .insert("dr".to_string(), client(bad, &["orders"]));
            assert!(settings.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_client_with_no_database_is_refused() {
        let mut settings = listening();
        settings.clients.insert("dr".to_string(), client(PIN, &[]));
        assert!(settings.validate().is_err());
    }

    #[test]
    fn one_key_cannot_name_two_clients() {
        let mut settings = listening();
        settings
            .clients
            .insert("other".to_string(), client(PIN, &["payroll"]));
        assert!(settings.validate().is_err());
    }

    #[test]
    fn a_signed_pair_is_not_a_hex_byte() {
        let signed = format!("+a{}", &PIN[2..]);
        assert!(parse_spki("prod", &signed).is_err());
        assert_eq!(parse_spki("prod", PIN).expect("valid pin")[0], 0x9f);
    }

    #[test]
    fn a_source_with_no_seed_is_refused() {
        let mut settings = MirrorSettings::default();
        settings.sources.insert(
            "prod".to_string(),
            MirrorSourceSettings {
                seeds: Vec::new(),
                server_name: "nodedb".to_string(),
                tls: tls(),
            },
        );
        assert!(settings.validate().is_err());
    }
}
