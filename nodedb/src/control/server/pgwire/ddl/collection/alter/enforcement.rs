//! `ALTER COLLECTION ... SET {RETENTION,LEGAL_HOLD,APPEND_ONLY,LAST_VALUE_CACHE}`
//! — non-schema enforcement knobs propagated through `CatalogEntry::PutCollection`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

/// Handle ALTER COLLECTION enforcement commands: SET RETENTION, SET/RELEASE LEGAL_HOLD,
/// SET APPEND_ONLY, SET LAST_VALUE_CACHE.
pub fn alter_collection_enforcement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    kind: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let upper = sql.to_uppercase();

    let name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(tenant_id, &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' not found")))?;

    match kind {
        "retention" => {
            let value = extract_set_value(&upper, "RETENTION")
                .ok_or_else(|| sqlstate_error("42601", "SET RETENTION requires = 'duration'"))?;

            crate::data::executor::enforcement::retention::parse_retention_period(&value)
                .map_err(|e| sqlstate_error("22023", &e))?;

            coll.retention_period = Some(value);
        }
        "legal_hold" => {
            if upper.contains("LEGAL_HOLD = TRUE") || upper.contains("LEGAL_HOLD=TRUE") {
                let tag = extract_tag_value(&upper).ok_or_else(|| {
                    sqlstate_error("42601", "SET LEGAL_HOLD = TRUE requires TAG 'name'")
                })?;

                if coll.legal_holds.iter().any(|h| h.tag == tag) {
                    return Err(sqlstate_error(
                        "23505",
                        &format!("legal hold tag '{tag}' already exists on {name}"),
                    ));
                }

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                coll.legal_holds
                    .push(crate::control::security::catalog::LegalHold {
                        tag,
                        created_at: now,
                        created_by: identity.username.clone(),
                    });
            } else if upper.contains("LEGAL_HOLD = FALSE") || upper.contains("LEGAL_HOLD=FALSE") {
                let tag = extract_tag_value(&upper).ok_or_else(|| {
                    sqlstate_error("42601", "SET LEGAL_HOLD = FALSE requires TAG 'name'")
                })?;

                let before = coll.legal_holds.len();
                coll.legal_holds.retain(|h| h.tag != tag);
                if coll.legal_holds.len() == before {
                    return Err(sqlstate_error(
                        "42704",
                        &format!("legal hold tag '{tag}' not found on {name}"),
                    ));
                }
            } else {
                return Err(sqlstate_error(
                    "42601",
                    "ALTER COLLECTION SET LEGAL_HOLD requires = TRUE TAG 'name' or = FALSE TAG 'name'",
                ));
            }
        }
        "append_only" => {
            if coll.append_only {
                return Err(sqlstate_error(
                    "42710",
                    &format!("collection '{name}' is already append-only"),
                ));
            }
            coll.append_only = true;
        }
        "last_value_cache" => {
            if !coll.collection_type.is_timeseries() {
                return Err(sqlstate_error(
                    "42809",
                    &format!("'{name}' is not a timeseries collection"),
                ));
            }
            let val = extract_set_value(&upper, "LAST_VALUE_CACHE").ok_or_else(|| {
                sqlstate_error("42601", "SET LAST_VALUE_CACHE requires = TRUE or = FALSE")
            })?;
            coll.lvc_enabled = val.eq_ignore_ascii_case("TRUE");
        }
        _ => {
            return Err(sqlstate_error(
                "42601",
                &format!("unknown ALTER COLLECTION enforcement kind: '{kind}'"),
            ));
        }
    }

    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        catalog
            .put_collection(&coll)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    state.schema_version.bump();

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}

/// Extract value from `SET KEY = 'value'` pattern.
fn extract_set_value(upper: &str, key: &str) -> Option<String> {
    let pattern = format!("{key} =");
    let pos = upper
        .find(&pattern)
        .or_else(|| upper.find(&format!("{key}=")))?;
    let after = upper[pos..].split('=').nth(1)?.trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .unwrap_or(value.len());
    Some(value[..end].to_string())
}

/// Extract TAG value from `TAG 'name'` pattern.
fn extract_tag_value(upper: &str) -> Option<String> {
    let pos = upper.find("TAG ")?;
    let after = upper[pos + 4..].trim();
    let value = after.trim_start_matches('\'').trim_start_matches('"');
    let end = value
        .find('\'')
        .or_else(|| value.find('"'))
        .or_else(|| value.find(' '))
        .unwrap_or(value.len());
    if end == 0 {
        return None;
    }
    Some(value[..end].to_string())
}
