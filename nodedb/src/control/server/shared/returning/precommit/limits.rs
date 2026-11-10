// SPDX-License-Identifier: BUSL-1.1

//! The response size limits and the bounded payload encoder.

use crate::data::executor::response_codec::RowsPayload;

pub(super) fn encode_rows(rows: &RowsPayload, maximum: usize) -> crate::Result<Vec<u8>> {
    let mut output = BoundedOutput {
        bytes: Vec::new(),
        maximum,
        exceeded: false,
    };
    if let Err(error) = zerompk::write_msgpack(&mut output, rows) {
        if output.exceeded {
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!("RETURNING payload bytes exceed limit {maximum}"),
            });
        }
        return Err(crate::Error::Codec {
            detail: format!("RETURNING response serialization: {error}"),
        });
    }
    Ok(output.bytes)
}

struct BoundedOutput {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "RETURNING payload byte limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn limit(name: &str, actual: usize, maximum: usize) -> crate::Result<()> {
    if actual > maximum {
        return Err(crate::Error::ExecutionLimitExceeded {
            detail: format!("RETURNING {name} {actual} exceeds limit {maximum}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_query::expr::{BinaryOp, SqlExpr};
    use nodedb_types::{DatabaseId, TenantId, Value};

    use super::super::fixtures::{call, computed, finalize, limits, literal, registry};
    use super::super::types::PrecommitReturning;
    use crate::control::security::redaction::RedactionStore;
    use crate::control::sequence::{SequenceAccess, SessionSequenceAccess};
    use crate::control::server::response_shape::types::ShapedRow;
    use crate::control::server::shared::returning::transcript::AccessorTranscript;
    use crate::control::server::shared::returning::{ReturningColumns, ReturningSpec};

    #[test]
    fn late_evaluation_and_encoded_size_errors_remain_typed() {
        let registry = registry();
        let access =
            SessionSequenceAccess::new(&registry, None, DatabaseId::new(1), TenantId::new(7));
        let snapshot = RedactionStore::new().snapshot_rules(TenantId::new(7), "users", &[]);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![
                computed(
                    "next",
                    call("nextval", vec![literal(Value::String("s".into()))]),
                ),
                computed(
                    "bad",
                    SqlExpr::BinaryOp {
                        left: Box::new(literal(Value::Integer(1))),
                        op: BinaryOp::Div,
                        right: Box::new(literal(Value::Integer(0))),
                    },
                ),
            ]),
            precommit: None,
        };
        let crs = nodedb_crs::CustomCrsSet::empty();
        let mut request = PrecommitReturning {
            spec: &spec,
            source_columns: &[],
            redaction: &snapshot,
            sequences: &access,
            projection: None,
            crs_set: crs,
            limits: limits(),
        };
        let transcript = AccessorTranscript::default();
        assert!(matches!(
            finalize(&request, vec![ShapedRow::new()], &transcript),
            Err(crate::Error::DivisionByZero)
        ));
        assert_eq!(transcript.entries().len(), 1);
        assert!(matches!(
            finalize(&request, vec![ShapedRow::new()], &transcript),
            Err(crate::Error::DivisionByZero)
        ));
        assert_eq!(access.nextval("s").expect("next"), 2);
        let spec = ReturningSpec {
            columns: ReturningColumns::Named(vec![computed(
                "value",
                literal(Value::String("large".into())),
            )]),
            precommit: None,
        };
        request.spec = &spec;
        request.limits.payload_bytes = 1;
        assert!(matches!(
            finalize(
                &request,
                vec![ShapedRow::new()],
                &AccessorTranscript::default()
            ),
            Err(crate::Error::ExecutionLimitExceeded { .. })
        ));
    }
}
