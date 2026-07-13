// SPDX-License-Identifier: BUSL-1.1

//! Result-shaping inputs for simple and prepared pgwire execution.

use pgwire::api::results::FieldFormat;

use crate::control::server::response_shape::schema::OutputSchema;

#[derive(Clone, Copy)]
pub(in crate::control::server::pgwire::handler) struct ResultShaping<'a> {
    pub projection: Option<&'a OutputSchema>,
    pub formats: &'a [FieldFormat],
}
