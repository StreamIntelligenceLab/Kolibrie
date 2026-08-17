/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod binding;
mod error;
mod format;
mod model;
mod parser;
mod scanner;
mod version;

#[cfg(test)]
mod tests;

pub use error::RdfMessageError;
pub use format::MessageBaseFormat;
pub use model::{MessageQuad, RdfMessage, RdfMessageLog};
pub use parser::RdfMessageLogParser;
pub use version::VersionLabel;

/// Parse an RDF Message Log in the given serialization
pub fn parse_message_log(
    input: &str,
    format: MessageBaseFormat,
) -> Result<RdfMessageLog, RdfMessageError> {
    RdfMessageLogParser::new(format).parse(input)
}
