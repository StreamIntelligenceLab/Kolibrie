/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Turning RDF terms into the numbers a model consumes.
//!
//! Pure conversion, and deliberately nothing else. Loading the rows to convert needs
//! to run a query, which is a layer above this one, so
//! [`query_training_rows`](crate::execute_query::query_training_rows) lives there and
//! this module depends on nothing in the crate.

use std::error::Error;

#[cfg(feature = "ml")]
use std::collections::HashMap;

pub(crate) type RdfTerm = String;
type LoaderResult<T> = Result<T, Box<dyn Error>>;

// Not gated: program.rs and execute_query.rs use this one in the default build.
pub(crate) fn rdf_term_to_f64(term: &RdfTerm) -> LoaderResult<f64> {
    let trimmed = term.trim();
    if let Ok(value) = trimmed.parse::<f64>() {
        return Ok(value);
    }

    if trimmed.starts_with('"') {
        if let Some(end_quote) = trimmed[1..].find('"') {
            let end_quote = end_quote + 1;
            let lexical = &trimmed[1..end_quote];
            let rest = &trimmed[end_quote + 1..];
            let datatype = rest.strip_prefix("^^<").and_then(|dt| dt.strip_suffix('>'));
            let accepted = matches!(
                datatype,
                Some("http://www.w3.org/2001/XMLSchema#float")
                    | Some("http://www.w3.org/2001/XMLSchema#double")
                    | Some("http://www.w3.org/2001/XMLSchema#integer")
                    | Some("http://www.w3.org/2001/XMLSchema#decimal")
                    | Some("http://www.w3.org/2001/XMLSchema#long")
            );
            if accepted || datatype.is_none() {
                return lexical.parse::<f64>().map_err(|_| {
                    format!("Non-numeric RDF term in neural feature vector: {}", term).into()
                });
            }
        }
    }

    Err(format!("Non-numeric RDF term in neural feature vector: {}", term).into())
}

// Gated: only the ml-gated modules build feature vectors.
#[cfg(feature = "ml")]
pub(crate) fn build_feature_vec(
    row: &HashMap<String, RdfTerm>,
    feature_vars: &[&str],
) -> LoaderResult<Vec<f64>> {
    feature_vars
        .iter()
        .map(|var| {
            let key = var.trim_start_matches('?');
            let term = row
                .get(key)
                .or_else(|| row.get(*var))
                .ok_or_else(|| format!("Missing feature variable {}", var))?;
            rdf_term_to_f64(term)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rdf_term_to_f64_xsd_types() {
        assert_eq!(rdf_term_to_f64(&"42".to_string()).unwrap(), 42.0);
        assert_eq!(
            rdf_term_to_f64(&"\"3.5\"^^<http://www.w3.org/2001/XMLSchema#double>".to_string())
                .unwrap(),
            3.5
        );
        assert!(rdf_term_to_f64(&"http://example.org/value".to_string()).is_err());
        assert!(rdf_term_to_f64(&"\"abc\"".to_string()).is_err());
    }
}
