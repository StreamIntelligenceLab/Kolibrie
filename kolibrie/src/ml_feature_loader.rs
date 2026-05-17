/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use std::error::Error;

use crate::execute_query::{execute_query, execute_query_rayon_parallel2_volcano};
use crate::parser::parse_sparql_query;
use crate::sparql_database::SparqlDatabase;

pub type RdfTerm = String;
type LoaderResult<T> = Result<T, Box<dyn Error>>;

pub fn query_training_rows(
    db: &mut SparqlDatabase,
    select_query: &str,
) -> LoaderResult<Vec<HashMap<String, RdfTerm>>> {
    let (_, parsed) = parse_sparql_query(select_query).map_err(|err| {
        format!("failed to parse training data query: {err:?}")
    })?;

    let variables: Vec<String> = parsed
        .1
        .iter()
        .filter_map(|(kind, var, _)| {
            if *kind == "VAR" || var.starts_with('?') {
                Some(var.trim_start_matches('?').to_string())
            } else {
                None
            }
        })
        .collect();

    if variables.is_empty() {
        return Err("training data query must SELECT at least one variable".into());
    }

    let rows = if select_query.contains("<<") {
        execute_query_rayon_parallel2_volcano(select_query, db)
    } else {
        execute_query(select_query, db)
    };
    Ok(rows
        .into_iter()
        .map(|row| {
            variables
                .iter()
                .cloned()
                .zip(row)
                .collect::<HashMap<String, RdfTerm>>()
        })
        .collect())
}

pub fn rdf_term_to_f64(term: &RdfTerm) -> LoaderResult<f64> {
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

pub fn build_feature_vec(
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
            rdf_term_to_f64(&"\"3.5\"^^<http://www.w3.org/2001/XMLSchema#double>".to_string()).unwrap(),
            3.5
        );
        assert!(rdf_term_to_f64(&"http://example.org/value".to_string()).is_err());
        assert!(rdf_term_to_f64(&"\"abc\"".to_string()).is_err());
    }
}
