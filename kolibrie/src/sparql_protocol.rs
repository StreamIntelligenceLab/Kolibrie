/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The SPARQL 1.1 Protocol over HTTP: request text in, response text out.
//!
//! Answers the three request forms the specification defines — a `GET` carrying
//! `?query=`, a `POST` of `application/sparql-query` or `application/sparql-update`,
//! and a form-encoded `POST` with a `query=` or `update=` parameter.
//!
//! # No sockets
//!
//! [`answer`] takes a request as a string and returns a response as a string. It
//! neither listens nor accepts, so the caller chooses the transport, the threading
//! and the port. The library used to own a `TcpListener` and a serving loop in
//! `utils`, which made a store crate responsible for running a server; those are
//! gone and nothing but a single example had used them.
//!
//! This is also not the same thing as the `kolibrie-http-server` binary. That serves
//! a JSON API of its own — `/query`, `/rsp/register`, `/datalogmtl/*` — and accepts
//! none of the protocol's content types. The two share only a mapping from failures
//! to status codes, which is duplicated and has to be changed in both places.
//!
//! ```no_run
//! use kolibrie::sparql_database::SparqlDatabase;
//! use kolibrie::sparql_protocol::answer;
//!
//! let mut database = SparqlDatabase::new();
//! let response = answer(
//!     &mut database,
//!     "GET /sparql?query=SELECT%20%3Fs%20WHERE%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D HTTP/1.1\r\n\r\n",
//! );
//! assert!(response.starts_with("HTTP/1.1 200 OK"));
//! ```

use crate::sparql_database::SparqlDatabase;
use percent_encoding::percent_decode;
use std::collections::HashMap;
use url::Url;

/// Answers one request against `database`, returning a complete HTTP response.
///
/// The ML policy applies: a request naming a model the host has not approved is
/// refused rather than run, and the refusal carries its own status.
///
/// # Status codes
///
/// `200` for a query or update that ran, `400` for one that did not parse or was sent
/// to the wrong form, `403` or `503` for a policy refusal, and `400` for a method or
/// path this does not serve. A failure body is a bare code, never the request text: a
/// parse error carries the offending query, and echoing it back would hand it to
/// whoever sent it.
///
/// # Panics
///
/// If `request` is not parseable as an HTTP request at all. The protocol layer above
/// this is expected to have read a complete request.
pub fn answer(database: &mut SparqlDatabase, request: &str) -> String {
    let context = database.ml_context().for_http();
    let (status, body) = crate::execute_query::with_ml_context(database, &context, |database| {
        dispatch(database, request)
    });
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// Which operation the request asks for, and the outcome of running it.
fn dispatch(database: &mut SparqlDatabase, request: &str) -> (&'static str, String) {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Request::new(&mut headers);
    parsed.parse(request.as_bytes()).unwrap();

    match parsed.method.unwrap_or("") {
        "GET" => {
            let url =
                Url::parse(&("http://localhost".to_owned() + parsed.path.unwrap_or("/"))).unwrap();
            let pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
            if let Some(query) = pairs.get("query") {
                return query_result(database, query);
            }
        }
        "POST" => {
            let content_type = parsed
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("Content-Type"))
                .map(|header| header.value);
            // The body is everything past the header section, which httparse does not
            // hand back.
            let body = request.split("\r\n\r\n").nth(1);
            match (content_type, body) {
                (Some(b"application/sparql-query"), Some(body)) => {
                    return query_result(database, body)
                }
                (Some(b"application/sparql-update"), Some(body)) => {
                    return update_result(database, body)
                }
                (Some(b"application/x-www-form-urlencoded"), Some(body)) => {
                    let params = form_parameters(body);
                    if let Some(query) = params.get("query") {
                        return query_result(database, query);
                    }
                    if let Some(update) = params.get("update") {
                        return update_result(database, update);
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }

    ("400 Bad Request", "Bad Request".to_string())
}

/// The status and body for one query.
pub(crate) fn query_result(database: &mut SparqlDatabase, query: &str) -> (&'static str, String) {
    match crate::execute_query::execute_sparql_query(query, database) {
        Ok(rows) => (
            "200 OK",
            rows.into_iter()
                .map(|row| row.join("\t"))
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        Err(error) => {
            // Matched on the variant rather than on a rendered string. This is the
            // same status table the HTTP server keeps in `ml_error_response`: two
            // copies, in two crates, that have to be changed together.
            use crate::execute_query::QueryError;
            use crate::ml_policy::MlError;
            match &error {
                QueryError::Ml(MlError::Disabled) => {
                    ("503 Service Unavailable", "ML_FEATURE_DISABLED".to_string())
                }
                QueryError::Ml(MlError::Forbidden) => ("403 Forbidden", "ML_FORBIDDEN".to_string()),
                QueryError::Ml(MlError::InvalidArtifact) => {
                    ("403 Forbidden", "ML_INVALID_ARTIFACT".to_string())
                }
                QueryError::Ml(MlError::InvalidConfiguration) => (
                    "503 Service Unavailable",
                    "ML_INVALID_CONFIGURATION".to_string(),
                ),
                // The message is deliberately not sent: a parse failure carries the
                // offending query text.
                _ => ("400 Bad Request", "QUERY_EXECUTION_FAILED".to_string()),
            }
        }
    }
}

/// The status and body for one update.
///
/// Public because the outcome text is what a caller applying an update outside a
/// request wants; `SparqlDatabase::handle_update` used to delegate here and has been
/// removed, since a store should not reach up into the protocol that serves it.
pub fn update_result(database: &mut SparqlDatabase, update: &str) -> (&'static str, String) {
    if let Ok(summary) = crate::execute_query::execute_sparql_update(update, database) {
        return (
            "200 OK",
            format!(
                "Update Successful (inserted {}, deleted {})",
                summary.inserted_quads, summary.deleted_quads
            ),
        );
    }
    // Historical standalone INSERT/DELETE aliases parse into the same update operation
    if crate::execute_query::execute_sparql_update_compat(update, database).is_ok() {
        return ("200 OK", "Update Successful".to_string());
    }
    ("400 Bad Request", "Update Failed".to_string())
}

/// `name=value` pairs from a form-encoded body, with `+` read as a space.
fn form_parameters(body: &str) -> HashMap<String, String> {
    body.split('&')
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_component(name), decode_component(value))
        })
        .collect()
}

fn decode_component(component: &str) -> String {
    let normalized = component
        .bytes()
        .map(|byte| if byte == b'+' { b' ' } else { byte })
        .collect::<Vec<_>>();
    percent_decode(&normalized).decode_utf8_lossy().into_owned()
}
