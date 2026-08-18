/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Assigns a timestamp to an RDF Message, either from arrival time or from a
//! property carried by the message itself
//!
//! Timestamps are epoch seconds because the RSP windows measure width and slide
//! in seconds

use super::model::RdfMessage;
use crate::rsp_engine::TimestampAssignment;
use crate::utils::current_timestamp;
use chrono::{DateTime, NaiveDateTime};
use log::warn;

/// Where the timestamp of a message came from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampSource {
    /// Arrival time, either requested or used as a fallback
    SysTime,
    /// Read from the message content
    Extracted,
}

/// Assign a timestamp in epoch seconds to a message
///
/// Under [`TimestampAssignment::Property`] the first quad whose predicate
/// matches wins. A missing property, an unreadable message or a value that is
/// not a recognizable instant all fall back to arrival time
pub fn extract_timestamp(
    message: &RdfMessage,
    policy: &TimestampAssignment,
) -> (usize, TimestampSource) {
    let property = match policy {
        TimestampAssignment::SysTime => return (current_timestamp() as usize, TimestampSource::SysTime),
        TimestampAssignment::Property(iri) => iri,
    };

    let quads = match message.to_quads() {
        Ok(quads) => quads,
        Err(err) => {
            warn!("cannot read message for timestamp extraction: {}, using system time", err);
            return (current_timestamp() as usize, TimestampSource::SysTime);
        }
    };

    match quads.iter().find(|quad| quad.predicate == *property) {
        Some(quad) => match parse_timestamp_value(&quad.object) {
            Some(seconds) => (seconds as usize, TimestampSource::Extracted),
            None => {
                warn!(
                    "cannot parse {} value {:?} as a timestamp, using system time",
                    property, quad.object
                );
                (current_timestamp() as usize, TimestampSource::SysTime)
            }
        },
        None => {
            warn!("message carries no {}, using system time", property);
            (current_timestamp() as usize, TimestampSource::SysTime)
        }
    }
}

/// Read a lexical value as epoch seconds
///
/// Message quads carry bare lexical values, the `^^xsd:dateTime` datatype has
/// already been stripped by the binding layer, so the text is parsed directly
pub fn parse_timestamp_value(lexical: &str) -> Option<u64> {
    let value = lexical.trim();
    if value.is_empty() {
        return None;
    }

    // A plain integer is already epoch seconds
    if value.chars().all(|c| c.is_ascii_digit()) {
        return value.parse::<u64>().ok();
    }

    // Instants carrying a zone, such as 2026-01-01T00:00:00Z or +02:00
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return u64::try_from(parsed.timestamp()).ok();
    }

    // Zoneless instants are read as UTC
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(value, format) {
            return u64::try_from(naive.and_utc().timestamp()).ok();
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf_messages::{parse_message_log, MessageBaseFormat};

    fn first_message(input: &str, format: MessageBaseFormat) -> RdfMessage {
        parse_message_log(input, format).unwrap().messages.remove(0)
    }

    #[test]
    fn parses_rfc3339_with_zulu() {
        assert_eq!(parse_timestamp_value("2026-01-01T00:00:00Z"), Some(1767225600));
    }

    #[test]
    fn parses_rfc3339_with_offset() {
        // 01:00:00+01:00 is the same instant as 00:00:00Z
        assert_eq!(
            parse_timestamp_value("2026-01-01T01:00:00+01:00"),
            parse_timestamp_value("2026-01-01T00:00:00Z")
        );
    }

    #[test]
    fn parses_zoneless_datetime_as_utc() {
        assert_eq!(parse_timestamp_value("2026-01-01T00:00:00"), Some(1767225600));
    }

    #[test]
    fn parses_plain_epoch_seconds() {
        assert_eq!(parse_timestamp_value("1767225600"), Some(1767225600));
    }

    #[test]
    fn rejects_unparsable_values() {
        assert_eq!(parse_timestamp_value("not a date"), None);
        assert_eq!(parse_timestamp_value(""), None);
    }

    #[test]
    fn extracts_first_matching_property() {
        let input = concat!(
            "<http://example.org/o1> <http://www.w3.org/ns/sosa/resultTime> \"2026-01-01T00:00:00Z\" .\n",
        );
        let message = first_message(input, MessageBaseFormat::NTriples);
        let policy = TimestampAssignment::Property("http://www.w3.org/ns/sosa/resultTime".to_string());
        let (ts, source) = extract_timestamp(&message, &policy);
        assert_eq!(source, TimestampSource::Extracted);
        assert_eq!(ts, 1767225600);
    }

    #[test]
    fn falls_back_when_property_absent() {
        let input = "<http://example.org/o1> <http://example.org/other> \"x\" .\n";
        let message = first_message(input, MessageBaseFormat::NTriples);
        let policy = TimestampAssignment::Property("http://www.w3.org/ns/sosa/resultTime".to_string());
        let (_, source) = extract_timestamp(&message, &policy);
        assert_eq!(source, TimestampSource::SysTime);
    }

    #[test]
    fn falls_back_when_value_unparsable() {
        let input =
            "<http://example.org/o1> <http://www.w3.org/ns/sosa/resultTime> \"yesterday\" .\n";
        let message = first_message(input, MessageBaseFormat::NTriples);
        let policy = TimestampAssignment::Property("http://www.w3.org/ns/sosa/resultTime".to_string());
        let (_, source) = extract_timestamp(&message, &policy);
        assert_eq!(source, TimestampSource::SysTime);
    }

    #[test]
    fn systime_policy_never_reads_the_message() {
        let input = "<http://example.org/o1> <http://example.org/p> \"x\" .\n";
        let message = first_message(input, MessageBaseFormat::NTriples);
        let (ts, source) = extract_timestamp(&message, &TimestampAssignment::SysTime);
        assert_eq!(source, TimestampSource::SysTime);
        assert!(ts > 0);
    }
}
