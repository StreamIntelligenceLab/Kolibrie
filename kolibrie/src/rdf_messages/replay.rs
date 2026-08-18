/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Feeds an RDF Message Log into a running RSP engine, one message at a time,
//! assigning each message the timestamp its stream registration asks for

use super::error::RdfMessageError;
use super::format::MessageBaseFormat;
use super::model::{RdfMessage, RdfMessageLog};
use super::parser::RdfMessageLogParser;
use super::timestamp::{extract_timestamp, TimestampSource};
use crate::rsp_engine::{RSPEngine, StreamSource, TimestampAssignment};
use log::warn;
use shared::triple::Triple;
use std::hash::Hash;
use std::path::{Path, PathBuf};

/// What a replay pushed into the engine
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayStats {
    pub messages: usize,
    pub triples: usize,
    /// Messages that fell back to arrival time because no timestamp was readable
    pub systime_fallbacks: usize,
}

/// Push every message of a log into one stream of the engine
pub fn replay_log<O>(
    engine: &mut RSPEngine<Triple, O>,
    stream_iri: &str,
    log: &RdfMessageLog,
    policy: &TimestampAssignment,
) -> Result<ReplayStats, RdfMessageError>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
{
    let mut stats = ReplayStats::default();
    let mut last_ts: Option<usize> = None;

    for message in log.iter() {
        let (ts, source) = extract_timestamp(message, policy);
        if source == TimestampSource::SysTime {
            stats.systime_fallbacks += 1;
        }

        // Windows advance on rising timestamps only, a regression still enters
        // the open windows but will not trigger a firing
        if let Some(previous) = last_ts {
            if ts < previous {
                warn!(
                    "message timestamp {} goes back from {} on stream {}, windows will not fire on it",
                    ts, previous, stream_iri
                );
            }
        }
        last_ts = Some(ts);

        stats.triples += push_message(engine, stream_iri, message, ts)?;
        stats.messages += 1;
    }

    Ok(stats)
}

/// Read a message log from disk and push it into one stream of the engine
///
/// The serialization is inferred from the file extension
pub fn replay_file<O>(
    engine: &mut RSPEngine<Triple, O>,
    stream_iri: &str,
    path: &Path,
    policy: &TimestampAssignment,
) -> Result<ReplayStats, RdfMessageError>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
{
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();
    let format = MessageBaseFormat::from_extension(extension)?;

    let text = std::fs::read_to_string(path)
        .map_err(|e| RdfMessageError::Binding(format!("cannot read {}: {}", path.display(), e)))?;
    let log = RdfMessageLogParser::new(format).parse(&text)?;

    replay_log(engine, stream_iri, &log, policy)
}

/// Replay every `file:` source the engine was registered with
///
/// Sources on other transports are reported and skipped, the syntax layer keeps
/// them but this helper only knows how to read files
pub fn replay_registered_sources<O>(
    engine: &mut RSPEngine<Triple, O>,
) -> Result<Vec<ReplayStats>, RdfMessageError>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
{
    // Snapshot the registrations so the engine can be borrowed mutably below
    let sources: Vec<StreamSource> = engine.stream_sources().to_vec();

    let mut all_stats = Vec::new();
    for source in sources {
        match source_to_path(&source.source) {
            Some(path) => {
                let stats = replay_file(engine, &source.stream_iri, &path, &source.timestamp)?;
                all_stats.push(stats);
            }
            None => warn!(
                "stream {} has non-file source {}, skipping replay",
                source.stream_iri, source.source
            ),
        }
    }
    Ok(all_stats)
}

/// Push one message into a stream at the given timestamp, returning the triple count
fn push_message<O>(
    engine: &mut RSPEngine<Triple, O>,
    stream_iri: &str,
    message: &RdfMessage,
    ts: usize,
) -> Result<usize, RdfMessageError>
where
    O: Clone + Hash + Eq + Send + 'static + From<Vec<(String, String)>>,
{
    let ntriples = message.to_ntriples()?;
    if ntriples.trim().is_empty() {
        return Ok(0);
    }

    let triples = engine.parse_data(&ntriples);
    let count = triples.len();
    for triple in triples {
        engine.add_to_stream(stream_iri, triple, ts);
    }
    Ok(count)
}

/// Turn a `file:` source into a local path
///
/// Both `file:///data/log.nt` and the Windows `file:///D:/data/log.nt` form are
/// accepted, a plain path is taken as-is
fn source_to_path(source: &str) -> Option<PathBuf> {
    let trimmed = source.trim().trim_start_matches('<').trim_end_matches('>');

    let rest = match trimmed.strip_prefix("file://") {
        Some(rest) => rest,
        None => {
            // Anything carrying another scheme belongs to a different transport
            if trimmed.contains("://") {
                return None;
            }
            return Some(PathBuf::from(trimmed));
        }
    };

    // file://host/path is not a local file, only the empty authority is
    let path = rest.strip_prefix('/').unwrap_or(rest);
    if path.is_empty() {
        return None;
    }

    // A Windows path arrives as /D:/data, the leading slash has to go
    let looks_like_drive = path
        .as_bytes()
        .get(1)
        .is_some_and(|b| *b == b':');
    if looks_like_drive {
        Some(PathBuf::from(path))
    } else {
        Some(PathBuf::from(format!("/{}", path)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_unix_file_iri() {
        assert_eq!(
            source_to_path("file:///data/log.nt"),
            Some(PathBuf::from("/data/log.nt"))
        );
    }

    #[test]
    fn reads_windows_file_iri() {
        assert_eq!(
            source_to_path("file:///D:/data/log.nt"),
            Some(PathBuf::from("D:/data/log.nt"))
        );
    }

    #[test]
    fn reads_bare_path() {
        assert_eq!(
            source_to_path("data/log.nt"),
            Some(PathBuf::from("data/log.nt"))
        );
    }

    #[test]
    fn rejects_other_transports() {
        assert_eq!(source_to_path("mqtt://broker/topic"), None);
        assert_eq!(source_to_path("https://example.org/log.nt"), None);
    }
}
