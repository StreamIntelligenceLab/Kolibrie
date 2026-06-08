/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use shared::terms::TriplePattern;
use shared::triple::Triple;
use shared::dictionary::Dictionary;
use datalog::reasoning::matches_rule_pattern;
use std::sync::{Arc, RwLock};

/// IRI identifying a named stream.
pub type StreamIri = String;

/// A BGP event pattern: one or more triple patterns that must all match
/// the triples of one incoming RDF event graph.
pub type EventPattern = Vec<TriplePattern>;

/// The composite primary key for a stream channel.
/// A list of variable names whose bindings together identify one logical channel.
pub type ChannelKey = Vec<String>;

/// A key binding: the actual values for a specific channel.
pub type KeyBinding = HashMap<String, u32>;

/// Staleness policy for a stream shape.
#[derive(Debug, Clone)]
pub struct StalenessPolicy {
    /// If no new event arrives within max_gap_ms, the channel's reading expires.
    pub max_gap_ms: u64,
}

/// A stream shape declaration.
#[derive(Debug, Clone)]
pub struct StreamShape {
    pub stream_iri:    StreamIri,
    pub event_pattern: EventPattern,
    pub channel_key:   ChannelKey,
    pub staleness:     StalenessPolicy,
}

/// The active state of one logical channel (one distinct key binding).
#[derive(Debug, Clone)]
pub struct ChannelState {
    /// The triples belonging to the most recent event on this channel.
    pub active_triples:  Vec<Triple>,
    /// When this reading became valid (absolute ms).
    pub validity_start:  u64,
    /// When this reading expires (absolute ms = last_event_time + max_gap_ms).
    pub expiry_time:     u64,
    /// Timestamp of the most recent event on this channel.
    pub last_event_time: u64,
}

/// An incoming RDF event: a small graph of triples arriving together at time t.
#[derive(Debug, Clone)]
pub struct RdfEvent {
    pub stream_iri: StreamIri,
    pub timestamp:  u64,
    pub triples:    Vec<Triple>,
}

/// Stable, hashable representation of a channel key (sorted pairs).
type ChannelId = (usize, Vec<(String, u32)>);

fn key_binding_to_id(shape_idx: usize, binding: &KeyBinding) -> ChannelId {
    let mut pairs: Vec<(String, u32)> = binding.iter()
        .map(|(k, &v)| (k.clone(), v))
        .collect();
    pairs.sort();
    (shape_idx, pairs)
}

/// The shape ingester. Maintains channel state per shape and emits
/// step-function-derived (triple, validity_interval) pairs to the evaluator.
pub struct ShapeIngester {
    shapes:     Vec<StreamShape>,
    /// Per (shape_index, sorted_key_pairs), the current channel state.
    channels:   HashMap<ChannelId, ChannelState>,
    #[allow(dead_code)]
    dictionary: Arc<RwLock<Dictionary>>,
}

impl ShapeIngester {
    pub fn new(shapes: Vec<StreamShape>, dictionary: Arc<RwLock<Dictionary>>) -> Self {
        Self { shapes, channels: HashMap::new(), dictionary }
    }

    /// Process an incoming event. Returns triples to insert into the store at
    /// the event's timestamp (newly active triples for matching channels).
    pub fn process_event(&mut self, event: &RdfEvent) -> Vec<(Triple, u64)> {
        let mut to_insert = Vec::new();

        for (shape_idx, shape) in self.shapes.iter().enumerate() {
            if shape.stream_iri != event.stream_iri { continue; }

            let Some(bindings) = match_event_pattern(&shape.event_pattern, &event.triples)
            else { continue; };

            let key_binding: KeyBinding = shape.channel_key.iter()
                .filter_map(|var| bindings.get(var).map(|&v| (var.clone(), v)))
                .collect();

            if key_binding.len() != shape.channel_key.len() {
                continue;
            }

            let channel_id = key_binding_to_id(shape_idx, &key_binding);
            let new_state = ChannelState {
                active_triples:  event.triples.clone(),
                validity_start:  event.timestamp,
                expiry_time:     event.timestamp + shape.staleness.max_gap_ms,
                last_event_time: event.timestamp,
            };

            for triple in &event.triples {
                to_insert.push((triple.clone(), event.timestamp));
            }

            self.channels.insert(channel_id, new_state);
        }

        to_insert
    }

    /// Check for expired channels at time t.
    pub fn expired_channels(&self, t: u64) -> Vec<ChannelId> {
        self.channels.iter()
            .filter(|(_, state)| state.expiry_time < t)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Remove expired channels at time t.
    pub fn evict_expired(&mut self, t: u64) {
        self.channels.retain(|_, state| state.expiry_time >= t);
    }

    /// Returns all currently active channel states.
    pub fn active_channels(&self) -> &HashMap<ChannelId, ChannelState> {
        &self.channels
    }
}

/// Attempt to match an EventPattern against the triples of one incoming event.
/// Returns Some(bindings) if all patterns match, None if any pattern fails.
fn match_event_pattern(
    pattern: &EventPattern,
    event_triples: &[Triple],
) -> Option<HashMap<String, u32>> {
    let mut bindings: HashMap<String, u32> = HashMap::new();
    for triple_pattern in pattern {
        let mut matched = false;
        for triple in event_triples {
            let mut b = bindings.clone();
            if matches_rule_pattern(triple_pattern, triple, &mut b) {
                bindings = b;
                matched = true;
                break;
            }
        }
        if !matched { return None; }
    }
    Some(bindings)
}
