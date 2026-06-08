/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use shared::terms::TriplePattern;

/// Interval [start, end] in milliseconds, representing a range into the past.
/// start=0, end=0 means exactly now.
/// start=1000, end=5000 means "between 1 and 5 seconds ago."
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Interval {
    pub start: u64,   // inclusive lower bound, ms into past
    pub end:   u64,   // inclusive upper bound, ms into past
}

impl Interval {
    /// Returns true if offset_ms falls within [start, end].
    pub fn contains_offset(&self, offset_ms: u64) -> bool {
        offset_ms >= self.start && offset_ms <= self.end
    }

    /// Given current time t, return the absolute time range [t-end, t-start].
    pub fn absolute_range(&self, t: u64) -> (u64, u64) {
        let lo = t.saturating_sub(self.end);
        let hi = t.saturating_sub(self.start);
        (lo, hi)
    }
}

/// A temporal atom: a triple pattern optionally wrapped in a past metric operator.
/// All operators are past-only (forward-propagating fragment, suitable for streaming).
#[derive(Debug, Clone)]
pub enum TemporalAtom {
    /// Base(?s, ?p, ?o): plain triple pattern at current time t.
    Base(TriplePattern),

    /// Diamond[a,b] phi: phi holds at SOME t' in [t-b, t-a].
    Diamond { interval: Interval, inner: Box<TemporalAtom> },

    /// Box[a,b] phi: phi holds at EVERY active t' in [t-b, t-a].
    /// Vacuously true if no facts exist in the range.
    Box_ { interval: Interval, inner: Box<TemporalAtom> },

    /// Prev[a,b] phi: phi holds at the MOST RECENT t' in [t-b, t-a].
    Prev { interval: Interval, inner: Box<TemporalAtom> },

    /// phi Since[a,b] psi:
    ///   EXISTS t' in [t-b, t-a]: psi holds at t'
    ///   AND FORALL t'' in (t', t]: phi holds at t''
    Since {
        interval: Interval,
        phi: Box<TemporalAtom>,   // continuation condition (holds after reset)
        psi: Box<TemporalAtom>,   // reset/trigger condition
    },
}

/// A DatalogMTL^RDF rule. Head is always a plain triple pattern.
/// Body is a conjunction of TemporalAtoms evaluated left to right.
#[derive(Debug, Clone)]
pub struct DatalogMTLRule {
    pub id:   String,
    pub head: TriplePattern,
    pub body: Vec<TemporalAtom>,
}
