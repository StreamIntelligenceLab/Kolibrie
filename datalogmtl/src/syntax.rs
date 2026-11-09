/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The rule and operator vocabulary: what a DatalogMTL program can say

use shared::terms::TriplePattern;

/// Interval [start, end] in milliseconds, representing a range into the past
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Interval {
    /// Inclusive bound nearer the evaluation point
    pub start: u64,
    /// Inclusive bound further from it
    pub end: u64,
}

impl Interval {
    /// Returns true if offset_ms falls within [start, end]
    pub fn contains_offset(&self, offset_ms: u64) -> bool {
        offset_ms >= self.start && offset_ms <= self.end
    }

    /// Given current time t, return the absolute time range [t-end, t-start]
    pub fn absolute_range(&self, t: u64) -> (u64, u64) {
        let lo = t.saturating_sub(self.end);
        let hi = t.saturating_sub(self.start);
        (lo, hi)
    }
}

/// A temporal atom: a triple pattern optionally wrapped in a past metric operator
#[derive(Debug, Clone)]
pub enum TemporalAtom {
    /// Base(?s, ?p, ?o): plain triple pattern at current time t
    Base(TriplePattern),

    /// `Diamond[a,b]` phi: phi holds at SOME t' in [t-b, t-a]
    Diamond {
        /// How far back to look
        interval: Interval,
        /// What to look for
        inner: Box<TemporalAtom>,
    },

    /// `Box[a,b]` phi: phi holds at EVERY integer point t' in [t-b, t-a] (dense semantics — an
    /// integer point with no supporting fact fails Box)
    Box_ {
        /// Window that must be satisfied throughout
        interval: Interval,
        /// What must hold at every point of it
        inner: Box<TemporalAtom>,
    },

    /// `Prev[a,b]` phi: phi holds at the MOST RECENT t' in [t-b, t-a]
    Prev {
        /// Window to search backwards through
        interval: Interval,
        /// What to find the most recent occurrence of
        inner: Box<TemporalAtom>,
    },

    /// phi `Since[a,b]` psi: EXISTS t' in [t-b, t-a]: psi holds at t' AND FORALL t'' in (t', t]:
    /// phi holds at t''
    Since {
        /// Window in which the trigger must have occurred
        interval: Interval,
        /// Continuation condition, required at every point since the trigger
        phi: Box<TemporalAtom>,
        /// Trigger condition, whose occurrence starts the interval
        psi: Box<TemporalAtom>,
    },

    /// `DiamondPlus[a,b]` phi: phi holds at SOME t' in [t+a, t+b]
    DiamondPlus {
        /// How far forward to look
        interval: Interval,
        /// What to look for
        inner: Box<TemporalAtom>,
    },

    /// `BoxPlus[a,b]` phi: phi holds at EVERY integer point t' in [t+a, t+b]
    BoxPlus {
        /// Forward window that must be satisfied throughout
        interval: Interval,
        /// What must hold at every point of it
        inner: Box<TemporalAtom>,
    },

    /// phi `Until[a,b]` psi: EXISTS t' in [t+a, t+b]: psi holds at t' AND FORALL t'' in [t, t'):
    /// phi holds at t''
    Until {
        /// Window in which the trigger must occur
        interval: Interval,
        /// Continuation condition, required at every point until the trigger
        phi: Box<TemporalAtom>,
        /// Trigger condition, whose occurrence ends the interval
        psi: Box<TemporalAtom>,
    },

    /// Conjunction of atoms, all evaluated at the SAME time point
    Conj(Vec<TemporalAtom>),
}

/// Evaluation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Incremental data; only past/present operators allowed
    Streaming,
    /// Whole timeline known; past and future operators allowed
    Static,
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Streaming
    }
}

/// A DatalogMTL^RDF rule
#[derive(Debug, Clone)]
pub struct DatalogMTLRule {
    /// Identifier used in validation errors and metrics
    pub id: String,
    /// Conclusion, derived at the time point being evaluated
    pub head: TriplePattern,
    /// Premises, evaluated left to right with bindings threaded through, so a later atom sees what
    /// the earlier ones bound
    pub body: Vec<TemporalAtom>,
}
