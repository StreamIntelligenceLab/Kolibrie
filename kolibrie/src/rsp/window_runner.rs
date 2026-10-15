/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Driving one window's S2R to R2R cycle.
//!
//! Owns a window together with the operator that evaluates over it, so the engine
//! can hold several independently and coordinate only where they must join.

use crate::rsp::s2r::{
    CSPARQLWindow, ContentContainer, ProbabilisticOccurrence, Report, ReportStrategy, Tick,
};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::Receiver;

pub use crate::rsp::s2r::WindowTriple;

#[derive(Debug, Clone)]
/// A window's configuration, before it is instantiated.
pub struct WindowSpec {
    /// Width in time units. Bounds are half-open, so this many timestamps.
    pub width: usize,
    /// How far the window advances each step. Equal to `width` makes it tumbling.
    pub slide: usize,
    /// Conditions for reporting, combined with AND.
    pub report_strategies: Vec<ReportStrategy>,
    /// What advances the window's clock.
    pub tick: Tick,
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self {
            width: 100,
            slide: 10,
            report_strategies: vec![ReportStrategy::OnWindowClose],
            tick: Tick::TimeDriven,
        }
    }
}

/// One window together with the plumbing that moves its content onward.
pub struct WindowRunner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    inner: CSPARQLWindow<I>,
    receiver: Option<Receiver<ContentContainer<I>>>,
}

// Summarised rather than derived: the wrapped window holds the buffered content, and
// whether a receiver is still attached is the state a reader is usually after.
impl<I> Debug for WindowRunner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowRunner")
            .field("inner", &self.inner)
            .field("attached", &self.receiver.is_some())
            .finish()
    }
}

impl<I> WindowRunner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    /// A runner for a window built from `spec`, identified by `uri`.
    pub fn new(spec: WindowSpec, uri: String) -> Self {
        let mut report = Report::new();
        for strategy in spec.report_strategies {
            report.add(strategy);
        }
        Self {
            inner: CSPARQLWindow::new(spec.width, spec.slide, report, spec.tick, uri),
            receiver: None,
        }
    }

    /// Begins buffering this window's reports so [`Self::drain`] can collect them.
    pub fn start_receiver(&mut self) {
        if self.receiver.is_none() {
            self.receiver = Some(self.inner.register());
        }
    }

    /// Adds an item at `ts`. Equivalent to [`Self::add_to_window`].
    pub fn push(&mut self, item: I, ts: usize) {
        self.inner.add_to_window(item, ts);
    }

    /// Takes every report buffered since the last drain, leaving none behind.
    ///
    /// Requires [`Self::start_receiver`] first; without it nothing is buffered and
    /// this always returns empty.
    pub fn drain(&mut self) -> Vec<ContentContainer<I>> {
        let mut out = Vec::new();
        if let Some(rx) = &self.receiver {
            while let Ok(c) = rx.try_recv() {
                out.push(c);
            }
        }
        out
    }

    /// Adds a certain item at `ts`.
    pub fn add_to_window(&mut self, item: I, ts: usize) {
        self.inner.add_to_window(item, ts);
    }

    /// Adds an uncertain arrival, preserving its seed identity.
    pub fn add_probabilistic_to_window(&mut self, occurrence: ProbabilisticOccurrence<I>) {
        self.inner.add_probabilistic_to_window(occurrence);
    }

    /// A channel receiving this window's reports.
    pub fn register(&mut self) -> Receiver<ContentContainer<I>> {
        self.inner.register()
    }

    /// Installs a callback invoked on each report.
    pub fn register_callback(
        &mut self,
        f: Box<dyn FnMut(ContentContainer<I>) -> () + Send + 'static>,
    ) {
        self.inner.register_callback(f);
    }

    /// Reports every active window's merged content immediately, ignoring the
    /// report strategies. See [`CSPARQLWindow::flush`].
    pub fn flush(&mut self) {
        self.inner.flush();
    }

    /// Closes the output channel.
    pub fn stop(&mut self) {
        self.inner.stop();
    }
}
