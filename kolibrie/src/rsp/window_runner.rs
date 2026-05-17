/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::rsp::s2r::{CSPARQLWindow, ContentContainer, Report, ReportStrategy, Tick};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::Receiver;

pub use crate::rsp::s2r::WindowTriple;

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub width: usize,
    pub slide: usize,
    pub report_strategies: Vec<ReportStrategy>,
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

pub struct WindowRunner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    inner: CSPARQLWindow<I>,
    receiver: Option<Receiver<ContentContainer<I>>>,
}

impl<I> WindowRunner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
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

    pub fn start_receiver(&mut self) {
        if self.receiver.is_none() {
            self.receiver = Some(self.inner.register());
        }
    }

    pub fn push(&mut self, item: I, ts: usize) {
        self.inner.add_to_window(item, ts);
    }

    pub fn drain(&mut self) -> Vec<ContentContainer<I>> {
        let mut out = Vec::new();
        if let Some(rx) = &self.receiver {
            while let Ok(c) = rx.try_recv() {
                out.push(c);
            }
        }
        out
    }

    pub fn add_to_window(&mut self, item: I, ts: usize) {
        self.inner.add_to_window(item, ts);
    }

    pub fn register(&mut self) -> Receiver<ContentContainer<I>> {
        self.inner.register()
    }

    pub fn register_callback(
        &mut self,
        f: Box<dyn FnMut(ContentContainer<I>) -> () + Send + 'static>,
    ) {
        self.inner.register_callback(f);
    }

    pub fn flush(&mut self) {
        self.inner.flush();
    }

    pub fn stop(&mut self) {
        self.inner.stop();
    }
}
