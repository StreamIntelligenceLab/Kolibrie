/*
 * Copyright © 2025 Volodymyr Khadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! S2R: cutting a stream into finite relations.
//!
//! The stream-to-relation stage, implemented by [`CSPARQLWindow`]. A window
//! accumulates arriving triples and, when it fires, hands its content to the R2R
//! stage.
//!
//! Bounds are half-open: a `RANGE n` window holds `n` timestamps, not `n + 1`.
//!
//! [`ReportStrategy`] decides *when* a window fires and [`Tick`] decides what makes
//! time advance — arriving data or the wall clock. Those two together, not the width
//! alone, determine what a consumer sees.

#[cfg(not(test))]
use log::{debug, warn}; // Use log crate when building application
use std::collections::hash_map::{IntoKeys, Keys};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use shared::hybrid::{EventKey, SeedId};
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{channel, Sender};
// Only the `#[cfg(test)]` Consumer harness below needs these.
#[cfg(test)]
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::thread;
use std::{f64, mem};
#[cfg(test)]
use std::{println as warn, println as debug};

/// When a window reports its content downstream.
///
/// Several strategies combine with **AND**, not OR: a window with both
/// `NonEmptyContent` and `OnWindowClose` reports only when both hold. Adding a
/// strategy therefore makes a window report less often, never more.
#[derive(Clone, Debug)]
pub enum ReportStrategy {
    /// Only when the window holds something.
    NonEmptyContent,
    /// Only when the content differs from the last report.
    OnContentChange,
    /// Only once the window's close time has passed. The default.
    OnWindowClose,
    /// Every `n` time units, regardless of content.
    Periodic(usize),
}
impl Default for ReportStrategy {
    fn default() -> Self {
        ReportStrategy::OnWindowClose
    }
}
/// What makes the window's clock advance.
///
/// Together with [`ReportStrategy`] this decides what a consumer sees; the width
/// alone does not. A time-driven window can fire with nothing in it, while a
/// tuple-driven one cannot advance at all until data arrives.
#[derive(Clone, Debug)]
pub enum Tick {
    /// Time advances with the clock. The default.
    TimeDriven,
    /// Time advances only as tuples arrive.
    TupleDriven,
    /// Time advances per batch of arrivals.
    BatchDriven,
}
impl Default for Tick {
    fn default() -> Self {
        Tick::TimeDriven
    }
}

/// The set of [`ReportStrategy`] conditions a window must satisfy to fire.
#[derive(Debug)]
pub struct Report<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    strategies: Vec<ReportStrategy>,
    last_change: ContentContainer<I>,
}

impl<I> Report<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    /// A report with no strategies, which reports unconditionally — the `all`
    /// over an empty set is true.
    pub fn new() -> Report<I> {
        Report {
            strategies: Vec::new(),
            last_change: ContentContainer::new(),
        }
    }
    /// Adds a condition. Conditions combine with AND, so each one added can only
    /// make the window report less often.
    pub fn add(&mut self, strategy: ReportStrategy) {
        self.strategies.push(strategy);
    }
    /// Whether every condition holds for this window and content.
    ///
    /// Not a pure predicate: `OnContentChange` records the content it saw, so
    /// calling this twice with the same content gives different answers.
    pub fn report(&mut self, window: &Window, content: &ContentContainer<I>, ts: usize) -> bool {
        self.strategies.iter().all(|strategy| match strategy {
            ReportStrategy::NonEmptyContent => content.len() > 0,
            ReportStrategy::OnContentChange => {
                let comp = content.eq(&self.last_change);
                self.last_change = content.clone();
                comp
            }
            ReportStrategy::OnWindowClose => window.close <= ts,
            ReportStrategy::Periodic(period) => ts % period == 0,
        })
    }
}

/// One window's extent, half-open as `[open, close)`.
///
/// Half-open is why a `RANGE n` window holds `n` timestamps rather than `n + 1`.
#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub struct Window {
    open: usize,
    close: usize,
}

/// One arrival of an uncertain item, carrying the seed identity it was registered
/// under.
///
/// The seed id travels with the occurrence because the same arrival may land in
/// several overlapping windows, and it must stay one seed — counting the same
/// uncertainty twice would inflate every probability derived from it.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ProbabilisticOccurrence<I> {
    /// The item itself.
    pub item: I,
    /// Which arrival this was.
    pub event: EventKey,
    /// The seed registered for that arrival.
    pub seed_id: SeedId,
}

/// A window's content: the items in it and when each was last seen.
///
/// Deterministic and probabilistic items are tracked separately, because a
/// probabilistic one needs its seed identity preserved and a deterministic one does
/// not. An item re-added at a later timestamp keeps the later one.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ContentContainer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    elements: HashMap<I, usize>,
    deterministic_items: HashSet<I>,
    probabilistic_occurrences: Vec<ProbabilisticOccurrence<I>>,
    last_timestamp_changed: usize,
    origin: String
}

impl<I> ContentContainer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    fn new() -> ContentContainer<I> {
        ContentContainer {
            elements: HashMap::new(),
            deterministic_items: HashSet::new(),
            probabilistic_occurrences: Vec::new(),
            last_timestamp_changed: 0,
            origin: String::default()
        }
    }
    fn new_with_origin(origin : &str) -> ContentContainer<I> {
        ContentContainer {
            elements: HashMap::new(),
            deterministic_items: HashSet::new(),
            probabilistic_occurrences: Vec::new(),
            last_timestamp_changed: 0,
            origin: origin.to_string()
        }
    }
    /// Number of distinct items, counting an item re-added at several timestamps
    /// once.
    pub fn len(&self) -> usize {
        self.elements.len()
    }
    fn add(&mut self, triple: I, ts: usize) {
        self.deterministic_items.insert(triple.clone());
        self.add_element(triple, ts);
    }
    fn add_element(&mut self, triple: I, ts: usize) {
        self.elements
            .entry(triple)
            .and_modify(|last_ts| *last_ts = (*last_ts).max(ts))
            .or_insert(ts);
        self.last_timestamp_changed = ts;
    }
    fn add_probabilistic(&mut self, occurrence: ProbabilisticOccurrence<I>) {
        self.add_element(occurrence.item.clone(), occurrence.event.event_time);
        if !self.probabilistic_occurrences.iter().any(|existing| existing.seed_id == occurrence.seed_id) {
            self.probabilistic_occurrences.push(occurrence);
        }
    }
    /// Timestamp of the most recent addition.
    pub fn get_last_timestamp_changed(&self) -> usize {
        self.last_timestamp_changed
    }

    /// The items, without their timestamps.
    pub fn iter(&self) -> Keys<'_, I, usize> {
        self.elements.keys()
    }
    /// The items paired with the last timestamp each was seen at.
    pub fn iter_with_timestamps(&self) -> impl Iterator<Item = (&I, usize)> {
        self.elements.iter().map(|(item, ts)| (item, *ts))
    }
    /// The uncertain arrivals in this window, with their seed identities.
    pub fn probabilistic_occurrences(&self) -> &[ProbabilisticOccurrence<I>] {
        &self.probabilistic_occurrences
    }
    /// Whether `item` arrived as a certain fact rather than a probabilistic one.
    pub fn is_deterministic(&self, item: &I) -> bool {
        self.deterministic_items.contains(item)
    }
    /// Consumes the container, yielding its items.
    pub fn into_iter(mut self) -> IntoKeys<I, usize> {
        let map = mem::take(&mut self.elements);
        map.into_keys()
    }
}

/// A sliding window over a stream: the S2R implementation.
///
/// Holds every currently-active window, so one arrival can be added to several at
/// once when they overlap. Reports through either a channel or a callback, whichever
/// the caller installed.
pub struct CSPARQLWindow<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    width: usize,
    slide: usize,
    t_0: usize,
    active_windows: HashMap<Window, ContentContainer<I>>,
    report: Report<I>,
    tick: Tick,
    app_time: usize,
    consumer: Option<Sender<ContentContainer<I>>>,
    // Make callbacks Send so they can be safely transferred to worker threads
    call_back: Option<Box<dyn FnMut(ContentContainer<I>) -> () + Send + 'static>>,
    uri: String
}


// Cannot derive: `call_back` is a `dyn FnMut`. Summarised besides, since the active
// windows hold the buffered stream content.
impl<I> Debug for CSPARQLWindow<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CSPARQLWindow")
            .field("uri", &self.uri)
            .field("width", &self.width)
            .field("slide", &self.slide)
            .field("t_0", &self.t_0)
            .field("app_time", &self.app_time)
            .field("active_windows", &self.active_windows.len())
            .field("tick", &self.tick)
            .field("has_consumer", &self.consumer.is_some())
            .field("has_callback", &self.call_back.is_some())
            .finish_non_exhaustive()
    }
}

impl<I> CSPARQLWindow<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    /// A window of `width`, advancing by `slide`.
    ///
    /// `slide == width` makes it tumbling; a smaller slide makes it overlap, so an
    /// arrival will appear in more than one window.
    pub fn new(width: usize, slide: usize, report: Report<I>, tick: Tick, uri: String) -> CSPARQLWindow<I> {
        CSPARQLWindow {
            slide,
            width,
            t_0: 0,
            app_time: 0,
            report,
            consumer: None,
            active_windows: HashMap::new(),
            tick,
            call_back: None,
            uri
        }
    }
    /// Adds a certain item at `ts`, to every active window whose extent contains
    /// it.
    pub fn add_to_window(&mut self, event_item: I, ts: usize) {
        let event_time = ts;
        self.scope(&event_time);

        let test = self
            .active_windows
            .clone()
            .into_iter()
            .filter_map(|(window, mut content)| {
                debug!(
                    "Processing Window [{:?}, {:?}) for element ({:?},{:?})",
                    window.open, window.close, event_item, ts
                );
                if window.open <= event_time && event_time < window.close {
                    debug!(
                        "Adding element [{:?}] to Window [{:?},{:?})",
                        event_item, window.open, window.close
                    );
                    content.add(event_item.clone(), ts);
                    Some((window, content))
                } else {
                    debug!(
                        "Scheduling for Eviction [{:?},{:?})",
                        window.open, window.close
                    );
                    None
                }
            })
            .collect::<HashMap<Window, ContentContainer<I>>>();

        let max = self
            .active_windows
            .iter()
            .filter(|(window, content)| self.report.report(window, content, ts))
            .max_by(|(w1, _), (w2, _)| w1.close.cmp(&w2.close));
        if let Some(max_window) = max {
            match self.tick {
                Tick::TimeDriven => {
                    if ts > self.app_time {
                        self.app_time = ts;
                        // notify consumers
                        debug!("Window triggers! {:?}", max_window);
                        // multithreaded consumer using channel
                        if let Some(sender) = &self.consumer {
                            if let Err(e) = sender.send(max_window.1.clone()) {
                                warn!("Failed to send window content to consumer: {:?}", e);
                            }
                        }
                        // single threaded consumer using callback
                        if let Some(call_back) = &mut self.call_back {
                            (call_back)(max_window.1.clone());
                        }
                    }
                }
                _ => (),
            };
        }

        self.active_windows = test;
    }
    /// Adds an uncertain arrival, preserving its seed identity across every
    /// overlapping window it lands in.
    pub fn add_probabilistic_to_window(&mut self, occurrence: ProbabilisticOccurrence<I>) {
        let event_time = occurrence.event.event_time;
        self.scope(&event_time);
        let updated = self.active_windows.clone().into_iter().filter_map(|(window, mut content)| {
            if window.open <= event_time && event_time < window.close {
                content.add_probabilistic(occurrence.clone());
                Some((window, content))
            } else {
                None
            }
        }).collect::<HashMap<Window, ContentContainer<I>>>();

        let max = self.active_windows.iter()
            .filter(|(window, content)| self.report.report(window, content, event_time))
            .max_by(|(left, _), (right, _)| left.close.cmp(&right.close));
        if let Some(max_window) = max {
            if matches!(self.tick, Tick::TimeDriven) && event_time > self.app_time {
                self.app_time = event_time;
                if let Some(sender) = &self.consumer {
                    let _ = sender.send(max_window.1.clone());
                }
                if let Some(callback) = &mut self.call_back {
                    callback(max_window.1.clone());
                }
            }
        }
        self.active_windows = updated;
    }
    fn scope(&mut self, event_time: &usize) {
        // long c_sup = (long) Math.ceil(((double) Math.abs(t_e - t0) / (double) slide)) * slide;
        let _temp = (*event_time as f64 - self.t_0 as f64).abs();
        let _temp = ((*event_time as f64 - self.t_0 as f64).abs() / (self.slide as f64)).ceil();
        let c_sup = ((*event_time as f64 - self.t_0 as f64).abs() / (self.slide as f64)).ceil()
            * self.slide as f64;
        // long o_i = c_sup - width;
        let mut o_i = c_sup - self.width as f64;
        debug!(
            "Calculating the Windows to Open. First one opens at [{:?}] and closes at [{:?}]",
            o_i, c_sup
        );
        // log.debug("Calculating the Windows to Open. First one opens at [" + o_i + "] and closes at [" + c_sup + "]");
        //
        loop {
            debug!(
                "Computing Window [{:?},{:?}) if absent",
                o_i,
                (o_i + self.width as f64)
            );
            let window = Window {
                open: o_i as usize,
                close: (o_i + self.width as f64) as usize,
            };
            if let None = self.active_windows.get(&window) {
                self.active_windows.insert(window, ContentContainer::new_with_origin(&self.uri));
            }
            o_i += self.slide as f64;
            if o_i > *event_time as f64 {
                break;
            }
        }
    }
    /// Returns a channel receiving this window's reports.
    ///
    /// Replaces any previously registered channel. [`Self::stop`] drops it.
    pub fn register(&mut self) -> Receiver<ContentContainer<I>> {
        let (send, recv) = channel::<ContentContainer<I>>();
        self.consumer.replace(send);
        recv
    }
    /// Installs a callback invoked on each report, as an alternative to
    /// [`Self::register`]. Both may be active at once.
    pub fn register_callback(
        &mut self,
        function: Box<dyn FnMut(ContentContainer<I>) -> () + Send + 'static>,
    ) {
        self.call_back.replace(function);
    }
    /// Reports the merged content of every active window immediately, ignoring the
    /// report strategies.
    ///
    /// This is how a single-threaded engine gets results out without waiting for
    /// windows to close. Merging means an overlapping arrival appears once, but the
    /// result mixes windows that would normally have fired separately.
    /// Emits nothing when the merged content is empty.
    pub fn flush(&mut self) {
        let mut merged = ContentContainer::new_with_origin(&self.uri);
        for content in self.active_windows.values() {
            for occurrence in content.probabilistic_occurrences() {
                merged.add_probabilistic(occurrence.clone());
            }
            for (item, ts) in content.iter_with_timestamps() {
                if content.is_deterministic(item) {
                    merged.add(item.clone(), ts);
                }
            }
        }

        if merged.len() > 0 {
            if let Some(call_back) = &mut self.call_back {
                (call_back)(merged.clone());
            }
            if let Some(sender) = &self.consumer {
                let _ = sender.send(merged);
            }
        }
    }
    /// Drops the output channel, so the receiver sees the stream end.
    ///
    /// Does not stop a registered callback.
    pub fn stop(&mut self) {
        self.consumer.take();
    }
}

// Test-only: drains a window's output channel on a background thread so a test can
// assert on what the window emitted. Marked `#[cfg(test)]` rather than carrying an
// `#[allow(dead_code)]`, which had hidden the fact that nothing in production uses it.
#[cfg(test)]
struct ConsumerInner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    data: Mutex<Vec<ContentContainer<I>>>,
}

#[cfg(test)]
struct Consumer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    inner: Arc<ConsumerInner<I>>,
}

#[cfg(test)]
impl<I> Consumer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send + 'static,
{
    fn new() -> Consumer<I> {
        Consumer {
            inner: Arc::new(ConsumerInner {
                data: Mutex::new(Vec::new()),
            }),
        }
    }
    fn start(&self, receiver: Receiver<ContentContainer<I>>) {
        let consumer_temp = self.inner.clone();
        thread::spawn(move || loop {
            match receiver.recv() {
                Ok(content) => {
                    debug!("Found graph {:?}", content);
                    consumer_temp.data.lock().unwrap().push(content);
                }
                Err(_) => {
                    debug!("Shutting down!");
                    break;
                }
            }
        });
    }
    fn len(&self) -> usize {
        self.inner.data.lock().unwrap().len()
    }
}

/// A triple as it travels through a window, in decoded text form.
///
/// Text rather than dictionary ids because a window is not bound to any one
/// store's dictionary.
#[derive(Eq, PartialEq, Clone, Debug, Hash)]
pub struct WindowTriple {
    /// Subject.
    pub s: String,
    /// Predicate.
    pub p: String,
    /// Object.
    pub o: String,
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_window() {
        let mut report = Report::new();
        report.add(ReportStrategy::OnWindowClose);
        let mut window = CSPARQLWindow {
            width: 10,
            slide: 2,
            app_time: 0,
            t_0: 0,
            active_windows: HashMap::new(),
            report,
            tick: Tick::TimeDriven,
            consumer: None,
            call_back: None,
            uri: "test_window".to_string()
        };
        let receiver = window.register();
        let consumer = Consumer::new();
        consumer.start(receiver);

        for i in 0..10 {
            let triple = WindowTriple {
                s: format!("s{}", i),
                p: "p".to_string(),
                o: "o".to_string(),
            };
            window.add_to_window(triple, i);
        }

        window.stop();
        thread::sleep(Duration::from_secs(1));
        assert_eq!(4, consumer.len());
    }
    #[test]
    fn test_window_with_call_back() {
        let mut report = Report::new();
        report.add(ReportStrategy::OnWindowClose);
        let mut window = CSPARQLWindow {
            width: 10,
            slide: 2,
            app_time: 0,
            t_0: 0,
            active_windows: HashMap::new(),
            report,
            tick: Tick::TimeDriven,
            consumer: None,
            call_back: None,
            uri: "test_window".to_string()
        };
        let recieved_data = Arc::new(Mutex::new(Vec::new()));
        let data_clone = Arc::clone(&recieved_data);
        let call_back = move |content| {
            println!("Content: {:?}", content);
            recieved_data.lock().unwrap().push(content);
        };
        window.register_callback(Box::new(call_back));

        for i in 0..10 {
            let triple = WindowTriple {
                s: format!("s{}", i),
                p: "p".to_string(),
                o: "o".to_string(),
            };
            window.add_to_window(triple, i);
        }

        window.stop();
        assert_eq!(4, data_clone.lock().unwrap().len());
    }
}
