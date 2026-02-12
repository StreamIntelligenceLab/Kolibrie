/*
 * Copyright © 2025 Volodymyr Khadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[cfg(not(test))]
use log::{debug, warn}; // Use log crate when building application
use std::collections::hash_set::{IntoIter, Iter};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::{f64, mem};
#[cfg(test)]
use std::{println as warn, println as debug};

#[derive(Clone, Debug)]
pub enum ReportStrategy {
    NonEmptyContent,
    OnContentChange,
    OnWindowClose,
    Periodic(usize),
}
impl Default for ReportStrategy {
    fn default() -> Self {
        ReportStrategy::OnWindowClose
    }
}
#[derive(Clone, Debug)]
pub enum Tick {
    TimeDriven,
    TupleDriven,
    BatchDriven,
}
impl Default for Tick {
    fn default() -> Self {
        Tick::TimeDriven
    }
}

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
    pub fn new() -> Report<I> {
        Report {
            strategies: Vec::new(),
            last_change: ContentContainer::new(),
        }
    }
    pub fn add(&mut self, strategy: ReportStrategy) {
        self.strategies.push(strategy);
    }
    pub fn report(&mut self, window: &Window, content: &ContentContainer<I>, ts: usize) -> bool {
        self.strategies.iter().all(|strategy| match strategy {
            ReportStrategy::NonEmptyContent => content.len() > 0,
            ReportStrategy::OnContentChange => {
                let comp = content.eq(&self.last_change);
                self.last_change = content.clone();
                comp
            }
            ReportStrategy::OnWindowClose => window.close < ts,
            ReportStrategy::Periodic(period) => ts % period == 0,
        })
    }
}

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub struct Window {
    open: usize,
    close: usize,
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct ContentContainer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    elements: HashSet<I>,
    last_timestamp_changed: usize,
    origin: String
}

impl<I> ContentContainer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    fn new() -> ContentContainer<I> {
        ContentContainer {
            elements: HashSet::new(),
            last_timestamp_changed: 0,
            origin: String::default()
        }
    }
    fn new_with_origin(origin : &str) -> ContentContainer<I> {
        ContentContainer {
            elements: HashSet::new(),
            last_timestamp_changed: 0,
            origin: origin.to_string()
        }
    }
    pub fn len(&self) -> usize {
        self.elements.len()
    }
    fn add(&mut self, triple: I, ts: usize) {
        self.elements.insert(triple);
        self.last_timestamp_changed = ts;
    }
    pub fn get_last_timestamp_changed(&self) -> usize {
        self.last_timestamp_changed
    }

    pub fn iter(&self) -> Iter<'_, I> {
        self.elements.iter()
    }
    pub fn into_iter(mut self) -> IntoIter<I> {
        let map = mem::take(&mut self.elements);
        map.into_iter()
    }
}

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

impl<I> CSPARQLWindow<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
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
                if window.open <= event_time && event_time <= window.close {
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
    pub fn register(&mut self) -> Receiver<ContentContainer<I>> {
        let (send, recv) = channel::<ContentContainer<I>>();
        self.consumer.replace(send);
        recv
    }
    pub fn register_callback(
        &mut self,
        function: Box<dyn FnMut(ContentContainer<I>) -> () + Send + 'static>,
    ) {
        self.call_back.replace(function);
    }
    pub fn stop(&mut self) {
        self.consumer.take();
    }
}

#[allow(dead_code)]
struct ConsumerInner<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    data: Mutex<Vec<ContentContainer<I>>>,
}

#[allow(dead_code)]
struct Consumer<I>
where
    I: Eq + PartialEq + Clone + Debug + Hash + Send,
{
    inner: Arc<ConsumerInner<I>>,
}

#[allow(dead_code)]
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
#[derive(Eq, PartialEq, Clone, Debug, Hash)]
pub struct WindowTriple {
    pub s: String,
    pub p: String,
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
        assert_eq!(5, consumer.len());
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
        assert_eq!(5, data_clone.lock().unwrap().len());
    }
}