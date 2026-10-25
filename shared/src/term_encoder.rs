/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`TermEncoder`]: reading and interning terms without naming how they are shared.
//!
//! Term ids are only meaningful against the [`Dictionary`] that minted them, so
//! anything holding its own term space — a store on disk, a window's content, a
//! second engine — has to intern through the consumer's dictionary. Today that is
//! done by hand: callers assign `Arc<RwLock<Dictionary>>` from one component to
//! another and hope the two stay aligned. This is the type that lets the arrangement
//! be stated rather than remembered.
//!
//! # Scoped, not per-call, and that is the whole design
//!
//! [`with_terms`](TermEncoder::with_terms) lends the dictionary for the duration of a
//! closure and acquires at most once. There is deliberately no `encode(&self, &str)`
//! convenience method, because `benches/contended_dictionary.rs` measured what that
//! shape costs once more than one thread uses it:
//!
//! | threads | encode, per-call lock | decode, per-call lock |
//! |---|---:|---:|
//! | 2 | 1.6x slower than one thread | 3.3x slower |
//! | 4 | 2.2x slower | 5.4x slower |
//! | 8 | 3.9x slower | 12.3x slower |
//!
//! Per-call locking does not merely fail to scale; it is slower than a single
//! thread as soon as there are two, because each acquisition does an atomic
//! read-modify-write on one cache line and the acquisitions ping-pong between
//! cores. Reads suffer more than writes, since the operation they guard is smaller.
//!
//! A per-call method would therefore be the obvious thing to reach for and the wrong
//! one, so it is not offered. Batch the work inside one `with_terms` instead — which
//! is what the 125 call sites that hold a guard across several operations already do
//! by hand.
//!
//! # Two modes
//!
//! [`LocalTerms`] holds the dictionary outright and takes no lock, for a single
//! thread. [`SharedTerms`] wraps the `Arc<RwLock<_>>` the rest of the workspace
//! already passes around, for the case the `rsp_engine` creates when it spawns a
//! thread per window plus a coordinator.
//!
//! Only `SharedTerms` is `Sync`, which is the point rather than a limitation: a
//! field that crosses threads will not accept the local one, so the choice is
//! checked at compile time instead of being a convention.

use crate::dictionary::Dictionary;
use std::cell::RefCell;
use std::sync::{Arc, RwLock};

/// Reading and interning terms, without naming how they are stored or shared.
///
/// Object-safe, so heterogeneous encoders can sit behind one `&dyn TermEncoder`.
/// `with_terms` returns nothing for that reason — a generic return would not be —
/// so a closure that needs a result captures it.
pub trait TermEncoder {
    /// Lends the dictionary for the duration of `visit`, acquiring at most once.
    ///
    /// Do as much as possible inside one call. See the module documentation for the
    /// measured cost of the alternative.
    fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary));
}

/// A dictionary owned outright, with no lock, for a single thread.
///
/// Not `Sync`, deliberately: anything that crosses a thread boundary will reject it
/// and want [`SharedTerms`].
#[derive(Debug, Default)]
pub struct LocalTerms(RefCell<Dictionary>);

impl LocalTerms {
    /// An empty encoder.
    pub fn new() -> Self {
        Self(RefCell::new(Dictionary::new()))
    }

    /// Adopts an existing dictionary.
    pub fn from_dictionary(dictionary: Dictionary) -> Self {
        Self(RefCell::new(dictionary))
    }

    /// Takes the dictionary back out, to hand to something that wants it shared.
    pub fn into_dictionary(self) -> Dictionary {
        self.0.into_inner()
    }
}

impl TermEncoder for LocalTerms {
    fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary)) {
        visit(&mut self.0.borrow_mut());
    }
}

/// A dictionary shared across threads, behind the handle the workspace already uses.
///
/// Wraps rather than replaces `Arc<RwLock<Dictionary>>`, so it can be handed an
/// existing handle and keep one term space across a store, a reasoner and the
/// engine's window threads.
#[derive(Debug, Clone)]
pub struct SharedTerms(Arc<RwLock<Dictionary>>);

impl SharedTerms {
    /// An empty encoder, shareable.
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(Dictionary::new())))
    }

    /// Shares an existing handle, so this encoder and its current holders agree.
    pub fn from_handle(handle: Arc<RwLock<Dictionary>>) -> Self {
        Self(handle)
    }

    /// The underlying handle, for the components that still take one directly.
    ///
    /// This is the seam: while `Reasoner::dictionary` and `SparqlDatabase` are typed
    /// in terms of the handle, an encoder has to be able to produce it. It is the
    /// reason this type is a wrapper rather than a replacement.
    pub fn handle(&self) -> Arc<RwLock<Dictionary>> {
        Arc::clone(&self.0)
    }
}

impl Default for SharedTerms {
    fn default() -> Self {
        Self::new()
    }
}

impl TermEncoder for SharedTerms {
    fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary)) {
        visit(&mut self.0.write().unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_modes_agree_on_the_ids_they_mint() {
        let local = LocalTerms::new();
        let shared = SharedTerms::new();
        let mut from_local = Vec::new();
        let mut from_shared = Vec::new();
        for encoder in [&local as &dyn TermEncoder, &shared as &dyn TermEncoder] {
            encoder.with_terms(&mut |dict| {
                let ids: Vec<u32> = ["a", "b", "a", "c"]
                    .iter()
                    .map(|t| dict.encode(t))
                    .collect();
                if from_local.is_empty() {
                    from_local = ids;
                } else {
                    from_shared = ids;
                }
            });
        }
        assert_eq!(from_local, from_shared);
        assert_eq!(
            from_local[0], from_local[2],
            "a repeated term reuses its id"
        );
        assert_ne!(from_local[0], from_local[1]);
    }

    #[test]
    fn a_shared_encoder_keeps_one_term_space_with_its_handle() {
        let shared = SharedTerms::new();
        let handle = shared.handle();
        let mut id = 0;
        shared.with_terms(&mut |dict| id = dict.encode("urn:x"));
        // The handle another component already holds decodes what this encoder minted.
        assert_eq!(handle.read().unwrap().decode(id), Some("urn:x"));
    }

    #[test]
    fn a_local_encoder_can_be_handed_over_to_be_shared() {
        let local = LocalTerms::new();
        let mut id = 0;
        local.with_terms(&mut |dict| id = dict.encode("urn:y"));
        let shared = SharedTerms::from_handle(Arc::new(RwLock::new(local.into_dictionary())));
        shared.with_terms(&mut |dict| assert_eq!(dict.decode(id), Some("urn:y")));
    }
}
