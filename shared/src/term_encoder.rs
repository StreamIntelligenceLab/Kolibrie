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
//! second engine — has to intern through the consumer's dictionary. That used to be
//! done by hand: callers assigned `Arc<RwLock<Dictionary>>` from one component to
//! another and hoped the two stayed aligned. These are the types that let the
//! arrangement be stated rather than remembered, and no signature in the workspace
//! names the handle any more.
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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

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
/// One term space, kept across a store, a reasoner and the engine's window threads.
/// Cloning it shares that space rather than copying it, which is how a component
/// adopts another's encoding; [`view`](Self::view) is the copy, and is a different
/// thing on purpose.
///
/// A lock behind a newtype, so no signature has to name one: reach the terms through
/// [`with_terms`](TermEncoder::with_terms),
/// [`with_terms_read`](Self::with_terms_read), [`read`](Self::read) or
/// [`write`](Self::write).
#[derive(Debug, Clone)]
pub struct SharedTerms(Arc<RwLock<Dictionary>>);

impl SharedTerms {
    /// An empty encoder, shareable.
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(Dictionary::new())))
    }

    /// Shares a term space that already has terms in it.
    ///
    /// For the callers that parse or load a dictionary first and only then decide who
    /// shares it. To share a space that is *already* shared, clone the encoder: both
    /// clones encode through the one space, which is the point of the type.
    pub fn from_dictionary(dictionary: Dictionary) -> Self {
        Self(Arc::new(RwLock::new(dictionary)))
    }

    /// Whether two encoders encode through the same space, as opposed to two spaces
    /// that happen to agree today.
    ///
    /// The property callers actually test when they ask whether two components are
    /// isolated. It compares identity, not contents: two separately built spaces
    /// holding identical terms are still not the same space, because the next term
    /// interned into either is invisible to the other.
    pub fn shares_space_with(&self, other: &SharedTerms) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Interns `text` and returns its id.
    ///
    /// The common case, named so a caller does not reach for the handle to do it.
    pub fn encode(&self, text: &str) -> u32 {
        let mut id = 0;
        self.with_terms(&mut |dictionary| id = dictionary.encode(text));
        id
    }

    /// Interns three terms under one lock, which is what a triple needs.
    pub fn encode_triple(&self, subject: &str, predicate: &str, object: &str) -> (u32, u32, u32) {
        let mut ids = (0, 0, 0);
        self.with_terms(&mut |dictionary| {
            ids = (
                dictionary.encode(subject),
                dictionary.encode(predicate),
                dictionary.encode(object),
            );
        });
        ids
    }

    /// The string `id` stands for, if this term space knows it.
    pub fn decode(&self, id: u32) -> Option<String> {
        let mut text = None;
        self.with_terms(&mut |dictionary| {
            text = dictionary.decode(id).map(str::to_string);
        });
        text
    }

    /// Borrows the space for reading, for as long as the returned value lives.
    ///
    /// The third way to reach the terms, beside the closure scopes and
    /// [`view`](Self::view), and the one for code whose borrow does not fit a closure:
    /// a decode loop in the middle of a function, a guard held across a `match`, a
    /// pair of borrows compared against each other. Several lookups share one
    /// acquisition, exactly as holding a lock did.
    ///
    /// Poisoning is absorbed rather than reported, like the scopes: a thread that
    /// panicked while interning leaves the space usable, since a `Dictionary` has no
    /// invariant spanning two statements. Callers that want it as an error use
    /// [`try_with_terms`](Self::try_with_terms).
    ///
    /// # Deadlock
    ///
    /// Holding this across a call that writes the same space deadlocks, as any
    /// reader-writer lock does. The closure scopes make the extent obvious, so prefer
    /// them where the borrow does fit one.
    #[inline]
    pub fn read(&self) -> TermsRef<'_> {
        TermsRef(self.0.read().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    /// Borrows the space for interning, for as long as the returned value lives.
    ///
    /// See [`read`](Self::read). This excludes every other borrow of the space, so a
    /// hot loop that merely decodes should take `read` instead: the engine's parallel
    /// decode is parallel only because it does.
    #[inline]
    pub fn write(&self) -> TermsMut<'_> {
        TermsMut(self.0.write().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }
}

/// A shared term space, borrowed for reading. See [`SharedTerms::read`].
///
/// Derefs to [`Dictionary`], so it answers `decode` and the rest of the read API, and
/// nothing that interns -- `encode` takes `&mut Dictionary`, which only
/// [`TermsMut`] hands out. The borrow lasts as long as this value, so dropping it
/// early is what releases the space.
#[derive(Debug)]
pub struct TermsRef<'a>(RwLockReadGuard<'a, Dictionary>);

impl std::ops::Deref for TermsRef<'_> {
    type Target = Dictionary;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A shared term space, borrowed for interning. See [`SharedTerms::write`].
///
/// Derefs mutably to [`Dictionary`], so `encode` is callable through it. Excludes
/// every other borrow for as long as it lives.
#[derive(Debug)]
pub struct TermsMut<'a>(RwLockWriteGuard<'a, Dictionary>);

impl std::ops::Deref for TermsMut<'_> {
    type Target = Dictionary;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for TermsMut<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A term space whose lock a panicking thread left poisoned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoisonedTerms;

impl std::fmt::Display for PoisonedTerms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the term space's lock is poisoned")
    }
}

impl std::error::Error for PoisonedTerms {}

/// A read-only copy of a term space.
///
/// What a caller gets when it needs to decode many ids and cannot hold the shared
/// lock across the walk -- printing a graph, serialising a result, comparing two
/// runs. It answers [`decode`](Self::decode) and nothing else.
///
/// # Why it is not just a `Dictionary`
///
/// It used to be, and that was a trap. An owned `Dictionary` has `encode` on it, and
/// interning into a copy is not merely ineffective: the copy's next id starts
/// wherever the shared space's was, so a term interned here can be handed a number
/// the shared space later gives to a *different* term. A fact built from such an id
/// corrupts the fact base rather than losing it. `semi_naive_parallel` does exactly
/// that and is unsound for it.
///
/// Documentation did not stop it, so the type does: there is no way to intern
/// through this. To intern, use the shared encoder.
///
/// # It can still be stale
///
/// A term interned after the view is taken is missing from it, and decoding such an
/// id yields `None`. That is sound rather than wrong -- a blank, not someone else's
/// term -- and every caller is safe for one reason: the ids it decodes come from
/// facts that were already stored, so they predate the view.
#[derive(Debug, Clone)]
pub struct TermView(Dictionary);

impl TermView {
    /// The string `id` stands for, or `None` if this view does not have it.
    pub fn decode(&self, id: u32) -> Option<&str> {
        self.0.decode(id)
    }

    /// The string `id` stands for, following quoted triples through `qt_store`.
    pub fn decode_term(
        &self,
        id: u32,
        qt_store: &crate::quoted_triple_store::QuotedTripleStore,
    ) -> Option<String> {
        self.0.decode_term(id, qt_store)
    }

    /// How many terms the view holds.
    pub fn len(&self) -> usize {
        self.0.id_to_string.len()
    }

    /// Whether the view holds nothing.
    pub fn is_empty(&self) -> bool {
        self.0.id_to_string.is_empty()
    }
}


/// Immutable access to the copy, so anything taking `&Dictionary` still works.
///
/// This does not reopen the hazard: `Dictionary::encode` takes `&mut self`, and a
/// `Deref` only ever yields `&`. Interning through a view remains unexpressible,
/// while the read-only helpers that already take `&Dictionary` -- formatting, label
/// lookups, comparison output -- need no change.
impl std::ops::Deref for TermView {
    type Target = Dictionary;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SharedTerms {
    /// Runs `visit` against the term space for reading only.
    ///
    /// # Why this exists separately from `with_terms`
    ///
    /// [`with_terms`](TermEncoder::with_terms) takes a *write* lock, because it hands
    /// out `&mut Dictionary`. That is wrong for a hot read path in two ways at once:
    /// a parallel pass would serialise on it, and nothing in the pass needs to
    /// intern. The execution engine decodes a whole chunk of results under one lock,
    /// once per chunk across rayon's workers, and writing there would undo the
    /// parallelism.
    ///
    /// A copy is the other wrong answer: [`view`](Self::view) clones the dictionary,
    /// which is right once per dump and ruinous once per chunk.
    ///
    /// So this is the third option -- a shared lock, held only for the call, with no
    /// guard escaping and no copy made.
    pub fn with_terms_read<R>(&self, visit: impl FnOnce(&Dictionary) -> R) -> R {
        let guard = self.0.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        visit(&guard)
    }

    /// Runs `visit` against the term space, reporting a poisoned lock instead of
    /// panicking on it.
    ///
    /// [`with_terms`](TermEncoder::with_terms) unwraps, which is right for callers
    /// that have nothing better to do than fail. These do: `datalogmtl`'s parsers
    /// return `Result` and the HTTP server turns the error into a response, so a
    /// thread that panicked elsewhere while holding this lock should not also take
    /// down the request handling here.
    ///
    /// # Errors
    ///
    /// The lock is poisoned, meaning some thread panicked while holding it.
    pub fn try_with_terms<R>(
        &self,
        visit: impl FnOnce(&mut Dictionary) -> R,
    ) -> Result<R, PoisonedTerms> {
        let mut guard = self.0.write().map_err(|_| PoisonedTerms)?;
        Ok(visit(&mut guard))
    }

    /// A read-only copy, for decoding many ids without holding the lock.
    ///
    /// See [`TermView`] for when this is the right tool and what it cannot do.
    pub fn view(&self) -> TermView {
        let mut copy = Dictionary::new();
        self.with_terms(&mut |dictionary| copy = dictionary.clone());
        TermView(copy)
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
mod scope_tests {
    use super::*;

    #[test]
    fn a_read_scope_sees_what_was_interned_and_cannot_intern() {
        let terms = SharedTerms::new();
        let id = terms.encode("urn:a");
        // `&Dictionary` is all the scope hands out, so `encode` is not callable here:
        // the compiler enforces read-only rather than the comment asking for it.
        let seen = terms.with_terms_read(|dictionary| dictionary.decode(id).map(str::to_string));
        assert_eq!(seen.as_deref(), Some("urn:a"));
    }

    #[test]
    fn read_scopes_do_not_exclude_each_other() {
        // The property the execution engine depends on: its per-chunk decode runs
        // across rayon's workers, and a write scope would serialise them. Nesting is
        // the cheapest observable proof that these take a shared lock -- under a
        // write lock this would deadlock rather than fail.
        let terms = SharedTerms::new();
        let id = terms.encode("urn:nested");
        let both = terms.with_terms_read(|outer| {
            terms.with_terms_read(|inner| {
                (
                    outer.decode(id).map(str::to_string),
                    inner.decode(id).map(str::to_string),
                )
            })
        });
        assert_eq!(both.0.as_deref(), Some("urn:nested"));
        assert_eq!(both.1.as_deref(), Some("urn:nested"));
    }

    #[test]
    fn a_write_scope_reaches_the_shared_space() {
        let terms = SharedTerms::new();
        let mut minted = 0;
        terms.with_terms(&mut |dictionary| minted = dictionary.encode("urn:written"));
        assert_eq!(terms.decode(minted).as_deref(), Some("urn:written"));
        // and the read scope agrees, so the two are one space
        assert_eq!(
            terms.with_terms_read(|d| d.decode(minted).map(str::to_string)).as_deref(),
            Some("urn:written")
        );
    }

    #[test]
    fn a_fallible_scope_returns_the_visitor_s_value() {
        let terms = SharedTerms::new();
        let id = terms
            .try_with_terms(|dictionary| dictionary.encode("urn:fallible"))
            .expect("an unpoisoned space");
        assert_eq!(terms.decode(id).as_deref(), Some("urn:fallible"));
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
    fn a_cloned_encoder_keeps_the_one_term_space() {
        let shared = SharedTerms::new();
        let held_elsewhere = shared.clone();
        let mut id = 0;
        shared.with_terms(&mut |dict| id = dict.encode("urn:x"));
        // The clone another component already holds decodes what this encoder minted.
        // Cloning used to mean passing the handle; it is the same sharing either way,
        // which is why the handle methods could go.
        assert_eq!(held_elsewhere.read().decode(id), Some("urn:x"));
        assert!(held_elsewhere.shares_space_with(&shared));
    }

    #[test]
    fn two_separately_built_spaces_are_not_the_one_space() {
        // The distinction `shares_space_with` exists to draw: equal contents today,
        // and still two spaces, because the next term interned into one is invisible
        // to the other.
        let a = SharedTerms::new();
        let b = SharedTerms::new();
        assert_eq!(a.encode("urn:same"), b.encode("urn:same"), "same id, by luck");
        assert!(!a.shares_space_with(&b));
        let only_in_a = a.encode("urn:later");
        assert_eq!(b.decode(only_in_a), None);
    }

    #[test]
    fn a_local_encoder_can_be_handed_over_to_be_shared() {
        let local = LocalTerms::new();
        let mut id = 0;
        local.with_terms(&mut |dict| id = dict.encode("urn:y"));
        let shared = SharedTerms::from_dictionary(local.into_dictionary());
        shared.with_terms(&mut |dict| assert_eq!(dict.decode(id), Some("urn:y")));
    }
}
