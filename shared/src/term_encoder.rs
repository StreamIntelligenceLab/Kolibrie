/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`TermEncoder`]: reading and interning terms without naming how they are shared

use crate::dictionary::Dictionary;
use std::cell::RefCell;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Reading and interning terms, without naming how they are stored or shared
pub trait TermEncoder {
    /// Lends the dictionary for the duration of `visit`, acquiring at most once
    fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary));
}

/// A dictionary owned outright, with no lock, for a single thread
#[derive(Debug, Default)]
pub struct LocalTerms(RefCell<Dictionary>);

impl LocalTerms {
    /// An empty encoder
    pub fn new() -> Self {
        Self(RefCell::new(Dictionary::new()))
    }

    /// Adopts an existing dictionary
    pub fn from_dictionary(dictionary: Dictionary) -> Self {
        Self(RefCell::new(dictionary))
    }

    /// Takes the dictionary back out, to hand to something that wants it shared
    pub fn into_dictionary(self) -> Dictionary {
        self.0.into_inner()
    }
}

impl TermEncoder for LocalTerms {
    fn with_terms(&self, visit: &mut dyn FnMut(&mut Dictionary)) {
        visit(&mut self.0.borrow_mut());
    }
}

/// A dictionary shared across threads, behind the handle the workspace already uses
#[derive(Debug, Clone)]
pub struct SharedTerms(Arc<RwLock<Dictionary>>);

impl SharedTerms {
    /// An empty encoder, shareable
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(Dictionary::new())))
    }

    /// Shares a term space that already has terms in it
    pub fn from_dictionary(dictionary: Dictionary) -> Self {
        Self(Arc::new(RwLock::new(dictionary)))
    }

    /// Whether two encoders encode through the same space, as opposed to two spaces that happen to
    /// agree today
    pub fn shares_space_with(&self, other: &SharedTerms) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Interns `text` and returns its id
    pub fn encode(&self, text: &str) -> u32 {
        let mut id = 0;
        self.with_terms(&mut |dictionary| id = dictionary.encode(text));
        id
    }

    /// Interns three terms under one lock, which is what a triple needs
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

    /// The string `id` stands for, if this term space knows it
    pub fn decode(&self, id: u32) -> Option<String> {
        let mut text = None;
        self.with_terms(&mut |dictionary| {
            text = dictionary.decode(id).map(str::to_string);
        });
        text
    }

    /// Borrows the space for reading, for as long as the returned value lives
    #[inline]
    pub fn read(&self) -> TermsRef<'_> {
        TermsRef(
            self.0
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    /// Borrows the space for interning, for as long as the returned value lives
    #[inline]
    pub fn write(&self) -> TermsMut<'_> {
        TermsMut(
            self.0
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// A shared term space, borrowed for reading
#[derive(Debug)]
pub struct TermsRef<'a>(RwLockReadGuard<'a, Dictionary>);

impl std::ops::Deref for TermsRef<'_> {
    type Target = Dictionary;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A shared term space, borrowed for interning
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

/// A term space whose lock a panicking thread left poisoned
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoisonedTerms;

impl std::fmt::Display for PoisonedTerms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the term space's lock is poisoned")
    }
}

impl std::error::Error for PoisonedTerms {}

/// A read-only copy of a term space
#[derive(Debug, Clone)]
pub struct TermView(Dictionary);

impl TermView {
    /// The string `id` stands for, or `None` if this view does not have it
    pub fn decode(&self, id: u32) -> Option<&str> {
        self.0.decode(id)
    }

    /// The string `id` stands for, following quoted triples through `qt_store`
    pub fn decode_term(
        &self,
        id: u32,
        qt_store: &crate::quoted_triple_store::QuotedTripleStore,
    ) -> Option<String> {
        self.0.decode_term(id, qt_store)
    }

    /// How many terms the view holds
    pub fn len(&self) -> usize {
        self.0.id_to_string.len()
    }

    /// Whether the view holds nothing
    pub fn is_empty(&self) -> bool {
        self.0.id_to_string.is_empty()
    }
}

/// Immutable access to the copy, so anything taking `&Dictionary` still works
impl std::ops::Deref for TermView {
    type Target = Dictionary;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SharedTerms {
    /// Runs `visit` against the term space for reading only
    pub fn with_terms_read<R>(&self, visit: impl FnOnce(&Dictionary) -> R) -> R {
        let guard = self
            .0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        visit(&guard)
    }

    /// Runs `visit` against the term space, reporting a poisoned lock instead of panicking on it
    pub fn try_with_terms<R>(
        &self,
        visit: impl FnOnce(&mut Dictionary) -> R,
    ) -> Result<R, PoisonedTerms> {
        let mut guard = self.0.write().map_err(|_| PoisonedTerms)?;
        Ok(visit(&mut guard))
    }

    /// A read-only copy, for decoding many ids without holding the lock
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
        let seen = terms.with_terms_read(|dictionary| dictionary.decode(id).map(str::to_string));
        assert_eq!(seen.as_deref(), Some("urn:a"));
    }

    #[test]
    fn read_scopes_do_not_exclude_each_other() {
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
        assert_eq!(
            terms
                .with_terms_read(|d| d.decode(minted).map(str::to_string))
                .as_deref(),
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
        assert_eq!(held_elsewhere.read().decode(id), Some("urn:x"));
        assert!(held_elsewhere.shares_space_with(&shared));
    }

    #[test]
    fn two_separately_built_spaces_are_not_the_one_space() {
        let a = SharedTerms::new();
        let b = SharedTerms::new();
        assert_eq!(
            a.encode("urn:same"),
            b.encode("urn:same"),
            "same id, by luck"
        );
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
