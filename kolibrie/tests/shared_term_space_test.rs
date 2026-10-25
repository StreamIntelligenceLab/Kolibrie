/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! A store and a reasoner that adopt one term space stay in step.
//!
//! `kolibrie-http-server` used to assign the dictionary handle to a reasoner, infer,
//! and then assign it *back* to the store with the comment "sync the enriched
//! dictionary back so SPARQL can decode the new terms". That assignment handed the
//! store the handle it already held, and the comment described copy semantics the
//! type does not have. Removing it relies on the invariant tested here, so the
//! invariant is tested rather than assumed.

use datalog::reasoning::Reasoner;
use kolibrie::sparql_database::SparqlDatabase;

#[test]
fn a_term_interned_by_the_reasoner_decodes_through_the_store() {
    let mut database = SparqlDatabase::new();
    database.add_triple_parts("urn:a", "urn:p", "urn:b");

    let mut reasoner = Reasoner::new();
    reasoner.adopt_terms(database.terms());

    // A term the store has never seen, interned on the reasoner's side only.
    let id = reasoner.dictionary.write().unwrap().encode("urn:brand-new");

    // No sync step. The store decodes it because the encoding is shared, not copied.
    assert_eq!(
        database.dictionary().decode(id),
        Some("urn:brand-new"),
        "a shared term space needs no copying back"
    );
}

#[test]
fn the_store_and_the_reasoner_agree_on_ids_for_the_same_term() {
    let database = SparqlDatabase::new();
    let from_store = database.dictionary_mut().encode("urn:shared");

    let mut reasoner = Reasoner::new();
    reasoner.adopt_terms(database.terms());
    let from_reasoner = reasoner.dictionary.write().unwrap().encode("urn:shared");

    assert_eq!(
        from_store, from_reasoner,
        "one term space must mint one id for one term"
    );
}

#[test]
fn a_reasoner_with_its_own_term_space_does_not_agree() {
    // The failure the invariant above prevents, stated so the test above is not
    // trivially true: two independent dictionaries assign ids by insertion order,
    // so they collide rather than agree.
    let database = SparqlDatabase::new();
    database.dictionary_mut().encode("urn:first");
    let store_id = database.dictionary_mut().encode("urn:second");

    let lonely = Reasoner::new();
    let lonely_id = lonely.dictionary.write().unwrap().encode("urn:second");

    assert_ne!(
        store_id, lonely_id,
        "an unshared reasoner mints a different id, which is why adopt_terms exists"
    );
}
