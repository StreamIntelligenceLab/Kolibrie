/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Materialising what N3 rules entail, so that queries over the store see it

use super::KolibrieDB;
use n3::terms::{Compound, TermStore};
use n3::{N3Error, N3Reasoner};
use shared::rdf_term;
use shared::triple::Triple;
use std::collections::HashMap;

const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

/// How the N3 reasoner spells the blank nodes a rule's conclusion introduces
const WITNESS: &str = "_:kolibrie-n3-";

impl KolibrieDB {
    /// Adds to the default graph the facts of an N3 document and every triple its forward rules
    /// entail from them and the stored default graph, returning how many triples were new
    ///
    /// # Errors
    /// Returns the parse error for malformed N3, [`N3Error::Unsupported`] for backward rules
    /// `<=` and `log:query`, whose conclusions a store cannot hold, and [`N3Error::Fuse`] or
    /// [`N3Error::Limit`] when reasoning stops; the store is unchanged then
    pub fn materialize_n3(&mut self, n3_document: &str) -> Result<usize, N3Error> {
        let document = n3::parse_document(n3_document, None)?;
        // Held throughout, so no other user of the dictionary sees it taken out
        let mut terms = self.dictionary.write();
        let mut store = TermStore::new();
        store.dictionary = std::mem::take(&mut *terms);
        let mut reasoner = N3Reasoner::with_store(store);
        // The reasoner works in the store's own index, so no triple is copied
        let adopted = reasoner.adopt_facts(std::mem::take(&mut self.dataset_index));
        let stored = reasoner.fact_count();
        let entailed = adopted.and_then(|()| entail(&mut reasoner, &document));
        let new = reasoner.facts_since(stored);
        let triples = entailed.map(|()| Storable::new(&mut reasoner.store).triples(&new));
        let mut index = reasoner.take_facts();
        *terms = std::mem::take(&mut reasoner.store.dictionary);
        drop(terms);
        // Every new fact goes back in below only if the run succeeded, as a storable triple
        for triple in &new {
            index.delete_triple(triple);
        }
        self.dataset_index = index;
        let triples = triples?;
        for (prefix, iri) in document.prefixes {
            self.prefixes.insert(prefix, iri);
        }
        Ok(triples
            .iter()
            .filter(|triple| self.insert_triple(triple))
            .count())
    }
}

/// Loads the document and saturates, refusing rules a store cannot honour
fn entail(reasoner: &mut N3Reasoner, document: &n3::syntax::Document) -> Result<(), N3Error> {
    reasoner.load(document)?;
    refuse_unstorable(reasoner)?;
    reasoner.run()?;
    // A forward rule can conclude a backward rule
    refuse_unstorable(reasoner)
}

fn refuse_unstorable(reasoner: &N3Reasoner) -> Result<(), N3Error> {
    if reasoner.has_backward_rules() {
        Err(N3Error::Unsupported(
            "backward rules '<=' when materializing: what they conclude is proved on demand, \
             never stored, so a query would miss it"
                .to_string(),
        ))
    } else if reasoner.has_queries() {
        Err(N3Error::Unsupported(
            "'log:query' when materializing: query the store with SPARQL instead".to_string(),
        ))
    } else {
        Ok(())
    }
}

/// Rewrites reasoner facts into triples the store can hold
struct Storable<'a> {
    store: &'a mut TermStore,
    scope: u64,
    rewritten: HashMap<u32, u32>,
    lists: usize,
    out: Vec<Triple>,
}

impl<'a> Storable<'a> {
    fn new(store: &'a mut TermStore) -> Self {
        Storable {
            store,
            scope: rdf_term::next_document_scope(),
            rewritten: HashMap::new(),
            lists: 0,
            out: Vec::new(),
        }
    }

    /// `facts` with lists as `rdf:first`/`rdf:rest` chains, each chain before its first use,
    /// leaving out every fact with a formula or a variable in it
    fn triples(mut self, facts: &[Triple]) -> Vec<Triple> {
        for fact in facts {
            let terms = [fact.subject, fact.predicate, fact.object].map(|id| self.term(id));
            if let [Some(subject), Some(predicate), Some(object)] = terms {
                self.out.push(Triple {
                    subject,
                    predicate,
                    object,
                });
            }
        }
        self.out
    }

    /// The id `id` is stored as, or `None` for a term no RDF triple can hold
    fn term(&mut self, id: u32) -> Option<u32> {
        if let Some(&done) = self.rewritten.get(&id) {
            return Some(done);
        }
        let stored = match self.store.compound(id) {
            None => match self.store.dictionary.decode(id) {
                Some(spelling) if spelling.starts_with(WITNESS) => {
                    let label = rdf_term::spell_blank_node(&spelling[2..], self.scope);
                    self.store.dictionary.encode(&label)
                }
                _ => id,
            },
            Some(Compound::List(items)) => {
                let items = items.clone();
                self.list(&items)?
            }
            Some(Compound::Formula(_) | Compound::Var(_) | Compound::Logic(_)) => return None,
        };
        self.rewritten.insert(id, stored);
        Some(stored)
    }

    /// The head of a chain holding `items`, the chain's triples queued
    fn list(&mut self, items: &[u32]) -> Option<u32> {
        let values = items
            .iter()
            .map(|&item| self.term(item))
            .collect::<Option<Vec<u32>>>()?;
        let first = self.store.dictionary.encode(RDF_FIRST);
        let rest = self.store.dictionary.encode(RDF_REST);
        let mut tail = self.store.dictionary.encode(RDF_NIL);
        let mut chain = Vec::with_capacity(values.len() * 2);
        for value in values.into_iter().rev() {
            self.lists += 1;
            let label = rdf_term::spell_blank_node(&format!(".list{}", self.lists), self.scope);
            let cell = self.store.dictionary.encode(&label);
            chain.push(Triple {
                subject: cell,
                predicate: rest,
                object: tail,
            });
            chain.push(Triple {
                subject: cell,
                predicate: first,
                object: value,
            });
            tail = cell;
        }
        chain.reverse();
        self.out.extend(chain);
        Some(tail)
    }
}
