/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Forward N3 reasoning: semi-naive saturation over a Kolibrie [`DatasetIndex`]

use crate::error::N3Error;
use crate::matcher::{
    atom_constants, shape_of, Delta, DeltaGroups, Matcher, Sequence, Vocabulary,
};
use crate::rows::{RowJoin, RowPlan};
use crate::rules::{compile_backward_rule, compile_rule, formula_node, Atom, N3Rule, PTerm};
use crate::syntax::{
    Document, Node, Statement, LOG_IMPLIED_BY, LOG_IMPLIES, LOG_OUTPUT_STRING, LOG_QUERY,
};
use crate::terms::{Compound, TermStore};
use shared::dataset_index::DatasetIndex;
use shared::id_hash::TermBuildHasher;
use shared::rule_index::WILDCARD;
use shared::terms::Binding;
use shared::triple::Triple;
use std::cell::{Cell, OnceCell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

/// Most new facts a round scans directly before it indexes them instead
const SCAN_LIMIT: usize = 64;

/// Resource budgets that stop a run which would not reach a fixpoint
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Most facts the store may hold, input included
    pub max_facts: usize,
    /// Most saturation rounds
    pub max_rounds: usize,
    /// How deep backward rules may call each other before a run stops with
    /// [`N3Error::Limit`]
    pub max_backward_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_facts: 5_000_000,
            max_rounds: 100_000,
            max_backward_depth: 32,
        }
    }
}

/// Facts, rules, and the terms both are encoded against
#[derive(Debug, Default)]
pub struct N3Reasoner {
    /// The term dictionary and compound-term structure
    pub store: TermStore,
    facts: DatasetIndex,
    /// Every fact, with the order it was added in
    known: Sequence,
    pub(crate) rules: Vec<Rc<N3Rule>>,
    rule_sources: HashSet<(u32, u32)>,
    rule_facts: DatasetIndex,
    /// Input rules not yet encoded into `rule_facts`, which only a premise naming
    /// `log:implies` or `log:impliedBy` reads
    unencoded_rules: Vec<(Statement, String)>,
    /// Each premise's constants, `WILDCARD` for its variables, to the rules it belongs to, so a
    /// round visits only the rules its new facts could feed
    ///
    /// Not `shared::rule_index::RuleIndex`: that files premises by pairs of positions, so a
    /// predicate every rule shares, such as `rdf:type`, makes every fact a candidate for every rule.
    premises: HashMap<[u32; 3], Vec<usize>, TermBuildHasher>,
    /// How many of `rules` are filed in `premises`
    indexed: usize,
    /// Each filed rule's plan for joining rows of ids, when it is plain Datalog
    row_plans: Vec<Option<Rc<RowPlan>>>,
    /// Firings that rested on a test of the whole graph, rechecked once reasoning ends
    retained: Vec<(usize, Binding)>,
    list_terms: BTreeSet<u32>,
    /// Rules proved on demand: `{ head } <= { body }`
    pub(crate) backward: Vec<N3Rule>,
    /// `{ body } log:query { head }`, answered once reasoning is done
    pub(crate) queries: Vec<N3Rule>,
    /// The query answers, in the order they were found
    answers: Vec<Triple>,
    /// Set when a backward proof hit the depth limit
    cut_off: Cell<bool>,
    /// Set when a firing asserted a universal statement, which makes rules worth evaluating again
    universals_added: bool,
    /// The rule firings that asserted universal statements, so none asserts twice
    asserted: HashSet<(usize, Vec<(String, u32)>)>,
    /// Proved backward goals, valid for the fact count recorded alongside
    proofs: RefCell<HashMap<[Option<u32>; 3], Vec<Triple>>>,
    proofs_for: Cell<usize>,
    cuts: Cell<usize>,
    /// The instant `time:localTime` reports
    clock: OnceCell<(i64, u32)>,
    derived: Vec<Triple>,
    fresh: usize,
    /// Budgets for [`N3Reasoner::run`]
    pub limits: Limits,
    /// What a proof needs, kept only once [`N3Reasoner::enable_proofs`] is called
    pub(crate) proofs_on: bool,
    /// The number each source rule has in a proof, by index into `rules`
    pub(crate) rule_numbers: HashMap<usize, usize>,
    /// The number each source backward rule has in a proof, by index into `backward`
    pub(crate) backward_numbers: HashMap<usize, usize>,
    /// The label of the source each universal fact came from, by index into `backward`
    pub(crate) universal_labels: HashMap<usize, String>,
    pub(crate) next_rule_number: usize,
    /// The label input facts are attributed to in a proof
    pub(crate) source_label: String,
    /// What a proof writes before a witness's number, chosen so no source blank node has it
    pub(crate) witness_prefix: String,
    /// The source label of each input fact
    pub(crate) fact_labels: HashMap<Triple, String>,
    /// How each derived fact was first derived: the rule's index and the binding it fired with
    pub(crate) derivations: HashMap<Triple, (usize, Binding)>,
    /// The derived `log:implies` triple each derived rule came from, by index into `rules`
    pub(crate) carried: HashMap<usize, Triple>,
    /// The built-in calls a proof being written justifies by re-evaluation, with the built-in
    pub(crate) builtin_steps: HashMap<Triple, String>,
    /// The derived `log:impliedBy` triple each derived backward rule came from, by index into
    /// `backward`
    pub(crate) carried_backward: HashMap<usize, Triple>,
    /// The forward rule and binding that asserted each universal statement, by index into
    /// `backward`
    pub(crate) universal_origins: HashMap<usize, (usize, Binding)>,
    /// Each query's source statement, `{ … } log:query { … }`, by index into `queries`
    pub(crate) query_sources: HashMap<usize, Statement>,
    /// How each query answer was found: the query's index and the binding it answered with
    pub(crate) query_answers: HashMap<Triple, (usize, Binding)>,
}

impl N3Reasoner {
    /// An empty reasoner with default limits
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty reasoner over an existing term store, so ids carry over in both directions
    pub fn with_store(store: TermStore) -> Self {
        Self {
            store,
            ..Self::default()
        }
    }

    /// Fixes the instant `time:localTime` reports, in milliseconds since the Unix epoch, instead
    /// of reading the system clock when a rule first asks; has no effect once the clock was read
    pub fn pin_clock(&mut self, millis: i64) {
        let _ = self
            .clock
            .set((millis.div_euclid(1000), millis.rem_euclid(1000) as u32));
    }

    /// Adds already-encoded triples as facts, and those that are `log:implies` triples between
    /// formulas as rules
    ///
    /// # Errors
    /// Returns [`N3Error::Unsupported`] for backward rules and rules this engine cannot evaluate
    pub fn load_triples(&mut self, triples: &[Triple]) -> Result<(), N3Error> {
        let implies = self.store.lookup(LOG_IMPLIES);
        let implied_by = self.store.lookup(LOG_IMPLIED_BY);
        for triple in triples {
            if Some(triple.predicate) == implied_by {
                if let (Some(head), Some(body)) = (
                    formula_node(&self.store, triple.subject),
                    formula_or_true(&self.store, triple.object),
                ) {
                    let rule = compile_backward_rule(&mut self.store, &head, &body)?;
                    self.rule_facts.insert(triple);
                    self.backward.push(rule);
                    continue;
                }
            }
            if Some(triple.predicate) == implies {
                if let (Some(body), Some(head)) = (
                    formula_node(&self.store, triple.subject),
                    formula_node(&self.store, triple.object),
                ) {
                    let rule = compile_rule(&mut self.store, &body, &head)?;
                    self.rule_sources.insert((triple.subject, triple.object));
                    self.rule_facts.insert(triple);
                    self.rules.push(Rc::new(rule));
                    continue;
                }
            }
            self.insert(triple.clone());
        }
        Ok(())
    }

    /// Adds a document's facts and rules
    ///
    /// # Errors
    /// Returns [`N3Error::Unsupported`] for backward rules, queries, and rules this engine
    /// cannot evaluate yet
    pub fn load(&mut self, document: &Document) -> Result<(), N3Error> {
        for statement in &document.statements {
            if let Node::Iri(predicate) = &statement.predicate {
                if predicate == LOG_IMPLIED_BY {
                    let rule = compile_backward_rule(
                        &mut self.store,
                        &statement.subject,
                        &statement.object,
                    )?;
                    self.unencoded_rules
                        .push((statement.clone(), self.source_label.clone()));
                    self.next_rule_number += 1;
                    self.backward_numbers
                        .insert(self.backward.len(), self.next_rule_number);
                    self.backward.push(rule);
                    continue;
                }
                if predicate == LOG_QUERY {
                    let query =
                        compile_rule(&mut self.store, &statement.subject, &statement.object)?;
                    if self.proofs_on {
                        self.query_sources
                            .insert(self.queries.len(), statement.clone());
                    }
                    self.queries.push(query);
                    continue;
                }
                if predicate == LOG_IMPLIES {
                    let rule =
                        compile_rule(&mut self.store, &statement.subject, &statement.object)?;
                    self.unencoded_rules
                        .push((statement.clone(), self.source_label.clone()));
                    self.next_rule_number += 1;
                    self.rule_numbers
                        .insert(self.rules.len(), self.next_rule_number);
                    self.rules.push(Rc::new(rule));
                    continue;
                }
            }
            if universal(statement) {
                // A fact about any value of its variables is answered on demand, never listed.
                let rule = compile_backward_rule(
                    &mut self.store,
                    &Node::Formula(vec![statement.clone()]),
                    &Node::typed("true", crate::syntax::XSD_BOOLEAN),
                )?;
                self.universal_labels
                    .insert(self.backward.len(), self.source_label.clone());
                self.backward.push(rule);
                continue;
            }
            let triple = self.store.encode_statement(statement);
            if self.proofs_on && !self.fact_labels.contains_key(&triple) {
                self.fact_labels
                    .insert(triple.clone(), self.source_label.clone());
            }
            self.insert(triple);
        }
        Ok(())
    }

    /// Keeps what [`N3Reasoner::proof_n3`] needs to justify the output; call before loading
    pub fn enable_proofs(&mut self) {
        self.proofs_on = true;
    }

    /// Names the source the facts loaded next come from, as proofs cite it
    pub fn set_source_label(&mut self, label: impl Into<String>) {
        self.source_label = label.into();
    }

    fn insert(&mut self, triple: Triple) -> bool {
        if !self.known.contains_key(&triple) {
            self.known.insert(triple.clone(), self.known.len());
            for term in [triple.subject, triple.object] {
                if matches!(self.store.compound(term), Some(Compound::List(items)) if !items.is_empty())
                {
                    self.list_terms.insert(term);
                }
            }
            self.facts.insert(&triple);
            true
        } else {
            false
        }
    }

    /// Every fact the store holds, input and derived, in the order they were added
    pub fn facts(&self) -> Vec<Triple> {
        let mut facts = self.facts.query(None, None, None);
        facts.sort_by_key(|triple| self.known.get(triple).copied().unwrap_or(usize::MAX));
        facts
    }

    /// The facts derived so far, in derivation order
    pub fn derived(&self) -> &[Triple] {
        &self.derived
    }

    /// Saturates the store under the forward rules and returns the derived facts
    ///
    /// Rules that introduce existentials are deferred: ordinary rules saturate first, then one
    /// existential rule fires, and ordinary reasoning resumes on what it added. An existential
    /// rule whose conclusion could satisfy another's goes before it.
    ///
    /// # Errors
    /// Returns [`N3Error::Limit`] when a budget in [`N3Reasoner::limits`] runs out, and
    /// [`N3Error::Unsupported`] when a derived rule cannot be evaluated
    pub fn run(&mut self) -> Result<&[Triple], N3Error> {
        if self.premises_name_rules() || (self.proofs_on && !self.unencoded_rules.is_empty()) {
            self.encode_rule_facts();
        }
        let mut pending: Vec<Triple> = self.facts();
        let mut fresh_rules: Vec<usize> = (0..self.rules.len()).collect();
        loop {
            self.saturate(&mut pending, &mut fresh_rules)?;
            let mut progressed = false;
            for rule_index in self.waiting_order() {
                let rule = Rc::clone(&self.rules[rule_index]);
                let before = pending.len();
                let solutions = self.matcher().solve(&rule.body, None, Binding::new());
                for binding in solutions {
                    self.apply(rule_index, binding, &mut pending, &mut fresh_rules)?;
                }
                if pending.len() > before {
                    progressed = true;
                    break;
                }
            }
            if !progressed {
                self.recheck_retained()?;
                self.answer_queries()?;
                if self.cut_off.get() {
                    return Err(N3Error::Limit(format!(
                        "backward rules nested deeper than {}",
                        self.limits.max_backward_depth
                    )));
                }
                return Ok(&self.derived);
            }
        }
    }

    /// Runs the rules without existentials to a fixpoint, semi-naively from `pending`
    fn saturate(
        &mut self,
        pending: &mut Vec<Triple>,
        fresh_rules: &mut Vec<usize>,
    ) -> Result<(), N3Error> {
        let mut rounds = 0;
        loop {
            let ordinary_fresh: Vec<usize> = fresh_rules
                .iter()
                .copied()
                .filter(|&i| !self.rules[i].waits())
                .collect();
            if pending.is_empty() && ordinary_fresh.is_empty() {
                return Ok(());
            }
            rounds += 1;
            if rounds > self.limits.max_rounds {
                return Err(N3Error::Limit(format!("{} rounds", self.limits.max_rounds)));
            }
            fresh_rules.retain(|i| self.rules[*i].waits());
            self.index_new_rules();
            let delta: Vec<Triple> = std::mem::take(pending);
            let mut full: BTreeSet<usize> = ordinary_fresh.into_iter().collect();
            if !self.backward.is_empty() {
                full.extend((0..self.rules.len()).filter(|&i| self.asks_backward(&self.rules[i])));
            }
            let mut candidates = full.clone();
            for triple in &delta {
                for mask in 0..8u8 {
                    let key = |bit: u8, id: u32| if mask & bit == 0 { id } else { WILDCARD };
                    let key = [
                        key(1, triple.subject),
                        key(2, triple.predicate),
                        key(4, triple.object),
                    ];
                    if let Some(rules) = self.premises.get(&key) {
                        candidates.extend(rules);
                    }
                }
            }
            let needs_delta = candidates.iter().any(|i| !full.contains(i));
            // Many new facts are grouped by the constants each candidate premise has, so a premise
            // scans only the new facts that agree with them, in the order they were added.
            let groups = if needs_delta && delta.len() > SCAN_LIMIT {
                let shapes: BTreeSet<u8> = candidates
                    .iter()
                    .filter(|i| !full.contains(i))
                    .flat_map(|&i| &self.rules[i].body)
                    .filter(|atom| atom.builtin.is_none())
                    .map(|atom| shape_of(atom_constants(atom)))
                    .collect();
                DeltaGroups::new(&delta, &shapes)
            } else {
                DeltaGroups::default()
            };
            let seed = if delta.len() > SCAN_LIMIT {
                Delta::Grouped(&groups)
            } else {
                Delta::Facts(&delta)
            };
            for rule_index in candidates {
                if self.rules[rule_index].waits() {
                    continue;
                }
                let full_rule = full.contains(&rule_index);
                if let Some(plan) = self.row_plan(rule_index) {
                    let join = RowJoin {
                        facts: &self.facts,
                        sequence: &self.known,
                    };
                    for row in join.solutions(&plan, seed, full_rule) {
                        self.commit(plan.conclusion(&row), pending, fresh_rules)?;
                    }
                    continue;
                }
                let rule = Rc::clone(&self.rules[rule_index]);
                let solutions = self.rule_solutions(&rule, seed, full_rule);
                for binding in solutions {
                    self.apply(rule_index, binding, pending, fresh_rules)?;
                }
            }
        }
    }

    /// The row plan for a rule, while nothing in the run needs the general join: no backward
    /// rules, whose proofs each premise lookup also asks for, no logic variables, and no proofs
    fn row_plan(&self, rule_index: usize) -> Option<Rc<RowPlan>> {
        if self.proofs_on || !self.backward.is_empty() || self.store.has_logic_vars() {
            return None;
        }
        self.row_plans.get(rule_index).cloned().flatten()
    }

    /// Files the premises of rules added since the last call by their constants
    fn index_new_rules(&mut self) {
        let vocabulary = Vocabulary::of(&self.store);
        for rule in &self.rules[self.row_plans.len()..] {
            self.row_plans
                .push(RowPlan::compile(rule, &vocabulary).map(Rc::new));
        }
        let key = |position: &PTerm| match position {
            PTerm::Const(id) => *id,
            _ => WILDCARD,
        };
        for rule_id in self.indexed..self.rules.len() {
            for atom in self.rules[rule_id]
                .body
                .iter()
                .filter(|a| a.builtin.is_none())
            {
                let entry = self
                    .premises
                    .entry([key(&atom.subject), key(&atom.predicate), key(&atom.object)])
                    .or_default();
                if entry.last() != Some(&rule_id) {
                    entry.push(rule_id);
                }
            }
        }
        self.indexed = self.rules.len();
    }

    /// Stores new conclusion triples, queues them for the next round, and adopts derived rules;
    /// returns how many were new
    fn commit(
        &mut self,
        conclusion: Vec<Triple>,
        pending: &mut Vec<Triple>,
        fresh_rules: &mut Vec<usize>,
    ) -> Result<usize, N3Error> {
        let mut added = 0;
        for triple in conclusion {
            if !self.insert(triple.clone()) {
                continue;
            }
            added += 1;
            self.derived.push(triple.clone());
            if self.adopt_derived_rule(&triple)? {
                fresh_rules.push(self.rules.len() - 1);
            }
            pending.push(triple);
        }
        if self.known.len() > self.limits.max_facts {
            return Err(N3Error::Limit(format!("{} facts", self.limits.max_facts)));
        }
        Ok(added)
    }

    /// Fires a rule for one binding and stores what it concludes, remembering the firing when it
    /// rested on a test of the whole graph
    fn apply(
        &mut self,
        rule_index: usize,
        binding: Binding,
        pending: &mut Vec<Triple>,
        fresh_rules: &mut Vec<usize>,
    ) -> Result<(), N3Error> {
        let rule = Rc::clone(&self.rules[rule_index]);
        let conclusion = self.fire(&rule, &binding)?;
        if self.proofs_on {
            for triple in &conclusion {
                if !self.known.contains_key(triple) {
                    self.derivations
                        .entry(triple.clone())
                        .or_insert_with(|| (rule_index, binding.clone()));
                }
            }
        }
        if self.commit(conclusion, pending, fresh_rules)? > 0 && rule.deferred {
            self.retained.push((rule_index, binding));
        }
        if std::mem::take(&mut self.universals_added) {
            // A new universal assertion can answer premises without any new fact.
            self.proofs.borrow_mut().clear();
            let asking: Vec<usize> = (0..self.rules.len())
                .filter(|&i| self.asks_backward(&self.rules[i]))
                .collect();
            fresh_rules.extend(asking);
        }
        Ok(())
    }

    /// Whether a premise of `rule` could be answered by a backward rule, whose answers change
    /// with facts that need not match the premise itself
    fn asks_backward(&self, rule: &N3Rule) -> bool {
        rule.body
            .iter()
            .filter(|a| a.builtin.is_none())
            .any(|premise| {
                self.backward.iter().flat_map(|b| &b.head).any(|head| {
                    premise
                        .positions()
                        .into_iter()
                        .zip(head.positions())
                        .all(|(x, y)| compatible(x, y))
                })
            })
    }

    /// Answers every `log:query` over the saturated graph, without adding the answers to it
    fn answer_queries(&mut self) -> Result<(), N3Error> {
        let queries = self.queries.clone();
        let mut seen = HashSet::new();
        for (query_index, query) in queries.iter().enumerate() {
            let solutions = self.matcher().solve(&query.body, None, Binding::new());
            for binding in solutions {
                for triple in self.fire(query, &binding)? {
                    if seen.insert(triple.clone()) {
                        if self.proofs_on {
                            self.query_answers
                                .insert(triple.clone(), (query_index, binding.clone()));
                        }
                        self.answers.push(triple);
                    }
                }
            }
        }
        Ok(())
    }

    /// What a run outputs: the query answers when the program has queries, else the derived facts
    pub fn output(&self) -> &[Triple] {
        if self.queries.is_empty() {
            &self.derived
        } else {
            &self.answers
        }
    }

    /// Checks that every firing that rested on a test of the whole graph still has its premise
    fn recheck_retained(&mut self) -> Result<(), N3Error> {
        for (rule_index, binding) in std::mem::take(&mut self.retained) {
            let body = self.rules[rule_index].body.clone();
            if self
                .matcher()
                .solve(&body, None, binding.clone())
                .is_empty()
            {
                return Err(N3Error::Fuse(
                    "a rule fired on a test of the current graph that later facts contradict"
                        .to_string(),
                ));
            }
            self.retained.push((rule_index, binding));
        }
        Ok(())
    }

    /// The rules that wait for saturation, in source order except where one must go first: a
    /// whole-graph test before an existential rule whose premises its conclusion can feed, and an
    /// existential rule before another whose piece it completes or covers
    fn waiting_order(&self) -> Vec<usize> {
        let mut waiting: Vec<usize> = (0..self.rules.len())
            .filter(|&i| self.rules[i].waits())
            .collect();
        let mut placed = Vec::with_capacity(waiting.len());
        while !waiting.is_empty() {
            let ready = waiting
                .iter()
                .position(|&candidate| {
                    !waiting
                        .iter()
                        .any(|&other| other != candidate && self.must_precede(other, candidate))
                })
                .unwrap_or(0);
            placed.push(waiting.remove(ready));
        }
        placed
    }

    /// Whether waiting rule `first` must run before waiting rule `second`
    fn must_precede(&self, first: usize, second: usize) -> bool {
        let (a, b) = (&self.rules[first], &self.rules[second]);
        match (a.has_existentials, b.has_existentials) {
            (true, true) => self.precedes(first, second),
            (false, true) => feeds(&a.head, &b.body),
            _ => false,
        }
    }

    /// Whether existential rule `first` must run before `second`: it completes the other's
    /// piece together with existing facts, or its conclusion covers the other's and not the
    /// reverse
    fn precedes(&self, first: usize, second: usize) -> bool {
        let (a, b) = (&self.rules[first].head, &self.rules[second].head);
        let completes_ab = self.completes(a, b);
        let completes_ba = self.completes(b, a);
        if completes_ab != completes_ba {
            return completes_ab;
        }
        covers(a, b) && !covers(b, a)
    }

    /// Whether `head` together with existing facts could satisfy every existential atom of
    /// `piece`, using at least one fact
    ///
    /// An existential that a fact supplies is an individual that already exists, so the head can
    /// only reach it through a universal or a constant, never through a fresh witness of its own.
    fn completes(&self, head: &[Atom], piece: &[Atom]) -> bool {
        let key = |position: &PTerm| match position {
            PTerm::Const(id) => Some(*id),
            _ => None,
        };
        let piece: Vec<&Atom> = piece.iter().filter(|atom| atom.has_exists()).collect();
        let mut from_facts = BTreeSet::new();
        let mut from_head = Vec::new();
        for atom in &piece {
            let matching: Vec<&Atom> = head
                .iter()
                .filter(|candidate| {
                    atom.positions()
                        .into_iter()
                        .zip(candidate.positions())
                        .all(|(x, y)| compatible(x, y))
                })
                .collect();
            if !matching.is_empty() {
                from_head.push((atom, matching));
                continue;
            }
            let found = !self
                .facts
                .query(key(&atom.subject), key(&atom.predicate), key(&atom.object))
                .is_empty();
            if !found {
                return false;
            }
            for position in atom.positions() {
                if let PTerm::Exists(label) = position {
                    from_facts.insert(label.clone());
                }
            }
        }
        if from_facts.is_empty() {
            return false;
        }
        from_head.iter().all(|(atom, matching)| {
            matching.iter().any(|candidate| {
                atom.positions()
                    .into_iter()
                    .zip(candidate.positions())
                    .all(|(x, y)| match x {
                        PTerm::Exists(label) if from_facts.contains(label) => {
                            !matches!(y, PTerm::Exists(_))
                        }
                        _ => true,
                    })
            })
        })
    }

    /// Encodes the input rules as `log:implies` and `log:impliedBy` triples between formulas
    fn encode_rule_facts(&mut self) {
        let implies = self.store.iri(LOG_IMPLIES);
        for (statement, label) in std::mem::take(&mut self.unencoded_rules) {
            let triple = self.store.encode_statement(&statement);
            if self.proofs_on {
                self.fact_labels.entry(triple.clone()).or_insert(label);
            }
            if triple.predicate == implies {
                self.rule_sources.insert((triple.subject, triple.object));
            }
            self.rule_facts.insert(&triple);
        }
    }

    /// Whether some premise names `log:implies` or `log:impliedBy`, so it reads rules as data
    fn premises_name_rules(&self) -> bool {
        let names: Vec<u32> = [LOG_IMPLIES, LOG_IMPLIED_BY]
            .iter()
            .filter_map(|iri| self.store.lookup(iri))
            .collect();
        if names.is_empty() || self.unencoded_rules.is_empty() {
            return false;
        }
        fn mentions(atoms: &[Atom], names: &[u32]) -> bool {
            atoms.iter().any(|atom| {
                matches!(atom.predicate, PTerm::Const(id) if names.contains(&id))
                    || atom.positions().into_iter().any(|p| match p {
                        PTerm::Formula(inner) => mentions(inner, names),
                        PTerm::List(items) => items.iter().any(
                            |item| matches!(item, PTerm::Formula(inner) if mentions(inner, names)),
                        ),
                        _ => false,
                    })
            })
        }
        self.rules
            .iter()
            .map(|rule| &**rule)
            .chain(&self.backward)
            .chain(&self.queries)
            .any(|rule| mentions(&rule.body, &names))
    }

    /// Turns a derived `{…} log:implies {…}` triple into a rule; true if it added one
    fn adopt_derived_rule(&mut self, triple: &Triple) -> Result<bool, N3Error> {
        if !self.unencoded_rules.is_empty()
            && [LOG_IMPLIES, LOG_IMPLIED_BY]
                .iter()
                .any(|iri| self.store.lookup(iri) == Some(triple.predicate))
        {
            self.encode_rule_facts();
        }
        if self
            .store
            .lookup(LOG_IMPLIED_BY)
            .is_some_and(|id| id == triple.predicate)
        {
            if let (Some(head), Some(body)) = (
                formula_node(&self.store, triple.subject),
                formula_or_true(&self.store, triple.object),
            ) {
                if self.rule_sources.insert((triple.subject, triple.object)) {
                    let rule = compile_backward_rule(&mut self.store, &head, &body)?;
                    if self.proofs_on {
                        self.carried_backward
                            .insert(self.backward.len(), triple.clone());
                    }
                    self.backward.push(rule);
                }
            }
            return Ok(false);
        }
        let Some(implies) = self.store.lookup(LOG_IMPLIES) else {
            return Ok(false);
        };
        if triple.predicate != implies || !self.rule_sources.insert((triple.subject, triple.object))
        {
            return Ok(false);
        }
        let (Some(body), Some(head)) = (
            formula_node(&self.store, triple.subject),
            formula_node(&self.store, triple.object),
        ) else {
            return Ok(false);
        };
        let rule = compile_rule(&mut self.store, &body, &head)?;
        if self.proofs_on {
            self.carried.insert(self.rules.len(), triple.clone());
        }
        self.rules.push(Rc::new(rule));
        Ok(true)
    }

    /// A matcher over this reasoner's facts and terms
    pub(crate) fn matcher(&mut self) -> Matcher<'_> {
        let vocabulary = Vocabulary::of(&self.store);
        if self.proofs_for.get() != self.known.len() {
            self.proofs.borrow_mut().clear();
            self.proofs_for.set(self.known.len());
        }
        Matcher {
            store: &mut self.store,
            facts: &self.facts,
            rule_facts: &self.rule_facts,
            list_terms: &self.list_terms,
            sequence: &self.known,
            vocabulary,
            backward: &self.backward,
            goals: Vec::new(),
            cut_off: &self.cut_off,
            proofs: &self.proofs,
            cuts: &self.cuts,
            max_depth: self.limits.max_backward_depth,
            clock: &self.clock,
            in_rule: Cell::new(false),
        }
    }

    /// Every binding of the rule body that uses at least one delta fact, or all bindings when
    /// `full`
    fn rule_solutions(&mut self, rule: &N3Rule, delta: Delta<'_>, full: bool) -> Vec<Binding> {
        let data_atoms: Vec<usize> = (0..rule.body.len())
            .filter(|&i| rule.body[i].builtin.is_none())
            .collect();
        if data_atoms.is_empty() {
            return if full {
                self.matcher().solve(&rule.body, None, Binding::new())
            } else {
                Vec::new()
            };
        }
        let mut seen: HashSet<Vec<(String, u32)>> = HashSet::new();
        let mut out = Vec::new();
        let starts: Vec<Option<usize>> = if full {
            vec![None]
        } else {
            data_atoms.into_iter().map(Some).collect()
        };
        for start in starts {
            let solutions =
                self.matcher()
                    .solve(&rule.body, start.map(|i| (i, delta)), Binding::new());
            for binding in solutions {
                let mut key: Vec<(String, u32)> =
                    binding.iter().map(|(k, v)| (k.clone(), *v)).collect();
                key.sort();
                if seen.insert(key) {
                    out.push(binding);
                }
            }
        }
        out
    }

    /// The conclusion triples of one firing; empty when its existential pieces already hold
    fn fire(&mut self, rule: &N3Rule, binding: &Binding) -> Result<Vec<Triple>, N3Error> {
        if rule.fuse {
            let mut bound: Vec<String> = binding
                .iter()
                .filter(|(name, _)| !name.starts_with("_:"))
                .map(|(name, &id)| format!("?{name} = {}", self.store.n3(id)))
                .collect();
            bound.sort();
            return Err(N3Error::Fuse(format!(
                "a forbidden premise holds ({})",
                bound.join(", ")
            )));
        }
        if rule.has_existentials && self.conclusion_holds(rule, binding) {
            return Ok(Vec::new());
        }
        let mut witnesses = HashMap::new();
        let mut out = Vec::with_capacity(rule.head.len());
        if let Some(term) = &rule.unquote {
            let triples = match self.instantiate(term, binding, &mut witnesses) {
                Some(id) => match self.store.compound(id).cloned() {
                    Some(Compound::Formula(triples)) => triples
                        .iter()
                        .map(|t| Triple {
                            subject: self.substitute(t.subject, binding),
                            predicate: self.substitute(t.predicate, binding),
                            object: self.substitute(t.object, binding),
                        })
                        .collect(),
                    _ => Vec::new(),
                },
                None => Vec::new(),
            };
            return Ok(triples);
        }
        for atom in &rule.head {
            // A conclusion triple with a variable the premise left unbound is not concluded.
            let (Some(subject), Some(predicate), Some(object)) = (
                self.instantiate(&atom.subject, binding, &mut witnesses),
                self.instantiate(&atom.predicate, binding, &mut witnesses),
                self.instantiate(&atom.object, binding, &mut witnesses),
            ) else {
                continue;
            };
            out.push(Triple {
                subject,
                predicate,
                object,
            });
        }
        if !rule.universal.is_empty() {
            let mut key: Vec<(String, u32)> =
                binding.iter().map(|(k, v)| (k.clone(), *v)).collect();
            key.sort();
            if !self
                .asserted
                .insert((std::ptr::from_ref(rule) as usize, key))
            {
                return Ok(out);
            }
        }
        for atom in &rule.universal {
            let positions = [&atom.subject, &atom.predicate, &atom.object]
                .map(|p| self.instantiate_open(p, binding, &mut witnesses));
            let [Some(subject), Some(predicate), Some(object)] = positions else {
                continue;
            };
            let assertion = N3Rule {
                body: Vec::new(),
                head: vec![Atom::new(subject, predicate, object)],
                has_existentials: false,
                deferred: false,
                fuse: false,
                unquote: None,
                universal: Vec::new(),
            };
            if !self
                .backward
                .iter()
                .any(|b| b.body.is_empty() && b.head == assertion.head)
            {
                if self.proofs_on {
                    if let Some(origin) = self.rules.iter().position(|r| std::ptr::eq(&**r, rule)) {
                        self.universal_origins
                            .insert(self.backward.len(), (origin, binding.clone()));
                    }
                }
                self.backward.push(assertion);
                self.universals_added = true;
            }
        }
        Ok(out)
    }

    /// A conclusion position with every variable but the conclusion's own universals replaced by
    /// its value, so the atom stays a pattern over those universals
    fn instantiate_open(
        &mut self,
        position: &PTerm,
        binding: &Binding,
        witnesses: &mut HashMap<String, u32>,
    ) -> Option<PTerm> {
        Some(match position {
            PTerm::Var(name) if name.starts_with(crate::rules::UNIVERSAL_HEAD) => position.clone(),
            PTerm::List(items) => PTerm::List(
                items
                    .iter()
                    .map(|item| self.instantiate_open(item, binding, witnesses))
                    .collect::<Option<Vec<_>>>()?,
            ),
            other => PTerm::Const(self.instantiate(other, binding, witnesses)?),
        })
    }

    /// Whether some facts already satisfy the conclusion, existentials read as variables
    fn conclusion_holds(&mut self, rule: &N3Rule, binding: &Binding) -> bool {
        let as_pattern: Vec<Atom> = rule
            .head
            .iter()
            .map(|atom| {
                Atom::new(
                    exists_as_var(&atom.subject),
                    exists_as_var(&atom.predicate),
                    exists_as_var(&atom.object),
                )
            })
            .collect();
        !self
            .matcher()
            .solve(&as_pattern, None, binding.clone())
            .is_empty()
    }

    /// A term with the quoted variables the rule's own variables of the same name bind replaced
    /// by their values
    fn substitute(&mut self, id: u32, binding: &Binding) -> u32 {
        match self.store.compound(id).cloned() {
            Some(Compound::Var(name)) => binding.get(&name).copied().unwrap_or(id),
            Some(Compound::List(items)) => {
                let items = items.iter().map(|&i| self.substitute(i, binding)).collect();
                self.store.list(items)
            }
            Some(Compound::Formula(triples)) => {
                let triples = triples
                    .iter()
                    .map(|t| Triple {
                        subject: self.substitute(t.subject, binding),
                        predicate: self.substitute(t.predicate, binding),
                        object: self.substitute(t.object, binding),
                    })
                    .collect();
                self.store.formula(triples)
            }
            Some(Compound::Logic(_)) | None => id,
        }
    }

    /// The id a conclusion position stands for under `binding`, minting existential witnesses;
    /// `None` when it holds a variable the premise left unbound
    fn instantiate(
        &mut self,
        position: &PTerm,
        binding: &Binding,
        witnesses: &mut HashMap<String, u32>,
    ) -> Option<u32> {
        Some(match position {
            PTerm::Const(id) => *id,
            PTerm::Var(name) => {
                crate::matcher::settle(&mut self.store, *binding.get(name)?, binding)?
            }
            PTerm::List(items) => {
                let mut ids = Vec::with_capacity(items.len());
                for item in items {
                    ids.push(self.instantiate(item, binding, witnesses)?);
                }
                self.store.list(ids)
            }
            PTerm::Formula(atoms) => {
                let mut triples = Vec::with_capacity(atoms.len());
                for atom in atoms {
                    triples.push(Triple {
                        subject: self.instantiate(&atom.subject, binding, witnesses)?,
                        predicate: self.instantiate(&atom.predicate, binding, witnesses)?,
                        object: self.instantiate(&atom.object, binding, witnesses)?,
                    });
                }
                self.store.formula(triples)
            }
            PTerm::Exists(label) => {
                if let Some(&id) = witnesses.get(label) {
                    id
                } else {
                    self.fresh += 1;
                    let id = self
                        .store
                        .dictionary
                        .encode(&format!("_:kolibrie-n3-.e{}", self.fresh));
                    witnesses.insert(label.clone(), id);
                    id
                }
            }
        })
    }

    /// The `log:outputString` texts a run outputs, joined in order of their subjects, or `None`
    /// when there are none: from the query answers when there are queries, else from every fact
    pub fn output_strings(&self) -> Option<String> {
        let predicate = self.store.lookup(LOG_OUTPUT_STRING)?;
        let triples = if self.queries.is_empty() {
            self.facts.query(None, Some(predicate), None)
        } else {
            self.answers
                .iter()
                .filter(|t| t.predicate == predicate)
                .cloned()
                .collect()
        };
        let mut entries: Vec<(String, String)> = triples
            .into_iter()
            .filter_map(|triple| match self.store.node(triple.object) {
                Node::Literal { lexical, .. } => Some((self.store.n3(triple.subject), lexical)),
                _ => None,
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        entries.sort();
        Some(entries.into_iter().map(|(_, text)| text).collect())
    }

    /// What [`N3Reasoner::output`] returns, as N3 text, one statement per line
    pub fn output_n3(&self) -> String {
        let mut out = String::new();
        for triple in self.output() {
            out.push_str(&self.store.n3_triple(triple));
            out.push('\n');
        }
        out
    }

    /// The derived facts as N3 text, one statement per line
    pub fn derived_n3(&self) -> String {
        let mut out = String::new();
        for triple in &self.derived {
            out.push_str(&self.store.n3_triple(triple));
            out.push('\n');
        }
        out
    }
}

/// Whether a statement has a variable outside any quoted formula, so it holds for every value
fn universal(statement: &Statement) -> bool {
    fn open(node: &Node) -> bool {
        match node {
            Node::Var(_) => true,
            Node::List(items) => items.iter().any(open),
            _ => false,
        }
    }
    open(&statement.subject) || open(&statement.predicate) || open(&statement.object)
}

/// The node behind a formula id, or the literal `true` that stands for the empty premise
fn formula_or_true(store: &TermStore, id: u32) -> Option<Node> {
    formula_node(store, id).or_else(|| {
        let node = store.node(id);
        matches!(&node, Node::Literal { lexical, datatype, .. }
            if lexical == "true" && datatype.as_deref() == Some(crate::syntax::XSD_BOOLEAN))
        .then_some(node)
    })
}

fn exists_as_var(position: &PTerm) -> PTerm {
    match position {
        PTerm::Exists(label) => PTerm::Var(format!("?exists {label}")),
        PTerm::List(items) => PTerm::List(items.iter().map(exists_as_var).collect()),
        PTerm::Formula(atoms) => PTerm::Formula(
            atoms
                .iter()
                .map(|a| {
                    Atom::new(
                        exists_as_var(&a.subject),
                        exists_as_var(&a.predicate),
                        exists_as_var(&a.object),
                    )
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Whether two pattern positions could denote the same term
fn compatible(left: &PTerm, right: &PTerm) -> bool {
    match (left, right) {
        (PTerm::Const(a), PTerm::Const(b)) => a == b,
        (PTerm::List(a), PTerm::List(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| compatible(x, y))
        }
        _ => true,
    }
}

/// Whether some conclusion atom could match some premise atom that is looked up in facts
fn feeds(head: &[Atom], body: &[Atom]) -> bool {
    body.iter()
        .filter(|atom| atom.builtin.is_none())
        .any(|premise| {
            head.iter().any(|conclusion| {
                premise
                    .positions()
                    .into_iter()
                    .zip(conclusion.positions())
                    .all(|(x, y)| compatible(x, y))
            })
        })
}

/// Whether every existential atom of `piece` could be produced by some atom of `head`
fn covers(head: &[Atom], piece: &[Atom]) -> bool {
    piece.iter().filter(|atom| atom.has_exists()).all(|atom| {
        head.iter().any(|candidate| {
            atom.positions()
                .into_iter()
                .zip(candidate.positions())
                .all(|(a, b)| compatible(a, b))
        })
    })
}
