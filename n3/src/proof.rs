/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Proofs of what a run outputs, in eyeron's `pe:` vocabulary, so eyeron's checker can verify them

use crate::builtins::Builtin;
use crate::parser::{LOCAL_EXISTENTIAL, LOCAL_UNIVERSAL};
use crate::reasoner::N3Reasoner;
use crate::rules::{Atom, PTerm};
use crate::syntax::Node;
use crate::syntax::Statement;
use shared::terms::Binding;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};

/// The proof vocabulary
const PE: &str = "https://eyereasoner.github.io/pe#";

/// What a rule step cites
enum Citation {
    /// A source rule, by number
    Number(usize),
    /// A rule given as its statement, which the proof justifies first when `true`
    Carried(Triple, bool),
    /// A rule given as the text of its statement
    Written(String),
}

/// How one premise of a rule step is justified
enum Premise {
    /// A triple justified by its own step
    Triple(Triple),
    /// A built-in call, justified by re-evaluating it
    Builtin(Triple, String),
}

impl N3Reasoner {
    /// A proof of what the run output, for eyeron's `--check-proof`: the claims, then one step for
    /// every triple they rest on
    ///
    /// The run must have had [`N3Reasoner::enable_proofs`] called before loading. A triple
    /// with no recorded support is written as `pe:unproven`, which a checker rejects.
    pub fn proof_n3(&mut self) -> String {
        let claims: Vec<Triple> = self.output().to_vec();
        self.witness_prefix = self.free_witness_prefix();
        let mut out = format!("@prefix pe: <{PE}> .\n\n");
        for claim in &claims {
            out.push_str(&self.statement_text(claim));
            out.push('\n');
        }
        let mut done = HashSet::new();
        let mut steps = Vec::new();
        for claim in &claims {
            self.justify(claim, &mut done, &mut steps);
        }
        for step in steps {
            out.push('\n');
            out.push_str(&step);
        }
        out
    }

    /// Writes the step for `triple`, then the steps for whatever it uses, each once, depth first
    /// without recursion, since a derivation chain can be very long
    fn justify(&mut self, triple: &Triple, done: &mut HashSet<Triple>, steps: &mut Vec<String>) {
        let mut stack = vec![triple.clone()];
        while let Some(triple) = stack.pop() {
            if !done.insert(triple.clone()) {
                continue;
            }
            let mut uses = self.step(&triple, steps);
            uses.reverse();
            stack.extend(uses);
        }
    }

    /// Writes the step for one triple and returns the triples it uses, in order
    fn step(&mut self, triple: &Triple, steps: &mut Vec<String>) -> Vec<Triple> {
        let conclusion = self.quoted(triple);
        if let Some(iri) = self.builtin_steps.get(triple) {
            steps.push(format!("{conclusion}\n  pe:builtin <{iri}>.\n"));
            return Vec::new();
        }
        let derivation = self
            .derivations
            .get(triple)
            .cloned()
            .filter(|(rule_index, binding)| {
                // A rule that restates its own premise derives nothing a proof can rest on.
                let rule = std::rc::Rc::clone(&self.rules[*rule_index]);
                !rule.body.iter().any(|atom| {
                matches!(self.premise(atom, binding), Some(Premise::Triple(t)) if t == *triple)
            })
            });
        if let Some((rule_index, binding)) = derivation {
            let rule = std::rc::Rc::clone(&self.rules[rule_index]);
            let citation = match self.rule_numbers.get(&rule_index) {
                Some(number) => Citation::Number(*number),
                None => match self.carried.get(&rule_index).cloned() {
                    Some(source) => Citation::Carried(source, true),
                    None => return self.unproven(&conclusion, "a rule with no source", steps),
                },
            };
            return self.rule_step(&conclusion, citation, &binding, &rule.body, "", steps);
        }
        if let Some(label) = self.fact_labels.get(triple) {
            steps.push(format!("{conclusion}\n  pe:fact \"{}\".\n", escape(label)));
            return Vec::new();
        }
        if self.is_list_step(triple) {
            let iri = self
                .store
                .dictionary
                .decode(triple.predicate)
                .unwrap_or("")
                .to_string();
            steps.push(format!("{conclusion}\n  pe:builtin <{iri}>.\n"));
            return Vec::new();
        }
        if let Some(label) = self.universal_fact_for(triple) {
            steps.push(format!("{conclusion}\n  pe:fact \"{}\".\n", escape(&label)));
            return Vec::new();
        }
        if let Some((origin, binding)) = self.universal_assertion_for(triple) {
            let rule = std::rc::Rc::clone(&self.rules[origin]);
            let citation = match self.rule_numbers.get(&origin) {
                Some(number) => Citation::Number(*number),
                None => match self.carried.get(&origin).cloned() {
                    Some(source) => Citation::Carried(source, true),
                    None => return self.unproven(&conclusion, "a rule with no source", steps),
                },
            };
            return self.rule_step(&conclusion, citation, &binding, &rule.body, "", steps);
        }
        let traced = self.matcher().trace(triple);
        if let Some((rule_index, solution, body)) = traced {
            if let Some(label) = self.universal_labels.get(&rule_index) {
                steps.push(format!("{conclusion}\n  pe:fact \"{}\".\n", escape(label)));
                return Vec::new();
            }
            let citation = match self.backward_numbers.get(&rule_index) {
                Some(&number) => Citation::Number(number),
                None => match self.carried_backward.get(&rule_index).cloned() {
                    Some(source) => Citation::Carried(source, true),
                    None => {
                        return self.unproven(&conclusion, "a backward rule with no source", steps)
                    }
                },
            };
            return self.rule_step(&conclusion, citation, &solution, &body, "#0#", steps);
        }
        // An answer no fact or rule supports is justified by applying its query.
        if let Some((query_index, binding)) = self.query_answers.get(triple).cloned() {
            let body = self.queries[query_index].body.clone();
            let Some(source) = self.query_sources.get(&query_index).cloned() else {
                return self.unproven(&conclusion, "a query with no source", steps);
            };
            // Written out as the source spells it, so its premises are in the order the step lists.
            let as_rule = format!(
                "{{ {} <{}> {} . }}",
                self.render(&source.subject),
                crate::syntax::LOG_IMPLIES,
                self.render(&source.object)
            );
            return self.rule_step(
                &conclusion,
                Citation::Written(as_rule),
                &binding,
                &body,
                "",
                steps,
            );
        }
        self.unproven(&conclusion, "no recorded support", steps)
    }

    /// The source label of a universal fact `triple` is an instance of, existentials matching any
    /// blank node
    fn universal_fact_for(&self, triple: &Triple) -> Option<String> {
        let mut indexes: Vec<&usize> = self.universal_labels.keys().collect();
        indexes.sort();
        indexes.into_iter().find_map(|&index| {
            let head = self.backward.get(index)?.head.first()?;
            self.instance_of(head, triple)
                .then(|| self.universal_labels[&index].clone())
        })
    }

    /// The forward firing that asserted a universal statement `triple` is an instance of
    fn universal_assertion_for(&self, triple: &Triple) -> Option<(usize, Binding)> {
        let mut indexes: Vec<&usize> = self.universal_origins.keys().collect();
        indexes.sort();
        indexes.into_iter().find_map(|&index| {
            let head = self.backward.get(index)?.head.first()?;
            self.instance_of(head, triple)
                .then(|| self.universal_origins[&index].clone())
        })
    }

    /// Whether a ground triple is an instance of a pattern, its existentials matching blank nodes
    fn instance_of(&self, pattern: &Atom, triple: &Triple) -> bool {
        let mut binding = Binding::new();
        for (position, id) in
            pattern
                .positions()
                .into_iter()
                .zip([triple.subject, triple.predicate, triple.object])
        {
            let ok = match position {
                PTerm::Const(c) => *c == id,
                PTerm::Exists(_) => self
                    .store
                    .dictionary
                    .decode(id)
                    .is_some_and(|s| s.starts_with("_:")),
                PTerm::Var(name) => *binding.entry(name.clone()).or_insert(id) == id,
                _ => false,
            };
            if !ok {
                return false;
            }
        }
        true
    }

    fn unproven(&self, conclusion: &str, reason: &str, steps: &mut Vec<String>) -> Vec<Triple> {
        steps.push(format!("{conclusion}\n  pe:unproven \"{reason}\".\n"));
        Vec::new()
    }

    /// Whether a triple is `rdf:first` or `rdf:rest` of a native list, which holds by construction
    fn is_list_step(&self, triple: &Triple) -> bool {
        let names = |iri| self.store.lookup(iri);
        let is_list = matches!(self.store.compound(triple.subject), Some(crate::terms::Compound::List(items)) if !items.is_empty());
        is_list
            && (Some(triple.predicate) == names(crate::matcher::RDF_FIRST)
                || Some(triple.predicate) == names(crate::matcher::RDF_REST))
    }

    /// Writes a rule step: what it cites, its bindings, and its premises under them; `prefix` is
    /// what renaming the rule apart put in front of its variable names
    fn rule_step(
        &mut self,
        conclusion: &str,
        citation: Citation,
        binding: &Binding,
        body: &[Atom],
        prefix: &str,
        steps: &mut Vec<String>,
    ) -> Vec<Triple> {
        let mut needed = Vec::new();
        let citation = match citation {
            Citation::Number(number) => format!("{number}"),
            Citation::Carried(source, justify) => {
                if justify {
                    needed.push(source.clone());
                }
                self.carried_text(&source)
            }
            Citation::Written(text) => text,
        };
        let premises: Option<Vec<Premise>> = body
            .iter()
            .map(|atom| self.premise(atom, binding))
            .collect();
        let Some(premises) = premises else {
            return self.unproven(conclusion, "a premise could not be written out", steps);
        };
        let mut step = format!("{conclusion}\n  pe:rule {citation}");
        let bindings = self.bindings(binding, prefix);
        if !bindings.is_empty() {
            step.push_str(";\n  pe:binding\n    ");
            step.push_str(&bindings.join(",\n    "));
        }
        let uses: Vec<String> = premises
            .iter()
            .map(|premise| match premise {
                Premise::Triple(t) | Premise::Builtin(t, _) => self.quoted(t),
            })
            .collect();
        if !uses.is_empty() {
            step.push_str(";\n  pe:uses\n    ");
            step.push_str(&uses.join(",\n    "));
        }
        step.push_str(".\n");
        steps.push(step);
        for premise in premises {
            match premise {
                Premise::Triple(t) => needed.push(t),
                Premise::Builtin(t, iri) => {
                    if !self.derivations.contains_key(&t) && !self.fact_labels.contains_key(&t) {
                        self.builtin_steps.insert(t.clone(), iri);
                    }
                    needed.push(t);
                }
            }
        }
        needed
    }

    /// A premise of a rule step as the triple it matched under the step's binding
    fn premise(&mut self, atom: &Atom, binding: &Binding) -> Option<Premise> {
        let mut matcher = self.matcher();
        let position = |m: &mut crate::matcher::Matcher<'_>, p: &PTerm| match atom.builtin {
            Some(Builtin::Scoped(_)) => m.quote_all(p, binding),
            _ => m.ground(p, binding),
        };
        let triple = Triple {
            subject: position(&mut matcher, &atom.subject)?,
            predicate: position(&mut matcher, &atom.predicate)?,
            object: position(&mut matcher, &atom.object)?,
        };
        Some(match atom.builtin {
            Some(_) => {
                let iri = self.store.dictionary.decode(triple.predicate)?.to_string();
                Premise::Builtin(triple, iri)
            }
            None => Premise::Triple(triple),
        })
    }

    /// The step's bindings of the rule's own variables, under the names the rule spells them,
    /// sorted by name; `prefix` is stripped from the names renaming put it on
    fn bindings(&mut self, binding: &Binding, prefix: &str) -> Vec<String> {
        let mut named: Vec<(String, u32)> = binding
            .iter()
            .filter_map(|(name, &id)| {
                let name = name.strip_prefix(prefix)?;
                let written = name
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_')
                    && !name.starts_with("__")
                    && !name.starts_with("_:");
                written.then(|| (name.to_string(), id))
            })
            .collect();
        named.sort();
        named
            .into_iter()
            .filter_map(|(name, id)| {
                let value = crate::matcher::settle(&mut self.store, id, binding)?;
                Some(format!(
                    "[ pe:var \"{}\"; pe:value {} ]",
                    escape(&name),
                    self.term_text(value)
                ))
            })
            .collect()
    }

    /// A carried rule's statement, `true` as a side written as the empty graph
    fn carried_text(&self, triple: &Triple) -> String {
        let statement = self.store.statement(triple);
        let side = |node: &Node| match node {
            Node::Literal { lexical, .. } if lexical == "true" => "{}".to_string(),
            other => self.render(other),
        };
        format!(
            "{{ {} {} {} . }}",
            side(&statement.subject),
            self.render(&statement.predicate),
            side(&statement.object)
        )
    }

    fn quoted(&self, triple: &Triple) -> String {
        format!("{{ {} }}", self.statement_text(triple))
    }

    fn statement_text(&self, triple: &Triple) -> String {
        let statement = self.store.statement(triple);
        format!(
            "{} {} {} .",
            self.render(&statement.subject),
            self.render(&statement.predicate),
            self.render(&statement.object)
        )
    }

    fn render(&self, node: &Node) -> String {
        render(node, &self.witness_prefix)
    }

    /// `sk_`, or a longer prefix if a source blank node label starts with it
    fn free_witness_prefix(&self) -> String {
        let source: Vec<String> = self
            .store
            .dictionary
            .string_to_id
            .keys()
            .filter_map(|spelling| spelling.strip_prefix("_:kolibrie-doc-"))
            .map(|label| proof_label(&format!("kolibrie-doc-{label}"), ""))
            .collect();
        let mut prefix = "sk_".to_string();
        while source.iter().any(|label| label.starts_with(&prefix)) {
            prefix.insert(2, '0');
        }
        prefix
    }

    fn term_text(&self, id: u32) -> String {
        self.render(&self.store.node(id))
    }
}

/// A term as N3, its blank nodes labelled as eyeron reads the source and a witness by its number
/// after `witness`
fn render(node: &Node, witness: &str) -> String {
    Renderer {
        witness,
        scopes: Vec::new(),
        declared: 0,
    }
    .term(node)
}

/// Writes one term, naming the quantifiers of each formula in it
struct Renderer<'a> {
    witness: &'a str,
    /// For each formula being written, outermost first, the names its own binders are written as
    scopes: Vec<HashMap<String, String>>,
    declared: usize,
}

impl Renderer<'_> {
    fn term(&mut self, node: &Node) -> String {
        if let Some(name) = self.binder_name(node) {
            return name;
        }
        match node {
            Node::Blank(label) => format!("_:{}", proof_label(label, self.witness)),
            // A rule's blank node stands for a variable, but the rule spells it as a blank node.
            Node::Var(name) => match unrenamed(name).strip_prefix("_:") {
                Some(label) => format!("_:{}", proof_label(label, self.witness)),
                None => format!("?{}", variable_name(name)),
            },
            Node::List(items) => {
                let members: Vec<String> = items.iter().map(|item| self.term(item)).collect();
                format!("({})", members.join(" "))
            }
            Node::Formula(statements) => self.formula(statements),
            other => other.to_string(),
        }
    }

    /// A formula, its own `@forAll` and `@forSome` names declared before its statements
    fn formula(&mut self, statements: &[Statement]) -> String {
        let mut universals = Vec::new();
        let mut existentials = Vec::new();
        for statement in statements {
            for node in [&statement.subject, &statement.predicate, &statement.object] {
                own_binders(node, 0, &mut universals, &mut existentials);
            }
        }
        let mut scope = HashMap::new();
        let mut declarations = String::new();
        for (keyword, names) in [("@forAll", &universals), ("@forSome", &existentials)] {
            if names.is_empty() {
                continue;
            }
            let written: Vec<String> = names
                .iter()
                .map(|name| {
                    self.declared += 1;
                    let iri = format!("<urn:kolibrie:quantified:{}>", self.declared);
                    scope.insert(name.clone(), iri.clone());
                    iri
                })
                .collect();
            declarations.push_str(&format!("{keyword} {} . ", written.join(", ")));
        }
        self.scopes.push(scope);
        let inner: Vec<String> = statements
            .iter()
            .map(|s| {
                format!(
                    "{} {} {} .",
                    self.term(&s.subject),
                    self.term(&s.predicate),
                    self.term(&s.object)
                )
            })
            .collect();
        self.scopes.pop();
        format!("{{ {declarations}{} }}", inner.join(" "))
    }

    /// The name a formula's own binder is declared as, found by the depth its name records
    fn binder_name(&self, node: &Node) -> Option<String> {
        let (depth, name) = local_binder(node)?;
        let scope = self.scopes.len().checked_sub(depth + 1)?;
        self.scopes[scope].get(name).cloned()
    }
}

/// A formula's own universal or existential, with how many formulas above the one it is in
/// binds it
fn local_binder(node: &Node) -> Option<(usize, &str)> {
    let name = match node {
        Node::Var(name) => name.strip_prefix(LOCAL_UNIVERSAL).map(|_| name.as_str()),
        Node::Blank(label) => label
            .strip_prefix(LOCAL_EXISTENTIAL)
            .map(|_| label.as_str()),
        _ => None,
    }?;
    let tail = name
        .strip_prefix(LOCAL_UNIVERSAL)
        .or_else(|| name.strip_prefix(LOCAL_EXISTENTIAL))?;
    let (depth, _) = tail.split_once('.')?;
    Some((depth.parse().ok()?, name))
}

/// Collects, in order of first occurrence, the binders `node` holds of the formula `level`
/// formulas above it
fn own_binders(
    node: &Node,
    level: usize,
    universals: &mut Vec<String>,
    existentials: &mut Vec<String>,
) {
    match node {
        Node::List(items) => {
            for item in items {
                own_binders(item, level, universals, existentials);
            }
        }
        Node::Formula(statements) => {
            for statement in statements {
                for inner in [&statement.subject, &statement.predicate, &statement.object] {
                    own_binders(inner, level + 1, universals, existentials);
                }
            }
        }
        other => {
            if let Some((depth, name)) = local_binder(other) {
                let names = if matches!(other, Node::Var(_)) {
                    &mut *universals
                } else {
                    &mut *existentials
                };
                if depth == level && !names.iter().any(|n| n == name) {
                    names.push(name.to_string());
                }
            }
        }
    }
}

/// A variable name as the rule spells it: without the prefix renaming apart added, in the
/// characters an N3 variable may use
fn variable_name(name: &str) -> String {
    unrenamed(name)
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// A variable name without the prefixes renaming apart added
fn unrenamed(name: &str) -> &str {
    let mut rest = name;
    while let Some(after) = rest.strip_prefix('#') {
        rest = after.split_once('#').map_or(after, |(_, tail)| tail);
    }
    rest
}

/// A blank node label without the scope this crate adds to keep documents apart, a witness
/// labelled by its number after `witness`
fn proof_label(label: &str, witness: &str) -> String {
    let mut rest = label;
    if let Some(after) = rest.strip_prefix("kolibrie-doc-") {
        rest = after.split_once('-').map_or(after, |(_, tail)| tail);
        if let Some(after) = rest.strip_prefix(".f") {
            if let Some((_, tail)) = after.split_once('-') {
                rest = tail;
            }
        }
        rest = rest.strip_prefix('.').unwrap_or(rest);
    } else if let Some(serial) = rest.strip_prefix("kolibrie-n3-.e") {
        return format!("{witness}{serial}");
    }
    rest.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn escape(text: &str) -> String {
    shared::rdf_term::escape_lexical(text)
}
