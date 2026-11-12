/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Canonical N3 text for a set of statements, so two reasoners' outputs compare up to blank
//! node names

use crate::syntax::{Node, Statement};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};

const REFINEMENT_ROUNDS: usize = 6;

/// Sorted, deduplicated statements with blank nodes relabelled by their position in the graph
pub fn canonical_lines(statements: &[Statement]) -> Vec<String> {
    canonicalise(statements, &BTreeMap::new())
}

fn collect_blanks(node: &Node, out: &mut BTreeSet<String>) {
    match node {
        Node::Blank(label) => {
            out.insert(label.clone());
        }
        Node::List(items) => items.iter().for_each(|item| collect_blanks(item, out)),
        Node::Formula(_) | Node::Iri(_) | Node::Literal { .. } | Node::Var(_) => {}
    }
}

fn canonicalise(statements: &[Statement], outer: &BTreeMap<String, String>) -> Vec<String> {
    let mut blanks = BTreeSet::new();
    for statement in statements {
        for node in [&statement.subject, &statement.predicate, &statement.object] {
            collect_blanks(node, &mut blanks);
        }
    }
    blanks.retain(|label| !outer.contains_key(label));

    let mut colours: BTreeMap<String, u64> = blanks.iter().map(|b| (b.clone(), 0)).collect();
    for _ in 0..REFINEMENT_ROUNDS {
        let names: BTreeMap<String, String> = colours
            .iter()
            .map(|(label, colour)| (label.clone(), format!("c{colour:x}")))
            .chain(outer.iter().map(|(k, v)| (k.clone(), v.clone())))
            .collect();
        let mut signatures: HashMap<String, Vec<String>> = HashMap::new();
        for statement in statements {
            let line = render_statement(statement, &names);
            for (slot, node) in [&statement.subject, &statement.predicate, &statement.object]
                .into_iter()
                .enumerate()
            {
                let mut here = BTreeSet::new();
                collect_blanks(node, &mut here);
                for label in here {
                    signatures
                        .entry(label)
                        .or_default()
                        .push(format!("{slot}|{line}"));
                }
            }
        }
        for (label, colour) in colours.iter_mut() {
            let mut signature = signatures.remove(label).unwrap_or_default();
            signature.sort();
            let mut hasher = DefaultHasher::new();
            signature.hash(&mut hasher);
            *colour = hasher.finish();
        }
    }

    let mut ordered: Vec<(&String, &u64)> = colours.iter().collect();
    ordered.sort_by_key(|(label, colour)| (**colour, (*label).clone()));
    let mut names: BTreeMap<String, String> = outer.clone();
    for (rank, (label, _)) in ordered.into_iter().enumerate() {
        names.insert(label.clone(), format!("_:c{}_{rank}", outer.len()));
    }
    let mut lines: Vec<String> = statements
        .iter()
        .map(|s| render_statement(s, &names))
        .collect();
    lines.sort();
    lines.dedup();
    lines
}

fn render_statement(statement: &Statement, names: &BTreeMap<String, String>) -> String {
    format!(
        "{} {} {} .",
        render(&statement.subject, names),
        render(&statement.predicate, names),
        render(&statement.object, names)
    )
}

fn render(node: &Node, names: &BTreeMap<String, String>) -> String {
    match node {
        Node::Blank(label) => names
            .get(label)
            .cloned()
            .unwrap_or_else(|| "_:?".to_string()),
        Node::List(items) => {
            let members: Vec<String> = items.iter().map(|item| render(item, names)).collect();
            format!("({})", members.join(" "))
        }
        Node::Formula(statements) => format!("{{{}}}", canonicalise(statements, names).join(" ")),
        other => other.to_string(),
    }
}
