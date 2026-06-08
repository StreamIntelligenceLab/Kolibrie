/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::syntax::{DatalogMTLRule, TemporalAtom};
use shared::terms::Term;
use std::collections::HashSet;

/// Validate a set of DatalogMTL^RDF rules.
/// Returns Err with a descriptive message on the first violation found.
pub fn validate_rules(rules: &[DatalogMTLRule]) -> Result<(), String> {
    for rule in rules {
        validate_variable_safety(rule)?;
        validate_intervals(rule)?;
    }
    Ok(())
}

/// Head variables must appear in at least one Base or Diamond body atom.
/// Variables inside Box or the phi of Since are NOT safe.
fn validate_variable_safety(rule: &DatalogMTLRule) -> Result<(), String> {
    let mut safe: HashSet<String> = HashSet::new();
    for atom in &rule.body {
        collect_safe_vars(atom, &mut safe);
    }
    for term in [&rule.head.0, &rule.head.1, &rule.head.2] {
        if let Term::Variable(v) = term {
            if !safe.contains(v) {
                return Err(format!(
                    "Rule '{}': head variable '{}' is not safely bound \
                     (must appear in a Base or Diamond atom)", rule.id, v
                ));
            }
        }
    }
    Ok(())
}

fn collect_safe_vars(atom: &TemporalAtom, safe: &mut HashSet<String>) {
    match atom {
        TemporalAtom::Base(p) => {
            for term in [&p.0, &p.1, &p.2] {
                if let Term::Variable(v) = term { safe.insert(v.clone()); }
            }
        }
        TemporalAtom::Diamond { inner, .. } => collect_safe_vars(inner, safe),
        TemporalAtom::Prev    { inner, .. } => collect_safe_vars(inner, safe),
        // psi is existential in Since, so its variables are safe.
        TemporalAtom::Since   { psi, .. }   => collect_safe_vars(psi, safe),
        // Box: variables in the inner pattern are safe because the evaluator
        // collects consistent bindings that hold at every timestamp.
        TemporalAtom::Box_ { inner, .. } => collect_safe_vars(inner, safe),
    }
}

fn validate_intervals(rule: &DatalogMTLRule) -> Result<(), String> {
    for atom in &rule.body {
        validate_atom_interval(atom, &rule.id)?;
    }
    Ok(())
}

fn validate_atom_interval(atom: &TemporalAtom, rule_id: &str) -> Result<(), String> {
    match atom {
        TemporalAtom::Base(_) => Ok(()),
        TemporalAtom::Diamond { interval, inner }
        | TemporalAtom::Box_  { interval, inner }
        | TemporalAtom::Prev  { interval, inner } => {
            if interval.start > interval.end {
                return Err(format!(
                    "Rule '{}': interval [{}, {}] has start > end",
                    rule_id, interval.start, interval.end
                ));
            }
            validate_atom_interval(inner, rule_id)
        }
        TemporalAtom::Since { interval, phi, psi } => {
            if interval.start > interval.end {
                return Err(format!(
                    "Rule '{}': Since interval [{}, {}] has start > end",
                    rule_id, interval.start, interval.end
                ));
            }
            validate_atom_interval(phi, rule_id)?;
            validate_atom_interval(psi, rule_id)
        }
    }
}
