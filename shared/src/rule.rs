/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::terms::{Term, TriplePattern};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct FilterCondition {
    pub variable: String,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Rule {
    pub premise: Vec<TriplePattern>,
    /// Negated body atoms (NOT X) for single-stratum negation-as-failure.
    /// Every variable appearing here must also appear in `premise` (rule safety).
    pub negative_premise: Vec<TriplePattern>,
    pub filters: Vec<FilterCondition>,
    pub conclusion: Vec<TriplePattern>,
}

/// Returns an iterator over the variable names bound by a triple pattern.
pub fn pattern_variables(pat: &TriplePattern) -> impl Iterator<Item = &str> {
    let (s, p, o) = pat;
    [s, p, o].into_iter().filter_map(|t| {
        if let Term::Variable(v) = t { Some(v.as_str()) } else { None }
    })
}

/// Check that every variable in `negative_premise` is bound by at least one positive premise.
///
/// Returns `Err` describing the first unsafe variable found.
pub fn check_rule_safety(rule: &Rule) -> Result<(), String> {
    let bound: HashSet<&str> = rule.premise.iter()
        .flat_map(pattern_variables)
        .collect();
    for pat in &rule.negative_premise {
        for var in pattern_variables(pat) {
            if !bound.contains(var) {
                return Err(format!(
                    "unsafe negation: variable '{}' in NOT body is not bound by any positive premise",
                    var
                ));
            }
        }
    }
    Ok(())
}