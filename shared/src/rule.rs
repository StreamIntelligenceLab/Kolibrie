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

/// A comparison a candidate binding must satisfy for a rule to fire.
///
/// Operator and value are held as text and interpreted at evaluation time, so an
/// unrecognised operator is ignored rather than rejected at parse time. `value` is
/// compared numerically when both sides parse as numbers and by dictionary id when it
/// names another bound variable.
#[derive(Debug, Clone)]
pub struct FilterCondition {
    /// Name of the variable being constrained, without the leading `?`.
    pub variable: String,
    /// Comparison operator, as written: `=`, `!=`, `<`, `>`, `<=`, `>=`.
    pub operator: String,
    /// Right-hand side: a literal, or the name of another bound variable.
    pub value: String,
}

/// A Datalog rule: if every premise matches and the filters hold, derive the
/// conclusion.
///
/// Variables are shared by name across premises and conclusion, so repeating a name
/// is what expresses a join. Negation is single-stratum and must be *safe* — see
/// [`check_rule_safety`], which callers are expected to run before using a rule.
#[derive(Debug, Clone)]
pub struct Rule {
    /// Body atoms that must all match. Shared variable names join them.
    pub premise: Vec<TriplePattern>,
    /// Negated body atoms (NOT X) for single-stratum negation-as-failure.
    /// Every variable appearing here must also appear in `premise` (rule safety).
    pub negative_premise: Vec<TriplePattern>,
    /// Comparisons that must hold for a binding produced by the premises.
    pub filters: Vec<FilterCondition>,
    /// Atoms derived when the body matches. Every variable here should be bound by
    /// `premise`; one that is not resolves to term id 0 rather than being rejected.
    pub conclusion: Vec<TriplePattern>,
}

/// Returns an iterator over the variable names bound by a triple pattern.
pub fn pattern_variables(pat: &TriplePattern) -> impl Iterator<Item = &str> {
    let (s, p, o) = pat;
    [s, p, o].into_iter().filter_map(|t| {
        if let Term::Variable(v) = t {
            Some(v.as_str())
        } else {
            None
        }
    })
}

/// Check that every variable in `negative_premise` is bound by at least one positive premise.
///
/// Returns `Err` describing the first unsafe variable found.
pub fn check_rule_safety(rule: &Rule) -> Result<(), String> {
    let bound: HashSet<&str> = rule.premise.iter().flat_map(pattern_variables).collect();
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
