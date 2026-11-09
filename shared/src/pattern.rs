/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Matching a triple pattern against a fact, and building a fact from a pattern

use crate::dictionary::Dictionary;
use crate::terms::{Term, TriplePattern};
use crate::triple::Triple;
use std::collections::HashMap;

/// Matches `pattern` against `fact`, extending `variable_bindings` on success
pub fn matches_rule_pattern(
    pattern: &TriplePattern,
    fact: &Triple,
    variable_bindings: &mut HashMap<String, u32>,
) -> bool {
    let mut temp_bindings = variable_bindings.clone();

    let s_ok = match &pattern.0 {
        Term::Variable(v) => {
            if let Some(&bound) = temp_bindings.get(v) {
                bound == fact.subject
            } else {
                temp_bindings.insert(v.clone(), fact.subject);
                true
            }
        }
        Term::Constant(c) => *c == fact.subject,
        Term::QuotedTriple(_) => false,
    };
    if !s_ok {
        return false;
    }

    let p_ok = match &pattern.1 {
        Term::Variable(v) => {
            if let Some(&bound) = temp_bindings.get(v) {
                bound == fact.predicate
            } else {
                temp_bindings.insert(v.clone(), fact.predicate);
                true
            }
        }
        Term::Constant(c) => *c == fact.predicate,
        Term::QuotedTriple(_) => false,
    };
    if !p_ok {
        return false;
    }

    let o_ok = match &pattern.2 {
        Term::Variable(v) => {
            if let Some(&bound) = temp_bindings.get(v) {
                bound == fact.object
            } else {
                temp_bindings.insert(v.clone(), fact.object);
                true
            }
        }
        Term::Constant(c) => *c == fact.object,
        Term::QuotedTriple(_) => false,
    };

    if s_ok && p_ok && o_ok {
        *variable_bindings = temp_bindings;
        true
    } else {
        false
    }
}

/// Builds a `Triple` from a conclusion pattern and a set of bound variables
pub fn construct_triple(
    conclusion: &TriplePattern,
    vars: &HashMap<String, u32>,
    _dict: &mut Dictionary,
) -> Triple {
    fn resolve(term: &Term, vars: &HashMap<String, u32>) -> u32 {
        match term {
            Term::Variable(v) => vars.get(v).copied().unwrap_or(0),
            Term::Constant(c) => *c,
            Term::QuotedTriple(_) => 0,
        }
    }
    Triple {
        subject: resolve(&conclusion.0, vars),
        predicate: resolve(&conclusion.1, vars),
        object: resolve(&conclusion.2, vars),
    }
}
