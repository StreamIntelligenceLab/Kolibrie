/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Matching a triple pattern against a fact, and building a fact from a pattern.
//!
//! These are vocabulary operations with no reasoning in them: they relate a
//! `TriplePattern` to a `Triple` through a variable binding, and nothing more. They
//! lived in `datalog::reasoning::rules` for historical reasons, which made them
//! unreachable for any other engine without taking a dependency on the Datalog
//! reasoner.
//!
//! That mattered because they were the *entire* coupling between `datalogmtl` and
//! `datalog` — four imports. Moving them here lets the two reasoners be siblings over
//! a shared vocabulary rather than one depending on the other, which is what the
//! temporal, N3 and incremental work all need (architecture doc §4).

use crate::dictionary::Dictionary;
use crate::terms::{Term, TriplePattern};
use crate::triple::Triple;
use std::collections::HashMap;

/// Matches `pattern` against `fact`, extending `variable_bindings` on success.
///
/// Bindings are all-or-nothing: a pattern that matches on subject and predicate but
/// fails on object leaves `variable_bindings` untouched, so a caller can try
/// candidates in a loop without having to undo partial work.
///
/// A variable already bound must match the fact's value; an unbound variable binds to
/// it. Quoted triples never match — pattern matching over them is not defined here.
pub fn matches_rule_pattern(
    pattern: &TriplePattern,
    fact: &Triple,
    variable_bindings: &mut HashMap<String, u32>,
) -> bool {
    // Test against a copy so a late failure cannot leave half the bindings applied.
    let mut temp_bindings = variable_bindings.clone();

    // Subject
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

    // Predicate
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

    // Object
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

    // Only if ALL parts match, commit the bindings
    if s_ok && p_ok && o_ok {
        *variable_bindings = temp_bindings;
        true
    } else {
        false
    }
}

/// Builds a `Triple` from a conclusion pattern and a set of bound variables.
///
/// An unbound variable, or a quoted triple, resolves to id `0`. That is a
/// pre-existing convention carried over with the move rather than a chosen one: it
/// silently produces a fact mentioning whatever term holds id 0 instead of reporting
/// that the rule's conclusion was not fully bound.
///
/// `_dict` is currently unused. It is kept rather than dropped because §4 has this
/// function taking a `&mut impl TermEncoder` once parsing is split into
/// parse/prepare/execute: callers already thread a dictionary here, so that becomes a
/// change of type rather than of signature. If that split does not happen, this
/// parameter should go.
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
