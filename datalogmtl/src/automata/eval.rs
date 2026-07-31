/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Interval-native materialization: a naive fixpoint over interval-valued facts.
//! Each rule body is folded by conjunctive join; the head projection unions new
//! intervals into the database; iterate until nothing changes (or `max_iters`).
//! This is event-complexity — cost scales with interval endpoints, not the
//! horizon or interval widths.

use std::collections::HashMap;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use crate::syntax::DatalogMTLRule;
use super::interval::TInterval;
use super::relation::{conjoin, eval_atom, Database};

/// Materialize `rules` over interval-valued `facts`. `max_iters` caps the
/// fixpoint for programs that are not finitely materializable (their minimal
/// model is infinite/periodic — handled by the ω-automaton layer, future work).
pub fn materialize(
    rules: &[DatalogMTLRule],
    facts: Vec<(Triple, TInterval)>,
    max_iters: usize,
) -> Database {
    let mut db = Database::new();
    for (t, iv) in facts {
        db.add_raw(t, iv);
    }
    db.coalesce_all();

    for _ in 0..max_iters {
        let mut derived: Vec<(Triple, Vec<TInterval>)> = Vec::new();
        for rule in rules {
            derived.extend(eval_rule(rule, &db));
        }
        let mut changed = false;
        for (t, ints) in derived {
            if db.add_intervals(t, &ints) {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    db
}

/// Evaluate one rule against the current database, returning head facts with
/// the intervals over which the (conjunctive) body holds.
pub fn eval_rule(rule: &DatalogMTLRule, db: &Database) -> Vec<(Triple, Vec<TInterval>)> {
    let Some((first, rest)) = rule.body.split_first() else {
        return Vec::new();
    };
    let mut rel = eval_atom(first, db);
    for atom in rest {
        if rel.rows.is_empty() {
            return Vec::new();
        }
        let r = eval_atom(atom, db);
        rel = conjoin(&rel, &r);
    }

    let mut out = Vec::new();
    for (key, ivs) in &rel.rows {
        let binding: HashMap<String, u32> =
            rel.vars.iter().cloned().zip(key.iter().copied()).collect();
        if let Some(t) = resolve_head(&rule.head, &binding) {
            out.push((t, ivs.clone()));
        }
    }
    out
}

fn resolve_head(pattern: &TriplePattern, binding: &HashMap<String, u32>) -> Option<Triple> {
    let resolve = |term: &Term| -> Option<u32> {
        match term {
            Term::Constant(c) => Some(*c),
            Term::Variable(v) => binding.get(v).copied(),
            Term::QuotedTriple(_) => None,
        }
    };
    Some(Triple {
        subject: resolve(&pattern.0)?,
        predicate: resolve(&pattern.1)?,
        object: resolve(&pattern.2)?,
    })
}
