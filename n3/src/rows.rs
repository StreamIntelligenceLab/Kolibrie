/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Joins for rules that are plain Datalog, with each solution a row of ids instead of a map

use crate::matcher::{Delta, Vocabulary};
use crate::rules::{Atom, N3Rule, PTerm};
use shared::dataset_index::DatasetIndex;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};

/// A slot of a row that holds no value yet
const UNBOUND: u32 = u32::MAX;

/// One position of a compiled atom
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    Const(u32),
    Var(usize),
}

/// A rule whose premises are all looked up and whose conclusion only copies values, compiled so
/// a solution is a row with one column per variable
#[derive(Debug)]
pub(crate) struct RowPlan {
    width: usize,
    body: Vec<[Slot; 3]>,
    /// Each conclusion atom, `None` where it names a variable no premise binds
    head: Vec<Option<[Slot; 3]>>,
}

impl RowPlan {
    /// The plan for `rule`, or `None` when it needs more than lookups and copying: a built-in,
    /// a list or formula pattern, a witness, a test of the whole graph, rule data, or list triples
    pub(crate) fn compile(rule: &N3Rule, vocabulary: &Vocabulary) -> Option<Self> {
        if rule.body.is_empty()
            || rule.deferred
            || rule.fuse
            || rule.has_existentials
            || rule.unquote.is_some()
            || !rule.universal.is_empty()
        {
            return None;
        }
        let special = [vocabulary.implies, vocabulary.first, vocabulary.rest];
        let mut names: Vec<String> = Vec::new();
        let mut body = Vec::with_capacity(rule.body.len());
        for atom in &rule.body {
            if atom.builtin.is_some() || atom.flat.is_none() {
                return None;
            }
            if matches!(atom.predicate, PTerm::Const(id) if special.contains(&Some(id))) {
                return None;
            }
            body.push(slots(atom, &mut names)?);
        }
        let width = names.len();
        // A conclusion variable no premise binds gets a column past the row, and its atom is
        // left out, as the general path leaves it out.
        let mut head = Vec::with_capacity(rule.head.len());
        for atom in &rule.head {
            let compiled = slots(atom, &mut names)?;
            let within = compiled
                .iter()
                .all(|slot| !matches!(slot, Slot::Var(i) if *i >= width));
            head.push(within.then_some(compiled));
        }
        Some(RowPlan { width, body, head })
    }

    /// The conclusion triples one solution gives, in the order the conclusion writes them
    pub(crate) fn conclusion(&self, row: &[u32]) -> Vec<Triple> {
        let value = |slot: Slot| match slot {
            Slot::Const(id) => id,
            Slot::Var(i) => row[i],
        };
        self.head
            .iter()
            .flatten()
            .map(|[s, p, o]| Triple {
                subject: value(*s),
                predicate: value(*p),
                object: value(*o),
            })
            .collect()
    }
}

/// An atom's positions as slots, numbering each variable by first occurrence; `None` for any
/// other kind of position
fn slots(atom: &Atom, names: &mut Vec<String>) -> Option<[Slot; 3]> {
    let mut slot = |position: &PTerm| -> Option<Slot> {
        match position {
            PTerm::Const(id) => Some(Slot::Const(*id)),
            PTerm::Var(name) => Some(Slot::Var(
                names.iter().position(|n| n == name).unwrap_or_else(|| {
                    names.push(name.clone());
                    names.len() - 1
                }),
            )),
            _ => None,
        }
    };
    Some([
        slot(&atom.subject)?,
        slot(&atom.predicate)?,
        slot(&atom.object)?,
    ])
}

/// What a row join reads: the store, and the position each fact was added at
pub(crate) struct RowJoin<'a> {
    pub(crate) facts: &'a DatasetIndex,
    pub(crate) sequence: &'a HashMap<Triple, usize>,
}

impl RowJoin<'_> {
    /// Every solution of `plan` that uses at least one delta fact, or all of them when `full`,
    /// each once, in the order the general join finds them
    pub(crate) fn solutions(&self, plan: &RowPlan, delta: Delta<'_>, full: bool) -> Vec<Vec<u32>> {
        let starts: Vec<Option<usize>> = if full {
            vec![None]
        } else {
            (0..plan.body.len()).map(Some).collect()
        };
        let mut seen: HashSet<Vec<u32>> = HashSet::new();
        let mut out = Vec::new();
        for start in starts {
            for row in self.solve(plan, start.map(|i| (i, delta))) {
                if seen.insert(row.clone()) {
                    out.push(row);
                }
            }
        }
        out
    }

    fn solve(&self, plan: &RowPlan, seed: Option<(usize, Delta<'_>)>) -> Vec<Vec<u32>> {
        let mut remaining: Vec<usize> = (0..plan.body.len()).collect();
        let mut bound = vec![false; plan.width];
        let mut rows = vec![vec![UNBOUND; plan.width]];
        if let Some((first, delta)) = seed {
            remaining.retain(|&i| i != first);
            let atom = &plan.body[first];
            let constants = atom.map(|slot| match slot {
                Slot::Const(id) => Some(id),
                Slot::Var(_) => None,
            });
            rows = scan(atom, delta.matching(constants), rows);
            mark(atom, &mut bound);
        }
        while !remaining.is_empty() && !rows.is_empty() {
            // The most-bound premise next, the earliest on a tie, as the general join picks.
            let pick = remaining
                .iter()
                .enumerate()
                .min_by_key(|(position, &i)| {
                    let fixed = plan.body[i]
                        .iter()
                        .filter(|slot| match slot {
                            Slot::Const(_) => true,
                            Slot::Var(v) => bound[*v],
                        })
                        .count();
                    (std::cmp::Reverse(fixed), *position)
                })
                .map(|(position, _)| position)
                .unwrap_or(0);
            let next = remaining.remove(pick);
            let atom = &plan.body[next];
            rows = self.extend(atom, self.facts, rows);
            mark(atom, &mut bound);
        }
        rows
    }

    /// Extends each row by every fact of `index` the atom matches, the facts in the order added
    fn extend(&self, atom: &[Slot; 3], index: &DatasetIndex, rows: Vec<Vec<u32>>) -> Vec<Vec<u32>> {
        let mut out = Vec::new();
        for row in rows {
            let key = |slot: Slot| match slot {
                Slot::Const(id) => Some(id),
                Slot::Var(i) => (row[i] != UNBOUND).then_some(row[i]),
            };
            let mut candidates = index.query(key(atom[0]), key(atom[1]), key(atom[2]));
            if candidates.len() > 1 {
                candidates.sort_by_cached_key(|t| {
                    (
                        self.sequence.get(t).copied().unwrap_or(usize::MAX),
                        t.clone(),
                    )
                });
            }
            for fact in &candidates {
                if let Some(extended) = matched(atom, fact, &row) {
                    out.push(extended);
                }
            }
        }
        out
    }
}

/// Extends each row by every fact of `facts` the atom matches, in their order
fn scan(atom: &[Slot; 3], facts: &[Triple], rows: Vec<Vec<u32>>) -> Vec<Vec<u32>> {
    let mut out = Vec::new();
    for row in rows {
        for fact in facts {
            if let Some(extended) = matched(atom, fact, &row) {
                out.push(extended);
            }
        }
    }
    out
}

/// `row` extended by `fact` under `atom`, or `None` if they disagree
fn matched(atom: &[Slot; 3], fact: &Triple, row: &[u32]) -> Option<Vec<u32>> {
    let mut extended: Option<Vec<u32>> = None;
    for (slot, id) in atom.iter().zip([fact.subject, fact.predicate, fact.object]) {
        match *slot {
            Slot::Const(c) if c != id => return None,
            Slot::Const(_) => {}
            Slot::Var(i) => {
                let current = extended.as_ref().map_or(row[i], |e| e[i]);
                if current == UNBOUND {
                    extended.get_or_insert_with(|| row.to_vec())[i] = id;
                } else if current != id {
                    return None;
                }
            }
        }
    }
    Some(extended.unwrap_or_else(|| row.to_vec()))
}

fn mark(atom: &[Slot; 3], bound: &mut [bool]) {
    for slot in atom {
        if let Slot::Var(i) = slot {
            bound[*i] = true;
        }
    }
}
