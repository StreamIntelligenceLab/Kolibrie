/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reverse of [`crate::parser`]: render derived RDF triples back into MeTeoR's
//! textual atom form and coalesce per-atom integer tick-sets into closed
//! intervals, so datalogmtl output can be diffed line-for-line against MeTeoR.

use std::collections::{BTreeSet, HashMap};
use shared::dictionary::Dictionary;
use shared::triple::Triple;
use crate::parser::RDF_TYPE;

/// Render a triple as a MeTeoR atom string (no interval):
///   `(x, rdf:type, A)` → `A(x)`;  `(x, C, y)` → `C(x,y)`.
pub fn atom_string(triple: &Triple, dict: &Dictionary) -> String {
    let s = dict.decode(triple.subject).unwrap_or("?");
    let p = dict.decode(triple.predicate).unwrap_or("?");
    let o = dict.decode(triple.object).unwrap_or("?");
    if p == RDF_TYPE {
        format!("{}({})", o, s)
    } else {
        format!("{}({},{})", p, s, o)
    }
}

/// Coalesce a per-triple set of integer timepoints into MeTeoR-format lines
/// `Pred(args)@[l,r]`, one line per contiguous interval, globally sorted.
pub fn format_tick_sets(
    ticks: &HashMap<Triple, BTreeSet<u64>>,
    dict: &Dictionary,
) -> Vec<String> {
    let mut lines = Vec::new();
    for (triple, points) in ticks {
        let atom = atom_string(triple, dict);
        for (lo, hi) in coalesce(points) {
            lines.push(format!("{}@[{},{}]", atom, lo, hi));
        }
    }
    lines.sort();
    lines
}

/// Merge a sorted set of integer points into maximal contiguous closed intervals.
fn coalesce(points: &BTreeSet<u64>) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &t in points {
        match out.last_mut() {
            Some(last) if last.1 + 1 == t => last.1 = t,
            Some(last) if last.1 == t => {}
            _ => out.push((t, t)),
        }
    }
    out
}
