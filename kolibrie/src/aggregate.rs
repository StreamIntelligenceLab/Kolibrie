/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeSet, HashMap};

#[derive(Clone, Copy)]
pub(crate) struct Aggregate<'a> {
    pub kind: &'a str,
    pub distinct: bool,
}
impl<'a> Aggregate<'a> {
    pub fn parse(kind: &'a str) -> Self {
        match kind.strip_suffix("_DISTINCT") {
            Some(kind) => Self {
                kind,
                distinct: true,
            },
            None => Self {
                kind,
                distinct: false,
            },
        }
    }
    pub fn star_count<V: Ord>(&self, rows: &[HashMap<String, V>]) -> String {
        if !self.distinct {
            return rows.len().to_string();
        }
        let unique: BTreeSet<Vec<(&String, &V)>> = rows
            .iter()
            .map(|row| {
                let mut entries: Vec<_> = row.iter().collect();
                entries.sort_by(|left, right| left.0.cmp(right.0));
                entries
            })
            .collect();
        unique.len().to_string()
    }
}

pub(crate) fn value(kind: &str, values: &[&str]) -> Option<String> {
    let normalized = kind.to_ascii_uppercase();
    let descriptor = Aggregate::parse(&normalized);
    // Keep first-seen order for numeric accumulation
    let mut seen = BTreeSet::new();
    let values: Vec<&str> = values
        .iter()
        .copied()
        .filter(|v| !descriptor.distinct || seen.insert(*v))
        .collect();
    match descriptor.kind {
        "COUNT" => Some(values.len().to_string()),
        "SUM" | "AVG" => {
            let numbers = values
                .iter()
                .map(|v| v.parse::<f64>().ok())
                .collect::<Option<Vec<_>>>()?;
            let mut result = numbers.iter().sum::<f64>();
            if descriptor.kind == "AVG" && !numbers.is_empty() {
                result /= numbers.len() as f64;
            }
            Some(if result == 0.0 {
                "0".to_owned()
            } else {
                result.to_string()
            })
        }
        "MIN" => crate::term_order::minimum(values).map(str::to_string),
        "MAX" => crate::term_order::maximum(values).map(str::to_string),
        _ => None,
    }
}
