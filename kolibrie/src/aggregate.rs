/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::collections::BTreeSet;
#[derive(Clone, Copy)]
pub(crate) struct Aggregate<'a> {
    pub kind: &'a str,
    pub distinct: bool,
}
impl<'a> Aggregate<'a> {
    pub(crate) fn parse(kind: &'a str) -> Self {
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
            // Through the term's value, not its spelling: a typed literal keeps its
            // datatype now, and parsing the spelling would find no number at all,
            // turning every SUM over typed data into no answer.
            let numbers = values
                .iter()
                .map(|v| shared::rdf_term::numeric_value(v))
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
