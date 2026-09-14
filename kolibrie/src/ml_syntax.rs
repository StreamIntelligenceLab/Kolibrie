/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::parser::parse_sparql_query;
use shared::query::{GroupGraphPattern, MLPredictClause, NeuralRelationDecl};
pub fn lower_ml_predict_alias(
    ml_predict: &MLPredictClause<'_>,
) -> Result<NeuralRelationDecl, String> {
    let (_, parsed) = parse_sparql_query(ml_predict.input_raw)
        .map_err(|err| format!("failed to lower ML.PREDICT INPUT query: {err:?}"))?;
    let input_select = &parsed.variables;
    let mut input_where = Vec::new();
    collect_neural_input_patterns(&parsed.pattern, &mut input_where);
    let anchor_var = input_select
        .iter()
        .find_map(|(kind, var, _)| {
            if *kind == "VAR" || var.starts_with('?') {
                Some((*var).to_string())
            } else {
                None
            }
        })
        .or_else(|| {
            input_where.iter().find_map(|(s, p, o)| {
                [s, p, o]
                    .into_iter()
                    .find(|term| term.starts_with('?'))
                    .map(|term| (*term).to_string())
            })
        })
        .ok_or_else(|| {
            "ML.PREDICT alias lowering requires at least one input variable".to_string()
        })?;
    let feature_vars = if input_select.is_empty() {
        input_where
            .iter()
            .flat_map(|(s, p, o)| [s, p, o])
            .filter(|term| term.starts_with('?'))
            .map(|term| (*term).to_string())
            .collect::<Vec<_>>()
    } else {
        input_select
            .iter()
            .map(|(_, var, _)| (*var).to_string())
            .collect::<Vec<_>>()
    };
    Ok(NeuralRelationDecl {
        predicate: ml_predict.output.to_string(),
        model_name: ml_predict.model.to_string(),
        input_patterns: input_where
            .iter()
            .map(|triple| {
                (
                    triple.0.to_string(),
                    triple.1.to_string(),
                    triple.2.to_string(),
                )
            })
            .collect(),
        feature_vars,
        anchor_var,
    })
}

fn collect_neural_input_patterns<'a>(
    pattern: &'a GroupGraphPattern<'a>,
    output: &mut Vec<(&'a str, &'a str, &'a str)>,
) {
    match pattern {
        GroupGraphPattern::Bgp(patterns) => output.extend(patterns.iter().copied()),
        GroupGraphPattern::Join(patterns) | GroupGraphPattern::Union(patterns) => {
            for pattern in patterns {
                collect_neural_input_patterns(pattern, output);
            }
        }
        GroupGraphPattern::Graph { pattern, .. } => collect_neural_input_patterns(pattern, output),
        GroupGraphPattern::SubQuery(subquery) => {
            collect_neural_input_patterns(&subquery.query.pattern, output)
        }
        GroupGraphPattern::Unit
        | GroupGraphPattern::Filter(_)
        | GroupGraphPattern::Bind(_)
        | GroupGraphPattern::Values(_) => {}
    }
}
