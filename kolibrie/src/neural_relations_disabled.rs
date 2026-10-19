/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stand-ins for [`neural_relations`](crate::neural_relations) when the `ml` feature
//! is off.
//!
//! Every entry point here mirrors the real one's signature and refuses with
//! `ML_FEATURE_DISABLED`, so the rest of the crate compiles without PyO3 and a caller
//! gets a clear error rather than a missing symbol.
//!
//! The refusals are deliberately conditional where they can be: a request that
//! declares no models and no neural relations succeeds, because it asked for nothing
//! the feature provides. Only a request that actually needs a model fails.

pub use crate::ml_syntax::lower_ml_predict_alias;
// Registration is not gated, so the real one serves both builds.
pub use crate::neural_registry::register_neural_declarations;
use crate::sparql_database::SparqlDatabase;
use shared::query::TrainNeuralRelationDecl;
use std::collections::HashMap;

/// Always fails: training needs the `ml` feature.
pub fn execute_train_decl(
    _db: &mut SparqlDatabase,
    _train: &TrainNeuralRelationDecl,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("ML_FEATURE_DISABLED".into())
}
/// Succeeds when implicit materialisation is off or no neural relations are
/// declared, and otherwise refuses.
pub fn materialize_neural_relations_for_patterns(
    db: &mut SparqlDatabase,
    _patterns: &[(&str, &str, &str)],
    _prefixes: &HashMap<String, String>,
) -> Result<(), String> {
    if !db.implicit_neural_materialization() || db.neural_relation_decls.is_empty() {
        Ok(())
    } else {
        Err("ML_FEATURE_DISABLED".into())
    }
}
/// Always fails: running a neural program needs the `ml` feature.
pub fn execute_neural_program(_db: &mut SparqlDatabase, _program: &str) -> Result<(), String> {
    Err("ML_FEATURE_DISABLED".into())
}

/// The stand-in implementation of the query-evaluation hooks.
///
/// Mirrors the real [`NeuralRuntime`] and refuses
/// anything that needs a model, conditionally where it can: a query mentioning no
/// neural relation materialises nothing and succeeds, because it asked for nothing
/// the feature provides.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeuralRuntime;

impl crate::ml_runtime::MlRuntime for NeuralRuntime {
    fn materialize_for_patterns(
        &self,
        database: &mut SparqlDatabase,
        patterns: &[(&str, &str, &str)],
        prefixes: &HashMap<String, String>,
    ) -> Result<(), String> {
        materialize_neural_relations_for_patterns(database, patterns, prefixes)
    }

    fn execute_train_decl(
        &self,
        database: &mut SparqlDatabase,
        declaration: &TrainNeuralRelationDecl,
    ) -> Result<(), String> {
        execute_train_decl(database, declaration).map_err(|error| error.to_string())
    }

    fn execute_program(
        &self,
        database: &mut SparqlDatabase,
        program: &str,
    ) -> Result<(), crate::query_error::QueryError> {
        execute_neural_program(database, program).map_err(crate::query_error::QueryError::Execution)
    }

    fn execute_predict_clause(
        &self,
        _database: &mut SparqlDatabase,
        _clause: &shared::query::MLPredictClause<'_>,
        _rule: &mut shared::query::CombinedRule<'_>,
        _prefixes: &HashMap<String, String>,
    ) -> Result<Vec<shared::triple::Triple>, crate::query_error::QueryError> {
        // A policy refusal, not an execution failure: the rendered code stays
        // `QUERY_POLICY: ML_FEATURE_DISABLED`, which is what the caller was told
        // before these calls went through a trait.
        Err(crate::query_error::QueryError::Ml(
            crate::ml_policy::MlError::Disabled,
        ))
    }
}
