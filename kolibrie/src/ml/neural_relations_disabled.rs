/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Stand-ins for [`neural_relations`](crate::ml::neural_relations) when the `ml` feature is off

use crate::database::KolibrieDB;
pub use crate::ml::syntax::lower_ml_predict_alias;
pub use crate::neural_registry::register_neural_declarations;
use shared::query::TrainNeuralRelationDecl;
use std::collections::HashMap;

/// Always fails: training needs the `ml` feature
pub fn execute_train_decl(
    _db: &mut KolibrieDB,
    _train: &TrainNeuralRelationDecl,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("ML_FEATURE_DISABLED".into())
}
/// Succeeds when implicit materialisation is off or no neural relations are declared, and otherwise
/// refuses
pub fn materialize_neural_relations_for_patterns(
    db: &mut KolibrieDB,
    _patterns: &[(&str, &str, &str)],
    _prefixes: &HashMap<String, String>,
) -> Result<(), String> {
    if !db.implicit_neural_materialization() || db.neural_relation_decls.is_empty() {
        Ok(())
    } else {
        Err("ML_FEATURE_DISABLED".into())
    }
}
/// Always fails: running a neural program needs the `ml` feature
pub fn execute_neural_program(_db: &mut KolibrieDB, _program: &str) -> Result<(), String> {
    Err("ML_FEATURE_DISABLED".into())
}

/// The stand-in implementation of the query-evaluation hooks
#[derive(Debug, Clone, Copy, Default)]
pub struct NeuralRuntime;

impl crate::ml::runtime::MlRuntime for NeuralRuntime {
    fn materialize_for_patterns(
        &self,
        database: &mut KolibrieDB,
        patterns: &[(&str, &str, &str)],
        prefixes: &HashMap<String, String>,
    ) -> Result<(), String> {
        materialize_neural_relations_for_patterns(database, patterns, prefixes)
    }

    fn execute_train_decl(
        &self,
        database: &mut KolibrieDB,
        declaration: &TrainNeuralRelationDecl,
    ) -> Result<(), String> {
        execute_train_decl(database, declaration).map_err(|error| error.to_string())
    }

    fn execute_program(
        &self,
        database: &mut KolibrieDB,
        program: &str,
    ) -> Result<(), crate::query_error::QueryError> {
        execute_neural_program(database, program).map_err(crate::query_error::QueryError::Execution)
    }

    fn execute_predict_clause(
        &self,
        _database: &mut KolibrieDB,
        _clause: &shared::query::MLPredictClause<'_>,
        _rule: &mut shared::query::CombinedRule<'_>,
        _prefixes: &HashMap<String, String>,
    ) -> Result<Vec<shared::triple::Triple>, crate::query_error::QueryError> {
        Err(crate::query_error::QueryError::Ml(
            crate::ml_policy::MlError::Disabled,
        ))
    }
}
