/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! The four points at which query evaluation gives the ML layer a chance to act

use crate::database::KolibrieDB;
use crate::query_error::QueryError;
use shared::query::{CombinedRule, MLPredictClause, TrainNeuralRelationDecl};
use shared::triple::Triple;
use std::collections::HashMap;

/// What query evaluation needs from the ML layer
pub trait MlRuntime: Send + Sync {
    /// Derives the facts of any neural relation these patterns mention
    fn materialize_for_patterns(
        &self,
        database: &mut KolibrieDB,
        patterns: &[(&str, &str, &str)],
        prefixes: &HashMap<String, String>,
    ) -> Result<(), String>;

    /// Runs one `TRAIN NEURAL RELATION` declaration
    fn execute_train_decl(
        &self,
        database: &mut KolibrieDB,
        declaration: &TrainNeuralRelationDecl,
    ) -> Result<(), String>;

    /// Runs a request whose top level is an `ML.PREDICT`
    fn execute_program(&self, database: &mut KolibrieDB, program: &str) -> Result<(), QueryError>;

    /// Runs an `ML.PREDICT` clause inside a rule, rewriting the rule in place
    fn execute_predict_clause(
        &self,
        database: &mut KolibrieDB,
        clause: &MLPredictClause<'_>,
        rule: &mut CombinedRule<'_>,
        prefixes: &HashMap<String, String>,
    ) -> Result<Vec<Triple>, QueryError>;
}
