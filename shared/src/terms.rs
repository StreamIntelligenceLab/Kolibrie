/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Term {
    Variable(String),
    Constant(u32),
}

pub type TriplePattern = (Term, Term, Term);

#[derive(Debug)]
pub enum UnresolvedTerm {
    Var(String),
    Prefixed(String),
}

pub type UnresolvedTriple = (UnresolvedTerm, UnresolvedTerm, UnresolvedTerm);