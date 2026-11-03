/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::triple::Triple;

/// A declaration that certain facts carry a prior probability.
///
/// Seeds are the leaves of probabilistic inference: every derived probability is a
/// function of these. Each seed owns a distinct variable in the underlying formula,
/// which is why the ids here must be unique across a program.
#[derive(Debug, Clone)]
pub enum SeedSpec {
    /// One fact, probabilistically independent of every other seed.
    Independent {
        /// The fact the probability attaches to.
        triple: Triple,
        /// Prior probability, in `[0, 1]`.
        prob: f64,
        /// Identifies this seed's variable in the formula. Must be unique.
        seed_id: u32,
    },
    /// A set of mutually exclusive alternatives, at most one of which holds.
    ///
    /// Used where a prediction is a distribution over labels: the choices are not
    /// independent, so treating them as separate `Independent` seeds would assign
    /// non-zero probability to two labels holding at once.
    ExclusiveGroup {
        /// Identifies the group. Must be unique.
        group_id: u32,
        /// The alternatives. Their probabilities should sum to at most 1.
        choices: Vec<ExclusiveChoice>,
    },
}

/// One alternative within a [`SeedSpec::ExclusiveGroup`].
#[derive(Debug, Clone)]
pub struct ExclusiveChoice {
    /// The fact that holds if this alternative is the one taken.
    pub triple: Triple,
    /// Probability of this alternative, in `[0, 1]`.
    pub prob: f64,
    /// Identifies this choice within its group.
    pub choice_id: u32,
}
