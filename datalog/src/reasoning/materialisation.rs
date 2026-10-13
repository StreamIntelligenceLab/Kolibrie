//! Forward chaining: deriving every fact the rules entail.
//!
//! All of these run to a fixpoint. They differ in what they cost and in what they
//! record on the way, and the repair variant additionally alters the fact base
//! before it starts, so it is the one that can disagree about the result.
//!
//! | strategy | entry point | records per fact |
//! |---|---|---|
//! | naive | `Reasoner::infer_new_facts_naive` | — |
//! | semi-naive | `Reasoner::infer_new_facts_semi_naive` | — |
//! | semi-naive, parallel | `Reasoner::infer_new_facts_semi_naive_parallel` | — |
//! | semi-naive, repairing | `Reasoner::infer_new_facts_semi_naive_with_repairs` | — |
//! | provenance | [`provenance_semi_naive`] | a semiring tag |
//! | lineage | [`hybrid_materialisation`] | a Boolean lineage formula |
//! | SDD-seeded | [`sdd_seed_materialise`] | an SDD node |
//! | cross-window | [`cross_window_naive`], [`cross_window_incremental`] | an expiry time |
//!
//! The first four live in `pub(crate)` modules on purpose. Their methods are
//! inherent on [`Reasoner`](crate::reasoning::Reasoner) and so remain reachable
//! through the type; it is the module path, not the method, that is not API.

use shared::dictionary::Dictionary;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use std::collections::HashMap;

// These four contain only `impl Reasoner` blocks. Inherent impls are not namespaced
// by their module, so demoting the module does NOT hide the methods — callers reach
// `reasoner.infer_new_facts_semi_naive()` through the type, and 43 external call sites
// keep working. What this does buy: nothing outside can couple to these paths, and the
// modules leave the documentation ledger. Anything exporting a type or free function
// by path stays `pub` below.
pub(crate) mod semi_naive;
pub(crate) mod my_naive;
pub(crate) mod semi_naive_parallel;
pub(crate) mod semi_naive_with_repairs;

pub mod infer_generic;
pub mod provenance_infer_generic;
pub mod provenance_semi_naive;
pub mod hybrid_materialisation;
pub mod sdd_seed_materialise;
pub mod cross_window_naive;
pub mod cross_window_incremental;

/// The id a conclusion term stands for, given the bindings that satisfied the premises.
///
/// Returns 0 for an unbound variable (after warning on stderr) and for a quoted
/// triple, which is not resolved here. Term id 0 is a real dictionary id, so a
/// conclusion built from one is indistinguishable from a legitimately derived fact —
/// the warning is the only signal.
fn get_id_from_term(term: &Term, vars: &HashMap<String, u32>) -> u32 {
    match term {
        // If term is a variable, then get the value bound to the variable
        Term::Variable(v) => vars.get(v).copied().unwrap_or_else(|| {
            eprintln!(
                "Warning: Variable '{}' not found in bindings. Available variables: {:?}",
                v,
                vars.keys().collect::<Vec<_>>()
            );
            0
        }),
        Term::Constant(c) => *c,
        Term::QuotedTriple(_) => 0,
    }
}

/// Construct a new Triple from a conclusion pattern and bound variables
pub fn replace_variables_with_bound_values(
    conclusion: &TriplePattern,
    vars: &HashMap<String, u32>,
    dict: &mut Dictionary,
) -> Triple {
    let subject = get_id_from_term(&conclusion.0, vars);
    let predicate = get_id_from_term(&conclusion.1, vars);

    // Also some kind of get_id_from_term, but uses ml_output_placeholder or something as well.
    let object = match &conclusion.2 {
        Term::Variable(v) => {
            // Check if this variable is bound in the current context
            if let Some(&bound_value) = vars.get(v) {
                bound_value
            } else {

                // If not bound, create a new placeholder in the dictionary
                dict.encode(&format!("ml_output_placeholder_{}", v))
            }
        }
        Term::Constant(c) => *c,
        Term::QuotedTriple(_) => 0,
    };

    Triple {
        subject,
        predicate,
        object,
    }
}
