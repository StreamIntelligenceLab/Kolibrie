//! Forward chaining: deriving every fact the rules entail

use shared::dictionary::Dictionary;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use std::collections::HashMap;

pub(crate) mod my_naive;
pub(crate) mod semi_naive;
pub(crate) mod semi_naive_parallel;

pub mod cross_window_incremental;
pub mod cross_window_naive;
pub mod hybrid_materialisation;
pub mod infer_generic;
pub mod provenance_infer_generic;
pub mod provenance_semi_naive;
pub mod sdd_seed_materialise;

/// The id a conclusion term stands for, given the bindings that satisfied the premises
fn get_id_from_term(term: &Term, vars: &HashMap<String, u32>) -> u32 {
    match term {
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

    let object = match &conclusion.2 {
        Term::Variable(v) => {
            if let Some(&bound_value) = vars.get(v) {
                bound_value
            } else {
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
