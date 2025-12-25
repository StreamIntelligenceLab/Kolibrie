use shared::dictionary::Dictionary;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use std::collections::HashMap;

pub mod infer_generic;
pub mod semi_naive;
pub mod my_naive;
pub mod semi_naive_parallel;
pub mod semi_naive_with_repairs;

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
    }
}

/// Construct a new Triple from a conclusion pattern and bound variables
// TODO do not take dicitionary as mutability for clearer separation of concerns
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
                // TODO do not take dicitionary as mutability for clearer separation of concerns
                // If not bound, create a new placeholder in the dictionary
                dict.encode(&format!("ml_output_placeholder_{}", v))
            }
        }
        Term::Constant(c) => *c,
    };

    Triple {
        subject,
        predicate,
        object,
    }
}