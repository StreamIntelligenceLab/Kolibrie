//! Decoding bindings back to strings, for debugging and error messages.
//!
//! Nothing in evaluation needs this: rules are evaluated over `u32` ids throughout.
//! It exists so a binding can be printed.

use shared::dictionary::Dictionary;
use std::collections::HashMap;

/// Decodes a binding's ids back to their strings.
///
/// # Panics
///
/// If any id is absent from the dictionary. Only ids that came out of this
/// dictionary should be passed.
pub fn get_readable_binding(
    bindings: &HashMap<String, u32>,
    dictionary: &Dictionary,
) -> HashMap<String, String> {
    bindings
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                dictionary.id_to_string.get(value).unwrap().clone(),
            )
        })
        .collect::<HashMap<_, _>>()
}
