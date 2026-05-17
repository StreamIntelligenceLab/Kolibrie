use shared::dictionary::Dictionary;
use std::collections::HashMap;

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
