/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::dictionary::Dictionary;
use crate::terms::{Term, TriplePattern};
use crate::triple::Triple;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};

fn extract_join_parameters(premise: &TriplePattern) -> (String, String) {
    let (subject_term, _, object_term) = premise;

    let subject_var = match subject_term {
        Term::Variable(v) => v.clone(),
        Term::Constant(c) => {
            // For constants, create a synthetic variable name
            format!("__const_subj_{}", c)
        }
        Term::QuotedTriple(_) => "__quoted_subj".to_string(),
    };

    let object_var = match object_term {
        Term::Variable(v) => v.clone(),
        Term::Constant(c) => {
            // For constants, create a synthetic variable name
            format!("__const_obj_{}", c)
        }
        Term::QuotedTriple(_) => "__quoted_obj".to_string(),
    };

    (subject_var, object_var)
}

/// Check a candidate result row against the triple's predicate: a bound
/// predicate variable must match it, an unbound one gets bound to it.
/// Returns false if the row conflicts with this triple.
fn bind_predicate(
    result: &mut BTreeMap<String, String>,
    pred_var: Option<&str>,
    pred_id: u32,
    dict: &Dictionary,
) -> bool {
    match pred_var {
        None => true,
        Some(v) => match result.get(v) {
            Some(existing) => dict.string_to_id.get(existing) == Some(&pred_id),
            None => match dict.decode(pred_id) {
                Some(s) => {
                    result.insert(v.to_string(), s.to_string());
                    true
                }
                None => false,
            },
        },
    }
}

/// Ultra-fast hash join optimized for performance
pub fn perform_hash_join_for_rules(
    premise: &TriplePattern,
    triples: &[Triple],
    dict: &Dictionary,
    final_results: Vec<BTreeMap<String, String>>,
) -> Vec<BTreeMap<String, String>> {

    // Extract variable names from the premise
    let (subject, object) = extract_join_parameters(premise);

    if final_results.is_empty() {
        return Vec::new();
    }

    // A constant predicate is a pre-filter; a variable predicate is checked
    // and bound per result row in process_triple_fast.
    let (predicate_filter, predicate_var) = match &premise.1 {
        Term::Constant(c) => (Some(*c), None),
        Term::Variable(v) => (None, Some(v.as_str())),
        Term::QuotedTriple(_) => return Vec::new(),
    };

    // Constant subject/object terms in the premise must match the triple
    // exactly; term IDs are already dictionary-encoded, so compare directly.
    let subject_filter = match &premise.0 {
        Term::Constant(c) => Some(*c),
        _ => None,
    };
    let object_filter = match &premise.2 {
        Term::Constant(c) => Some(*c),
        _ => None,
    };

    // A premise like (?v, p, ?v) only matches triples whose subject equals
    // their object; the hash table below keys subject and object separately,
    // so the equality must be enforced here.
    let same_subject_object_var = matches!(
        (&premise.0, &premise.2),
        (Term::Variable(a), Term::Variable(b)) if a == b
    );

    // Pre-filter triples (this is very fast)
    let filtered_triples: Vec<&Triple> = triples
        .iter()
        .filter(|triple| {
            predicate_filter.map_or(true, |id| triple.predicate == id)
                && subject_filter.map_or(true, |id| triple.subject == id)
                && object_filter.map_or(true, |id| triple.object == id)
                && (!same_subject_object_var || triple.subject == triple.object)
        })
        .collect();

    if filtered_triples.is_empty() {
        return Vec::new();
    }

    // Build simple hash table - this is the key optimization
    let hash_table = build_simple_hash_table(
        &final_results,
        &subject,
        &object,
        dict,
    );

    // Parallel processing with minimal overhead
    let chunk_size = (filtered_triples.len() / rayon::current_num_threads().max(1)).max(1000);

    filtered_triples
        .par_chunks(chunk_size)
        .flat_map(|chunk| {
            let mut local_results = Vec::with_capacity(chunk.len());

            for triple in chunk {
                process_triple_fast(
                    triple,
                    &subject,
                    &object,
                    predicate_var,
                    &hash_table,
                    dict,
                    &mut local_results,
                );
            }

            local_results
        })
        .collect()
}

/// Simple, fast hash table structure
struct SimpleHashTable {
    both_bound: HashMap<(u32, u32), Vec<usize>>,
    subject_bound: HashMap<u32, Vec<usize>>,
    object_bound: HashMap<u32, Vec<usize>>,
    neither_bound: Vec<usize>,
    results: Vec<BTreeMap<String, String>>,
}

#[inline]
fn build_simple_hash_table(
    final_results: &[BTreeMap<String, String>],
    subject_var: &str,
    object_var: &str,
    dictionary: &Dictionary,
) -> SimpleHashTable {
    let mut both_bound = HashMap::new();
    let mut subject_bound = HashMap::new();
    let mut object_bound = HashMap::new();
    let mut neither_bound = Vec::new();

    for (idx, result) in final_results.iter().enumerate() {
        let subject_id = result.get(subject_var)
            .and_then(|s| dictionary.string_to_id.get(s).copied());
        let object_id = result.get(object_var)
            .and_then(|o| dictionary.string_to_id.get(o).copied());

        match (subject_id, object_id) {
            (Some(s_id), Some(o_id)) => {
                both_bound.entry((s_id, o_id)).or_insert_with(Vec::new).push(idx);
            }
            (Some(s_id), None) => {
                subject_bound.entry(s_id).or_insert_with(Vec::new).push(idx);
            }
            (None, Some(o_id)) => {
                object_bound.entry(o_id).or_insert_with(Vec::new).push(idx);
            }
            (None, None) => {
                neither_bound.push(idx);
            }
        }
    }

    SimpleHashTable {
        both_bound,
        subject_bound,
        object_bound,
        neither_bound,
        results: final_results.to_vec(),
    }
}

#[inline]
fn process_triple_fast(
    triple: &Triple,
    subject_var: &str,
    object_var: &str,
    predicate_var: Option<&str>,
    hash_table: &SimpleHashTable,
    dictionary: &Dictionary,
    local_results: &mut Vec<BTreeMap<String, String>>,
) {
    let subject_id = triple.subject;
    let object_id = triple.object;
    let predicate_id = triple.predicate;

    // Fast path: both variables bound
    if let Some(indices) = hash_table.both_bound.get(&(subject_id, object_id)) {
        for &idx in indices {
            let mut result = hash_table.results[idx].clone();
            if bind_predicate(&mut result, predicate_var, predicate_id, dictionary) {
                local_results.push(result);
            }
        }
        return;
    }

    // Subject bound path
    if let Some(indices) = hash_table.subject_bound.get(&subject_id) {
        for &idx in indices {
            let mut result = hash_table.results[idx].clone();
            if let Some(object_str) = dictionary.decode(object_id) {
                result.insert(object_var.to_string(), object_str.to_string());
                if bind_predicate(&mut result, predicate_var, predicate_id, dictionary) {
                    local_results.push(result);
                }
            }
        }
    }

    // Object bound path
    if let Some(indices) = hash_table.object_bound.get(&object_id) {
        for &idx in indices {
            let mut result = hash_table.results[idx].clone();
            if let Some(subject_str) = dictionary.decode(subject_id) {
                result.insert(subject_var.to_string(), subject_str.to_string());
                if bind_predicate(&mut result, predicate_var, predicate_id, dictionary) {
                    local_results.push(result);
                }
            }
        }
    }

    // Neither bound path
    for &idx in &hash_table.neither_bound {
        let mut result = hash_table.results[idx].clone();

        if let (Some(subject_str), Some(object_str)) =
            (dictionary.decode(subject_id), dictionary.decode(object_id)) {
            result.insert(subject_var.to_string(), subject_str.to_string());
            result.insert(object_var.to_string(), object_str.to_string());
            if bind_predicate(&mut result, predicate_var, predicate_id, dictionary) {
                local_results.push(result);
            }
        }
    }
}

