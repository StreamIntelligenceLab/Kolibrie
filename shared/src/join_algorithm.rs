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
use crate::triple::Triple;
use crate::index_manager::UnifiedIndex;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use rayon::prelude::*;

pub fn perform_join_par_simd_with_strict_filter_4_redesigned_streaming(
    subject_var: String,
    predicate: String,
    object_var: String,
    index_manager: &UnifiedIndex,  // ← Pass index instead of database
    dictionary: &Dictionary,
    final_results: Vec<BTreeMap<String, String>>,
    literal_filter: Option<String>,
) -> Vec<BTreeMap<String, String>> {
    
    if final_results.is_empty() {
        return Vec::new();
    }

    const MAX_CHUNK_RESULTS: usize = 50_000;
    const MAX_BINDINGS_PER_GROUP: usize = 1000;
    
    // Encode predicate and literal filter to IDs for faster comparison
    let predicate_id = dictionary.string_to_id. get(&predicate).copied();
    let literal_filter_id = literal_filter.as_ref()
        .and_then(|s| dictionary.string_to_id.get(s).copied());

    // Sort final_results by join keys for merge join
    let sorted_bindings = sort_bindings_for_merge_join(
        &final_results,
        &subject_var,
        &object_var,
        MAX_BINDINGS_PER_GROUP,
        dictionary,
    );

    // FIX: Use PSO index instead of POS for better ordering
    let mut filtered_triples: Vec<Triple> = if let Some(pred_id) = predicate_id {
        // Use PSO index (Predicate -> Subject -> Object)
        // This gives results sorted by subject first!
        if let Some(subject_map) = index_manager. pso.get(&pred_id) {
            // Collect subjects in sorted order
            let mut subjects: Vec<_> = subject_map.iter().collect();
            subjects.sort_unstable_by_key(|(subj, _)| *subj);  // Sort by subject

            subjects
                .par_iter()
                .flat_map(|(&subject, objects)| {
                    // Objects are in HashSet, convert to sorted Vec
                    let mut sorted_objects: Vec<u32> = objects.iter().copied().collect();
                    sorted_objects.sort_unstable();  // Sort objects within each subject

                    // Build triples - naturally sorted by (subject, object)!
                    sorted_objects
                        .into_iter()
                        .filter_map(|object| {
                            // Apply literal filter if present
                            if let Some(filter_id) = literal_filter_id {
                                if object != filter_id {
                                    return None;
                                }
                            }

                            Some(Triple {
                                subject,
                                predicate: pred_id,
                                object,
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Sort triples by subject, then object for efficient merge
    filtered_triples.sort_by(|a, b| {
        a.subject.cmp(&b. subject). then_with(|| a.object.cmp(&b. object))
    });

    // Parallel merge join with sorted data
    let chunk_size = (filtered_triples.len() / (rayon::current_num_threads() * 2)). max(100);
    
    let results = filtered_triples
        .par_chunks(chunk_size)
        . fold(
            || Vec::with_capacity(1000),
            |mut local_results, triple_chunk| {
                process_triple_chunk_redesigned_streaming(
                    triple_chunk,
                    &subject_var,
                    &object_var,
                    &sorted_bindings,
                    &mut local_results,
                    dictionary,
                );
                
                if local_results.len() > MAX_CHUNK_RESULTS {
                    local_results.truncate(MAX_CHUNK_RESULTS);
                }
                
                local_results
            },
        )
        .reduce(
            || Vec::new(),
            |mut acc, chunk| {
                acc.extend(chunk);
                acc
            },
        );

    results
}

#[inline(always)]
fn process_triple_chunk_redesigned_streaming(
    triple_chunk: &[Triple],
    subject_var: &str,
    object_var: &str,
    sorted_bindings: &SortedBindings,
    local_results: &mut Vec<BTreeMap<String, String>>,
    dictionary: &Dictionary,
) {
    const MAX_LOCAL_RESULTS: usize = 10_000;
    
    for triple in triple_chunk {
        if local_results.len() >= MAX_LOCAL_RESULTS {
            break;
        }
        
        // Process each triple efficiently
        process_join_efficiently_redesigned_streaming(
            triple.subject,
            triple.object,
            subject_var,
            object_var,
            sorted_bindings,
            local_results,
            dictionary,
        );
    }
}

#[inline(always)]
fn process_join_efficiently_redesigned_streaming(
    subject_id: u32,
    object_id: u32,
    subject_var: &str,
    object_var: &str,
    sorted_bindings: &SortedBindings,
    local_results: &mut Vec<BTreeMap<String, String>>,
    dictionary: &Dictionary,
) {
    const MAX_MATCHES_PER_PATTERN: usize = 100;
    
    // Use sorted-merge join based on binding pattern, working with IDs
    match (&sorted_bindings.both_vars_bound, &sorted_bindings.subject_var_bound, &sorted_bindings.object_var_bound) {
        // Direct lookup in sorted structure using IDs
        (Some(both_bound), _, _) => {
            let key = (subject_id, object_id);
            if let Some(indices) = binary_search_both_bound(both_bound, &key) {
                for &idx in indices.iter().take(MAX_MATCHES_PER_PATTERN) {
                    local_results.push(sorted_bindings.final_results[idx].clone());
                }
            }
        }
        
        // Merge with sorted subject bindings using IDs
        (_, Some(subject_bound), _) => {
            if let Some(indices) = binary_search_subject_bound(subject_bound, subject_id) {
                merge_join_subject_bound(
                    indices,
                    object_id,
                    object_var,
                    &sorted_bindings.final_results,
                    local_results,
                    MAX_MATCHES_PER_PATTERN,
                    dictionary,
                );
            }
        }
        
        // Merge with sorted object bindings using IDs
        (_, _, Some(object_bound)) => {
            if let Some(indices) = binary_search_object_bound(object_bound, object_id) {
                merge_join_object_bound(
                    indices,
                    subject_id,
                    subject_var,
                    &sorted_bindings.final_results,
                    local_results,
                    MAX_MATCHES_PER_PATTERN,
                    dictionary,
                );
            }
        }
        
        // Cartesian product with sorted iteration using IDs
        _ => {
            merge_join_neither_bound(
                subject_id,
                object_id,
                subject_var,
                object_var,
                &sorted_bindings.neither_var_bound,
                &sorted_bindings.final_results,
                local_results,
                MAX_MATCHES_PER_PATTERN / 2,
                dictionary,
            );
        }
    }
}

// Supporting data structures modified to use IDs
#[derive(Debug)]
struct SortedBindings {
    both_vars_bound: Option<Vec<((u32, u32), Vec<usize>)>>,
    subject_var_bound: Option<Vec<(u32, Vec<usize>)>>,
    object_var_bound: Option<Vec<(u32, Vec<usize>)>>,
    neither_var_bound: Vec<usize>,
    final_results: Arc<Vec<BTreeMap<String, String>>>,
}

fn sort_bindings_for_merge_join(
    final_results: &[BTreeMap<String, String>],
    subject_var: &str,
    object_var: &str,
    max_bindings_per_group: usize,
    dictionary: &Dictionary,
) -> SortedBindings {
    let mut both_vars_bound: HashMap<(u32, u32), Vec<usize>> = HashMap::new();
    let mut subject_var_bound: HashMap<u32, Vec<usize>> = HashMap::new();
    let mut object_var_bound: HashMap<u32, Vec<usize>> = HashMap::new();
    let mut neither_var_bound: Vec<usize> = Vec::new();

    // Classify bindings using IDs for faster comparison
    for (idx, result) in final_results.iter().enumerate() {
        let subject_binding_id = result.get(subject_var)
            .and_then(|s| dictionary.string_to_id.get(s).copied());
        let object_binding_id = result.get(object_var)
            .and_then(|o| dictionary.string_to_id.get(o).copied());

        match (subject_binding_id, object_binding_id) {
            (Some(s_id), Some(o_id)) => {
                let key = (s_id, o_id);
                let entry = both_vars_bound.entry(key).or_insert_with(Vec::new);
                if entry.len() < max_bindings_per_group {
                    entry.push(idx);
                }
            }
            (Some(s_id), None) => {
                let entry = subject_var_bound.entry(s_id).or_insert_with(Vec::new);
                if entry.len() < max_bindings_per_group {
                    entry.push(idx);
                }
            }
            (None, Some(o_id)) => {
                let entry = object_var_bound.entry(o_id).or_insert_with(Vec::new);
                if entry.len() < max_bindings_per_group {
                    entry.push(idx);
                }
            }
            (None, None) => {
                if neither_var_bound.len() < max_bindings_per_group {
                    neither_var_bound.push(idx);
                }
            }
        }
    }

    // Convert to sorted vectors for efficient binary search and merge
    let sorted_both_vars = if !both_vars_bound.is_empty() {
        let mut sorted: Vec<_> = both_vars_bound.into_iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        Some(sorted)
    } else {
        None
    };

    let sorted_subject_var = if !subject_var_bound.is_empty() {
        let mut sorted: Vec<_> = subject_var_bound.into_iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        Some(sorted)
    } else {
        None
    };

    let sorted_object_var = if !object_var_bound.is_empty() {
        let mut sorted: Vec<_> = object_var_bound.into_iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        Some(sorted)
    } else {
        None
    };

    SortedBindings {
        both_vars_bound: sorted_both_vars,
        subject_var_bound: sorted_subject_var,
        object_var_bound: sorted_object_var,
        neither_var_bound,
        final_results: Arc::new(final_results.to_vec()),
    }
}

// Binary search functions modified to use IDs
fn binary_search_both_bound<'a>(
    sorted_bindings: &'a [((u32, u32), Vec<usize>)],
    key: &(u32, u32),
) -> Option<&'a Vec<usize>> {
    sorted_bindings
        .binary_search_by(|probe| probe.0.cmp(key))
        .ok()
        .map(|idx| &sorted_bindings[idx].1)
}

fn binary_search_subject_bound<'a>(
    sorted_bindings: &'a [(u32, Vec<usize>)],
    subject_id: u32,
) -> Option<&'a Vec<usize>> {
    sorted_bindings
        .binary_search_by(|probe| probe.0.cmp(&subject_id))
        .ok()
        .map(|idx| &sorted_bindings[idx].1)
}

fn binary_search_object_bound<'a>(
    sorted_bindings: &'a [(u32, Vec<usize>)],
    object_id: u32,
) -> Option<&'a Vec<usize>> {
    sorted_bindings
        .binary_search_by(|probe| probe.0.cmp(&object_id))
        .ok()
        .map(|idx| &sorted_bindings[idx].1)
}

fn merge_join_subject_bound(
    indices: &[usize],
    object_id: u32,
    object_var: &str,
    final_results: &[BTreeMap<String, String>],
    local_results: &mut Vec<BTreeMap<String, String>>,
    max_matches: usize,
    dictionary: &Dictionary,
) {
    for &idx in indices.iter().take(max_matches) {
        let base_result = &final_results[idx];
        if let Some(existing_object) = base_result.get(object_var) {
            // Compare using ID instead of string
            if let Some(existing_object_id) = dictionary.string_to_id.get(existing_object) {
                if *existing_object_id == object_id {
                    local_results.push(base_result.clone());
                }
            }
        } else {
            let mut extended_result = base_result.clone();
            // Only decode to string when needed for result
            if let Some(object_str) = dictionary.decode(object_id) {
                extended_result.insert(object_var.to_string(), object_str.to_string());
                local_results.push(extended_result);
            }
        }
    }
}

fn merge_join_object_bound(
    indices: &[usize],
    subject_id: u32,
    subject_var: &str,
    final_results: &[BTreeMap<String, String>],
    local_results: &mut Vec<BTreeMap<String, String>>,
    max_matches: usize,
    dictionary: &Dictionary,
) {
    for &idx in indices.iter().take(max_matches) {
        let base_result = &final_results[idx];
        if let Some(existing_subject) = base_result.get(subject_var) {
            // Compare using ID instead of string
            if let Some(existing_subject_id) = dictionary.string_to_id.get(existing_subject) {
                if *existing_subject_id == subject_id {
                    local_results.push(base_result.clone());
                }
            }
        } else {
            let mut extended_result = base_result.clone();
            // Only decode to string when needed for result
            if let Some(subject_str) = dictionary.decode(subject_id) {
                extended_result.insert(subject_var.to_string(), subject_str.to_string());
                local_results.push(extended_result);
            }
        }
    }
}

fn merge_join_neither_bound(
    subject_id: u32,
    object_id: u32,
    subject_var: &str,
    object_var: &str,
    neither_indices: &[usize],
    final_results: &[BTreeMap<String, String>],
    local_results: &mut Vec<BTreeMap<String, String>>,
    max_matches: usize,
    dictionary: &Dictionary,
) {
    for &idx in neither_indices.iter().take(max_matches) {
        let base_result = &final_results[idx];
        let mut extended_result = base_result.clone();
        
        if !base_result.contains_key(subject_var) {
            // Only decode to string when needed for result
            if let Some(subject_str) = dictionary.decode(subject_id) {
                extended_result.insert(subject_var.to_string(), subject_str.to_string());
            }
        }
        if !base_result.contains_key(object_var) {
            // Only decode to string when needed for result
            if let Some(object_str) = dictionary.decode(object_id) {
                extended_result.insert(object_var.to_string(), object_str.to_string());
            }
        }
        
        local_results.push(extended_result);
    }
}

// Result compaction function
pub fn compact_results(results: Vec<BTreeMap<String, String>>) -> Vec<BTreeMap<String, String>> {
    use std::collections::HashSet;
    
    let mut seen = HashSet::new();
    let mut compacted = Vec::new();
    
    for result in results {
        // Create a hash of the result for deduplication
        let mut sorted_items: Vec<_> = result.iter().collect();
        sorted_items.sort_by_key(|&(k, _)| k);
        let hash_key = format!("{:?}", sorted_items);
        
        if seen.insert(hash_key) {
            compacted.push(result);
        }
    }
    
    compacted
}

/// Ultra-fast hash join optimized for performance
pub fn perform_hash_join_for_rules(
    subject_var: String,
    predicate: String,
    object_var: String,
    triples: Vec<Triple>,
    dictionary: &Dictionary,
    final_results: Vec<BTreeMap<String, String>>,
    literal_filter: Option<String>,
) -> Vec<BTreeMap<String, String>> {
    
    if final_results.is_empty() {
        return Vec::new();
    }

    // Fast predicate filtering
    let predicate_id = match dictionary.string_to_id.get(&predicate) {
        Some(&id) => id,
        None => return Vec::new(),
    };

    let literal_filter_id = literal_filter.as_ref()
        .and_then(|s| dictionary.string_to_id.get(s).copied());

    // Pre-filter triples (this is very fast)
    let filtered_triples: Vec<Triple> = triples
        .into_iter()
        .filter(|triple| {
            triple.predicate == predicate_id && 
            literal_filter_id.map_or(true, |filter_id| triple.object == filter_id)
        })
        .collect();

    if filtered_triples.is_empty() {
        return Vec::new();
    }

    // Build simple hash table - this is the key optimization
    let hash_table = build_simple_hash_table(&final_results, &subject_var, &object_var, dictionary);

    // Parallel processing with minimal overhead
    let chunk_size = (filtered_triples.len() / rayon::current_num_threads().max(1)).max(1000);
    
    filtered_triples
        .par_chunks(chunk_size)
        .flat_map(|chunk| {
            let mut local_results = Vec::with_capacity(chunk.len());
            
            for triple in chunk {
                process_triple_fast(
                    triple,
                    &subject_var,
                    &object_var,
                    &hash_table,
                    dictionary,
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
    hash_table: &SimpleHashTable,
    dictionary: &Dictionary,
    local_results: &mut Vec<BTreeMap<String, String>>,
) {
    let subject_id = triple.subject;
    let object_id = triple.object;

    // Fast path: both variables bound
    if let Some(indices) = hash_table.both_bound.get(&(subject_id, object_id)) {
        for &idx in indices {
            local_results.push(hash_table.results[idx].clone());
        }
        return;
    }

    // Subject bound path
    if let Some(indices) = hash_table.subject_bound.get(&subject_id) {
        for &idx in indices {
            let mut result = hash_table.results[idx].clone();
            if let Some(object_str) = dictionary.decode(object_id) {
                result.insert(object_var.to_string(), object_str.to_string());
                local_results.push(result);
            }
        }
    }

    // Object bound path
    if let Some(indices) = hash_table.object_bound.get(&object_id) {
        for &idx in indices {
            let mut result = hash_table.results[idx].clone();
            if let Some(subject_str) = dictionary.decode(subject_id) {
                result.insert(subject_var.to_string(), subject_str.to_string());
                local_results.push(result);
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
            local_results.push(result);
        }
    }
}
