/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[cfg(feature = "cuda")]
use std::os::raw::c_uint;

#[cfg(feature = "cuda")]
extern "C" {
    pub fn perform_hash_join_cuda(
        h_subjects: *const c_uint,
        h_predicates: *const c_uint,
        h_objects: *const c_uint,
        num_triples: c_uint,
        predicate_filter: c_uint,
        literal_filter: *const c_uint, // Pass null if no filter
        h_indices: *mut *mut c_uint,
        h_result_count: *mut c_uint,
    );
}

#[cfg(feature = "cuda")]
pub fn hash_join_cuda(
    subjects: &[u32],
    predicates: &[u32],
    objects: &[u32],
    predicate_filter: u32,
    literal_filter: Option<u32>,
) -> Vec<u32> {
    let num_triples = subjects.len() as c_uint;
    let literal_filter_ptr = literal_filter.as_ref().map_or(std::ptr::null(), |f| f as *const u32);

    let mut h_indices: *mut u32 = std::ptr::null_mut();
    let mut h_result_count: c_uint = 0;

    unsafe {
        perform_hash_join_cuda(
            subjects.as_ptr(),
            predicates.as_ptr(),
            objects.as_ptr(),
            num_triples,
            predicate_filter,
            literal_filter_ptr,
            &mut h_indices,
            &mut h_result_count,
        );

        // Convert the raw pointer into a Vec for Rust-managed memory
        let indices = Vec::from_raw_parts(h_indices, h_result_count as usize, h_result_count as usize);

        // No need to free explicitly; Vec takes ownership of the memory and will free it when dropped
        indices
    }
}
