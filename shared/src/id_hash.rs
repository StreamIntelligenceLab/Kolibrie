/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! A fast hasher for keys made of dictionary ids

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

/// Non-cryptographic hasher for dictionary ids (sequential, not user-controlled)
#[derive(Default, Clone, Copy, Debug)]
pub struct TermHasher(u64);

impl TermHasher {
    /// splitmix64 mixing step
    #[inline]
    fn mix(&mut self, value: u64) {
        let mut z = self
            .0
            .wrapping_add(value)
            .wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        self.0 = z ^ (z >> 31);
    }
}

impl Hasher for TermHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    /// Handles writes other than the integer fast paths, e.g. the `GraphId` discriminant
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buffer = [0u8; 8];
            buffer[..chunk.len()].copy_from_slice(chunk);
            self.mix(u64::from_le_bytes(buffer));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.mix(value as u64)
    }
    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.mix(value as u64)
    }
    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.mix(value)
    }
    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.mix(value as u64)
    }
    #[inline]
    fn write_isize(&mut self, value: isize) {
        self.mix(value as u64)
    }
}

/// Hasher builder for term-id keys, avoiding the default hasher's overhead on integers that are
/// already well distributed
pub type TermBuildHasher = BuildHasherDefault<TermHasher>;
/// A map keyed by dictionary id
pub type TermMap<V> = HashMap<u32, V, TermBuildHasher>;
/// A set of dictionary ids
pub type TermSet = HashSet<u32, TermBuildHasher>;
