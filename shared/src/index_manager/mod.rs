/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{HashMap, HashSet};
use crate::terms::*;
use crate::terms::Term::*;
use crate::triple::Triple;

pub use hexastore::HexastoreIndex;
pub use ops_single::OPSSingleIndex;
pub use osp_single::OSPSingleIndex;
pub use pos_single::POSSingleIndex;
pub use pso_single::PSOSingleIndex;
pub use sop_single::SOPSingleIndex;
pub use spo_single::SPOSingleIndex;
pub use single_table::SingleTableIndex;
pub mod hexastore;
pub mod ops_single;
pub mod osp_single;
pub mod pos_single;
pub mod pso_single;
pub mod sop_single;
pub mod spo_single;
pub mod single_table;


/// Describes which access patterns an index can serve efficiently.
#[derive(Debug, Clone)]
pub struct AccessPatternSupport {
    pub sp: bool,   // subject+predicate -> objects
    pub so: bool,   // subject+object -> predicates
    pub po: bool,   // predicate+object -> subjects
    pub ps: bool,   // predicate+subject -> objects
    pub os: bool,   // object+subject -> predicates
    pub op: bool,   // object+predicate -> subjects
}

pub trait TripleIndex: Send + Sync + std::fmt::Debug {
    // ── Mutation ──
    fn insert(&mut self, triple: &Triple) -> bool;
    fn delete(&mut self, triple: &Triple) -> bool;
    fn clear(&mut self);
    fn clone_empty(&self) -> Box<dyn TripleIndex>;

    // ── Pattern query ──
    /// Returns all triples matching the (s?, p?, o?) pattern.
    /// Always works regardless of existing indexes.
    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple>;

    /// Same as query but works with TriplePattern (for convenience).
    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple>;

    // ── Two-key scans ──
    // These return None if the index doesn't support this access path
    // efficiently — the engine will then fall back to query() + filter.
    fn scan_sp(&self, s: u32, p: u32) -> Option<&HashSet<u32>>;
    fn scan_so(&self, s: u32, o: u32) -> Option<&HashSet<u32>>;
    fn scan_po(&self, p: u32, o: u32) -> Option<&HashSet<u32>>;
    fn scan_ps(&self, p: u32, s: u32) -> Option<&HashSet<u32>>;
    fn scan_os(&self, o: u32, s: u32) -> Option<&HashSet<u32>>;
    fn scan_op(&self, o: u32, p: u32) -> Option<&HashSet<u32>>;

    // ── Bulk operations ──
    
    /// Absorb all triples from a slice. The default implementation
    /// calls insert() in a loop, concrete types can override with
    /// a faster path.
    fn build_from_triples(&mut self, triples: &[Triple]) {
        for triple in triples {
            self.insert(triple);
        }
    }

    /// Reclaim wasted memory / compact internal data structures.
    /// The default is to do nothing, concrete types override if they
    /// have internal structures that benefit from compaction.
    fn optimize(&mut self) {}

    // ── Metadata ──
    /// Reports which access patterns this index supports efficiently.
    fn supported_access_patterns(&self) -> AccessPatternSupport;
    fn triple_count(&self) -> usize {
        self.query(None, None, None).len()  // default: expensive but correct
    }

    // ── Cloning support for Box<dyn TripleIndex> ──
    fn clone_box(&self) -> Box<dyn TripleIndex>;
}

/// Allow `Clone` on `Box<dyn TripleIndex>`.
impl Clone for Box<dyn TripleIndex> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Helper function to remove a triple from a nested index structure and clean up empty collections
#[inline]
fn remove_from_index(
    index: &mut HashMap<u32, HashMap<u32, HashSet<u32>>>,
    key1: u32,
    key2: u32,
    value: u32,
) {
    if let Some(inner_map) = index.get_mut(&key1) {
        if let Some(set) = inner_map.get_mut(&key2) {
            set.remove(&value);
            // Clean up empty inner set
            if set.is_empty() {
                inner_map.remove(&key2);
            }
        }
        // Clean up empty inner map
        if inner_map.is_empty() {
            index.remove(&key1);
        }
    }
}
