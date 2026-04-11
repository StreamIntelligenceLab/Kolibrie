use std::collections::{HashMap, HashSet};

use crate::index_manager::*;
use crate::query::PlannedAccessPattern;
use crate::terms::*;
use crate::triple::Triple;

#[derive(Debug, Clone)]
pub enum BucketStore {
    // 0 Dynamic Variables
    D0_F0(bool),
    D0_F1(HashSet<u32>),
    D0_F2(HashSet<[u32; 2]>),
    D0_F3(HashSet<[u32; 3]>),

    // >0 Dynamic Variables, 0 Free Variables (Existence checks)
    D1_F0(HashSet<u32>),
    D2_F0(HashSet<[u32; 2]>),
    D3_F0(HashSet<[u32; 3]>),

    // >0 Dynamic Variables, >0 Free Variables (Map lookups)
    D1_F1(HashMap<u32, HashSet<u32>>),
    D1_F2(HashMap<u32, HashSet<[u32; 2]>>),
    D2_F1(HashMap<[u32; 2], HashSet<u32>>),
}

#[derive(Debug, Clone)]
pub struct DirectedBucket {
    pub pattern: TriplePattern,
    pub c_positions: Vec<usize>, // Constants
    pub d_positions: Vec<usize>, // Dynamic (Pipeline-bound)
    pub f_positions: Vec<usize>, // Free (Unbound)
    pub c_values: Vec<u32>,      // The actual constant values
    pub data: BucketStore,
}

impl DirectedBucket {
    pub fn new(planned: PlannedAccessPattern) -> Self {
        let mut c_positions = Vec::new();
        let mut d_positions = Vec::new();
        let mut f_positions = Vec::new();
        let mut c_values = Vec::new();

        let mut check_pos = |term: &Term, is_bound: bool, pos: usize| match term {
            Term::Constant(c) => {
                c_positions.push(pos);
                c_values.push(*c);
            }
            Term::Variable(_) => {
                if is_bound {
                    d_positions.push(pos);
                } else {
                    f_positions.push(pos);
                }
            }
        };

        check_pos(&planned.pattern.0, planned.bound_subject, 0);
        check_pos(&planned.pattern.1, planned.bound_predicate, 1);
        check_pos(&planned.pattern.2, planned.bound_object, 2);

        let data = match (d_positions.len(), f_positions.len()) {
            (0, 0) => BucketStore::D0_F0(false),
            (0, 1) => BucketStore::D0_F1(HashSet::new()),
            (0, 2) => BucketStore::D0_F2(HashSet::new()),
            (0, 3) => BucketStore::D0_F3(HashSet::new()),
            (1, 0) => BucketStore::D1_F0(HashSet::new()),
            (2, 0) => BucketStore::D2_F0(HashSet::new()),
            (3, 0) => BucketStore::D3_F0(HashSet::new()),
            (1, 1) => BucketStore::D1_F1(HashMap::new()),
            (1, 2) => BucketStore::D1_F2(HashMap::new()),
            (2, 1) => BucketStore::D2_F1(HashMap::new()),
            _ => unreachable!("Invalid number of variables in triple"),
        };

        Self {
            pattern: planned.pattern,
            c_positions,
            d_positions,
            f_positions,
            c_values,
            data,
        }
    }

    #[inline(always)]
    fn get_triple_field(triple: &Triple, pos: usize) -> u32 {
        match pos {
            0 => triple.subject,
            1 => triple.predicate,
            2 => triple.object,
            _ => unreachable!(),
        }
    }

    pub fn matches(&self, triple: &Triple) -> bool {
        for (i, &pos) in self.c_positions.iter().enumerate() {
            if Self::get_triple_field(triple, pos) != self.c_values[i] {
                return false;
            }
        }
        true
    }

    pub fn insert(&mut self, triple: &Triple) -> bool {
        let get_val = |pos| Self::get_triple_field(triple, pos);

        match &mut self.data {
            BucketStore::D0_F0(b) => {
                let old = *b;
                *b = true;
                !old
            }
            BucketStore::D0_F1(s) => s.insert(get_val(self.f_positions[0])),
            BucketStore::D0_F2(s) => {
                s.insert([get_val(self.f_positions[0]), get_val(self.f_positions[1])])
            }
            BucketStore::D0_F3(s) => s.insert([
                get_val(self.f_positions[0]),
                get_val(self.f_positions[1]),
                get_val(self.f_positions[2]),
            ]),
            BucketStore::D1_F0(s) => s.insert(get_val(self.d_positions[0])),
            BucketStore::D2_F0(s) => {
                s.insert([get_val(self.d_positions[0]), get_val(self.d_positions[1])])
            }
            BucketStore::D3_F0(s) => s.insert([
                get_val(self.d_positions[0]),
                get_val(self.d_positions[1]),
                get_val(self.d_positions[2]),
            ]),
            BucketStore::D1_F1(m) => m
                .entry(get_val(self.d_positions[0]))
                .or_default()
                .insert(get_val(self.f_positions[0])),
            BucketStore::D1_F2(m) => m
                .entry(get_val(self.d_positions[0]))
                .or_default()
                .insert([get_val(self.f_positions[0]), get_val(self.f_positions[1])]),
            BucketStore::D2_F1(m) => m
                .entry([get_val(self.d_positions[0]), get_val(self.d_positions[1])])
                .or_default()
                .insert(get_val(self.f_positions[0])),
        }
    }

    pub fn remove(&mut self, triple: &Triple) -> bool {
        let get_val = |pos| Self::get_triple_field(triple, pos);

        match &mut self.data {
            BucketStore::D0_F0(b) => {
                let old = *b;
                *b = false;
                old
            }
            BucketStore::D0_F1(s) => s.remove(&get_val(self.f_positions[0])),
            BucketStore::D0_F2(s) => {
                s.remove(&[get_val(self.f_positions[0]), get_val(self.f_positions[1])])
            }
            BucketStore::D0_F3(s) => s.remove(&[
                get_val(self.f_positions[0]),
                get_val(self.f_positions[1]),
                get_val(self.f_positions[2]),
            ]),
            BucketStore::D1_F0(s) => s.remove(&get_val(self.d_positions[0])),
            BucketStore::D2_F0(s) => {
                s.remove(&[get_val(self.d_positions[0]), get_val(self.d_positions[1])])
            }
            BucketStore::D3_F0(s) => s.remove(&[
                get_val(self.d_positions[0]),
                get_val(self.d_positions[1]),
                get_val(self.d_positions[2]),
            ]),
            BucketStore::D1_F1(m) => {
                let k = get_val(self.d_positions[0]);
                if let Some(set) = m.get_mut(&k) {
                    let removed = set.remove(&get_val(self.f_positions[0]));
                    if set.is_empty() {
                        m.remove(&k);
                    }
                    removed
                } else {
                    false
                }
            }
            BucketStore::D1_F2(m) => {
                let k = get_val(self.d_positions[0]);
                if let Some(set) = m.get_mut(&k) {
                    let removed =
                        set.remove(&[get_val(self.f_positions[0]), get_val(self.f_positions[1])]);
                    if set.is_empty() {
                        m.remove(&k);
                    }
                    removed
                } else {
                    false
                }
            }
            BucketStore::D2_F1(m) => {
                let k = [get_val(self.d_positions[0]), get_val(self.d_positions[1])];
                if let Some(set) = m.get_mut(&k) {
                    let removed = set.remove(&get_val(self.f_positions[0]));
                    if set.is_empty() {
                        m.remove(&k);
                    }
                    removed
                } else {
                    false
                }
            }
        }
    }

    pub fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let mut results = Vec::new();

        // Extract constants and queried dynamics once per query call
        let mut t_base = [0u32; 3];
        for (i, &pos) in self.c_positions.iter().enumerate() {
            t_base[pos] = self.c_values[i];
        }
        for &pos in &self.d_positions {
            t_base[pos] = match pos {
                0 => s.unwrap(),
                1 => p.unwrap(),
                2 => o.unwrap(),
                _ => unreachable!(),
            };
        }

        // Inline macro/closure to quickly instantiate the triple without inner loops
        let mut push_res = |f_vals: &[u32]| {
            let mut t = t_base;
            for (i, &pos) in self.f_positions.iter().enumerate() {
                t[pos] = f_vals[i];
            }
            results.push(Triple {
                subject: t[0],
                predicate: t[1],
                object: t[2],
            });
        };

        match &self.data {
            BucketStore::D0_F0(b) => {
                if *b {
                    push_res(&[]);
                }
            }
            BucketStore::D0_F1(set) => {
                for &f in set {
                    push_res(&[f]);
                }
            }
            BucketStore::D0_F2(set) => {
                for &f in set {
                    push_res(&f);
                }
            }
            BucketStore::D0_F3(set) => {
                for &f in set {
                    push_res(&f);
                }
            }
            BucketStore::D1_F0(set) => {
                if set.contains(&t_base[self.d_positions[0]]) {
                    push_res(&[]);
                }
            }
            BucketStore::D2_F0(set) => {
                if set.contains(&[t_base[self.d_positions[0]], t_base[self.d_positions[1]]]) {
                    push_res(&[]);
                }
            }
            BucketStore::D3_F0(set) => {
                if set.contains(&[
                    t_base[self.d_positions[0]],
                    t_base[self.d_positions[1]],
                    t_base[self.d_positions[2]],
                ]) {
                    push_res(&[]);
                }
            }
            BucketStore::D1_F1(map) => {
                if let Some(set) = map.get(&t_base[self.d_positions[0]]) {
                    for &f in set {
                        push_res(&[f]);
                    }
                }
            }
            BucketStore::D1_F2(map) => {
                if let Some(set) = map.get(&t_base[self.d_positions[0]]) {
                    for &f in set {
                        push_res(&f);
                    }
                }
            }
            BucketStore::D2_F1(map) => {
                if let Some(set) =
                    map.get(&[t_base[self.d_positions[0]], t_base[self.d_positions[1]]])
                {
                    for &f in set {
                        push_res(&[f]);
                    }
                }
            }
        }

        // Return immediately without the slow `.into_iter().filter().collect()`
        results
    }

    pub fn get_all_triples(&self) -> Vec<Triple> {
        let mut results = Vec::new();

        let reconstruct = |d_vals: &[u32], f_vals: &[u32]| {
            let mut t = [0; 3];
            for (i, &pos) in self.c_positions.iter().enumerate() {
                t[pos] = self.c_values[i];
            }
            for (i, &pos) in self.d_positions.iter().enumerate() {
                t[pos] = d_vals[i];
            }
            for (i, &pos) in self.f_positions.iter().enumerate() {
                t[pos] = f_vals[i];
            }
            Triple {
                subject: t[0],
                predicate: t[1],
                object: t[2],
            }
        };

        match &self.data {
            BucketStore::D0_F0(b) => {
                if *b {
                    results.push(reconstruct(&[], &[]));
                }
            }
            BucketStore::D0_F1(set) => {
                for &f in set {
                    results.push(reconstruct(&[], &[f]));
                }
            }
            BucketStore::D0_F2(set) => {
                for &f in set {
                    results.push(reconstruct(&[], &f));
                }
            }
            BucketStore::D0_F3(set) => {
                for &f in set {
                    results.push(reconstruct(&[], &f));
                }
            }
            BucketStore::D1_F0(set) => {
                for &d in set {
                    results.push(reconstruct(&[d], &[]));
                }
            }
            BucketStore::D2_F0(set) => {
                for &d in set {
                    results.push(reconstruct(&d, &[]));
                }
            }
            BucketStore::D3_F0(set) => {
                for &d in set {
                    results.push(reconstruct(&d, &[]));
                }
            }
            BucketStore::D1_F1(map) => {
                for (&d, set) in map {
                    for &f in set {
                        results.push(reconstruct(&[d], &[f]));
                    }
                }
            }
            BucketStore::D1_F2(map) => {
                for (&d, set) in map {
                    for &f in set {
                        results.push(reconstruct(&[d], &f));
                    }
                }
            }
            BucketStore::D2_F1(map) => {
                for (&d, set) in map {
                    for &f in set {
                        results.push(reconstruct(&d, &[f]));
                    }
                }
            }
        }
        results
    }

    pub fn clear(&mut self) {
        match &mut self.data {
            BucketStore::D0_F0(b) => *b = false,
            BucketStore::D0_F1(s) => s.clear(),
            BucketStore::D0_F2(s) => s.clear(),
            BucketStore::D0_F3(s) => s.clear(),
            BucketStore::D1_F0(s) => s.clear(),
            BucketStore::D2_F0(s) => s.clear(),
            BucketStore::D3_F0(s) => s.clear(),
            BucketStore::D1_F1(m) => m.clear(),
            BucketStore::D1_F2(m) => m.clear(),
            BucketStore::D2_F1(m) => m.clear(),
        }
    }

    pub fn shrink_to_fit(&mut self) {
        match &mut self.data {
            BucketStore::D0_F0(_) => {}
            BucketStore::D0_F1(s) => s.shrink_to_fit(),
            BucketStore::D0_F2(s) => s.shrink_to_fit(),
            BucketStore::D0_F3(s) => s.shrink_to_fit(),
            BucketStore::D1_F0(s) => s.shrink_to_fit(),
            BucketStore::D2_F0(s) => s.shrink_to_fit(),
            BucketStore::D3_F0(s) => s.shrink_to_fit(),
            BucketStore::D1_F1(m) => {
                for v in m.values_mut() {
                    v.shrink_to_fit();
                }
                m.shrink_to_fit();
            }
            BucketStore::D1_F2(m) => {
                for v in m.values_mut() {
                    v.shrink_to_fit();
                }
                m.shrink_to_fit();
            }
            BucketStore::D2_F1(m) => {
                for v in m.values_mut() {
                    v.shrink_to_fit();
                }
                m.shrink_to_fit();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BucketIndex {
    pub buckets: Vec<DirectedBucket>,
}

impl BucketIndex {
    pub fn new(patterns: Vec<PlannedAccessPattern>) -> Self {
        println!("[Bucket Debug] --- BucketIndex Initialization ---");
        println!(
            "[Bucket Debug] Requested planned patterns: {}",
            patterns.len()
        );
        let buckets: Vec<DirectedBucket> = patterns
            .into_iter()
            .enumerate()
            .map(|(i, pat)| {
                let b = DirectedBucket::new(pat);
                println!(
                    "[Bucket Debug]   Bucket [{}]: Pattern: {:?}, C={:?}, D={:?}, F={:?}",
                    i, b.pattern, b.c_positions, b.d_positions, b.f_positions
                );
                b
            })
            .collect();
        Self { buckets }
    }

    fn bucket_covers_query(
        bucket_pat: &TriplePattern,
        q_s: Option<u32>,
        q_p: Option<u32>,
        q_o: Option<u32>,
    ) -> bool {
        let (b_s, b_p, b_o) = bucket_pat;
        let s_safe = match b_s {
            Variable(_) => true,
            Constant(c) => q_s == Some(*c),
        };
        let p_safe = match b_p {
            Variable(_) => true,
            Constant(c) => q_p == Some(*c),
        };
        let o_safe = match b_o {
            Variable(_) => true,
            Constant(c) => q_o == Some(*c),
        };
        s_safe && p_safe && o_safe
    }
}

impl TripleIndex for BucketIndex {
    fn clone_empty(&self) -> Box<dyn TripleIndex> {
        let mut patterns = Vec::new();
        for b in &self.buckets {
            let mut bound_subject = false;
            let mut bound_predicate = false;
            let mut bound_object = false;

            for &pos in &b.d_positions {
                match pos {
                    0 => bound_subject = true,
                    1 => bound_predicate = true,
                    2 => bound_object = true,
                    _ => {}
                }
            }

            patterns.push(PlannedAccessPattern {
                pattern: b.pattern.clone(),
                bound_subject,
                bound_predicate,
                bound_object,
            });
        }
        Box::new(BucketIndex::new(patterns))
    }

    fn clone_box(&self) -> Box<dyn TripleIndex> {
        Box::new(self.clone())
    }

    fn triple_count(&self) -> usize {
        let mut unique: HashSet<Triple> = HashSet::new();
        for bucket in &self.buckets {
            unique.extend(bucket.get_all_triples());
        }
        unique.len()
    }

    fn supported_access_patterns(&self) -> AccessPatternSupport {
        AccessPatternSupport {
            sp: false,
            so: false,
            po: false,
            ps: false,
            os: false,
            op: false,
        }
    }

    fn insert(&mut self, triple: &Triple) -> bool {
        let mut inserted_anywhere = false;
        for bucket in &mut self.buckets {
            if bucket.matches(triple) && bucket.insert(triple) {
                inserted_anywhere = true;
            }
        }
        inserted_anywhere
    }

    fn delete(&mut self, triple: &Triple) -> bool {
        let mut deleted_anywhere = false;
        for bucket in &mut self.buckets {
            if bucket.matches(triple) && bucket.remove(triple) {
                deleted_anywhere = true;
            }
        }
        deleted_anywhere
    }

    fn build_from_triples(&mut self, triples: &[Triple]) {
        self.clear();
        for triple in triples {
            self.insert(triple);
        }
        self.optimize();
    }

    fn query(&self, s: Option<u32>, p: Option<u32>, o: Option<u32>) -> Vec<Triple> {
        let provided_positions = {
            let mut pos = Vec::new();
            if s.is_some() {
                pos.push(0);
            }
            if p.is_some() {
                pos.push(1);
            }
            if o.is_some() {
                pos.push(2);
            }
            pos
        };

        for b in self.buckets.iter() {
            if Self::bucket_covers_query(&b.pattern, s, p, o) {
                // Check if lengths match first to avoid allocating and sorting unless necessary
                if b.c_positions.len() + b.d_positions.len() == provided_positions.len() {
                    let mut expected_provided = b.c_positions.clone();
                    expected_provided.extend(&b.d_positions);
                    expected_provided.sort_unstable();

                    if expected_provided == provided_positions {
                        return b.query(s, p, o);
                    }
                }
            }
        }

        panic!(
            "[FATAL] NO EXACT MATCH FOUND! Query cannot be satisfied optimally by any bucket.\n\
            Query required: s={:?}, p={:?}, o={:?}\n\
            Provided Positions: {:?}",
            s, p, o, provided_positions
        );
    }

    fn get_matching_triples(&self, pattern: &TriplePattern) -> Vec<Triple> {
        let (s, p, o) = pattern;
        let sub = match s {
            Constant(x) => Some(*x),
            Variable(_) => None,
        };
        let pre = match p {
            Constant(x) => Some(*x),
            Variable(_) => None,
        };
        let obj = match o {
            Constant(x) => Some(*x),
            Variable(_) => None,
        };
        self.query(sub, pre, obj)
    }

    fn clear(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
    }

    fn scan_sp(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        None
    }
    fn scan_so(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        None
    }
    fn scan_po(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        None
    }
    fn scan_ps(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        None
    }
    fn scan_os(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        None
    }
    fn scan_op(&self, _: u32, _: u32) -> Option<&HashSet<u32>> {
        None
    }

    fn optimize(&mut self) {
        for bucket in &mut self.buckets {
            bucket.shrink_to_fit();
        }
    }
}
