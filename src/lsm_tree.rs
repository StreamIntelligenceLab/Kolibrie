use crate::triple::Triple;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct SSTable {
    pub triples: BTreeSet<Triple>,
}

impl SSTable {
    pub fn new() -> Self {
        SSTable {
            triples: BTreeSet::new(),
        }
    }

    pub fn add_triple(&mut self, triple: Triple) {
        self.triples.insert(triple);
    }
}

#[derive(Debug, Clone)]
pub struct LSMTree {
    memtable: BTreeMap<Triple, ()>,
    sstables: Vec<SSTable>,
    triples: BTreeSet<Triple>,
}

impl LSMTree {
    pub fn new() -> Self {
        LSMTree {
            memtable: BTreeMap::new(),
            sstables: Vec::new(),
            triples: BTreeSet::new(),
        }
    }

    pub fn insert(&mut self, triple: Triple) -> Result<(), String> {
        if self.memtable.contains_key(&triple) {
            return Err("Duplicate triple".to_string());
        }

        self.memtable.insert(triple, ());

        if self.memtable.len() > 1000 {
            self.flush_and_merge()?;
        }

        Ok(())
    }

    pub fn flush_and_merge(&mut self) -> Result<(), String> {
        let mut sstable = SSTable::new();
        let old_memtable = std::mem::take(&mut self.memtable);
        for (triple, _) in old_memtable {
            sstable.add_triple(triple);
        }
        self.sstables.push(sstable);

        if self.sstables.len() > 5 {
            self.merge_sstables()?;
        }

        Ok(())
    }

    fn merge_sstables(&mut self) -> Result<(), String> {
        let mut merged_sstable = SSTable::new();

        for sstable in self.sstables.drain(..) {
            for triple in sstable.triples {
                merged_sstable.add_triple(triple);
            }
        }

        // Append the merged SSTable to the main triples store
        for triple in merged_sstable.triples {
            self.triples.insert(triple);
        }

        self.sstables.clear();
        Ok(())
    }

    pub fn get_all_triples(&self) -> Vec<Triple> {
        let mut all_triples = Vec::new();
        // Get triples from memtable
        for (triple, _) in &self.memtable {
            all_triples.push(triple.clone());
        }
        // Get triples from sstables
        for sstable in &self.sstables {
            all_triples.extend(sstable.triples.iter().cloned());
        }
        // Get triples from merged triples
        all_triples.extend(self.triples.iter().cloned());
        all_triples
    }
}
