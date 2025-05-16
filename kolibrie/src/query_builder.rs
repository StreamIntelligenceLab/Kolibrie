/*
 * Copyright © 2024 ladroid
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::sparql_database::SparqlDatabase;
use shared::triple::Triple;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub struct QueryBuilder<'a> {
    db: &'a SparqlDatabase,
    subject_filter: Option<TripleFilter>,
    predicate_filter: Option<TripleFilter>,
    object_filter: Option<TripleFilter>,
    custom_filter: Option<Box<dyn Fn(&Triple) -> bool + 'a>>,
    join_conditions: Vec<JoinCondition>,
    join_db: Option<&'a SparqlDatabase>,
    distinct_results: bool,
    sort_key: Option<Box<dyn Fn(&Triple) -> String + 'a>>,
    sort_direction: SortDirection,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'a> Clone for QueryBuilder<'a> {
    fn clone(&self) -> Self {
        QueryBuilder {
            db:             self.db,
            subject_filter: self.subject_filter.clone(),
            predicate_filter: self.predicate_filter.clone(),
            object_filter:  self.object_filter.clone(),
            // we cannot clone custom closures, so we just drop them:
            custom_filter:  None,
            join_conditions: self.join_conditions.clone(),
            join_db:        self.join_db,
            distinct_results: self.distinct_results,
            // likewise drop any sort_key:
            sort_key:       None,
            sort_direction: self.sort_direction,
            limit:          self.limit,
            offset:         self.offset,
        }
    }
}

/// Defines different ways to filter triple components
pub enum TripleFilter {
    Exact(String),
    Contains(String),
    StartsWith(String),
    EndsWith(String),
    Custom(Box<dyn Fn(&str) -> bool>),
}

// Manual implementation of Debug for TripleFilter
impl fmt::Debug for TripleFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(s) => write!(f, "TripleFilter::Exact({})", s),
            Self::Contains(s) => write!(f, "TripleFilter::Contains({})", s),
            Self::StartsWith(s) => write!(f, "TripleFilter::StartsWith({})", s),
            Self::EndsWith(s) => write!(f, "TripleFilter::EndsWith({})", s),
            Self::Custom(_) => write!(f, "TripleFilter::Custom(<function>)"),
        }
    }
}

// Manual implementation of Clone for TripleFilter
impl Clone for TripleFilter {
    fn clone(&self) -> Self {
        match self {
            Self::Exact(s) => Self::Exact(s.clone()),
            Self::Contains(s) => Self::Contains(s.clone()),
            Self::StartsWith(s) => Self::StartsWith(s.clone()),
            Self::EndsWith(s) => Self::EndsWith(s.clone()),
            // We can't clone function pointers, so this is a limitation
            // In practice, you'd rarely need to clone a filter with a custom function
            Self::Custom(_) => panic!("Cannot clone TripleFilter::Custom"),
        }
    }
}

/// Defines conditions for joining two SPARQL databases
pub enum JoinCondition {
    OnSubject,
    OnPredicate,
    OnObject,
    Custom(Box<dyn Fn(&Triple, &Triple) -> bool>),
}

// Manual implementation of Debug for JoinCondition
impl fmt::Debug for JoinCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OnSubject => write!(f, "JoinCondition::OnSubject"),
            Self::OnPredicate => write!(f, "JoinCondition::OnPredicate"),
            Self::OnObject => write!(f, "JoinCondition::OnObject"),
            Self::Custom(_) => write!(f, "JoinCondition::Custom(<function>)"),
        }
    }
}

impl Clone for JoinCondition {
    fn clone(&self) -> Self {
        match self {
            JoinCondition::OnSubject    => JoinCondition::OnSubject,
            JoinCondition::OnPredicate  => JoinCondition::OnPredicate,
            JoinCondition::OnObject     => JoinCondition::OnObject,
            JoinCondition::Custom(_)    => {
                panic!("Cannot clone JoinCondition::Custom")
            }
        }
    }
}

/// Defines sort direction for query results
#[derive(Debug, Clone, Copy)]
pub enum SortDirection {
    Ascending,
    Descending,
}

impl<'a> QueryBuilder<'a> {
    // Rest of the implementation remains the same...
    
    /// Creates a new QueryBuilder for the given SparqlDatabase
    pub fn new(db: &'a SparqlDatabase) -> Self {
        Self {
            db,
            subject_filter: None,
            predicate_filter: None,
            object_filter: None,
            custom_filter: None,
            join_conditions: Vec::new(),
            join_db: None,
            distinct_results: false,
            sort_key: None,
            sort_direction: SortDirection::Ascending,
            limit: None,
            offset: None,
        }
    }
    
    /// Filter triples by exact subject value
    pub fn with_subject(mut self, subject: &str) -> Self {
        self.subject_filter = Some(TripleFilter::Exact(subject.to_string()));
        self
    }
    
    /// Filter triples by subject containing a substring
    pub fn with_subject_like(mut self, pattern: &str) -> Self {
        self.subject_filter = Some(TripleFilter::Contains(pattern.to_string()));
        self
    }
    
    /// Filter triples by subject starting with a substring
    pub fn with_subject_starting(mut self, prefix: &str) -> Self {
        self.subject_filter = Some(TripleFilter::StartsWith(prefix.to_string()));
        self
    }
    
    /// Filter triples by subject ending with a substring
    pub fn with_subject_ending(mut self, suffix: &str) -> Self {
        self.subject_filter = Some(TripleFilter::EndsWith(suffix.to_string()));
        self
    }
    
    /// Filter triples by exact predicate value
    pub fn with_predicate(mut self, predicate: &str) -> Self {
        self.predicate_filter = Some(TripleFilter::Exact(predicate.to_string()));
        self
    }
    
    /// Filter triples by predicate containing a substring
    pub fn with_predicate_like(mut self, pattern: &str) -> Self {
        self.predicate_filter = Some(TripleFilter::Contains(pattern.to_string()));
        self
    }
    
    /// Filter triples by predicate starting with a substring
    pub fn with_predicate_starting(mut self, prefix: &str) -> Self {
        self.predicate_filter = Some(TripleFilter::StartsWith(prefix.to_string()));
        self
    }
    
    /// Filter triples by predicate ending with a substring
    pub fn with_predicate_ending(mut self, suffix: &str) -> Self {
        self.predicate_filter = Some(TripleFilter::EndsWith(suffix.to_string()));
        self
    }
    
    /// Filter triples by exact object value
    pub fn with_object(mut self, object: &str) -> Self {
        self.object_filter = Some(TripleFilter::Exact(object.to_string()));
        self
    }
    
    /// Filter triples by object containing a substring
    pub fn with_object_like(mut self, pattern: &str) -> Self {
        self.object_filter = Some(TripleFilter::Contains(pattern.to_string()));
        self
    }
    
    /// Filter triples by object starting with a substring
    pub fn with_object_starting(mut self, prefix: &str) -> Self {
        self.object_filter = Some(TripleFilter::StartsWith(prefix.to_string()));
        self
    }
    
    /// Filter triples by object ending with a substring
    pub fn with_object_ending(mut self, suffix: &str) -> Self {
        self.object_filter = Some(TripleFilter::EndsWith(suffix.to_string()));
        self
    }
    
    /// Apply a custom filter function to all triples
    pub fn filter<F>(mut self, predicate: F) -> Self 
    where 
        F: Fn(&Triple) -> bool + 'a
    {
        self.custom_filter = Some(Box::new(predicate));
        self
    }
    
    /// Join with another SparqlDatabase
    pub fn join(mut self, other: &'a SparqlDatabase) -> Self {
        self.join_db = Some(other);
        self
    }
    
    /// Specify join condition on subject
    pub fn join_on_subject(mut self) -> Self {
        self.join_conditions.push(JoinCondition::OnSubject);
        self
    }
    
    /// Specify join condition on predicate
    pub fn join_on_predicate(mut self) -> Self {
        self.join_conditions.push(JoinCondition::OnPredicate);
        self
    }
    
    /// Specify join condition on object
    pub fn join_on_object(mut self) -> Self {
        self.join_conditions.push(JoinCondition::OnObject);
        self
    }
    
    /// Specify a custom join condition
    pub fn join_with<F>(mut self, condition: F) -> Self
    where
        F: Fn(&Triple, &Triple) -> bool + 'static
    {
        self.join_conditions.push(JoinCondition::Custom(Box::new(condition)));
        self
    }
    
    /// Return only distinct results
    pub fn distinct(mut self) -> Self {
        self.distinct_results = true;
        self
    }
    
    /// Order results by a specified key function
    pub fn order_by<F>(mut self, key: F) -> Self
    where
        F: Fn(&Triple) -> String + 'a
    {
        self.sort_key = Some(Box::new(key));
        self
    }
    
    /// Set the sort direction to descending (default is ascending)
    pub fn desc(mut self) -> Self {
        self.sort_direction = SortDirection::Descending;
        self
    }
    
    /// Set the sort direction to ascending (this is the default)
    pub fn asc(mut self) -> Self {
        self.sort_direction = SortDirection::Ascending;
        self
    }
    
    /// Limit the number of results
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }
    
    /// Skip the first n results
    pub fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }
    
    /// Get the raw triple results
    pub fn get_triples(self) -> BTreeSet<Triple> {
        self.apply_filters()
    }
    
    /// Get results as decoded (subject, predicate, object) tuples
    pub fn get_decoded_triples(self) -> Vec<(String, String, String)> {
        // Store a reference to the database
        let db = self.db;
        
        // Now call get_triples which consumes self
        let triples = self.get_triples();
        let mut results = Vec::with_capacity(triples.len());
        
        for triple in triples {
            let subject = db.dictionary.decode(triple.subject).unwrap_or("").to_string();
            let predicate = db.dictionary.decode(triple.predicate).unwrap_or("").to_string();
            let object = db.dictionary.decode(triple.object).unwrap_or("").to_string();
            results.push((subject, predicate, object));
        }
        
        results
    }
    
    /// Get only the subjects from the results
    pub fn get_subjects(self) -> Vec<String> {
        // Store a reference to the database
        let db = self.db;
        let distinct = self.distinct_results;
        
        // Now call get_triples which consumes self
        let triples = self.get_triples();
        let mut results = Vec::with_capacity(triples.len());
        
        for triple in triples {
            if let Some(s) = db.dictionary.decode(triple.subject) {
                results.push(s.to_string());
            }
        }
        
        if distinct {
            results.sort();
            results.dedup();
        }
        
        results
    }
    
    /// Get only the predicates from the results
    pub fn get_predicates(self) -> Vec<String> {
        // Store a reference to the database
        let db = self.db;
        let distinct = self.distinct_results;
        
        // Now call get_triples which consumes self
        let triples = self.get_triples();
        let mut results = Vec::with_capacity(triples.len());
        
        for triple in triples {
            if let Some(p) = db.dictionary.decode(triple.predicate) {
                results.push(p.to_string());
            }
        }
        
        if distinct {
            results.sort();
            results.dedup();
        }
        
        results
    }
    
    /// Get only the objects from the results
    pub fn get_objects(self) -> Vec<String> {
        // Store a reference to the database
        let db = self.db;
        let distinct = self.distinct_results;
        
        // Now call get_triples which consumes self
        let triples = self.get_triples();
        let mut results = Vec::with_capacity(triples.len());
        
        for triple in triples {
            if let Some(o) = db.dictionary.decode(triple.object) {
                results.push(o.to_string());
            }
        }
        
        if distinct {
            results.sort();
            results.dedup();
        }
        
        results
    }
    
    /// Count the number of results without retrieving them
    pub fn count(self) -> usize {
        self.get_triples().len()
    }
    
    /// Group results by a key function
    pub fn group_by<F, K>(self, key_fn: F) -> BTreeMap<K, Vec<Triple>>
    where
        F: Fn(&Triple) -> K,
        K: Ord,
    {
        let triples = self.get_triples();
        let mut groups = BTreeMap::new();
        
        for triple in triples {
            let key = key_fn(&triple);
            groups.entry(key).or_insert_with(Vec::new).push(triple);
        }
        
        groups
    }
    
    // PRIVATE HELPER METHODS
    
    // Applies all the configured filters and returns the matching triples
    fn apply_filters(self) -> BTreeSet<Triple> {
        let mut results = BTreeSet::new();
        
        // Apply basic filters
        for triple in &self.db.triples {
            let mut matches = true;
            
            // Check subject filter
            if let Some(filter) = &self.subject_filter {
                if let Some(subject) = self.db.dictionary.decode(triple.subject) {
                    matches &= Self::apply_filter(filter, subject);
                } else {
                    matches = false;
                }
            }
            
            // Check predicate filter
            if matches {
                if let Some(filter) = &self.predicate_filter {
                    if let Some(predicate) = self.db.dictionary.decode(triple.predicate) {
                        matches &= Self::apply_filter(filter, predicate);
                    } else {
                        matches = false;
                    }
                }
            }
            
            // Check object filter
            if matches {
                if let Some(filter) = &self.object_filter {
                    if let Some(object) = self.db.dictionary.decode(triple.object) {
                        matches &= Self::apply_filter(filter, object);
                    } else {
                        matches = false;
                    }
                }
            }
            
            // Apply custom filter if specified
            if matches && self.custom_filter.is_some() {
                matches &= self.custom_filter.as_ref().unwrap()(triple);
            }
            
            // Add matching triple
            if matches {
                results.insert(triple.clone());
            }
        }
        
        // Apply join if specified
        if let Some(other_db) = self.join_db {
            if !self.join_conditions.is_empty() {
                results = self.apply_join(results, other_db);
            }
        }
        
        // Apply distinct if requested
        // (BTreeSet already ensures uniqueness)
        
        // Apply sorting if specified
        if let Some(key_fn) = self.sort_key {
            let mut sorted: Vec<Triple> = results.into_iter().collect();
            match self.sort_direction {
                SortDirection::Ascending => sorted.sort_by_key(|t| key_fn(t)),
                SortDirection::Descending => {
                    sorted.sort_by(|a, b| key_fn(b).cmp(&key_fn(a)))
                }
            }
            results = sorted.into_iter().collect();
        }
        
        // Apply limit and offset
        if self.offset.is_some() || self.limit.is_some() {
            let offset = self.offset.unwrap_or(0);
            let sorted: Vec<Triple> = results.into_iter().collect();
            let sliced = if let Some(limit) = self.limit {
                let end = (offset + limit).min(sorted.len());
                sorted[offset..end].to_vec()
            } else {
                sorted[offset..].to_vec()
            };
            results = sliced.into_iter().collect();
        }
        
        results
    }
    
    // Helper method to apply a filter to a string value
    fn apply_filter(filter: &TripleFilter, value: &str) -> bool {
        match filter {
            TripleFilter::Exact(s) => value == s,
            TripleFilter::Contains(s) => value.contains(s),
            TripleFilter::StartsWith(s) => value.starts_with(s),
            TripleFilter::EndsWith(s) => value.ends_with(s),
            TripleFilter::Custom(f) => f(value),
        }
    }
    
    // Helper method to apply join conditions
    fn apply_join(&self, left_triples: BTreeSet<Triple>, right_db: &SparqlDatabase) -> BTreeSet<Triple> {
        let mut joined_triples = BTreeSet::new();
        
        for condition in &self.join_conditions {
            match condition {
                JoinCondition::OnSubject => {
                    for left_triple in &left_triples {
                        for right_triple in &right_db.triples {
                            if left_triple.subject == right_triple.subject {
                                joined_triples.insert(Triple {
                                    subject: left_triple.subject,
                                    predicate: left_triple.predicate,
                                    object: right_triple.object,
                                });
                            }
                        }
                    }
                },
                JoinCondition::OnPredicate => {
                    for left_triple in &left_triples {
                        for right_triple in &right_db.triples {
                            if left_triple.predicate == right_triple.predicate {
                                joined_triples.insert(Triple {
                                    subject: left_triple.subject,
                                    predicate: left_triple.predicate,
                                    object: right_triple.object,
                                });
                            }
                        }
                    }
                },
                JoinCondition::OnObject => {
                    for left_triple in &left_triples {
                        for right_triple in &right_db.triples {
                            if left_triple.object == right_triple.object {
                                joined_triples.insert(Triple {
                                    subject: left_triple.subject,
                                    predicate: right_triple.predicate,
                                    object: right_triple.object,
                                });
                            }
                        }
                    }
                },
                JoinCondition::Custom(cond_fn) => {
                    for left_triple in &left_triples {
                        for right_triple in &right_db.triples {
                            if cond_fn(left_triple, right_triple) {
                                joined_triples.insert(Triple {
                                    subject: left_triple.subject,
                                    predicate: right_triple.predicate,
                                    object: right_triple.object,
                                });
                            }
                        }
                    }
                },
            }
        }
        
        joined_triples
    }
}