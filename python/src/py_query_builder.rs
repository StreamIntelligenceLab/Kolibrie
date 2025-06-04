/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use pyo3::prelude::*;
use kolibrie::sparql_database::SparqlDatabase;
use kolibrie::query_builder::QueryBuilder;
use std::sync::{Arc, Mutex};

#[pyclass]
pub struct PySparqlDatabase {
    db: Arc<Mutex<SparqlDatabase>>,
}

#[pymethods]
impl PySparqlDatabase {
    #[new]
    fn new() -> Self {
        PySparqlDatabase {
            db: Arc::new(Mutex::new(SparqlDatabase::new())),
        }
    }

    #[pyo3(signature = (subject, predicate, object))]
    pub fn add_triple(&self, subject: &str, predicate: &str, object: &str) {
        if let Ok(mut db) = self.db.lock() {
            db.add_triple_parts(subject, predicate, object);
        }
    }

    /// Start building a query.
    fn query(&self) -> PyQueryBuilder {
        PyQueryBuilder {
            db: Arc::clone(&self.db),
            subject: None,
            predicate: None,
            object: None,
            distinct: false,
            limit: None,
            offset: None,
        }
    }
}

#[pyclass]
pub struct PyQueryBuilder {
    db: Arc<Mutex<SparqlDatabase>>,
    subject: Option<String>,
    predicate: Option<String>,
    object: Option<String>,
    distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[pymethods]
impl PyQueryBuilder {
    /// Set an exact subject filter.
    fn with_subject(&self, subj: &str) -> Self {
        Self {
            db: self.db.clone(),
            subject: Some(subj.to_owned()),
            predicate: self.predicate.clone(),
            object: self.object.clone(),
            distinct: self.distinct,
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Set an exact predicate filter.
    fn with_predicate(&self, pred: &str) -> Self {
        Self {
            db: self.db.clone(),
            subject: self.subject.clone(),
            predicate: Some(pred.to_owned()),
            object: self.object.clone(),
            distinct: self.distinct,
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Set an exact object filter.
    fn with_object(&self, obj: &str) -> Self {
        Self {
            db: self.db.clone(),
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            object: Some(obj.to_owned()),
            distinct: self.distinct,
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Only return distinct results.
    fn distinct(&self) -> Self {
        Self {
            db: self.db.clone(),
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            object: self.object.clone(),
            distinct: true,
            limit: self.limit,
            offset: self.offset,
        }
    }

    /// Limit the number of results.
    fn limit(&self, n: usize) -> Self {
        Self {
            db: self.db.clone(),
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            object: self.object.clone(),
            distinct: self.distinct,
            limit: Some(n),
            offset: self.offset,
        }
    }

    /// Skip the first n results.
    fn offset(&self, n: usize) -> Self {
        Self {
            db: self.db.clone(),
            subject: self.subject.clone(),
            predicate: self.predicate.clone(),
            object: self.object.clone(),
            distinct: self.distinct,
            limit: self.limit,
            offset: Some(n),
        }
    }

    /// Build and execute the query, returning decoded (s,p,o) tuples.
    fn get_decoded_triples(&self) -> Vec<(String,String,String)> {
        if let Ok(db) = self.db.lock() {
            let mut qb = QueryBuilder::new(&*db);
            if let Some(ref s) = self.subject      { qb = qb.with_subject(s); }
            if let Some(ref p) = self.predicate    { qb = qb.with_predicate(p); }
            if let Some(ref o) = self.object       { qb = qb.with_object(o); }
            if self.distinct                       { qb = qb.distinct(); }
            if let Some(n) = self.limit            { qb = qb.limit(n); }
            if let Some(n) = self.offset           { qb = qb.offset(n); }
            qb.get_decoded_triples()
        } else {
            Vec::new() // Return empty vector if lock fails
        }
    }

    /// Return just the decoded subjects.
    fn get_subjects(&self) -> Vec<String> {
        if let Ok(db) = self.db.lock() {
            let mut qb = QueryBuilder::new(&*db);
            if let Some(ref s) = self.subject      { qb = qb.with_subject(s); }
            if let Some(ref p) = self.predicate    { qb = qb.with_predicate(p); }
            if let Some(ref o) = self.object       { qb = qb.with_object(o); }
            if self.distinct                       { qb = qb.distinct(); }
            if let Some(n) = self.limit            { qb = qb.limit(n); }
            if let Some(n) = self.offset           { qb = qb.offset(n); }
            qb.get_subjects()
        } else {
            Vec::new()
        }
    }

    /// Return just the decoded predicates.
    fn get_predicates(&self) -> Vec<String> {
        if let Ok(db) = self.db.lock() {
            let mut qb = QueryBuilder::new(&*db);
            if let Some(ref s) = self.subject      { qb = qb.with_subject(s); }
            if let Some(ref p) = self.predicate    { qb = qb.with_predicate(p); }
            if let Some(ref o) = self.object       { qb = qb.with_object(o); }
            if self.distinct                       { qb = qb.distinct(); }
            if let Some(n) = self.limit            { qb = qb.limit(n); }
            if let Some(n) = self.offset           { qb = qb.offset(n); }
            qb.get_predicates()
        } else {
            Vec::new()
        }
    }

    /// Return just the decoded objects.
    fn get_objects(&self) -> Vec<String> {
        if let Ok(db) = self.db.lock() {
            let mut qb = QueryBuilder::new(&*db);
            if let Some(ref s) = self.subject      { qb = qb.with_subject(s); }
            if let Some(ref p) = self.predicate    { qb = qb.with_predicate(p); }
            if let Some(ref o) = self.object       { qb = qb.with_object(o); }
            if self.distinct                       { qb = qb.distinct(); }
            if let Some(n) = self.limit            { qb = qb.limit(n); }
            if let Some(n) = self.offset           { qb = qb.offset(n); }
            qb.get_objects()
        } else {
            Vec::new()
        }
    }

    /// Count the number of matches.
    fn count(&self) -> usize {
        if let Ok(db) = self.db.lock() {
            let mut qb = QueryBuilder::new(&*db);
            if let Some(ref s) = self.subject      { qb = qb.with_subject(s); }
            if let Some(ref p) = self.predicate    { qb = qb.with_predicate(p); }
            if let Some(ref o) = self.object       { qb = qb.with_object(o); }
            if self.distinct                       { qb = qb.distinct(); }
            if let Some(n) = self.limit            { qb = qb.limit(n); }
            if let Some(n) = self.offset           { qb = qb.offset(n); }
            qb.count()
        } else {
            0 // Return 0 if lock fails
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // register your classes here
    m.add_class::<PySparqlDatabase>()?;
    m.add_class::<PyQueryBuilder>()?;
    Ok(())
}