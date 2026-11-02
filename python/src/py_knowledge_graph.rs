/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use datalog::reasoning::Reasoner;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;

/// Represents a condition to filter rules
#[pyclass(name = "FilterCondition")]
#[derive(Debug, Clone)]
struct PyFilterCondition {
    #[pyo3(get, set)]
    variable: String,

    #[pyo3(get, set)]
    operator: String,

    #[pyo3(get, set)]
    value: String,
}

#[pymethods]
impl PyFilterCondition {
    #[new]
    fn new(variable: String, operator: String, value: String) -> Self {
        Self {
            variable,
            operator,
            value,
        }
    }
}

/// Represents a term in a triple pattern
#[pyclass(name = "Term")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Term {
    // Named rather than positional, so Python reads `term.name` and `term.id`
    // instead of `term._0`, which is what pyo3 calls an unnamed field and is not
    // something an example should have to show.
    #[pyo3(name = "Variable")]
    Variable { name: String },

    #[pyo3(name = "Constant")]
    Constant { id: u32 },
}

#[pymethods]
impl Term {
    fn __repr__(&self) -> String {
        match self {
            Term::Variable { name } => format!("Term.Variable({name:?})"),
            Term::Constant { id } => format!("Term.Constant({id})"),
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Represents a triple pattern used in rules
#[pyclass(name = "TriplePattern")]
#[derive(Debug, Clone)]
struct PyTriplePattern {
    #[pyo3(get, set)]
    subject: Term,

    #[pyo3(get, set)]
    predicate: Term,

    #[pyo3(get, set)]
    object: Term,
}

#[pymethods]
impl PyTriplePattern {
    #[new]
    fn new(subject: Term, predicate: Term, object: Term) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }
}

/// Represents a logical rule in the knowledge graph
#[pyclass(name = "Rule")]
#[derive(Debug, Clone)]
struct PyRule {
    #[pyo3(get, set)]
    premise: Vec<PyTriplePattern>,

    #[pyo3(get, set)]
    filters: Vec<PyFilterCondition>,

    #[pyo3(get, set)]
    conclusion: Vec<PyTriplePattern>,
}

#[pymethods]
impl PyRule {
    #[new]
    fn new(
        premise: Vec<PyTriplePattern>,
        filters: Vec<PyFilterCondition>,
        conclusion: Vec<PyTriplePattern>,
    ) -> Self {
        Self {
            premise,
            filters,
            conclusion,
        }
    }
}

#[pyclass(name = "KnowledgeGraph")]
struct PyKnowledgeGraph {
    inner: Reasoner,
}

#[pymethods]
impl PyKnowledgeGraph {
    #[new]
    fn new() -> Self {
        PyKnowledgeGraph {
            inner: Reasoner::new(),
        }
    }

    fn add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        self.inner.add_abox_triple(subject, predicate, object);
    }

    #[pyo3(signature = (subject=None, predicate=None, object=None))]
    fn query_abox(
        &mut self,
        subject: Option<&str>,
        predicate: Option<&str>,
        object: Option<&str>,
    ) -> Vec<(String, String, String)> {
        let results = self.inner.query_abox(subject, predicate, object);

        // Acquire read lock once for all decoding operations
        let decoded_results: Vec<(String, String, String)> = results
            .into_iter()
            .map(|triple| {
                let s = self.inner.decode_term(triple.subject).unwrap_or_default();
                let p = self.inner.decode_term(triple.predicate).unwrap_or_default();
                let o = self.inner.decode_term(triple.object).unwrap_or_default();
                (s, p, o)
            })
            .collect();

        decoded_results
    }

    fn add_rule(&mut self, rule: PyRule) {
        let converted_rule = shared::rule::Rule {
            premise: rule
                .premise
                .into_iter()
                .map(|p| {
                    (
                        convert_term(p.subject),
                        convert_term(p.predicate),
                        convert_term(p.object),
                    )
                })
                .collect(),
            negative_premise: vec![],
            filters: rule
                .filters
                .into_iter()
                .map(|f| shared::rule::FilterCondition {
                    variable: f.variable,
                    operator: f.operator,
                    value: f.value,
                })
                .collect(),

            conclusion: rule
                .conclusion
                .into_iter()
                .map(|c| {
                    (
                        convert_term(c.subject),
                        convert_term(c.predicate),
                        convert_term(c.object),
                    )
                })
                .collect(),
        };

        self.inner.add_rule(converted_rule);
    }

    fn infer_new_facts(&mut self) -> Vec<(String, String, String)> {
        let inferred = self.inner.infer_new_facts();

        // Acquire read lock once for all decoding operations
        let decoded_results: Vec<(String, String, String)> = inferred
            .into_iter()
            .map(|triple| {
                let s = self.inner.decode_term(triple.subject).unwrap_or_default();
                let p = self.inner.decode_term(triple.predicate).unwrap_or_default();
                let o = self.inner.decode_term(triple.object).unwrap_or_default();
                (s, p, o)
            })
            .collect();

        decoded_results
    }

    fn infer_new_facts_semi_naive(&mut self) -> Vec<(String, String, String)> {
        let inferred = self.inner.infer_new_facts_semi_naive();

        // Acquire read lock once for all decoding operations
        let decoded_results: Vec<(String, String, String)> = inferred
            .into_iter()
            .map(|triple| {
                let s = self.inner.decode_term(triple.subject).unwrap_or_default();
                let p = self.inner.decode_term(triple.predicate).unwrap_or_default();
                let o = self.inner.decode_term(triple.object).unwrap_or_default();
                (s, p, o)
            })
            .collect();

        decoded_results
    }

    fn backward_chaining(&self, query: PyTriplePattern) -> Vec<HashMap<String, Term>> {
        let rust_query = (
            convert_term(query.subject),
            convert_term(query.predicate),
            convert_term(query.object),
        );

        let results = self.inner.backward_chaining(&rust_query);

        results
            .into_iter()
            .map(|bindings| {
                bindings
                    .into_iter()
                    .map(|(key, value)| (key, convert_term_back(value)))
                    .collect()
            })
            .collect()
    }

    fn encode_term(&mut self, term: &str) -> u32 {
        // Acquire write lock for encoding
        let id = self.inner.encode_term(term);
        id
    }

    /// Adds every rule in an N3 document, and returns how many were added.
    ///
    /// `{ premise } => { conclusion } .` under `@prefix` declarations, which is how
    /// a rule is usually written down rather than assembled:
    ///
    /// ```text
    /// @prefix ex: <http://example.org/> .
    /// { ?x ex:hasParent ?y . ?y ex:hasParent ?z . } => { ?x ex:hasGrandparent ?z . }
    /// ```
    ///
    /// Building the same rule through `Rule`, `TriplePattern` and `Term` means
    /// naming every constant by the id `encode_term` gave it; this interns them for
    /// you, against this graph's own dictionary, so the rule matches its facts.
    ///
    /// One prefix block covers every rule in the document, so rules written
    /// together keep their context.
    fn add_n3_rules(&mut self, n3: &str) -> PyResult<usize> {
        let (_, (_prefixes, rules)) =
            datalog::parser_n3_logic::parse_n3_document(n3, &mut self.inner).map_err(|error| {
                PyValueError::new_err(format!("could not parse the N3 rules: {error}"))
            })?;
        let added = rules.len();
        for rule in rules {
            self.inner.add_rule(rule);
        }
        Ok(added)
    }

    /// The text an id stands for, or `None` if this graph never assigned it.
    ///
    /// The inverse of `encode_term`, and needed to read anything the graph answers
    /// in terms of ids rather than strings. `backward_chaining` is the case that
    /// forced it: its bindings carry `Term.Constant(0)`, which says nothing without
    /// this.
    fn decode_term(&self, id: u32) -> Option<String> {
        self.inner.decode_term(id)
    }
}

/// Converts Python `Term` to `reasoning::Term`
fn convert_term(term: Term) -> shared::terms::Term {
    match term {
        Term::Variable { name } => shared::terms::Term::Variable(name),
        Term::Constant { id } => shared::terms::Term::Constant(id),
    }
}

/// Converts `reasoning::Term` to Python `Term`
fn convert_term_back(term: shared::terms::Term) -> Term {
    match term {
        shared::terms::Term::Variable(name) => Term::Variable { name },
        shared::terms::Term::Constant(id) => Term::Constant { id },
        shared::terms::Term::QuotedTriple(_) => Term::Variable {
            name: "__quoted_triple".to_string(),
        },
    }
}

/// Python module initialization
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyKnowledgeGraph>()?;
    m.add_class::<PyRule>()?;
    m.add_class::<PyFilterCondition>()?;
    m.add_class::<PyTriplePattern>()?;
    m.add_class::<Term>()?;
    Ok(())
}
