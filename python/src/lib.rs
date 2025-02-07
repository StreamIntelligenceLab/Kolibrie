use pyo3::prelude::*;
use datalog::knowledge_graph::KnowledgeGraph;
use std::collections::HashMap;

/// Represents a condition to filter rules
#[pyclass]
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
        Self { variable, operator, value }
    }
}

/// Represents a term in a triple pattern
#[pyclass]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PyTerm {
    #[pyo3(name = "Variable")]
    Variable(String),

    #[pyo3(name = "Constant")]
    Constant(u32),
}

/// Represents a triple pattern used in rules
#[pyclass]
#[derive(Debug, Clone)]
struct PyTriplePattern {
    #[pyo3(get, set)]
    subject: PyTerm,
    
    #[pyo3(get, set)]
    predicate: PyTerm,
    
    #[pyo3(get, set)]
    object: PyTerm,
}

#[pymethods]
impl PyTriplePattern {
    #[new]
    fn new(subject: PyTerm, predicate: PyTerm, object: PyTerm) -> Self {
        Self { subject, predicate, object }
    }
}

/// Represents a logical rule in the knowledge graph
#[pyclass]
#[derive(Debug, Clone)]
struct PyRule {
    #[pyo3(get, set)]
    premise: Vec<PyTriplePattern>,
    
    #[pyo3(get, set)]
    filters: Vec<PyFilterCondition>,
    
    #[pyo3(get, set)]
    conclusion: PyTriplePattern,
}

#[pymethods]
impl PyRule {
    #[new]
    fn new(premise: Vec<PyTriplePattern>, filters: Vec<PyFilterCondition>, conclusion: PyTriplePattern) -> Self {
        Self { premise, filters, conclusion }
    }
}

#[pyclass]
struct PyKnowledgeGraph {
    inner: KnowledgeGraph,
}

#[pymethods]
impl PyKnowledgeGraph {
    #[new]
    fn new() -> Self {
        PyKnowledgeGraph {
            inner: KnowledgeGraph::new(),
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
        results
            .into_iter()
            .map(|triple| {
                let s = self.inner.dictionary.decode(triple.subject).unwrap_or_default().to_string();
                let p = self.inner.dictionary.decode(triple.predicate).unwrap_or_default().to_string();
                let o = self.inner.dictionary.decode(triple.object).unwrap_or_default().to_string();
                (s, p, o)
            })
            .collect()
    }

    fn add_rule(&mut self, rule: PyRule) {
        let converted_rule = shared::rule::Rule {
            premise: rule.premise
                .into_iter()
                .map(|p| (convert_term(p.subject), convert_term(p.predicate), convert_term(p.object)))
                .collect(),

            filters: rule.filters
                .into_iter()
                .map(|f| shared::rule::FilterCondition {
                    variable: f.variable,
                    operator: f.operator,
                    value: f.value,
                })
                .collect(),

            conclusion: (
                convert_term(rule.conclusion.subject),
                convert_term(rule.conclusion.predicate),
                convert_term(rule.conclusion.object),
            ),
        };

        self.inner.add_rule(converted_rule);
    }

    fn infer_new_facts(&mut self) -> Vec<(String, String, String)> {
        let inferred = self.inner.infer_new_facts();
        inferred
            .into_iter()
            .map(|triple| {
                let s = self.inner.dictionary.decode(triple.subject).unwrap_or_default().to_string();
                let p = self.inner.dictionary.decode(triple.predicate).unwrap_or_default().to_string();
                let o = self.inner.dictionary.decode(triple.object).unwrap_or_default().to_string();
                (s, p, o)
            })
            .collect()
    }

    fn infer_new_facts_semi_naive(&mut self) -> Vec<(String, String, String)> {
        let inferred = self.inner.infer_new_facts_semi_naive();
        inferred
            .into_iter()
            .map(|triple| {
                let s = self.inner.dictionary.decode(triple.subject).unwrap_or_default().to_string();
                let p = self.inner.dictionary.decode(triple.predicate).unwrap_or_default().to_string();
                let o = self.inner.dictionary.decode(triple.object).unwrap_or_default().to_string();
                (s, p, o)
            })
            .collect()
    }

    fn backward_chaining(&self, query: PyTriplePattern) -> Vec<HashMap<String, PyTerm>> {
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
        self.inner.dictionary.encode(term)
    }
}

/// Converts `PyTerm` to `knowledge_graph::Term`
fn convert_term(term: PyTerm) -> shared::terms::Term {
    match term {
        PyTerm::Variable(v) => shared::terms::Term::Variable(v),
        PyTerm::Constant(c) => shared::terms::Term::Constant(c),
    }
}

/// Converts `knowledge_graph::Term` to `PyTerm`
fn convert_term_back(term: shared::terms::Term) -> PyTerm {
    match term {
        shared::terms::Term::Variable(v) => PyTerm::Variable(v),
        shared::terms::Term::Constant(c) => PyTerm::Constant(c),
    }
}

/// Python module initialization
#[pymodule]
fn py_knowledge_graph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyKnowledgeGraph>()?;
    m.add_class::<PyRule>()?;             
    m.add_class::<PyFilterCondition>()?;  
    m.add_class::<PyTriplePattern>()?;    
    m.add_class::<PyTerm>()?;             
    Ok(())
}