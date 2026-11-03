/*
 * Copyright Â© 2024 Volodymyr Kadzhaia
 * Copyright Â© 2024 Pieter Bonte
 * KU Leuven â€” Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`ExecutionEngine`]: running a physical plan against a store.
//!
//! Walks the chosen plan and produces solutions. Every algorithmic decision was
//! already made during optimisation, so this makes none.

use super::super::operators::PhysicalOperator;
use super::super::types::{SubqueryProjection, SubquerySpec};
use super::exec_stats::exec_count;

use crate::sparql_database::SparqlDatabase;
use crate::term_order;
use rayon::prelude::*;

use shared::dataset_index::{GraphId, GraphTerm, QuadPattern};
use shared::query::SortDirection;
use shared::quoted_triple_store::is_quoted_triple_id;
use shared::terms::{Bindings, Term, UNBOUND};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Below this many rows a bind join runs on one thread, since splitting costs more in plan re-entry than it saves
const BIND_JOIN_MIN_CHUNK: usize = 64;

/// Rows a FILTER evaluates under a single dictionary read guard
const FILTER_LOCK_CHUNK: usize = 1024;

/// The RDF dataset visible to one query execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetView {
    /// Graphs forming the default graph. `FROM` replaces it, so this is not always
    /// just [`GraphId::Default`].
    pub default_graphs: Vec<GraphId>,
    /// Graphs `GRAPH` can address. A graph in the store but absent here is
    /// invisible to this query.
    pub named_graphs: HashSet<GraphId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streamertail_optimizer::Condition;
    use shared::dataset_index::Quad;

    fn encode(database: &SparqlDatabase, value: &str) -> u32 {
        database.dictionary.write().encode(value)
    }

    fn row(pairs: &[(&str, u32)]) -> HashMap<String, u32> {
        pairs
            .iter()
            .map(|(variable, value)| ((*variable).to_string(), *value))
            .collect()
    }

    fn sorted(bindings: Bindings) -> Vec<Vec<(String, u32)>> {
        let mut rows: Vec<Vec<(String, u32)>> = bindings
            .into_iter()
            .map(|row| {
                let mut pairs: Vec<(String, u32)> = row.into_iter().collect();
                pairs.sort();
                pairs
            })
            .collect();
        rows.sort();
        rows
    }

    #[test]
    fn hash_join_agrees_with_the_nested_loop_on_every_row_shape() {
        let cases: Vec<(Bindings, Bindings)> = vec![
            // Shared variable, several matches and one dangling value
            (
                vec![row(&[("x", 1), ("a", 10)]), row(&[("x", 2), ("a", 20)])].into(),
                vec![
                    row(&[("x", 1), ("b", 11)]),
                    row(&[("x", 1), ("b", 12)]),
                    row(&[("x", 3), ("b", 13)]),
                ]
                .into(),
            ),
            // Right side leaves the key variable unbound
            (
                vec![row(&[("x", 1), ("a", 10)])].into(),
                vec![row(&[("b", 11)]), row(&[("x", 1), ("b", 12)])].into(),
            ),
            // Left side leaves the key variable unbound
            (
                vec![row(&[("a", 10)]), row(&[("x", 1), ("a", 11)])].into(),
                vec![row(&[("x", 1), ("b", 12)]), row(&[("x", 2), ("b", 13)])].into(),
            ),
            // Conflicting values on the shared variable
            (vec![row(&[("x", 1)])].into(), vec![row(&[("x", 2)])].into()),
            // No shared variable at all: a Cartesian product
            (
                vec![row(&[("a", 1)]), row(&[("a", 2)])].into(),
                vec![row(&[("b", 3)]), row(&[("b", 4)])].into(),
            ),
        ];

        for (index, (left, right)) in cases.into_iter().enumerate() {
            let expected = sorted(ExecutionEngine::join_solution_sequences(
                left.clone(),
                right.clone(),
            ));
            let actual = sorted(ExecutionEngine::hash_join_solution_sequences(left, right));
            assert_eq!(
                actual, expected,
                "case {} disagreed with the nested loop",
                index
            );
        }
    }

    #[test]
    fn unit_and_union_preserve_solution_multiplicity() {
        let mut database = SparqlDatabase::new();
        let plan =
            PhysicalOperator::union(vec![PhysicalOperator::unit(), PhysicalOperator::unit()]);

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results, vec![HashMap::new(), HashMap::new()].into());
    }

    #[test]
    fn graph_variable_binds_before_filter_and_includes_empty_graphs() {
        let mut database = SparqlDatabase::new();
        let first = encode(&database, "http://example.com/first");
        let second = encode(&database, "http://example.com/second");
        database.dataset_index.create_graph(GraphId::Named(first));
        database.dataset_index.create_graph(GraphId::Named(second));

        let plan = PhysicalOperator::graph(
            PhysicalOperator::filter(
                PhysicalOperator::unit(),
                Condition::new("$g".to_string(), "=".to_string(), "?expected".to_string()),
            ),
            GraphTerm::Variable("$g".to_string()),
        );
        let context = ExecutionContext::new(DatasetView::from_database(&database));
        let mut input = HashMap::new();
        input.insert("expected".to_string(), second);

        let results = ExecutionEngine::execute_with_ids_and_input(
            &plan,
            &mut database,
            &context,
            vec![input].into(),
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results.row(0).expect("a row").get("g"), Some(second));
    }

    #[test]
    fn graph_scan_checks_repeated_variables_against_incoming_bindings() {
        let mut database = SparqlDatabase::new();
        let graph = encode(&database, "http://example.com/graph");
        let subject = encode(&database, "http://example.com/subject");
        let predicate = encode(&database, "http://example.com/predicate");
        let other = encode(&database, "http://example.com/other");
        database.add_quad(Quad {
            subject,
            predicate,
            object: subject,
            graph: GraphId::Named(graph),
        });
        database.add_quad(Quad {
            subject,
            predicate,
            object: other,
            graph: GraphId::Named(graph),
        });

        let scan = PhysicalOperator::quad_index_scan(QuadPattern {
            subject: Term::Variable("?value".to_string()),
            predicate: Term::Constant(predicate),
            object: Term::Variable("$value".to_string()),
            graph: GraphTerm::Default,
        });
        let plan = PhysicalOperator::graph(scan, GraphTerm::Named(graph));

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results.len(), 1);
        assert_eq!(results.row(0).expect("a row").get("value"), Some(subject));
    }

    #[test]
    fn scan_matches_a_variable_repeated_within_one_pattern() {
        let mut database = SparqlDatabase::new();
        let predicate = encode(&database, "http://example.com/self");
        let looping = encode(&database, "http://example.com/looping");
        let other = encode(&database, "http://example.com/other");
        database.add_quad(Quad {
            subject: looping,
            predicate,
            object: looping,
            graph: GraphId::Default,
        });
        database.add_quad(Quad {
            subject: looping,
            predicate,
            object: other,
            graph: GraphId::Default,
        });

        let plan = PhysicalOperator::quad_index_scan(QuadPattern {
            subject: Term::Variable("?x".to_string()),
            predicate: Term::Constant(predicate),
            object: Term::Variable("?x".to_string()),
            graph: GraphTerm::Default,
        });

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results.len(), 1);
        assert_eq!(results.row(0).expect("a row").get("x"), Some(looping));
    }

    #[test]
    fn merged_default_suppresses_duplicate_triples() {
        let mut database = SparqlDatabase::new();
        let graph = encode(&database, "http://example.com/graph");
        let subject = encode(&database, "http://example.com/subject");
        let predicate = encode(&database, "http://example.com/predicate");
        let object = encode(&database, "value");
        database.add_quad(Quad {
            subject,
            predicate,
            object,
            graph: GraphId::Default,
        });
        database.add_quad(Quad {
            subject,
            predicate,
            object,
            graph: GraphId::Named(graph),
        });

        let plan = PhysicalOperator::table_scan((
            Term::Variable("?s".to_string()),
            Term::Variable("?p".to_string()),
            Term::Variable("?o".to_string()),
        ));
        let dataset = DatasetView::new(
            [GraphId::Default, GraphId::Named(graph)],
            std::iter::empty(),
        );

        let results = ExecutionEngine::execute_with_ids_and_dataset(&plan, &mut database, &dataset);
        assert_eq!(results.len(), 1);
    }

    fn scan(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        dataset: DatasetView,
        incoming: Bindings,
    ) -> Bindings {
        let context = ExecutionContext::new(dataset);
        ExecutionEngine::execute_quad_scan_with_ids(database, pattern, &context, incoming)
    }

    fn candidates(
        database: &SparqlDatabase,
        graph: GraphId,
        subject: Option<u32>,
        predicate: Option<u32>,
        object: Option<u32>,
    ) -> Vec<(u32, u32, u32)> {
        database
            .dataset_index
            .query_graph(graph, subject, predicate, object)
            .into_iter()
            .map(|quad| (quad.subject, quad.predicate, quad.object))
            .collect()
    }

    fn add(
        database: &mut SparqlDatabase,
        subject: u32,
        predicate: u32,
        object: u32,
        graph: GraphId,
    ) {
        database.add_quad(Quad {
            subject,
            predicate,
            object,
            graph,
        });
    }

    fn variable(name: &str) -> Term {
        Term::Variable(name.to_string())
    }

    #[test]
    fn scan_emits_zero_one_and_many_matches_in_candidate_order() {
        let mut database = SparqlDatabase::new();
        let predicate = encode(&database, "http://example.com/p");
        let many = encode(&database, "http://example.com/many");
        let one = encode(&database, "http://example.com/one");
        let none = encode(&database, "http://example.com/none");
        let single = encode(&database, "single");
        for value in ["first", "second", "third"] {
            let object = encode(&database, value);
            add(&mut database, many, predicate, object, GraphId::Default);
        }
        add(&mut database, one, predicate, single, GraphId::Default);

        let pattern = QuadPattern {
            subject: variable("?s"),
            predicate: Term::Constant(predicate),
            object: variable("?o"),
            graph: GraphTerm::Default,
        };
        let incoming: Bindings = vec![
            row(&[("s", many), ("tag", 1)]),
            row(&[("s", none), ("tag", 2)]),
            row(&[("s", one), ("tag", 3)]),
            row(&[("s", many), ("tag", 1)]),
        ]
        .into();

        let results = scan(
            &database,
            &pattern,
            DatasetView::from_database(&database),
            incoming,
        );

        let many_rows: Bindings = candidates(
            &database,
            GraphId::Default,
            Some(many),
            Some(predicate),
            None,
        )
        .into_iter()
        .map(|(_, _, object)| row(&[("s", many), ("tag", 1), ("o", object)]))
        .collect();
        assert_eq!(many_rows.len(), 3);
        let mut expected = many_rows.clone();
        expected.push(row(&[("s", one), ("tag", 3), ("o", single)]));
        expected.extend(many_rows);
        assert_eq!(results, expected);
    }

    #[test]
    fn scan_rejects_repeated_variable_conflicts_before_extending_rows() {
        let mut database = SparqlDatabase::new();
        let predicate = encode(&database, "http://example.com/p");
        let b = encode(&database, "http://example.com/b");
        let a = encode(&database, "http://example.com/a");
        let c = encode(&database, "http://example.com/c");
        let d = encode(&database, "http://example.com/d");
        add(&mut database, a, predicate, b, GraphId::Default);
        add(&mut database, a, predicate, a, GraphId::Default);
        add(&mut database, c, predicate, c, GraphId::Default);
        add(&mut database, c, predicate, d, GraphId::Default);

        // `candidates` yields the index's hash order, which varies per process, so
        // this fixture can only be asserted on by content and never by position.
        // It must contain both self-referential and non-self-referential quads for
        // the conflict rejection below to be exercised at all.
        let all = candidates(&database, GraphId::Default, None, Some(predicate), None);
        assert!(all.iter().any(|(subject, _, object)| subject == object));
        assert!(all.iter().any(|(subject, _, object)| subject != object));

        let pattern = QuadPattern {
            subject: variable("?x"),
            predicate: Term::Constant(predicate),
            object: variable("$x"),
            graph: GraphTerm::Default,
        };
        let results = scan(
            &database,
            &pattern,
            DatasetView::from_database(&database),
            vec![
                row(&[]),
                row(&[("x", a), ("keep", 7)]),
                row(&[("x", d), ("keep", 8)]),
            ]
            .into(),
        );

        // Unbound `row(&[])` matches both self-referential quads; `x = a` extends and
        // keeps its other bindings; `x = d` is rejected because the only quad with
        // object `d` has subject `c`, and `?x`/`$x` are the same variable.
        let mut expected: Bindings = all
            .iter()
            .filter(|(subject, _, object)| subject == object)
            .map(|(subject, _, _)| row(&[("x", *subject)]))
            .collect();
        assert_eq!(expected.len(), 2);
        expected.push(row(&[("x", a), ("keep", 7)]));
        // Compared as a multiset: scan makes no ordering guarantee (bag semantics,
        // no ORDER BY) and its row order follows the hash order above.
        assert_eq!(sorted(results), sorted(expected));
    }

    #[test]
    fn scan_binds_visible_named_graphs_once_per_match() {
        let mut database = SparqlDatabase::new();
        let predicate = encode(&database, "http://example.com/p");
        let subject = encode(&database, "http://example.com/s");
        let first = encode(&database, "http://example.com/g1");
        let second = encode(&database, "http://example.com/g2");
        let hidden = encode(&database, "http://example.com/g3");
        let values: Vec<u32> = ["default", "one", "two", "three", "self"]
            .into_iter()
            .map(|value| encode(&database, value))
            .collect();
        add(
            &mut database,
            subject,
            predicate,
            values[0],
            GraphId::Default,
        );
        add(
            &mut database,
            subject,
            predicate,
            values[1],
            GraphId::Named(first),
        );
        add(
            &mut database,
            subject,
            predicate,
            values[2],
            GraphId::Named(second),
        );
        add(
            &mut database,
            subject,
            predicate,
            values[3],
            GraphId::Named(hidden),
        );
        add(
            &mut database,
            first,
            predicate,
            values[4],
            GraphId::Named(first),
        );
        let dataset = || {
            DatasetView::new(
                [GraphId::Default],
                [GraphId::Named(first), GraphId::Named(second)],
            )
        };

        let graph_pattern = QuadPattern {
            subject: variable("?s"),
            predicate: Term::Constant(predicate),
            object: variable("?o"),
            graph: GraphTerm::Variable("?g".to_string()),
        };
        let results = scan(
            &database,
            &graph_pattern,
            dataset(),
            vec![
                row(&[]),
                row(&[("g", second), ("tag", 1)]),
                row(&[("g", hidden), ("tag", 2)]),
            ]
            .into(),
        );
        let mut graphs = vec![GraphId::Named(first), GraphId::Named(second)];
        graphs.sort_unstable();
        let mut expected = Bindings::empty();
        for graph in graphs {
            let GraphId::Named(graph_id) = graph else {
                unreachable!()
            };
            for (s, _, o) in candidates(&database, graph, None, Some(predicate), None) {
                expected.push(row(&[("g", graph_id), ("s", s), ("o", o)]));
            }
        }
        assert_eq!(expected.len(), 3);
        expected.push(row(&[
            ("g", second),
            ("tag", 1),
            ("s", subject),
            ("o", values[2]),
        ]));
        assert_eq!(results, expected);

        let shared_pattern = QuadPattern {
            subject: variable("?g"),
            predicate: Term::Constant(predicate),
            object: variable("?o"),
            graph: GraphTerm::Variable("$g".to_string()),
        };
        let results = scan(&database, &shared_pattern, dataset(), vec![row(&[])].into());
        assert_eq!(results, vec![row(&[("g", first), ("o", values[4])])].into());

        let named_pattern = |graph| QuadPattern {
            subject: variable("?s"),
            predicate: Term::Constant(predicate),
            object: variable("?o"),
            graph: GraphTerm::Named(graph),
        };
        let results = scan(
            &database,
            &named_pattern(second),
            dataset(),
            vec![row(&[("tag", 3)]), row(&[("tag", 3)])].into(),
        );
        let second_row = row(&[("tag", 3), ("s", subject), ("o", values[2])]);
        assert_eq!(results, vec![second_row.clone(), second_row].into());
        let results = scan(
            &database,
            &named_pattern(hidden),
            dataset(),
            vec![row(&[])].into(),
        );
        assert!(results.is_empty());
    }

    #[test]
    fn scan_suppresses_merged_default_duplicates_for_each_input_row() {
        let mut database = SparqlDatabase::new();
        let graph = encode(&database, "http://example.com/graph");
        let subject = encode(&database, "http://example.com/subject");
        let predicate = encode(&database, "http://example.com/predicate");
        let shared = encode(&database, "shared");
        let extra = encode(&database, "extra");
        add(&mut database, subject, predicate, shared, GraphId::Default);
        add(
            &mut database,
            subject,
            predicate,
            shared,
            GraphId::Named(graph),
        );
        add(
            &mut database,
            subject,
            predicate,
            extra,
            GraphId::Named(graph),
        );

        let pattern = QuadPattern {
            subject: variable("?s"),
            predicate: Term::Constant(predicate),
            object: variable("?o"),
            graph: GraphTerm::Default,
        };
        let dataset = DatasetView::new(
            [GraphId::Default, GraphId::Named(graph)],
            std::iter::empty(),
        );
        let results = scan(
            &database,
            &pattern,
            dataset,
            vec![row(&[("tag", 1)]), row(&[("tag", 2)])].into(),
        );

        let expected: Bindings = [1, 2]
            .into_iter()
            .flat_map(|tag| {
                [shared, extra]
                    .into_iter()
                    .map(move |object| row(&[("tag", tag), ("s", subject), ("o", object)]))
            })
            .collect();
        assert_eq!(results, expected);
    }

    #[test]
    fn scan_matches_quoted_triples_through_the_general_path() {
        let mut database = SparqlDatabase::new();
        let predicate = encode(&database, "http://example.com/p");
        let source = encode(&database, "http://example.com/source");
        let first = encode(&database, "http://example.com/first");
        let second = encode(&database, "http://example.com/second");
        let plain = encode(&database, "http://example.com/plain");
        let alice = encode(&database, "http://example.com/alice");
        let bob = encode(&database, "http://example.com/bob");
        let one = encode(&database, "one");
        let two = encode(&database, "two");
        let (first_quoted, second_quoted) = {
            let mut store = database.quoted_triple_store.write().unwrap();
            (
                store.encode(first, predicate, one),
                store.encode(second, predicate, two),
            )
        };
        add(&mut database, first_quoted, source, alice, GraphId::Default);
        add(&mut database, second_quoted, source, bob, GraphId::Default);
        add(&mut database, plain, source, alice, GraphId::Default);

        let pattern = QuadPattern {
            subject: Term::QuotedTriple(Box::new((
                variable("?s"),
                Term::Constant(predicate),
                variable("?o"),
            ))),
            predicate: Term::Constant(source),
            object: variable("?who"),
            graph: GraphTerm::Default,
        };
        let results = scan(
            &database,
            &pattern,
            DatasetView::from_database(&database),
            vec![
                row(&[("tag", 1)]),
                row(&[("s", second), ("tag", 2)]),
                row(&[("s", second), ("who", alice)]),
            ]
            .into(),
        );

        let mut expected: Bindings =
            candidates(&database, GraphId::Default, None, Some(source), None)
                .into_iter()
                .filter_map(|(subject, _, who)| {
                    let (s, o) = if subject == first_quoted {
                        (first, one)
                    } else if subject == second_quoted {
                        (second, two)
                    } else {
                        return None;
                    };
                    Some(row(&[("tag", 1), ("s", s), ("o", o), ("who", who)]))
                })
                .collect();
        assert_eq!(expected.len(), 2);
        expected.push(row(&[("s", second), ("tag", 2), ("o", two), ("who", bob)]));
        assert_eq!(results, expected);
    }

    #[test]
    fn values_keeps_all_undef_and_heterogeneous_union_rows() {
        let mut database = SparqlDatabase::new();
        let value = encode(&database, "value");
        let plan = PhysicalOperator::union(vec![
            PhysicalOperator::values(vec!["?x".to_string()], vec![vec![Some(value)]]),
            PhysicalOperator::values(vec!["?y".to_string()], vec![vec![None]]),
        ]);

        let results = ExecutionEngine::execute_with_ids(&plan, &mut database);
        assert_eq!(results.len(), 2);
        assert_eq!(results.row(0).expect("a row").get("x"), Some(value));
        assert!(results.row(1).expect("a second row").is_empty());
    }
}

impl DatasetView {
    /// The normal database dataset: the physical default graph plus every
    pub fn from_database(database: &SparqlDatabase) -> Self {
        Self {
            default_graphs: vec![GraphId::Default],
            named_graphs: database.dataset_index.named_graphs().into_iter().collect(),
        }
    }

    /// Creates an explicit SPARQL dataset view
    pub fn new(
        default_graphs: impl IntoIterator<Item = GraphId>,
        named_graphs: impl IntoIterator<Item = GraphId>,
    ) -> Self {
        let mut seen_defaults = HashSet::new();
        let default_graphs = default_graphs
            .into_iter()
            .filter(|graph| seen_defaults.insert(*graph))
            .collect();
        let named_graphs = named_graphs
            .into_iter()
            .filter(|graph| matches!(graph, GraphId::Named(_)))
            .collect();
        Self {
            default_graphs,
            named_graphs,
        }
    }

    /// Creates a dataset with an empty query default and explicit named
    pub fn empty_default(named_graphs: impl IntoIterator<Item = GraphId>) -> Self {
        Self::new(std::iter::empty(), named_graphs)
    }

    /// Whether `GRAPH` may address this graph in the current dataset.
    pub fn is_named_visible(&self, graph: GraphId) -> bool {
        matches!(graph, GraphId::Named(_)) && self.named_graphs.contains(&graph)
    }
}

/// Recursive execution state. An active graph is established by a `GRAPH`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    /// Shared, so deriving a child context per `GRAPH` scope does not rebuild the graph sets
    pub dataset: Arc<DatasetView>,
    /// Graph established by an enclosing `GRAPH` clause, or `None` at the top
    /// level, where patterns match the default graph.
    pub active_graph: Option<GraphId>,
}

impl ExecutionContext {
    /// A top-level context over `dataset`, with no active graph.
    pub fn new(dataset: DatasetView) -> Self {
        Self {
            dataset: Arc::new(dataset),
            active_graph: None,
        }
    }

    fn with_active_graph(&self, graph: GraphId) -> Self {
        Self {
            dataset: Arc::clone(&self.dataset),
            active_graph: Some(graph),
        }
    }
}

/// Where one of a pattern's positions sends its value.
#[derive(Debug, Clone, Copy)]
enum ScanSlot {
    /// A fixed term: the candidate must equal it.
    Constant(u32),
    /// A variable: the candidate goes in this column, or must agree with it.
    Column(usize),
    /// A quoted triple pattern, which binds a variable number of nested variables
    /// and so takes the general path.
    Quoted,
}

/// How a scan writes a pattern's matches into rows of its output schema.
///
/// Built once per scan rather than per row. The map form resolved `?s`, `?p` and `?o`
/// by hashing their names again for every candidate quad, which is what a scan does
/// most of; here each position knows its column before the loop starts.
///
/// The output schema is the incoming schema followed by whatever the pattern adds, so
/// a base row is the incoming row padded with [`UNBOUND`] and needs no remapping.
#[derive(Debug)]
struct ScanLayout {
    schema: Vec<String>,
    subject: ScanSlot,
    predicate: ScanSlot,
    object: ScanSlot,
    /// Where a variable graph name is written, if the pattern has one.
    graph: Option<usize>,
}

impl ScanLayout {
    fn new(pattern: &QuadPattern, incoming: &[String]) -> Self {
        let mut schema = incoming.to_vec();
        let slot = |term: &Term, schema: &mut Vec<String>| match term {
            Term::Constant(value) => ScanSlot::Constant(*value),
            Term::QuotedTriple(triple) => {
                // The nested components bind variables too, and they need columns
                // even though the general path fills them by name.
                fn add_nested(term: &Term, schema: &mut Vec<String>) {
                    match term {
                        Term::Variable(variable) => {
                            let name = ExecutionEngine::normalize_variable(variable);
                            if !schema.iter().any(|existing| existing == name) {
                                schema.push(name.to_string());
                            }
                        }
                        Term::QuotedTriple(inner) => {
                            add_nested(&inner.0, schema);
                            add_nested(&inner.1, schema);
                            add_nested(&inner.2, schema);
                        }
                        Term::Constant(_) => {}
                    }
                }
                add_nested(&triple.0, schema);
                add_nested(&triple.1, schema);
                add_nested(&triple.2, schema);
                ScanSlot::Quoted
            }
            Term::Variable(variable) => {
                let name = ExecutionEngine::normalize_variable(variable);
                match schema.iter().position(|existing| existing == name) {
                    Some(column) => ScanSlot::Column(column),
                    None => {
                        schema.push(name.to_string());
                        ScanSlot::Column(schema.len() - 1)
                    }
                }
            }
        };
        let subject = slot(&pattern.subject, &mut schema);
        let predicate = slot(&pattern.predicate, &mut schema);
        let object = slot(&pattern.object, &mut schema);
        let graph = match &pattern.graph {
            GraphTerm::Variable(variable) => {
                let name = ExecutionEngine::normalize_variable(variable);
                Some(match schema.iter().position(|existing| existing == name) {
                    Some(column) => column,
                    None => {
                        schema.push(name.to_string());
                        schema.len() - 1
                    }
                })
            }
            _ => None,
        };
        Self {
            schema,
            subject,
            predicate,
            object,
            graph,
        }
    }

    fn width(&self) -> usize {
        self.schema.len()
    }

    /// An incoming row widened to the output schema.
    fn base_row(&self, incoming: &[u32]) -> Vec<u32> {
        let mut row = Vec::with_capacity(self.width());
        row.extend_from_slice(incoming);
        row.resize(self.width(), UNBOUND);
        row
    }

    /// Whether any position is a quoted triple pattern.
    fn has_quoted(&self) -> bool {
        matches!(self.subject, ScanSlot::Quoted)
            || matches!(self.predicate, ScanSlot::Quoted)
            || matches!(self.object, ScanSlot::Quoted)
    }

    /// The value a slot already has in `row`, which is what an index probe keys on.
    fn probe(&self, slot: ScanSlot, row: &[u32]) -> Option<u32> {
        match slot {
            ScanSlot::Constant(value) => Some(value),
            ScanSlot::Quoted => None,
            ScanSlot::Column(column) => match row[column] {
                value if value == UNBOUND => None,
                value => Some(value),
            },
        }
    }

    /// The three index-probe keys for a row.
    fn probe_keys(&self, row: &[u32]) -> (Option<u32>, Option<u32>, Option<u32>) {
        (
            self.probe(self.subject, row),
            self.probe(self.predicate, row),
            self.probe(self.object, row),
        )
    }

    /// Writes one position's value into `row`, or rejects the candidate.
    ///
    /// A variable already bound -- by the incoming row, or by an earlier position of
    /// this same pattern -- must agree, which is what makes `?x p ?x` a self-join.
    fn place(&self, slot: ScanSlot, value: u32, row: &mut [u32]) -> bool {
        match slot {
            ScanSlot::Constant(constant) => constant == value,
            ScanSlot::Quoted => false,
            ScanSlot::Column(column) => {
                if row[column] == UNBOUND {
                    row[column] = value;
                    true
                } else {
                    row[column] == value
                }
            }
        }
    }
}

/// How two sequences' columns line up in the sequence their join produces.
///
/// Built once per join rather than per row, which is the point of the shape: the
/// map form resolved every variable name again for every pair of rows it
/// considered, and a join considers a lot of pairs.
struct JoinLayout {
    /// The joined schema: the left's columns, then the right's new ones.
    schema: Vec<String>,
    /// Where each joined column comes from on the left, if anywhere.
    from_left: Vec<Option<usize>>,
    /// Where each joined column comes from on the right, if anywhere.
    from_right: Vec<Option<usize>>,
    /// Joined columns both sides carry: the variables a pair must agree on.
    shared: Vec<usize>,
}

impl JoinLayout {
    fn new(left: &[String], right: &[String]) -> Self {
        let mut schema: Vec<String> = left.to_vec();
        for name in right {
            if !schema.iter().any(|existing| existing == name) {
                schema.push(name.clone());
            }
        }
        let from_left: Vec<Option<usize>> = schema
            .iter()
            .map(|name| left.iter().position(|candidate| candidate == name))
            .collect();
        let from_right: Vec<Option<usize>> = schema
            .iter()
            .map(|name| right.iter().position(|candidate| candidate == name))
            .collect();
        let shared = (0..schema.len())
            .filter(|&column| from_left[column].is_some() && from_right[column].is_some())
            .collect();
        Self {
            schema,
            from_left,
            from_right,
            shared,
        }
    }

    /// Merges a pair if they agree on every shared variable.
    ///
    /// SPARQL compatibility: the two must agree only where they *both* bind, so a
    /// variable one side leaves unbound takes the other side's value. That is what
    /// makes `OPTIONAL` and `VALUES ... UNDEF` join correctly.
    fn merge(&self, left: &[u32], right: &[u32]) -> Option<Vec<u32>> {
        let mut merged = Vec::with_capacity(self.schema.len());
        for column in 0..self.schema.len() {
            let from_left = self.from_left[column].map_or(UNBOUND, |c| left[c]);
            let from_right = self.from_right[column].map_or(UNBOUND, |c| right[c]);
            merged.push(match (from_left, from_right) {
                (UNBOUND, value) => value,
                (value, UNBOUND) => value,
                (left, right) if left == right => left,
                _ => return None,
            });
        }
        Some(merged)
    }

    /// A row's key over the shared variables, or `None` if it leaves one unbound.
    ///
    /// An unkeyed row cannot be hashed, because it is compatible with anything.
    fn key(&self, row: &[u32], side: &[Option<usize>]) -> Option<Vec<u32>> {
        self.shared
            .iter()
            .map(|&column| match side[column].map(|c| row[c]) {
                Some(UNBOUND) | None => None,
                Some(id) => Some(id),
            })
            .collect()
    }
}

/// Execution engine for physical operators
#[derive(Debug)]
pub struct ExecutionEngine;

impl ExecutionEngine {
    /// Executes a physical operator and returns string results
    pub fn execute(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Vec<HashMap<String, String>> {
        let dataset = DatasetView::from_database(database);
        Self::execute_with_dataset(operator, database, &dataset)
    }

    /// Executes a plan against an explicit SPARQL dataset and decodes the results
    pub fn execute_with_dataset(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
        dataset: &DatasetView,
    ) -> Vec<HashMap<String, String>> {
        let id_results = Self::execute_with_ids_and_dataset(operator, database, dataset);

        // Convert ID results to string results only at the final step
        id_results
            .into_par_iter()
            .chunks(FILTER_LOCK_CHUNK)
            .flat_map_iter(|chunk| {
                let dict = database.dictionary.read();
                let qt_store = database.quoted_triple_store.read().unwrap();
                let decoded: Vec<HashMap<String, String>> = chunk
                    .into_iter()
                    .map(|id_result| {
                        id_result
                            .into_iter()
                            .map(|(var, id)| {
                                let value = dict
                                    .decode_term(id, &qt_store)
                                    .unwrap_or_else(|| "unknown".to_string());
                                (var, value)
                            })
                            .collect()
                    })
                    .collect();
                decoded.into_iter()
            })
            .collect()
    }

    /// Executes a physical operator and returns ID-based results for performance
    pub fn execute_with_ids(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
    ) -> Bindings {
        let dataset = DatasetView::from_database(database);
        Self::execute_with_ids_and_dataset(operator, database, &dataset)
    }

    /// Executes a plan against an explicit SPARQL dataset
    pub fn execute_with_ids_and_dataset(
        operator: &PhysicalOperator,
        database: &mut SparqlDatabase,
        dataset: &DatasetView,
    ) -> Bindings {
        let context = ExecutionContext::new(dataset.clone());
        Self::execute_with_ids_and_context(operator, database, &context)
    }

    /// Executes a plan with an explicit recursive execution context
    pub fn execute_with_ids_and_context(
        operator: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
    ) -> Bindings {
        Self::execute_with_ids_and_input(operator, database, context, Bindings::unit())
    }

    /// Executes a plan using the supplied solution sequence as input
    pub fn execute_with_ids_and_input(
        operator: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
        incoming: Bindings,
    ) -> Bindings {
        // `PhysicalOperator::output_variables` has to name every variable the plan can
        // bind, because a schema-and-tuples result has nowhere to put one it missed.
        // Checking it here, in debug only, puts every test and example in the
        // workspace behind that function rather than behind a handful of unit tests.
        #[cfg(debug_assertions)]
        let declared = operator.output_variables(incoming.schema());

        let produced =
            Self::execute_with_ids_and_input_inner(operator, database, context, incoming);

        #[cfg(debug_assertions)]
        for row in &produced {
            for variable in row.keys() {
                debug_assert!(
                    declared.iter().any(|name| name == variable),
                    "output_variables under-approximated: {operator:?} bound {variable:?}, \
                     declared {declared:?}"
                );
            }
        }

        produced
    }

    fn execute_with_ids_and_input_inner(
        operator: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
        mut incoming: Bindings,
    ) -> Bindings {
        if incoming.is_empty() {
            return Bindings::empty();
        }

        match operator {
            PhysicalOperator::Unit => incoming,
            PhysicalOperator::TableScan { pattern } => {
                Self::execute_quad_scan_with_ids(database, pattern, context, incoming)
            }
            PhysicalOperator::IndexScan { pattern } => {
                Self::execute_quad_scan_with_ids(database, pattern, context, incoming)
            }
            PhysicalOperator::Union { branches } => {
                let mut results = Bindings::empty();
                let last = branches.len().saturating_sub(1);
                for (index, branch) in branches.iter().enumerate() {
                    // Every branch evaluates against the same input, so only the last one may consume it
                    let branch_input = if index == last {
                        std::mem::take(&mut incoming)
                    } else {
                        incoming.clone()
                    };
                    results.append(Self::execute_with_ids_and_input(
                        branch,
                        database,
                        context,
                        branch_input,
                    ));
                }
                results
            }
            PhysicalOperator::Graph { input, graph } => {
                Self::execute_graph_with_ids(database, input, graph, context, incoming)
            }
            PhysicalOperator::Filter { input, condition } => {
                let input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);
                // The guard is taken per chunk rather than per row: one atomic.
                // Rows stay as cells throughout; rebuilding a map to ask the
                // condition about one was most of what this used to cost.
                let (schema, cells) = input_results.into_parts();
                let kept: Vec<Vec<u32>> = cells
                    .into_par_iter()
                    .chunks(FILTER_LOCK_CHUNK)
                    .flat_map_iter(|chunk| {
                        let dict = database.dictionary.read();
                        let kept: Vec<Vec<u32>> = chunk
                            .into_iter()
                            .filter(|row| condition.evaluate_with_ids(row, &schema, &dict))
                            .collect();
                        drop(dict);
                        kept.into_iter()
                    })
                    .collect();
                Bindings::from_parts(schema, kept)
            }
            PhysicalOperator::Projection { input, variables } => {
                let input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);

                // Strip '?' prefix from projection variables for matching
                let stripped_vars: Vec<String> = variables
                    .iter()
                    .map(|v| Self::normalize_variable(v).to_string())
                    .collect();

                // Projection is a column selection: the columns are resolved once
                // and every row is copied straight across them.
                let columns: Vec<usize> = stripped_vars
                    .iter()
                    .filter_map(|variable| input_results.column(variable))
                    .collect();
                let kept_schema: Vec<String> = columns
                    .iter()
                    .map(|&column| input_results.schema()[column].clone())
                    .collect();
                let cells: Vec<Vec<u32>> = input_results
                    .cells()
                    .par_iter()
                    .map(|row| columns.iter().map(|&column| row[column]).collect())
                    .collect();
                Bindings::from_parts(kept_schema, cells)
            }
            PhysicalOperator::BindJoin { left, right } => {
                let left_results =
                    Self::execute_with_ids_and_input(left, database, context, incoming);
                Self::execute_bind_join(right, database, context, left_results)
            }
            PhysicalOperator::HashJoin { left, right } => {
                let left_results =
                    Self::execute_with_ids_and_input(left, database, context, incoming);
                if left_results.is_empty() {
                    return Bindings::empty();
                }
                let right_results = Self::execute_with_ids_and_context(right, database, context);
                Self::hash_join_solution_sequences(left_results, right_results)
            }
            PhysicalOperator::NestedLoopJoin { left, right } => {
                let left_results =
                    Self::execute_with_ids_and_input(left, database, context, incoming);
                if left_results.is_empty() {
                    return Bindings::empty();
                }
                let right_results = Self::execute_with_ids_and_context(right, database, context);
                Self::join_solution_sequences(left_results, right_results)
            }
            PhysicalOperator::StarJoin { join_var, patterns } => {
                let _ = join_var;
                let mut results = incoming;
                for pattern in patterns {
                    let quad = QuadPattern {
                        subject: pattern.0.clone(),
                        predicate: pattern.1.clone(),
                        object: pattern.2.clone(),
                        graph: GraphTerm::Default,
                    };
                    results = Self::execute_quad_scan_with_ids(database, &quad, context, results);
                    if results.is_empty() {
                        break;
                    }
                }
                results
            }
            PhysicalOperator::InMemoryBuffer { content, origin: _ } => {
                Self::join_solution_sequences(incoming, content.clone())
            }
            PhysicalOperator::Subquery { inner, spec } => {
                // A subquery has its own variable scope and is evaluated once
                let inner_results =
                    Self::execute_with_ids_and_input(inner, database, context, Bindings::unit());
                let finalized = Self::finalize_subquery(inner_results, spec, database);
                Self::join_solution_sequences(incoming, finalized)
            }
            PhysicalOperator::Bind {
                input,
                function_name,
                arguments,
                output_variable,
            } => {
                let mut input_results =
                    Self::execute_with_ids_and_input(input, database, context, incoming);
                let output_var = Self::normalize_variable(output_variable);

                if function_name == "CONCAT" {
                    // Decode all needed values first
                    let dict = database.dictionary.read();
                    let decoded_values: Vec<Vec<String>> = input_results
                        .iter()
                        .map(|row| {
                            arguments
                                .iter()
                                .map(|arg| {
                                    let arg_stripped = Self::normalize_variable(arg);
                                    if Self::is_variable(arg) {
                                        if let Some(id) = row.get(arg_stripped) {
                                            dict.decode(id).unwrap_or("").to_string()
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        arg.trim_matches('"').to_string()
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    drop(dict);

                    // Now encode the concatenated results
                    let mut dict_write = database.dictionary.write();
                    let encoded: Vec<u32> = decoded_values
                        .iter()
                        .map(|decoded_row| dict_write.encode(&decoded_row.join("")))
                        .collect();
                    drop(dict_write);
                    input_results.bind_column(output_var, encoded);

                    input_results
                } else if let Some(func) = database.udf(function_name.as_str()) {
                    // Similar fix for UDF
                    let dict = database.dictionary.read();
                    let decoded_args: Vec<Vec<String>> = input_results
                        .iter()
                        .map(|row| {
                            arguments
                                .iter()
                                .map(|arg| {
                                    let arg_stripped = Self::normalize_variable(arg);
                                    if Self::is_variable(arg) {
                                        if let Some(id) = row.get(arg_stripped) {
                                            dict.decode(id).unwrap_or("").to_string()
                                        } else {
                                            String::new()
                                        }
                                    } else {
                                        arg.trim_matches('"').to_string()
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    drop(dict);

                    let mut dict_write = database.dictionary.write();
                    let encoded: Vec<u32> = decoded_args
                        .iter()
                        .map(|decoded_row| {
                            let resolved: Vec<&str> =
                                decoded_row.iter().map(String::as_str).collect();
                            dict_write.encode(&func.call(resolved))
                        })
                        .collect();
                    drop(dict_write);
                    input_results.bind_column(output_var, encoded);

                    input_results
                } else if function_name == "SUBJECT"
                    || function_name == "PREDICATE"
                    || function_name == "OBJECT"
                {
                    let qt_store = database.quoted_triple_store.read().unwrap();
                    // One component per solution, then one column. A solution whose
                    // argument is not a quoted triple contributes UNBOUND, which is
                    // what not inserting into its map used to mean.
                    let components: Vec<u32> = input_results
                        .iter()
                        .map(|row| {
                            let Some(arg) = arguments.first() else {
                                return UNBOUND;
                            };
                            let Some(id) = row.get(Self::normalize_variable(arg)) else {
                                return UNBOUND;
                            };
                            if !is_quoted_triple_id(id) {
                                return UNBOUND;
                            }
                            match qt_store.decode(id) {
                                Some((s, p, o)) => match function_name.as_str() {
                                    "SUBJECT" => s,
                                    "PREDICATE" => p,
                                    "OBJECT" => o,
                                    _ => unreachable!(),
                                },
                                None => UNBOUND,
                            }
                        })
                        .collect();
                    drop(qt_store);
                    input_results.bind_column(output_var, components);
                    input_results
                } else if function_name == "TRIPLE" {
                    if arguments.len() == 3 {
                        // One quoted triple per solution, then one column. A solution
                        // missing any component contributes UNBOUND, which is what
                        // not inserting into its map used to mean.
                        let quoted: Vec<u32> = input_results
                            .iter()
                            .map(|row| {
                                let args: Vec<Option<u32>> = arguments
                                    .iter()
                                    .map(|arg| {
                                        let arg_stripped = Self::normalize_variable(arg);
                                        if Self::is_variable(arg) {
                                            row.get(arg_stripped)
                                        } else {
                                            let mut dict = database.dictionary.write();
                                            Some(dict.encode(arg_stripped))
                                        }
                                    })
                                    .collect();
                                match (args[0], args[1], args[2]) {
                                    (Some(s), Some(p), Some(o)) => {
                                        let mut qt_store =
                                            database.quoted_triple_store.write().unwrap();
                                        qt_store.encode(s, p, o)
                                    }
                                    _ => UNBOUND,
                                }
                            })
                            .collect();
                        input_results.bind_column(output_var, quoted);
                    }
                    input_results
                } else if function_name == "isTRIPLE" {
                    let mut dict_write = database.dictionary.write();
                    let truths: Vec<u32> = input_results
                        .iter()
                        .map(|row| {
                            let Some(arg) = arguments.first() else {
                                return UNBOUND;
                            };
                            match row.get(Self::normalize_variable(arg)) {
                                Some(id) => dict_write.encode(if is_quoted_triple_id(id) {
                                    "true"
                                } else {
                                    "false"
                                }),
                                None => UNBOUND,
                            }
                        })
                        .collect();
                    drop(dict_write);
                    input_results.bind_column(output_var, truths);
                    input_results
                } else {
                    eprintln!("UDF_NOT_FOUND");
                    input_results
                }
            }
            PhysicalOperator::Values { variables, values } => {
                let stripped_vars: Vec<String> = variables
                    .iter()
                    .map(|v| Self::normalize_variable(v).to_string())
                    .collect();

                // Convert VALUES data to result rows
                let mut results = Bindings::empty();

                for value_row in values {
                    let mut row = HashMap::new();

                    for (i, var) in stripped_vars.iter().enumerate() {
                        if let Some(Some(value_id)) = value_row.get(i) {
                            row.insert(var.clone(), *value_id);
                        }
                    }

                    // An all-UNDEF row is still the unit solution mapping
                    results.push(row);
                }

                Self::join_solution_sequences(incoming, results)
            }
            PhysicalOperator::MLPredict {
                input,
                model_name,
                input_variables,
                output_variable,
                ..
            } => {
                if database.ml_context.approved_model(model_name).is_err() {
                    eprintln!("ML_EXECUTION_REJECTED");
                    return Bindings::empty();
                }
                let rows = Self::execute_with_ids_and_input(input, database, context, incoming);
                let data = {
                    let dict = database.dictionary.read();
                    rows.iter()
                        .map(|row| {
                            input_variables
                                .iter()
                                .map(|var| {
                                    row.get(Self::normalize_variable(var))
                                        .and_then(|id| dict.decode(id))
                                        .and_then(shared::rdf_term::numeric_value)
                                })
                                .collect::<Option<Vec<_>>>()
                        })
                        .collect::<Option<Vec<_>>>()
                };
                let Some(data) = data else {
                    return Bindings::empty();
                };
                match database.ml_context.predict(model_name, &data) {
                    Ok(predictions) => {
                        Self::merge_candle_predictions(rows, predictions, output_variable, database)
                    }
                    Err(_) => {
                        eprintln!("ML_EXECUTION_FAILED");
                        Bindings::empty()
                    }
                }
            }
        }
    }

    fn normalize_variable(variable: &str) -> &str {
        variable
            .strip_prefix('?')
            .or_else(|| variable.strip_prefix('$'))
            .unwrap_or(variable)
    }

    fn is_variable(value: &str) -> bool {
        value.starts_with('?') || value.starts_with('$')
    }

    fn finalize_subquery(
        rows: Bindings,
        spec: &SubquerySpec,
        database: &SparqlDatabase,
    ) -> Bindings {
        let mut rows = Self::aggregate_subquery_rows(rows, spec, database);
        Self::apply_subquery_order(&mut rows, &spec.order_conditions, database);

        if let Some(projection) = &spec.projection {
            let projected_variables: HashSet<String> = projection
                .iter()
                .map(Self::subquery_output_variable)
                .collect();
            // Projection selects columns rather than rewriting every row's map.
            let kept: Vec<String> = rows
                .schema()
                .iter()
                .filter(|variable| projected_variables.contains(*variable))
                .cloned()
                .collect();
            let columns: Vec<usize> = kept
                .iter()
                .filter_map(|variable| rows.column(variable))
                .collect();
            let cells: Vec<Vec<u32>> = rows
                .cells()
                .iter()
                .map(|row| columns.iter().map(|&column| row[column]).collect())
                .collect();
            rows = Bindings::from_parts(kept, cells);
        }

        if spec.distinct {
            let mut seen = HashSet::new();
            rows.retain(|row| {
                let mut key: Vec<_> = row
                    .iter()
                    .map(|(variable, value)| (variable.to_string(), value))
                    .collect();
                key.sort_unstable();
                seen.insert(key)
            });
        }

        if let Some(limit) = spec.limit {
            rows.truncate(limit);
        }
        rows
    }

    fn aggregate_subquery_rows(
        rows: Bindings,
        spec: &SubquerySpec,
        database: &SparqlDatabase,
    ) -> Bindings {
        let aggregates: Vec<&SubqueryProjection> = spec
            .projection
            .iter()
            .flatten()
            .filter(|projection| projection.kind != "VAR" && projection.kind != "*")
            .collect();
        if aggregates.is_empty() && spec.group_vars.is_empty() {
            return rows;
        }

        let mut groups: BTreeMap<Vec<Option<u32>>, Bindings> = BTreeMap::new();
        for row in rows {
            let key = spec
                .group_vars
                .iter()
                .map(|variable| row.get(Self::normalize_variable(variable)).copied())
                .collect();
            groups.entry(key).or_default().push(row);
        }
        if groups.is_empty() && spec.group_vars.is_empty() {
            groups.insert(Vec::new(), Bindings::empty());
        }

        groups
            .into_iter()
            .map(|(key, group)| {
                // Only the grouping key and the aggregates describe the group
                let mut result = HashMap::new();
                for (variable, id) in spec.group_vars.iter().zip(key) {
                    if let Some(id) = id {
                        result.insert(Self::normalize_variable(variable).to_string(), id);
                    }
                }

                for aggregate in &aggregates {
                    let input = Self::normalize_variable(&aggregate.variable);
                    let normalized_kind = aggregate.kind.to_ascii_uppercase();
                    let descriptor = crate::aggregate::Aggregate::parse(&normalized_kind);
                    if descriptor.kind == "COUNT" && input == "*" {
                        let count = if descriptor.distinct {
                            let unique: std::collections::BTreeSet<Vec<(&str, u32)>> =
                                group.iter().map(|row| row.iter().collect()).collect();
                            unique.len().to_string()
                        } else {
                            group.len().to_string()
                        };
                        let id = database.dictionary.write().encode(&count);
                        result.insert(Self::subquery_output_variable(aggregate), id);
                        continue;
                    }
                    let values = group
                        .iter()
                        .filter_map(|row| row.get(input))
                        .filter_map(|id| database.decode_any(id))
                        .collect::<Vec<_>>();
                    let value = crate::aggregate::value(
                        &aggregate.kind,
                        &values.iter().map(String::as_str).collect::<Vec<_>>(),
                    );
                    let output = Self::subquery_output_variable(aggregate);
                    if let Some(value) = value {
                        let id = database.dictionary.write().encode(&value);
                        result.insert(output, id);
                    } else {
                        result.remove(&output);
                    }
                }
                result
            })
            .collect()
    }

    fn apply_subquery_order(
        rows: &mut Bindings,
        conditions: &[(String, SortDirection)],
        database: &SparqlDatabase,
    ) {
        if conditions.is_empty() {
            return;
        }

        // Decode each ordering value once. Decoding inside the comparator
        //
        // Taken, not copied: the sorted rows are pushed back below, so leaving the
        // originals in place would append the sorted sequence to the unsorted one and
        // a following LIMIT would keep the wrong rows.
        let taken = std::mem::take(rows);
        let mut keyed: Vec<(Vec<Option<String>>, HashMap<String, u32>)> = taken
            .into_iter()
            .map(|row| {
                let keys = conditions
                    .iter()
                    .map(|(variable, _)| {
                        row.get(Self::normalize_variable(variable))
                            .and_then(|id| database.decode_any(*id))
                    })
                    .collect();
                (keys, row)
            })
            .collect();

        keyed.sort_by(|(left, _), (right, _)| {
            for (index, (_, direction)) in conditions.iter().enumerate() {
                let comparison =
                    term_order::compare(left[index].as_deref(), right[index].as_deref());
                let comparison = match direction {
                    SortDirection::Asc => comparison,
                    SortDirection::Desc => comparison.reverse(),
                };
                if comparison != std::cmp::Ordering::Equal {
                    return comparison;
                }
            }
            std::cmp::Ordering::Equal
        });

        rows.extend(keyed.into_iter().map(|(_, row)| row));
    }

    fn subquery_output_variable(projection: &SubqueryProjection) -> String {
        Self::normalize_variable(projection.alias.as_deref().unwrap_or(&projection.variable))
            .to_string()
    }

    fn execute_graph_with_ids(
        database: &SparqlDatabase,
        input: &PhysicalOperator,
        graph: &GraphTerm,
        context: &ExecutionContext,
        incoming: Bindings,
    ) -> Bindings {
        match graph {
            GraphTerm::Default => {
                let default_context = ExecutionContext {
                    dataset: Arc::clone(&context.dataset),
                    active_graph: None,
                };
                Self::execute_with_ids_and_input(input, database, &default_context, incoming)
            }
            GraphTerm::Named(graph_id) => {
                let graph_id = GraphId::Named(*graph_id);
                if !context.dataset.is_named_visible(graph_id)
                    || !database.dataset_index.graph_exists(graph_id)
                {
                    return Bindings::empty();
                }
                let graph_context = context.with_active_graph(graph_id);
                Self::execute_with_ids_and_input(input, database, &graph_context, incoming)
            }
            GraphTerm::Variable(variable) => {
                let variable = Self::normalize_variable(variable).to_string();
                let mut visible_graphs: Vec<_> =
                    context.dataset.named_graphs.iter().copied().collect();
                visible_graphs.sort_unstable();

                let mut results = Bindings::empty();
                for row in incoming {
                    if let Some(&bound_graph) = row.get(&variable) {
                        let graph = GraphId::Named(bound_graph);
                        if context.dataset.is_named_visible(graph)
                            && database.dataset_index.graph_exists(graph)
                        {
                            let graph_context = context.with_active_graph(graph);
                            results.append(Self::execute_with_ids_and_input(
                                input,
                                database,
                                &graph_context,
                                Bindings::from(vec![row]),
                            ));
                        }
                        continue;
                    }

                    for graph in &visible_graphs {
                        if !database.dataset_index.graph_exists(*graph) {
                            continue;
                        }
                        let GraphId::Named(graph_id) = *graph else {
                            continue;
                        };
                        let mut graph_row = row.clone();
                        graph_row.insert(variable.clone(), graph_id);
                        let graph_context = context.with_active_graph(*graph);
                        results.append(Self::execute_with_ids_and_input(
                            input,
                            database,
                            &graph_context,
                            Bindings::from(vec![graph_row]),
                        ));
                    }
                }
                results
            }
        }
    }

    fn execute_quad_scan_with_ids(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        context: &ExecutionContext,
        incoming: Bindings,
    ) -> Bindings {
        // Columns resolved once for the whole scan, not once per candidate quad.
        let layout = ScanLayout::new(pattern, incoming.schema());
        let mut rows: Vec<Vec<u32>> = Vec::new();

        // The visible graph set is the same for every incoming row, so it is built once
        let visible_graphs: Vec<GraphId> = if matches!(pattern.graph, GraphTerm::Variable(_)) {
            let mut graphs: Vec<GraphId> = context.dataset.named_graphs.iter().copied().collect();
            graphs.sort_unstable();
            graphs
        } else {
            Vec::new()
        };

        for incoming_row in incoming.cells() {
            let base = layout.base_row(incoming_row);
            match &pattern.graph {
                GraphTerm::Default => {
                    if let Some(active_graph) = context.active_graph {
                        Self::scan_one_graph(
                            database,
                            pattern,
                            &layout,
                            active_graph,
                            None,
                            &base,
                            &mut rows,
                        );
                    } else {
                        Self::scan_query_default(
                            database, pattern, &layout, context, &base, &mut rows,
                        );
                    }
                }
                GraphTerm::Named(graph_id) => {
                    let graph = GraphId::Named(*graph_id);
                    if context.dataset.is_named_visible(graph)
                        && database.dataset_index.graph_exists(graph)
                    {
                        Self::scan_one_graph(
                            database, pattern, &layout, graph, None, &base, &mut rows,
                        );
                    }
                }
                GraphTerm::Variable(_) => {
                    let column = layout.graph.expect("a variable graph has a column");
                    if base[column] != UNBOUND {
                        // The incoming row already fixed the graph name.
                        let graph = GraphId::Named(base[column]);
                        if context.dataset.is_named_visible(graph)
                            && database.dataset_index.graph_exists(graph)
                        {
                            Self::scan_one_graph(
                                database, pattern, &layout, graph, None, &base, &mut rows,
                            );
                        }
                    } else {
                        for graph in visible_graphs.iter().copied() {
                            if !database.dataset_index.graph_exists(graph) {
                                continue;
                            }
                            let GraphId::Named(graph_id) = graph else {
                                continue;
                            };
                            Self::scan_one_graph(
                                database,
                                pattern,
                                &layout,
                                graph,
                                Some(graph_id),
                                &base,
                                &mut rows,
                            );
                        }
                    }
                }
            }
        }

        Bindings::from_parts(layout.schema.clone(), rows)
    }

    fn scan_query_default(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        layout: &ScanLayout,
        context: &ExecutionContext,
        base: &[u32],
        rows: &mut Vec<Vec<u32>>,
    ) {
        let (subject, predicate, object) = layout.probe_keys(base);
        // Merging several graphs into one query default can surface the same triple twice
        let merges_graphs = context.dataset.default_graphs.len() > 1;
        let mut seen = HashSet::new();

        for graph in &context.dataset.default_graphs {
            exec_count!(SCAN_PROBES);
            for quad in database
                .dataset_index
                .query_graph(*graph, subject, predicate, object)
            {
                exec_count!(QUADS_EXAMINED);
                if merges_graphs && !seen.insert((quad.subject, quad.predicate, quad.object)) {
                    continue;
                }
                Self::match_quad(
                    database,
                    pattern,
                    layout,
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    None,
                    base,
                    rows,
                );
            }
        }
    }

    fn scan_one_graph(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        layout: &ScanLayout,
        graph: GraphId,
        fresh_graph: Option<u32>,
        base: &[u32],
        rows: &mut Vec<Vec<u32>>,
    ) {
        // The graph binding does not depend on the candidate quad, so it is placed
        // once per row rather than per quad -- and only copied when there is one to
        // place. A variable graph calls this once per visible graph per row, so an
        // unconditional copy here was an allocation per pair.
        let mut placed: Option<Vec<u32>> = None;
        if let (Some(graph_id), Some(column)) = (fresh_graph, layout.graph) {
            if base[column] != UNBOUND && base[column] != graph_id {
                return;
            }
            let mut copy = base.to_vec();
            copy[column] = graph_id;
            placed = Some(copy);
        }
        let base: &[u32] = placed.as_deref().unwrap_or(base);

        let (subject, predicate, object) = layout.probe_keys(base);
        exec_count!(SCAN_PROBES);
        for quad in database
            .dataset_index
            .query_graph(graph, subject, predicate, object)
        {
            exec_count!(QUADS_EXAMINED);
            Self::match_quad(
                database,
                pattern,
                layout,
                quad.subject,
                quad.predicate,
                quad.object,
                fresh_graph,
                &base,
                rows,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn match_quad(
        database: &SparqlDatabase,
        pattern: &QuadPattern,
        layout: &ScanLayout,
        subject: u32,
        predicate: u32,
        object: u32,
        fresh_graph: Option<u32>,
        base: &[u32],
        rows: &mut Vec<Vec<u32>>,
    ) {
        let _ = fresh_graph;

        // Quoted triples bind a variable number of nested variables, so they keep the
        // general map-based path. Every other pattern writes at most four cells.
        if layout.has_quoted() {
            let mut bindings: HashMap<String, u32> = layout
                .schema
                .iter()
                .zip(base.iter().copied())
                .filter(|&(_, id)| id != UNBOUND)
                .map(|(name, id)| (name.clone(), id))
                .collect();
            for (term, value) in [
                (&pattern.subject, subject),
                (&pattern.predicate, predicate),
                (&pattern.object, object),
            ] {
                if !Self::match_term_with_store(database, term, value, &mut bindings) {
                    return;
                }
            }
            let mut row = base.to_vec();
            for (name, value) in bindings {
                if let Some(column) = layout.schema.iter().position(|existing| *existing == name) {
                    row[column] = value;
                }
            }
            exec_count!(ROWS_EMITTED);
            rows.push(row);
            return;
        }

        let mut row = base.to_vec();
        if !layout.place(layout.subject, subject, &mut row)
            || !layout.place(layout.predicate, predicate, &mut row)
            || !layout.place(layout.object, object, &mut row)
        {
            return;
        }

        exec_count!(ROWS_EMITTED);
        rows.push(row);
    }

    fn match_term_with_store(
        database: &SparqlDatabase,
        term: &Term,
        value: u32,
        bindings: &mut HashMap<String, u32>,
    ) -> bool {
        match term {
            Term::Constant(constant) => *constant == value,
            Term::Variable(variable) => {
                let variable = Self::normalize_variable(variable);
                if let Some(&existing) = bindings.get(variable) {
                    existing == value
                } else {
                    bindings.insert(variable.to_string(), value);
                    true
                }
            }
            Term::QuotedTriple(pattern) => {
                if !is_quoted_triple_id(value) {
                    return false;
                }
                let components = {
                    let store = database.quoted_triple_store.read().unwrap();
                    store.decode(value)
                };
                let Some((subject, predicate, object)) = components else {
                    return false;
                };
                Self::match_term_with_store(database, &pattern.0, subject, bindings)
                    && Self::match_term_with_store(database, &pattern.1, predicate, bindings)
                    && Self::match_term_with_store(database, &pattern.2, object, bindings)
            }
        }
    }

    /// Feeds a solution sequence into a dependent plan in parallel chunks, concatenated in chunk order to match a sequential pass
    fn execute_bind_join(
        right: &PhysicalOperator,
        database: &SparqlDatabase,
        context: &ExecutionContext,
        left_results: Bindings,
    ) -> Bindings {
        if left_results.is_empty() {
            return Bindings::empty();
        }

        let threads = rayon::current_num_threads();
        let chunk_size = (left_results.len() / threads.max(1)).max(BIND_JOIN_MIN_CHUNK);
        if chunk_size >= left_results.len() {
            return Self::execute_with_ids_and_input(right, database, context, left_results);
        }

        let schema = left_results.schema().to_vec();
        let chunks: Vec<Bindings> = left_results
            .cells()
            .par_chunks(chunk_size)
            .map(|chunk| {
                Self::execute_with_ids_and_input(
                    right,
                    database,
                    context,
                    Bindings::from_parts(schema.clone(), chunk.to_vec()),
                )
            })
            .collect();
        let mut joined = Bindings::empty();
        for chunk in chunks {
            joined.append(chunk);
        }
        joined
    }

    fn join_solution_sequences(left: Bindings, right: Bindings) -> Bindings {
        if left.is_empty() || right.is_empty() {
            return Bindings::empty();
        }

        let layout = &JoinLayout::new(left.schema(), right.schema());
        let rows: Vec<Vec<u32>> = left
            .cells()
            .par_iter()
            .flat_map_iter(|left_row| {
                right
                    .cells()
                    .iter()
                    .filter_map(move |right_row| layout.merge(left_row, right_row))
            })
            .collect();
        Bindings::from_parts(layout.schema.clone(), rows)
    }

    /// Build/probe join that builds on the right side and probes in parallel with the left, so emitted order matches the nested loop
    fn hash_join_solution_sequences(left: Bindings, right: Bindings) -> Bindings {
        if left.is_empty() || right.is_empty() {
            return Bindings::empty();
        }

        let layout = &JoinLayout::new(left.schema(), right.schema());
        if layout.shared.is_empty() {
            return Self::join_solution_sequences(left, right);
        }

        let mut table: HashMap<Vec<u32>, Vec<&Vec<u32>>> = HashMap::new();
        let mut unkeyed: Vec<&Vec<u32>> = Vec::new();
        for right_row in right.cells() {
            match layout.key(right_row, &layout.from_right) {
                Some(key) => table.entry(key).or_default().push(right_row),
                None => unkeyed.push(right_row),
            }
        }

        // Only a left row that leaves a key variable unbound has to consider
        let any_left_row_unkeyed = left
            .cells()
            .iter()
            .any(|row| layout.key(row, &layout.from_left).is_none());
        let all_right: Vec<&Vec<u32>> = if any_left_row_unkeyed {
            right.cells().iter().collect()
        } else {
            Vec::new()
        };

        let rows: Vec<Vec<u32>> = left
            .cells()
            .par_iter()
            .flat_map_iter(|left_row| {
                // A fully bound left row probes the table plus the unhashable rows; a partially bound one can match anything
                let (probed, residual) = match layout.key(left_row, &layout.from_left) {
                    Some(key) => (
                        table.get(&key).map(Vec::as_slice).unwrap_or(&[]),
                        unkeyed.as_slice(),
                    ),
                    None => (all_right.as_slice(), &[][..]),
                };
                probed
                    .iter()
                    .chain(residual.iter())
                    .filter_map(move |right_row| layout.merge(left_row, right_row))
            })
            .collect();
        Bindings::from_parts(layout.schema.clone(), rows)
    }

    /// Merges string-valued Candle predictions back into id-encoded query rows
    fn merge_candle_predictions(
        mut input_results: Bindings,
        predictions: Vec<String>,
        output_variable: &str,
        database: &SparqlDatabase,
    ) -> Bindings {
        let output_var = output_variable.strip_prefix('?').unwrap_or(output_variable);

        let mut dict = database.dictionary.write();
        let encoded: Vec<u32> = predictions
            .iter()
            .take(input_results.len())
            .map(|prediction| dict.encode(prediction))
            .collect();
        drop(dict);
        input_results.bind_column(output_var, encoded);

        input_results
    }
}
