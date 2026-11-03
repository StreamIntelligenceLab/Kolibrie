/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! [`PhysicalOperator`]: how a query is executed.
//!
//! The optimizer's output. Each node names a concrete algorithm — which join, which
//! index, whether a side is materialised — so running a physical plan involves no
//! further choices.

use super::super::types::SubquerySpec;
use super::super::Condition;
use shared::dataset_index::{GraphTerm, QuadPattern};
use shared::terms::{Bindings, Term, TriplePattern};

/// Physical operators represent the actual execution plan after optimization
#[derive(Debug, Clone)]
/// How a query is executed: a concrete algorithm per node.
///
/// The optimizer's output. Nothing here is left to choose at run time, which is why
/// the join variants name their algorithm rather than saying "join".
pub enum PhysicalOperator {
    /// The SPARQL unit table: one empty solution mapping.
    Unit,
    /// Scan without an index, examining every candidate quad.
    ///
    /// Dispatches to the same executor as [`IndexScan`](Self::IndexScan); the two
    /// are distinguished for costing, not execution.
    TableScan {
        /// Pattern to match.
        pattern: QuadPattern,
    },
    /// Scan through an index, visiting only quads that can match.
    IndexScan {
        /// Pattern to match; its bound positions select the index.
        pattern: QuadPattern,
    },
    /// Multiset union. No duplicate elimination is performed.
    Union {
        /// Branches, concatenated in order.
        branches: Vec<PhysicalOperator>,
    },
    /// Evaluate an input plan under a fixed or variable named graph.
    Graph {
        /// Plan evaluated inside that graph.
        input: Box<PhysicalOperator>,
        /// Which graph: fixed, or a variable bound to each in turn.
        graph: GraphTerm,
    },
    /// Drop solutions failing a condition.
    Filter {
        /// Plan whose solutions are filtered.
        input: Box<PhysicalOperator>,
        /// Condition each surviving solution satisfies.
        condition: Condition,
    },
    /// Pipelined dependent join whose left solutions feed the right side, so its scans probe indexes with bound values
    BindJoin {
        /// Executed first; each of its solutions drives the right side.
        left: Box<PhysicalOperator>,
        /// Re-entered once per left solution, with those bindings pushed in.
        right: Box<PhysicalOperator>,
    },
    /// Build/probe join over the shared-variable key of two independently executed sides
    HashJoin {
        /// Build side.
        left: Box<PhysicalOperator>,
        /// Probe side.
        right: Box<PhysicalOperator>,
    },
    /// Materialized pairwise join over both sides, and the algorithm for Cartesian products
    NestedLoopJoin {
        /// Outer side.
        left: Box<PhysicalOperator>,
        /// Inner side, re-scanned per outer solution.
        right: Box<PhysicalOperator>,
    },
    /// Join several patterns that all share one variable, in a single pass.
    ///
    /// Cheaper than a chain of pairwise joins when the shared variable is the only
    /// link, because no intermediate result is materialised per pair.
    StarJoin {
        /// The variable every pattern shares.
        join_var: String,
        /// Patterns joined on it.
        patterns: Vec<TriplePattern>,
    },
    /// Restrict each solution to a set of variables, keeping duplicates.
    Projection {
        /// Plan being projected.
        input: Box<PhysicalOperator>,
        /// Variables retained.
        variables: Vec<String>,
    },
    /// Solutions supplied directly rather than computed.
    InMemoryBuffer {
        /// The solutions.
        content: Bindings,
        /// Where they came from, for diagnostics.
        origin: String,
    },
    /// A nested plan evaluated independently, then joined on its projection.
    Subquery {
        /// The nested plan.
        inner: Box<PhysicalOperator>,
        /// What it projects and how its modifiers apply.
        spec: SubquerySpec,
    },
    /// Extend each solution with a value computed by a registered function.
    Bind {
        /// Plan being extended.
        input: Box<PhysicalOperator>,
        /// Function resolved against the store's UDFs at execution time.
        function_name: String,
        /// Variables passed as arguments.
        arguments: Vec<String>,
        /// Variable the result binds to.
        output_variable: String,
    },
    /// An inline table of candidate solutions.
    Values {
        /// Column variables, in order.
        variables: Vec<String>,
        /// Rows; `None` is `UNDEF` and leaves the variable unbound.
        values: Vec<Vec<Option<u32>>>,
    },
    /// Bind a variable from a model's prediction over each input solution.
    MLPredict {
        /// Solutions to predict over.
        input: Box<PhysicalOperator>,
        /// Model name, as declared.
        model_name: String,
        /// Artifact resolved at planning time, so execution does not re-consult the
        /// ML policy.
        model_path: String,
        /// Feature variables, in model input order.
        input_variables: Vec<String>,
        /// Variable the prediction binds to.
        output_variable: String,
    },
}

impl PhysicalOperator {
    /// Creates the SPARQL unit table.
    pub fn unit() -> Self {
        Self::Unit
    }

    /// Creates a new table scan physical operator
    pub fn table_scan(pattern: TriplePattern) -> Self {
        Self::quad_table_scan(QuadPattern {
            subject: pattern.0,
            predicate: pattern.1,
            object: pattern.2,
            graph: GraphTerm::Default,
        })
    }

    /// Creates a graph-aware table scan.
    pub fn quad_table_scan(pattern: QuadPattern) -> Self {
        Self::TableScan { pattern }
    }

    /// Creates a new index scan physical operator
    pub fn index_scan(pattern: TriplePattern) -> Self {
        Self::quad_index_scan(QuadPattern {
            subject: pattern.0,
            predicate: pattern.1,
            object: pattern.2,
            graph: GraphTerm::Default,
        })
    }

    /// Creates a graph-aware index scan.
    pub fn quad_index_scan(pattern: QuadPattern) -> Self {
        Self::IndexScan { pattern }
    }

    /// Creates a multiset UNION physical operator.
    pub fn union(branches: Vec<PhysicalOperator>) -> Self {
        Self::Union { branches }
    }

    /// Creates a GRAPH physical operator.
    pub fn graph(input: PhysicalOperator, graph: GraphTerm) -> Self {
        Self::Graph {
            input: Box::new(input),
            graph,
        }
    }

    /// Creates a new filter physical operator
    pub fn filter(input: PhysicalOperator, condition: Condition) -> Self {
        Self::Filter {
            input: Box::new(input),
            condition,
        }
    }

    /// Creates a new bind join physical operator
    pub fn bind_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::BindJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new hash join physical operator
    pub fn hash_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new nested loop join physical operator
    pub fn nested_loop_join(left: PhysicalOperator, right: PhysicalOperator) -> Self {
        Self::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a new projection physical operator
    pub fn projection(input: PhysicalOperator, variables: Vec<String>) -> Self {
        Self::Projection {
            input: Box::new(input),
            variables,
        }
    }

    /// A plan node supplying solutions directly.
    pub fn buffer(content: Bindings, origin: String) -> Self {
        Self::InMemoryBuffer { content, origin }
    }

    /// Creates a new subquery physical operator
    pub fn subquery(inner: PhysicalOperator, spec: SubquerySpec) -> Self {
        Self::Subquery {
            inner: Box::new(inner),
            spec,
        }
    }

    /// Creates a new bind physical operator
    pub fn bind(
        input: PhysicalOperator,
        function_name: String,
        arguments: Vec<String>,
        output_variable: String,
    ) -> Self {
        Self::Bind {
            input: Box::new(input),
            function_name,
            arguments,
            output_variable,
        }
    }

    /// Creates a new values physical operator
    pub fn values(variables: Vec<String>, values: Vec<Vec<Option<u32>>>) -> Self {
        Self::Values { variables, values }
    }

    /// Creates a new ML.PREDICT physical operator
    pub fn ml_predict(
        input: PhysicalOperator,
        model_name: String,
        model_path: String,
        input_variables: Vec<String>,
        output_variable: String,
    ) -> Self {
        Self::MLPredict {
            input: Box::new(input),
            model_name,
            model_path,
            input_variables,
            output_variable,
        }
    }

    /// Executes the physical operator and returns string-based results
    pub fn execute(
        &self,
        database: &mut crate::sparql_database::SparqlDatabase,
    ) -> Vec<std::collections::HashMap<String, String>> {
        super::super::execution::ExecutionEngine::execute(self, database)
    }

    /// Executes the physical operator and returns ID-based results for performance
    pub fn execute_with_ids(
        &self,
        database: &mut crate::sparql_database::SparqlDatabase,
    ) -> Bindings {
        super::super::execution::ExecutionEngine::execute_with_ids(self, database)
    }
}

/// Which variables a plan's solutions can bind.
///
/// The engine builds its rows a variable at a time: it starts from the unit solution
/// mapping, scans add whatever their pattern names, and a union concatenates branches
/// that bind different sets. Nothing ever asked what the whole set would be, because a
/// `HashMap` per row never needed to know.
///
/// A schema and tuples does need to know, so that a variable name is resolved to a
/// column once per plan instead of hashed once per row. That is the entire reason this
/// exists.
///
/// # Over-approximation is allowed, under-approximation is not
///
/// The returned set may contain a variable that no solution ends up binding -- an
/// `OPTIONAL` that never matches, a branch of a union that yields nothing. Such a
/// column is simply unbound in every row, which is representable. A variable *missing*
/// from the set has nowhere to go, so this must never under-approximate. The engine
/// carries a `debug_assert` to that effect, which puts the 161 tests that reach it --
/// measured, by making the engine panic and counting what failed -- behind this
/// function rather than a handful of unit tests. Deleting a scan's variables, a
/// join's right side, or a `BIND` output from this function all trip it.
impl PhysicalOperator {
    /// The variables solutions of this plan can bind, given the variables its input
    /// already binds.
    ///
    /// Names are returned without their leading `?` or `$`, in a deterministic order:
    /// the input's variables first, in their order, then new ones as the plan
    /// introduces them.
    pub fn output_variables(&self, input: &[String]) -> Vec<String> {
        let mut variables = input.to_vec();
        self.extend_output_variables(&mut variables);
        variables
    }

    fn extend_output_variables(&self, variables: &mut Vec<String>) {
        match self {
            Self::Unit => {}
            Self::TableScan { pattern } | Self::IndexScan { pattern } => {
                push_quad_variables(pattern, variables);
            }
            Self::Union { branches } => {
                // Every branch sees the same input, and the result is their
                // concatenation, so the schema is their union.
                for branch in branches {
                    branch.extend_output_variables(variables);
                }
            }
            Self::Graph { input, graph } => {
                input.extend_output_variables(variables);
                if let GraphTerm::Variable(name) = graph {
                    push_variable(name, variables);
                }
            }
            Self::Filter { input, .. } => input.extend_output_variables(variables),
            Self::Projection {
                input,
                variables: projected,
            } => {
                // Projection replaces the schema rather than adding to it, and a
                // projected variable the input never bound stays a column, unbound.
                let mut inner = Vec::new();
                input.extend_output_variables(&mut inner);
                variables.clear();
                for name in projected {
                    push_variable(name, variables);
                }
            }
            Self::BindJoin { left, right } => {
                // The right side is re-entered with the left's solutions pushed in, so
                // it sees them as its input.
                left.extend_output_variables(variables);
                right.extend_output_variables(variables);
            }
            Self::HashJoin { left, right } | Self::NestedLoopJoin { left, right } => {
                // Both sides run independently; the right side starts from the unit
                // mapping rather than from this plan's input.
                left.extend_output_variables(variables);
                let mut from_right = Vec::new();
                right.extend_output_variables(&mut from_right);
                for name in from_right {
                    push_variable(&name, variables);
                }
            }
            Self::StarJoin { join_var, patterns } => {
                push_variable(join_var, variables);
                for pattern in patterns {
                    push_triple_variables(pattern, variables);
                }
            }
            Self::InMemoryBuffer { content, .. } => {
                // Buffered solutions carry their own variables, and are joined in.
                for row in content {
                    let mut names: Vec<&str> = row.keys().collect();
                    names.sort();
                    for name in names {
                        push_variable(name, variables);
                    }
                }
            }
            Self::Subquery { inner, spec } => {
                match &spec.projection {
                    // An explicit projection list names the subquery's output; its
                    // alias is what the outer query sees.
                    Some(projections) => {
                        for projection in projections {
                            // Same rule the executor applies in
                            // `ExecutionEngine::subquery_output_variable`: the alias
                            // when renamed, otherwise the variable itself.
                            let name = projection.alias.as_deref().unwrap_or(&projection.variable);
                            push_variable(name, variables);
                        }
                    }
                    // SELECT * keeps whatever the inner plan bound. The subquery has
                    // its own scope, so the inner plan starts from the unit mapping.
                    None => {
                        let mut from_inner = Vec::new();
                        inner.extend_output_variables(&mut from_inner);
                        for name in from_inner {
                            push_variable(&name, variables);
                        }
                    }
                }
            }
            Self::Bind {
                input,
                output_variable,
                ..
            }
            | Self::MLPredict {
                input,
                output_variable,
                ..
            } => {
                input.extend_output_variables(variables);
                push_variable(output_variable, variables);
            }
            Self::Values {
                variables: names, ..
            } => {
                for name in names {
                    push_variable(name, variables);
                }
            }
        }
    }
}

/// Appends a variable name, without its sigil, if it is not already present.
fn push_variable(name: &str, variables: &mut Vec<String>) {
    let name = name
        .strip_prefix('?')
        .or_else(|| name.strip_prefix('$'))
        .unwrap_or(name);
    if !variables.iter().any(|existing| existing == name) {
        variables.push(name.to_string());
    }
}

fn push_term_variables(term: &Term, variables: &mut Vec<String>) {
    match term {
        Term::Variable(name) => push_variable(name, variables),
        // A quoted triple pattern's components are themselves terms and may be
        // variables, which SPARQL-star binds like any other.
        Term::QuotedTriple(triple) => {
            push_term_variables(&triple.0, variables);
            push_term_variables(&triple.1, variables);
            push_term_variables(&triple.2, variables);
        }
        Term::Constant(_) => {}
    }
}

fn push_triple_variables(pattern: &TriplePattern, variables: &mut Vec<String>) {
    push_term_variables(&pattern.0, variables);
    push_term_variables(&pattern.1, variables);
    push_term_variables(&pattern.2, variables);
}

fn push_quad_variables(pattern: &QuadPattern, variables: &mut Vec<String>) {
    push_term_variables(&pattern.subject, variables);
    push_term_variables(&pattern.predicate, variables);
    push_term_variables(&pattern.object, variables);
    if let GraphTerm::Variable(name) = &pattern.graph {
        push_variable(name, variables);
    }
}
