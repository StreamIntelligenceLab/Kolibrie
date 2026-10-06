# Volcano Optimizer

This module implements a Volcano-style query optimizer with cost-based optimization for the Kolibrie SPARQL database engine. Standard SELECT queries and the `WHERE` clauses of the six supported Update forms follow one path from the existing parser, through recursive logical operators and this optimizer, to physical execution. `GRAPH` and `UNION` do not use a separate evaluator.

## Architecture

The volcano optimizer is structured into several focused modules:

```
streamertail_optimizer/
├── mod.rs                    // Main module file with public API
├── operators/
│   ├── mod.rs               // Operator module index
│   ├── logical.rs           // LogicalOperator enum and constructors
│   └── physical.rs          // PhysicalOperator enum and constructors
├── cost/
│   ├── mod.rs               // Cost module index
│   └── estimator.rs         // CostEstimator with cardinality estimation
├── execution/
│   ├── mod.rs               // Execution module index
│   └── engine.rs            // ExecutionEngine for physical operators
├── stats/
│   ├── mod.rs               // Statistics module index
│   └── database_stats.rs    // DatabaseStats gathering and management
├── types.rs                 // Common types (Condition, IdResult)
├── optimizer.rs             // Main Streamertail implementation
└── utils.rs                 // Utility functions for plan building
```

## Key Components

### LogicalOperator
Represents high-level query operations before optimization:
- `Unit`: One input solution for an empty group
- `Scan`: Graph-aware quad-pattern scanning (default scans wrap a triple pattern)
- `Graph`: Fixed- or variable-named-graph scope
- `Union`: Multiset branch concatenation
- `Selection`: Filtering with conditions
- `Projection`: Variable projection
- `Join`: Binary join operations

### PhysicalOperator
Represents concrete execution plans after optimization:
- `Unit`: Pass-through execution for an empty group
- `TableScan` / `IndexScan`: Different scan strategies
- `Graph`: Graph-scoped child execution with graph-variable binding
- `Union`: Execution of every branch against the same input
- `Filter`: Condition filtering
- `BindJoin` / `HashJoin` / `NestedLoopJoin`: Join algorithms
- `Projection`: Variable projection

### CostEstimator
Provides cost and cardinality estimation for optimization:
- Estimates operator execution costs
- Performs cardinality estimation based on database statistics
- Uses selectivity estimation for filtering operations

### ExecutionEngine
Executes physical operators with performance optimizations:
- ID-based execution for reduced string operations
- A shared dataset/execution context for default, merged-default, and visible named graphs
- Input-binding propagation across scans, `GRAPH`, `UNION`, filters, binds, and joins
- Parallel execution using Rayon
- Index-aware scanning strategies
- SIMD-optimized join algorithms

### DatabaseStats
Gathers and maintains database statistics for cost estimation:
- Predicate, subject, and object cardinality tracking
- Join selectivity caching
- Fast sampling-based statistics gathering

## Usage

### Basic Usage

```rust
use kolibrie::streamertail_optimizer::*;
use shared::terms::{Term, TriplePattern};

// Create database and optimizer
let mut database = SparqlDatabase::new();
let mut optimizer = Streamertail::new(&database);

// Create logical plan
let logical_plan = LogicalOperator::scan((
    Term::Variable("person".to_string()),
    Term::Constant(name_id),
    Term::Variable("name".to_string()),
));

// Optimize and execute
let physical_plan = optimizer.find_best_plan(&logical_plan);
let results = optimizer.execute_plan(&physical_plan, &mut database);
```

### Complex Query Example

```rust
// Join with filter
let name_scan = LogicalOperator::scan(name_pattern);
let age_scan = LogicalOperator::scan(age_pattern);
let join = LogicalOperator::join(name_scan, age_scan);

let condition = Condition::new("age".to_string(), ">".to_string(), "25".to_string());
let filtered = LogicalOperator::selection(join, condition);

let physical_plan = optimizer.find_best_plan(&filtered);
let results = optimizer.execute_plan(&physical_plan, &mut database);
```

## Optimization Features

### Cost-Based Optimization
- Dynamic programming with memoization for plan enumeration
- Multiple join algorithms with cost comparison
- Index vs. table scan selection based on selectivity
- Join reordering based on cost estimates

### Performance Optimizations
- ID-based execution to reduce string operations
- Parallel execution using Rayon for CPU-intensive operations
- Index-aware scanning with multiple access patterns
- SIMD-optimized join algorithms for large datasets

### Statistics and Estimation
- Sampling-based statistics gathering for large datasets
- Cardinality estimation using database statistics
- Selectivity estimation for filtering operations
- Join selectivity caching for repeated queries

## Join Algorithms

Three algorithms with genuinely different execution, chosen by cost:

1. **BindJoin**: the left side executes first and its solutions are fed into
   the right side as incoming bindings, so right-side scans probe indexes with
   values that are already bound. Cheapest when the left side is small or
   selective, because the right side is never scanned standalone. Left rows are
   processed in parallel chunks.
2. **HashJoin**: both sides execute independently, a hash table is built on the
   shared-variable key, and the other side probes it in parallel. Wins when both
   sides are large, where one scan of the right side beats one probe per left
   row.
3. **NestedLoopJoin**: both sides are materialized and joined pairwise. Suits
   tiny inputs, and is the only algorithm that applies to a Cartesian product,
   where there is no shared variable to hash on.

Selection is by estimated cost, which combines each side's cost, its
cardinality, and the estimated join output. Because the algorithms differ in
which of those terms they pay, no single one wins unconditionally.

## Join Ordering

Before physical planning, each uninterrupted group of scans sharing one graph
scope is reordered: the most selective pattern runs first, and every later
pattern must share a variable with those already placed. A disconnected pattern
is only chosen when nothing connected remains, since that step is a Cartesian
product. Source order breaks ties, and GRAPH, UNION, FILTER, BIND, VALUES and
subquery boundaries are never crossed.

Candidate patterns are costed as bound scans: a position already bound by the
prefix divides the estimate by that position's domain size, which is what makes
an anchored path step cheap and an unanchored one expensive.

## Index Strategies

The optimizer leverages multiple index access patterns:

- **SPO**: Subject-Predicate-Object lookup
- **PSO**: Predicate-Subject-Object lookup  
- **OSP**: Object-Subject-Predicate lookup
- **POS**: Predicate-Object-Subject lookup
- **SOP**: Subject-Object-Predicate lookup
- **OPS**: Object-Predicate-Subject lookup

Index selection is based on the bound variables in triple patterns.

## Performance Considerations

### Memory Management
- Streaming execution for large result sets
- Result compaction to reduce memory usage
- Spill-to-disk for memory-intensive operations

### Parallelization
- Parallel statistics gathering
- Parallel join execution
- Parallel filtering operations

### Caching
- Memoization of optimized plans
- Statistics caching for repeated access
- Join selectivity caching

## Future Improvements

1. **Advanced Optimization Rules**
   - Filter pushdown optimization
   - Join reordering with bushy trees
   - Materialized view utilization

2. **Adaptive Optimization**
   - Runtime statistics feedback
   - Query plan adaptation
   - Machine learning-based cost estimation

3. **Distributed Execution**
   - Distributed join algorithms
   - Data partitioning strategies
   - Network-aware optimization

## Testing

The module includes comprehensive tests for:
- Cost estimation accuracy
- Cardinality estimation
- Join algorithm correctness
- Optimization rule application

Run tests with:
```bash
cargo test streamertail_optimizer
```

## Examples

See the `examples/` directory for complete working examples:
- `simple_volcano.rs`: Basic optimizer usage
- `complex_queries.rs`: Advanced optimization scenarios
- `performance_benchmarks.rs`: Performance testing
