# Volcano Optimizer

This module implements a Volcano-style query optimizer with cost-based optimization for the Kolibrie SPARQL database engine. The optimizer has been refactored from a monolithic file into a modular architecture for better maintainability and extensibility.

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
- `Scan`: Triple pattern scanning
- `Selection`: Filtering with conditions
- `Projection`: Variable projection
- `Join`: Binary join operations

### PhysicalOperator
Represents concrete execution plans after optimization:
- `TableScan` / `IndexScan`: Different scan strategies
- `Filter`: Condition filtering
- `HashJoin` / `NestedLoopJoin` / `ParallelJoin`: Join algorithms
- `OptimizedHashJoin`: High-performance join variant
- `Projection`: Variable projection

### CostEstimator
Provides cost and cardinality estimation for optimization:
- Estimates operator execution costs
- Performs cardinality estimation based on database statistics
- Uses selectivity estimation for filtering operations

### ExecutionEngine
Executes physical operators with performance optimizations:
- ID-based execution for reduced string operations
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

The optimizer supports multiple join algorithms:

1. **OptimizedHashJoin**: High-performance hash join with optimizations
2. **HashJoin**: Standard hash join implementation
3. **NestedLoopJoin**: Simple nested loop (for small datasets)
4. **ParallelJoin**: SIMD-optimized parallel join algorithm

Join selection is based on:
- Input cardinality estimates
- Available memory
- Data distribution characteristics

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