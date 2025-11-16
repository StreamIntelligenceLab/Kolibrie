# Volcano Query Planner Integration with RSP Engine

## Executive Summary

This document details the complete integration of the Volcano query optimizer with Kolibrie's RSP (RDF Stream Processing) engine to provide unified query planning for RSP-QL queries. The integration enables cost-based optimization for both individual window processing and cross-window joins using proven Volcano optimizer infrastructure.

## Problem Statement

The original RSP engine implementation had several limitations:

1. **Fixed Parameters**: Required manual configuration of window parameters instead of RSP-QL parsing
2. **Independent Windows**: Each window processed independently without cross-window joins
3. **Missing Join Logic**: No joins occurred when variables appeared in multiple window blocks
4. **Custom Join Implementation**: Attempted to reimplement join logic instead of leveraging existing optimizers
5. **No Query Planning**: Lacked unified query plans for complex multi-window scenarios

## Solution Architecture

### 1. RSP-QL Query Planner

The new architecture introduces a comprehensive query planner that:

- Parses RSP-QL queries to extract window specifications and shared variables
- Generates logical plans for individual windows using Volcano operators
- Creates unified physical plans that combine window processing with cross-window joins
- Leverages proven Volcano join algorithms (hash join, nested loop, parallel join)
- Applies cost-based optimization techniques to RSP-QL queries

### 2. Unified Query Plan Structure

```rust
pub struct RSPQueryPlan {
    pub window_plans: Vec<PhysicalOperator>,           // Individual window operators
    pub cross_window_join_plan: Option<PhysicalOperator>, // Unified join plan
    pub shared_variables: Vec<String>,                  // Variables triggering joins
    pub output_variables: Vec<String>,                  // Final output variables
}
```

### 3. Query Plan Generation Process

1. **RSP-QL Parsing**: Extract windows, shared variables, temporal parameters
2. **Logical Plan Creation**: Generate logical operators for each window
3. **Physical Plan Generation**: Convert to optimized physical operators
4. **Cross-Window Join Planning**: Create unified join plans for shared variables
5. **Cost-Based Optimization**: Apply Volcano optimization techniques
6. **Plan Execution**: Execute optimized physical plans

## Implementation Details

### Core Components

#### RSPQueryPlan Structure
- **Window Plans**: Individual `PhysicalOperator` instances for each window
- **Join Plans**: Unified `PhysicalOperator` for cross-window joins using shared variables
- **Variable Analysis**: Automatic detection of shared variables across window blocks
- **Output Projection**: Proper variable binding and projection handling

#### Query Planning Functions
```rust
fn create_rsp_query_plan(query_config: &RSPQueryConfig) -> Result<RSPQueryPlan, String>
fn create_window_logical_plan(query: &str) -> Result<LogicalOperator, String>
fn convert_to_physical_plan(logical: LogicalOperator) -> PhysicalOperator
fn create_cross_window_join_plan(window_plans: &[PhysicalOperator], shared_variables: &[String]) -> Result<PhysicalOperator, String>
```

#### Volcano Integration Points
- **Logical Operators**: `Scan`, `Selection`, `Projection`, `Join`
- **Physical Operators**: `TableScan`, `IndexScan`, `HashJoin`, `NestedLoopJoin`, `ParallelJoin`, `OptimizedHashJoin`
- **Execution Engine**: `ExecutionEngine::execute()` for plan execution
- **Cost Estimation**: Leverages existing cost models and statistics

### Cross-Window Join Semantics

#### When Joins Occur
Cross-window joins are triggered when:
- Multiple windows are defined in the same RSP-QL query
- Variables with identical names appear in different window blocks
- Results from windows arrive within the same temporal window

#### Join Execution Process
1. **Variable Detection**: Identify shared variables across window blocks
2. **Result Collection**: Gather results from all windows for each timestamp
3. **Join Coordination**: Wait for results from all participating windows
4. **Volcano Execution**: Execute optimized join plan using Volcano algorithms
5. **Result Streaming**: Apply R2S operators to joined results

#### Example RSP-QL Query with Joins
```rspql
SELECT ?sensor ?temperature ?humidity ?location
FROM NAMED WINDOW :tempWindow ON <http://streams/temp> [RANGE 30 STEP 10]
FROM NAMED WINDOW :humidWindow ON <http://streams/humid> [RANGE 25 STEP 8]
WHERE {
    WINDOW :tempWindow {
        ?sensor a :TemperatureSensor .
        ?sensor :hasTemperature ?temperature .
        ?sensor :locatedIn ?location .
    }
    WINDOW :humidWindow {
        ?sensor a :HumiditySensor .      # Same ?sensor variable = JOIN
        ?sensor :hasHumidity ?humidity .
    }
}
```

**Generated Query Plan**:
- Window 1: `Scan(?sensor, ?temperature, ?location)` for temperature data
- Window 2: `Scan(?sensor, ?humidity)` for humidity data  
- Join Plan: `OptimizedHashJoin(Window1, Window2)` on shared variable `?sensor`
- Output: Combined results where sensor IDs match across both windows

## Technical Improvements

### 1. Standards Compliance
- **Full RSP-QL Support**: Parses complete RSP-QL syntax including RETRIEVE and REGISTER clauses
- **Semantic Correctness**: Proper cross-window join semantics when variables are shared
- **Temporal Coordination**: Respects window timing constraints for join operations

### 2. Performance Optimizations
- **Cost-Based Planning**: Uses Volcano cost models for optimal join ordering
- **Algorithm Selection**: Automatic selection between hash joins, nested loop joins, etc.
- **Parallel Execution**: Leverages existing parallel join implementations
- **Memory Management**: Efficient handling of window result collections

### 3. Architectural Benefits
- **Code Reuse**: Leverages proven Volcano join algorithms instead of custom implementations
- **Extensibility**: Easy to add new optimization rules and join algorithms
- **Maintainability**: Unified codebase for query planning and execution
- **Testability**: Well-defined interfaces for testing and validation

## Usage Examples

### Basic RSP-QL with Volcano Planning
```rust
let rsp_ql_query = r#"
    REGISTER RSTREAM <http://output> AS
    SELECT ?sensor ?value
    FROM NAMED WINDOW :w1 ON <http://stream> [RANGE 10 STEP 5]
    WHERE {
        WINDOW :w1 { ?sensor :hasValue ?value . }
    }
"#;

let engine = RSPBuilder::new()
    .add_rsp_ql_query(rsp_ql_query)
    .set_query_execution_mode(QueryExecutionMode::Volcano)
    .build()?;

// Inspect generated query plan
let plan = engine.get_query_plan();
println!("Window plans: {:?}", plan.window_plans);
println!("Join plan: {:?}", plan.cross_window_join_plan);
```

### Multi-Window Join Example
```rust
let complex_query = r#"
    SELECT ?building ?temperature ?humidity ?co2
    FROM NAMED WINDOW :temp ON <stream1> [RANGE 30 STEP 10]  
    FROM NAMED WINDOW :humid ON <stream2> [RANGE 25 STEP 8]
    FROM NAMED WINDOW :co2 ON <stream3> [RANGE 20 STEP 5]
    WHERE {
        WINDOW :temp { 
            ?sensor :locatedIn ?building .
            ?sensor :hasTemp ?temperature . 
        }
        WINDOW :humid { 
            ?sensor :hasHumid ?humidity .    # Join on ?sensor
        }
        WINDOW :co2 { 
            ?building :hasCO2 ?co2 .         # Join on ?building
        }
    }
"#;
```

**Generated Query Plan**:
1. Individual window scans for each data stream
2. Hash join on `?sensor` between temp and humidity windows  
3. Hash join on `?building` to incorporate CO2 data
4. Final projection of all requested variables

## Examples and Demonstrations

### Available Examples

1. **`basic_rsp_ql_example.rs`**
   - Single window with Volcano optimization
   - Basic RSP-QL parsing and execution

2. **`multi_window_example.rs`**  
   - Multiple windows with different configurations
   - Cross-window coordination demonstration

3. **`volcano_comparison_example.rs`**
   - Performance comparison: Standard vs Volcano execution
   - Benchmarking and optimization analysis

4. **`cross_window_join_example.rs`**
   - Shared variable detection and joining
   - Temporal correlation of heterogeneous streams

5. **`volcano_query_planner_example.rs`** ⭐
   - **Complete Volcano integration demonstration**
   - Query plan generation and inspection
   - Unified physical plans for complex multi-window queries
   - Cost-based optimization showcase

### Running Examples
```bash
# Basic functionality
cargo run --example basic_rsp_ql_example

# Multi-window processing
cargo run --example multi_window_example

# Performance comparison
cargo run --example volcano_comparison_example

# Cross-window joins
cargo run --example cross_window_join_example

# Volcano query planner (comprehensive demo)
cargo run --example volcano_query_planner_example
```

## Future Enhancements

### Short-term Improvements
1. **Full SparqlDatabase Integration**: Complete integration with database execution engine
2. **Advanced Cost Models**: RSP-specific cost estimation for streaming data
3. **Dynamic Reoptimization**: Adaptive query plan adjustment based on stream characteristics

### Long-term Vision
1. **Distributed Query Planning**: Multi-node RSP-QL query execution
2. **Memory-Aware Optimization**: Window result caching and memory management  
3. **Stream Statistics**: Real-time statistics collection for better cost estimation
4. **Adaptive Join Selection**: Dynamic algorithm selection based on data patterns

## Conclusion

The integration of the Volcano query planner with the RSP engine represents a significant advancement in stream processing query optimization. By leveraging proven query optimization techniques and applying them to RSP-QL semantics, we achieve:

- **Standards Compliance**: Full RSP-QL specification support
- **Performance**: Cost-based optimization for complex streaming queries
- **Correctness**: Proper cross-window join semantics  
- **Maintainability**: Unified architecture using proven algorithms
- **Extensibility**: Easy integration of new optimization techniques

The implementation demonstrates how traditional query optimization techniques can be successfully adapted for stream processing, providing a solid foundation for advanced RSP-QL query execution.

## References

- RSP-QL Specification: W3C Community Group Report
- Volcano Optimizer: "The Volcano Optimizer Generator" by Graefe & McKenna
- Kolibrie RSP Engine: `kolibrie/src/rsp.rs`
- Volcano Implementation: `kolibrie/src/volcano_optimizer/`
- Examples: `kolibrie/examples/rsp_ql_engine/`

---

**Status**: ✅ Implemented and Tested  
**Version**: 1.0  
**Last Updated**: January 2025