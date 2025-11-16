# RSP Engine Improvements: RSP-QL Query Parsing with Volcano Query Planner

This directory contains examples demonstrating the improved RSP engine that uses parsed RSP-QL queries with integrated Volcano query planner for unified window processing and cross-window joins.

## Overview

The RSP engine has been enhanced to:

1. **Parse RSP-QL queries** to extract window specifications and queries
2. **Support multiple windows** with different configurations
3. **Route data to appropriate streams** based on stream IRIs
4. **Generate window-specific SPARQL queries** from RSP-QL syntax
5. **Use Volcano optimizer** for cost-based query execution on window data
6. **Perform cross-window joins** on shared variables between multiple windows
7. **Generate unified query plans** that combine window processing and cross-window joins using Volcano optimizer infrastructure

## Key Improvements

### Before (Fixed Parameters)
```rust
let mut engine = RSPBuilder::new(window_size, slide)
    .add_tick(Tick::TimeDriven)
    .add_report_strategy(ReportStrategy::OnWindowClose)
    .add_query("SELECT ?s WHERE{ ?s a <http://www.w3.org/test/SuperType>}")
    .add_r2s(StreamOperator::RSTREAM)
    .build();
```

### After (RSP-QL Query Parsing)
```rust
let rsp_ql_query = r#"
    REGISTER RSTREAM <http://out/stream> AS
    SELECT *
    FROM NAMED WINDOW :wind ON ?s [RANGE 10 STEP 5]
    WHERE {
        WINDOW :wind {
            ?s a <http://www.w3.org/test/SuperType> .
        }
    }
"#;

let mut engine = RSPBuilder::new()
    .add_rsp_ql_query(rsp_ql_query)
    .set_query_execution_mode(QueryExecutionMode::Volcano)
    .build()?;
```

## Architecture Changes

### RSP-QL Query Parsing
- Uses the existing parser from `kolibrie::parser::parse_combined_query`
- Extracts window specifications (width, slide, tick, report strategy)
- Generates individual SPARQL queries for each window block
- Supports multiple windows with different configurations

### Volcano Query Planner Integration
- Unified query plans for both window processing and cross-window joins
- Leverages existing Volcano optimizer join algorithms (hash join, nested loop, etc.)
- Cost-based optimization for complex multi-window scenarios
- Automatic operator selection and join reordering
- Parallel execution capabilities for complex window queries
- Configurable execution modes: Standard vs Volcano

### Multi-Window Support
- Each window can have different parameters (width, slide, etc.)
- Windows can monitor different streams
- Each window executes its own specific query
- Results are merged through the R2S operator

### Stream Routing
- Data can be routed to specific windows based on stream IRI
- `add_to_stream(stream_iri, data, timestamp)` method for targeted routing
- Legacy `add(data, timestamp)` method still available for backward compatibility

## Examples

### Basic RSP-QL Example
**File**: `basic_rsp_ql_example.rs`

Demonstrates:
- Single RSP-QL query parsing with Volcano optimizer
- Window configuration extraction
- Cost-based query execution on window data
- Basic data streaming

### Multi-Window Example  
**File**: `multi_window_example.rs`

Demonstrates:
- Multiple windows with different specifications
- Stream-specific data routing with Volcano optimization
- Complex RSP-QL query with RETRIEVE and REGISTER clauses
- Window information inspection

### Volcano Comparison Example
**File**: `volcano_comparison_example.rs`

Demonstrates:
- Performance comparison between Standard and Volcano execution modes
- Benchmarking window query execution
- Analysis of optimization benefits
- Recommendations for execution mode selection

### Cross-Window Join Example
**File**: `cross_window_join_example.rs`

Demonstrates:
- Cross-window joins on shared variables (?sensor appears in multiple windows)
- Temporal correlation of data from different streams
- Join semantics when variables appear across window blocks
- Combined results from multiple heterogeneous data streams

### Volcano Query Planner Example
**File**: `volcano_query_planner_example.rs`

Demonstrates:
- RSP-QL query plan generation using Volcano optimizer infrastructure
- Unified physical plans for window processing + cross-window joins
- Cost-based optimization techniques applied to streaming queries
- Query plan inspection and analysis capabilities
- Integration of proven join algorithms with RSP-QL semantics

## RSP-QL Query Structure

The improved engine supports full RSP-QL syntax:

```rspql
RETRIEVE SOME ACTIVE STREAM ?s FROM <http://my.org/catalog>
WITH {
    ?s a :Stream .
    ?s :hasDescriptor ?descriptor .
}
REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :wind ON ?s [RANGE 600 STEP 60]
FROM NAMED WINDOW :wind2 ON :stream2 [RANGE 300 STEP 30]
WHERE {
    WINDOW :wind {
        ?obs a ssn:Observation .
        ?obs ssn:hasSimpleResult ?value .
    }
    WINDOW :wind2 {
        ?obs2 a ssn:Observation .
        ?obs2 ssn:observedProperty ?prop2 .
    }
}
```

### Extracted Information

From the above query, the engine extracts:

1. **Window :wind**
   - Stream: `?s`
   - Width: 600 seconds
   - Slide: 60 seconds  
   - Query: `SELECT * WHERE { ?obs a ssn:Observation . ?obs ssn:hasSimpleResult ?value . }`

2. **Window :wind2**
   - Stream: `:stream2`
   - Width: 300 seconds
   - Slide: 30 seconds
   - Query: `SELECT * WHERE { ?obs2 a ssn:Observation . ?obs2 ssn:observedProperty ?prop2 . }`

## Running the Examples

```bash
# Basic single-window example with Volcano optimizer
cargo run --example basic_rsp_ql_example

# Multi-window example with Volcano optimization
cargo run --example multi_window_example

# Performance comparison between Standard and Volcano execution modes
cargo run --example volcano_comparison_example

# Cross-window joins on shared variables
cargo run --example cross_window_join_example

# Volcano query planner integration with unified query plans
cargo run --example volcano_query_planner_example
```

## Window Configuration Inspection

The improved engine provides introspection capabilities:

```rust
// Get information about configured windows
for window_info in engine.get_window_info() {
    println!("Window: {} -> Stream: {}", 
             window_info.window_iri, 
             window_info.stream_iri);
    println!("Width: {}, Slide: {}", 
             window_info.width, 
             window_info.slide);
    println!("Query: {}", window_info.query);
}
```

## Query Execution Modes

The RSP engine supports two execution modes for window queries:

### Standard Execution Mode
```rust
let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Standard));
let engine = RSPBuilder::new()
    .add_rsp_ql_query(query)
    .add_r2r(r2r)
    .set_query_execution_mode(QueryExecutionMode::Standard)
    .build()?;
```

**Characteristics:**
- Direct query execution without optimization
- Lower overhead for simple queries
- Fixed join order and execution plan
- Good for low-latency scenarios

### Volcano Execution Mode
```rust
let r2r = Box::new(SimpleR2R::with_execution_mode(QueryExecutionMode::Volcano));
let engine = RSPBuilder::new()
    .add_rsp_ql_query(query)
    .add_r2r(r2r)
    .set_query_execution_mode(QueryExecutionMode::Volcano)
    .build()?;
```

**Characteristics:**
- Cost-based query optimization
- Dynamic join reordering
- Parallel execution capabilities
- Better for complex queries with multiple joins

## Data Routing

### Stream-Specific Routing
```rust
// Route data to specific stream/window
engine.add_to_stream("<http://temp/stream>", triple, timestamp);
engine.add_to_stream("?sensor_stream", triple, timestamp);
```

### Broadcast Routing (Legacy)
```rust
// Add to all windows (backward compatibility)
engine.add(triple, timestamp);
```

## Benefits

1. **Declarative Configuration**: Window parameters extracted from RSP-QL syntax
2. **Multi-Window Support**: Handle complex queries with multiple windows
3. **Stream Isolation**: Different windows can monitor different streams
4. **Query Specificity**: Each window executes its own tailored query
5. **Standards Compliance**: Follows RSP-QL specification more closely
6. **Maintainability**: Configuration centralized in the query string
7. **Query Optimization**: Volcano optimizer provides cost-based optimization
8. **Performance Flexibility**: Choose between Standard and Volcano execution modes
9. **Cross-Window Joins**: Automatic joins on shared variables across multiple windows
10. **Semantic Correctness**: Proper RSP-QL semantics for multi-window queries

## Notes

- The examples use `SimpleR2R` with configurable execution modes
- Volcano optimizer provides better performance for complex queries
- Standard mode offers lower latency for simple queries
- Window timing uses simplified duration parsing (seconds)
- Error handling demonstrates graceful degradation
- Performance characteristics vary by query complexity

## Cross-Window Join Semantics

### When Joins Occur
Cross-window joins happen automatically when:
- Multiple windows are defined in the same RSP-QL query
- Variables with the same name appear in different window blocks
- Results from windows arrive within the same temporal window

### Join Process
```rspql
SELECT ?sensor ?tempValue ?humidValue
FROM NAMED WINDOW :temp ON <stream1> [RANGE 10 STEP 5]
FROM NAMED WINDOW :humid ON <stream2> [RANGE 8 STEP 3]  
WHERE {
    WINDOW :temp {
        ?sensor :hasTemperature ?tempValue .
    }
    WINDOW :humid {
        ?sensor :hasHumidity ?humidValue .    # Same ?sensor variable = JOIN
    }
}
```

**Process:**
1. Execute queries independently on each window
2. Collect results from all windows for each timestamp
3. Join results where shared variable values match (?sensor)
4. Combine variable bindings from all participating windows
5. Apply R2S operator to joined results

### Join Examples
- **Temperature + Humidity**: Join on shared ?sensor variable
- **Stock Price + Volume**: Join on shared ?symbol variable  
- **Vehicle Speed + Fuel**: Join on shared ?vehicleId variable

## Volcano Query Planner Architecture

### Query Plan Generation Process
1. **Parse RSP-QL**: Extract windows, shared variables, and query structure
2. **Create Window Plans**: Generate logical/physical plans for individual windows
3. **Build Join Plans**: Create cross-window join plans using Volcano operators
4. **Optimize Plans**: Apply cost-based optimization (join reordering, operator selection)
5. **Execute Plans**: Run optimized physical plans with proven join algorithms

### Query Plan Structure
```rust
pub struct RSPQueryPlan {
    pub window_plans: Vec<PhysicalOperator>,           // Individual window plans
    pub cross_window_join_plan: Option<PhysicalOperator>, // Unified join plan
    pub shared_variables: Vec<String>,                  // Variables triggering joins
    pub output_variables: Vec<String>,                  // Final output variables
}
```

### Volcano Integration Benefits
- **Reuse Proven Algorithms**: Hash joins, nested loop joins, parallel joins
- **Cost-Based Decisions**: Automatic selection of optimal join strategies
- **Unified Execution**: Single execution engine for all operations
- **Performance Optimization**: Advanced optimization techniques applied to RSP-QL
- **Extensibility**: Easy to add new join algorithms and optimization rules

## Related Files

- `kolibrie/src/rsp.rs` - RSP engine with Volcano query planner integration
- `kolibrie/src/volcano_optimizer/` - Volcano optimizer infrastructure
- `kolibrie/examples/sparql_syntax/rsp_ql_syntax/retrieve_multiple_window.rs` - Original RSP-QL parsing example
- `kolibrie/src/parser.rs` - RSP-QL query parser
- `kolibrie/src/execute_query.rs` - Query execution with Volcano support
- `shared/src/query.rs` - Query structure definitions