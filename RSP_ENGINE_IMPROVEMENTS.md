# RSP Engine Improvements Summary

## Overview

This document summarizes the major improvements made to the RSP (RDF Stream Processing) engine in Kolibrie to support RSP-QL query parsing, multi-window processing, Volcano optimizer integration, and cross-window joins on shared variables.

## Key Improvements

### 1. RSP-QL Query Parsing Integration

**Before**: The RSP engine required manual configuration of window parameters and fixed SPARQL queries:

```rust
let mut engine = RSPBuilder::new(window_size, slide)
    .add_tick(Tick::TimeDriven)
    .add_report_strategy(ReportStrategy::OnWindowClose)
    .add_query("SELECT ?s WHERE{ ?s a <http://www.w3.org/test/SuperType>}")
    .add_r2s(StreamOperator::RSTREAM)
    .build();
```

**After**: The RSP engine automatically parses RSP-QL queries to extract all configuration:

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
    .build()?;
```

### 2. Multi-Window Support with Cross-Window Joins

The engine now supports multiple windows with different configurations from a single RSP-QL query, including automatic joins on shared variables:

```rspql
REGISTER RSTREAM <http://out/stream> AS
SELECT *
FROM NAMED WINDOW :tempWindow ON <http://temp/stream> [RANGE 600 STEP 60]
FROM NAMED WINDOW :humidWindow ON <http://humid/stream> [RANGE 300 STEP 30]
WHERE {
    WINDOW :tempWindow {
        ?obs a ssn:Observation .
        ?obs ssn:observedProperty :Temperature .
    }
    WINDOW :humidWindow {
        ?obs2 a ssn:Observation .
        ?obs2 ssn:observedProperty :Humidity .
    }
}
```

Each window:
- Has its own parameters (width, slide, tick, report strategy)
- Monitors a specific stream
- Executes its own SPARQL query
- Can join with other windows on shared variables (e.g., ?obs appears in both windows)

### 3. Automatic Query Extraction and Cross-Window Joins

The engine extracts individual SPARQL queries from RSP-QL window blocks and identifies shared variables:

- **Window Block**: `WINDOW :tempWindow { ?obs a ssn:Observation . }`
- **Extracted Query**: `SELECT * WHERE { ?obs a ssn:Observation . }`
- **Shared Variables**: Variables appearing in multiple windows trigger automatic joins

### 4. Stream-Based Data Routing and Cross-Window Coordination

**New Method**: `add_to_stream(stream_iri, data, timestamp)`
- Routes data to specific windows based on stream IRI matching
- Enables targeted data flow control
- Coordinates timing for cross-window joins

**Legacy Support**: `add(data, timestamp)` still available for backward compatibility

### 5. Volcano Optimizer Integration

The engine now supports two execution modes for window queries:
- **Standard Mode**: Direct query execution without optimization
- **Volcano Mode**: Cost-based query optimization with parallel execution

### 6. Enhanced Configuration Inspection

```rust
// Inspect parsed window configurations
for window_info in engine.get_window_info() {
    println!("Window: {} -> Stream: {}", 
             window_info.window_iri, window_info.stream_iri);
    println!("Width: {}, Slide: {}", 
             window_info.width, window_info.slide);
    println!("Query: {}", window_info.query);
}
```

### 7. Cross-Window Join Processing

When multiple windows share variables, the engine automatically:
- Identifies shared variables across window blocks
- Collects results from all windows for each timestamp
- Performs joins on shared variable values
- Combines variable bindings from all participating windows
- Produces semantically correct RSP-QL results

**Example Join Process:**
```rspql
SELECT ?sensor ?temp ?humid
FROM NAMED WINDOW :temp ON <tempStream> [RANGE 10 STEP 5]
FROM NAMED WINDOW :humid ON <humidStream> [RANGE 8 STEP 3]
WHERE {
    WINDOW :temp { ?sensor :hasTemp ?temp . }
    WINDOW :humid { ?sensor :hasHumid ?humid . }  # Same ?sensor = JOIN
}
```
Results: Only sensors present in BOTH windows with matching values are returned.

## Architecture Changes

### New Data Structures

```rust
// Window configuration extracted from RSP-QL
pub struct RSPWindow {
    pub window_iri: String,
    pub stream_iri: String, 
    pub width: usize,
    pub slide: usize,
    pub tick: Tick,
    pub report_strategy: ReportStrategy,
    pub query: String,
}

// Cross-window join support
pub struct WindowResult {
    pub window_iri: String,
    pub results: Vec<BTreeMap<String, String>>,
    pub timestamp: usize,
}

// Complete RSP query configuration
pub struct RSPQueryConfig {
    pub windows: Vec<RSPWindow>,
    pub output_stream: String,
    pub stream_type: StreamOperator,
    pub shared_variables: Vec<String>, // Variables shared across windows
}
```

### Modified RSPBuilder

- **Removed**: Fixed window parameters (`width`, `slide`, `tick`, `report_strategy`)
- **Added**: `add_rsp_ql_query()` method for RSP-QL input
- **Added**: `set_query_execution_mode()` for Volcano optimizer selection
- **Enhanced**: Automatic query parsing, validation, and shared variable detection

### Updated RSPEngine

- **Multi-Window Processing**: Manages multiple `CSPARQLWindow` instances
- **Stream Routing**: Intelligent data distribution to appropriate windows  
- **Query Execution**: Per-window SPARQL query execution with Volcano optimization
- **Cross-Window Joins**: Automatic joining on shared variables with temporal coordination

## Integration Points

### Parser Integration
- Uses existing `kolibrie::parser::parse_combined_query()`
- Leverages `shared::query::*` structures for RSP-QL representation
- Supports full RSP-QL syntax including RETRIEVE and REGISTER clauses

### Window Specification Parsing
- Extracts window parameters from `[RANGE duration STEP duration]` syntax
- Converts ISO 8601 durations to numeric values
- Maps report strategies and tick types from RSP-QL keywords
- Identifies shared variables across window blocks for join processing

### Error Handling
- Graceful parsing error handling with descriptive messages
- Fallback to default values for optional parameters
- Result-based API for robust error propagation

## Usage Examples

### Basic Single Window
```rust
let rsp_ql_query = r#"
    REGISTER RSTREAM <http://out> AS
    SELECT ?sensor ?value
    FROM NAMED WINDOW :w1 ON <http://sensors> [RANGE 10 STEP 2]
    WHERE {
        WINDOW :w1 {
            ?sensor :hasValue ?value .
        }
    }
"#;

let engine = RSPBuilder::new()
    .add_rsp_ql_query(rsp_ql_query)
    .add_r2r(my_r2r_operator)
    .build()?;
```

### Multi-Window Complex Query
```rust
let rsp_ql_query = r#"
    RETRIEVE SOME ACTIVE STREAM ?s FROM <http://catalog>
    WITH { ?s a :Stream . }
    REGISTER RSTREAM <http://output> AS
    SELECT *
    FROM NAMED WINDOW :temp ON ?s [RANGE 600 STEP 60]
    FROM NAMED WINDOW :co2 ON :sensor2 [RANGE 300 STEP 30]
    WHERE {
        WINDOW :temp {
            ?obs a :Observation .
            ?obs :property :Temperature .
        }
        WINDOW :co2 {
            ?obs2 a :Observation .
            ?obs2 :property :CO2 .
        }
    }
"#;
```

## Benefits

1. **Standards Compliance**: Closer adherence to RSP-QL specification
2. **Declarative Configuration**: All parameters defined in the query string
3. **Multi-Window Processing**: Handle complex streaming scenarios with cross-window joins
4. **Stream Isolation**: Different windows monitor different streams
5. **Query Optimization**: Volcano optimizer for cost-based query execution
6. **Query Specificity**: Each window executes tailored SPARQL queries
7. **Semantic Correctness**: Proper cross-window joins on shared variables
8. **Maintainability**: Configuration centralized and version-controllable
9. **Introspection**: Easy inspection of parsed configurations and join logic

## Files Modified/Added

### Core Implementation
- `kolibrie/src/rsp.rs` - Complete rewrite of RSP engine
- `kolibrie/src/lib.rs` - Made RSP module public

### Examples
- `kolibrie/examples/rsp_ql_engine/basic_rsp_ql_example.rs`
- `kolibrie/examples/rsp_ql_engine/multi_window_example.rs`  
- `kolibrie/examples/rsp_ql_engine/volcano_comparison_example.rs`
- `kolibrie/examples/rsp_ql_engine/cross_window_join_example.rs`
- `kolibrie/examples/rsp_ql_engine/README.md`

### Configuration
- `kolibrie/Cargo.toml` - Added example entries

## Testing

The implementation includes comprehensive tests demonstrating:
- Single window RSP-QL processing with Volcano optimization
- Multi-window configurations with cross-window joins
- Stream-specific data routing and temporal coordination
- Volcano vs Standard execution mode comparison
- Cross-window join semantics on shared variables
- Error handling and graceful degradation

## Future Enhancements

1. **Performance Optimization**: Parallel cross-window join processing
2. **Enhanced R2R Integration**: Better integration with full R2R implementations
3. **Advanced Stream Operations**: Support for more complex stream algebra
4. **Query Optimization**: Advanced window query optimization and caching
5. **Join Optimization**: Optimized cross-window join algorithms
6. **Monitoring**: Runtime metrics and performance monitoring for joins
7. **Stream Discovery**: Dynamic stream registration and discovery
8. **Variable Binding**: Enhanced variable binding and type conversion

## Backward Compatibility

- Legacy `add(data, timestamp)` method maintained
- Existing R2R operator interfaces unchanged
- Previous test patterns still supported
- Gradual migration path available

This improvement represents a significant step forward in making Kolibrie's RSP engine more standards-compliant, flexible, and suitable for complex real-world streaming scenarios. The addition of cross-window joins on shared variables enables proper RSP-QL semantics for multi-stream analytics, while the Volcano optimizer integration provides performance benefits for complex window queries.