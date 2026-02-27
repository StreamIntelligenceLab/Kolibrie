# Kolibrie

<p align="center">
    <img src="docs/logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![GitHub Workflow Status](https://img.shields.io/github/commit-activity/t/StreamIntelligenceLab/Kolibri) -->
[![Status](https://img.shields.io/badge/status-stable-blue.svg)](https://github.com/StreamIntelligenceLab/Kolibrie/tree/main)
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Rust Version](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
[![Chat Server](https://img.shields.io/badge/chat-discord-7289da.svg)](https://discord.gg/KcFXrUUyYm)
<!--![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)-->

[ [English](README.md) | [Nederlands](docs/README.nl.md) | [Deutsch](docs/README.de.md) | [Español](docs/README.es.md) | [Français](docs/README.fr.md) | [日本語](docs/README.ja.md) ]

**Kolibrie** is a high-performance, concurrent, and feature-rich SPARQL query engine implemented in Rust. Designed for scalability and efficiency, it leverages Rust's robust concurrency model and advanced optimizations, including SIMD (Single Instruction, Multiple Data) and parallel processing with Rayon, to handle large-scale RDF (Resource Description Framework) datasets seamlessly.

With a comprehensive API, **Kolibrie** facilitates parsing, storing, and querying RDF data using SPARQL, Turtle, and N3 formats. Its advanced filtering, aggregation, join operations, and sophisticated optimization strategies make it a suitable choice for applications requiring complex semantic data processing. Additionally, the integration of the Volcano Optimizer and Knowledge Graph capabilities empowers users to perform cost-effective query planning and leverage rule-based inference for enhanced data insights.

## Research Context

**Kolibrie** is developed within the [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) at KU Leuven, under the supervision of Prof. Pieter Bonte. The Stream Intelligence Lab focuses on **Stream Reasoning**, an emerging research field that integrates logic-based techniques from artificial intelligence with data-driven machine learning approaches to derive timely and actionable insights from continuous data streams. Our research emphasizes applications in the Internet of Things (IoT) and Edge processing, enabling real-time decision-making in dynamic environments such as autonomous vehicles, robotics, and web analytics.

For more information about our research and ongoing projects, please visit the [Stream Intelligence Lab website](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Features

- **Efficient RDF Parsing**: Supports parsing RDF/XML, Turtle, and N3 formats with robust error handling and prefix management.
- **Concurrent Processing**: Utilizes Rayon and Crossbeam for parallel data processing, ensuring optimal performance on multi-core systems.
- **SIMD Optimizations**: Implements SIMD instructions for accelerated query filtering and aggregation.
- **Flexible Querying**: Supports complex SPARQL queries, including SELECT, INSERT, FILTER, GROUP BY, and VALUES clauses.
- **Volcano Optimizer**: Incorporates a cost-based query optimizer based on the Volcano model to determine the most efficient execution plans.
- **Reasoner**: Provides robust support for building and querying knowledge graphs, including ABox (instance-level) and TBox (schema-level) assertions, dynamic rule-based inference, and backward chaining.
- **Streaming and Sliding Windows**: Handles timestamped triples and sliding window operations for time-based data analysis.
- **Machine Learning Integration**: Seamlessly integrates with Python ML frameworks through PyO3 bindings.
- **Extensible Dictionary Encoding**: Efficiently encodes and decodes RDF terms using a customizable dictionary.
- **Comprehensive API**: Offers a rich set of methods for data manipulation, querying, and result processing.
- **Support Python**

> [!WARNING]
> utilizing CUDA is experimental and under the development

## Installation

### Native Installation

Ensure you have [Rust](https://www.rust-lang.org/tools/install) installed (version 1.60 or higher).

Clone the repository:

```bash
git clone https://github.com/StreamIntelligenceLab/Kolibrie.git
cd Kolibrie
```

Build the project:

```bash
cargo build --release
```

Then, include it in your project:

```rust
use kolibrie::SparqlDatabase;
```

### WebUI

To run webui:
```bash
cargo run --bin kolibrie-http-server
```

After that in the browser type `localhost:8080` or `0.0.0.0:8080`

### Docker Installation

**Kolibrie** provides Docker support with multiple configurations optimized for different use cases. The Docker setup automatically handles all dependencies including Rust, CUDA (for GPU builds), and Python ML frameworks which are fully integrated into Kolibrie.

#### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed
- [Docker Compose](https://docs.docker.com/compose/install/) installed
- For GPU support: [NVIDIA Docker runtime](https://github.com/NVIDIA/nvidia-docker) installed

#### Quick Start with Docker Compose

Kolibrie offers three deployment profiles:

**1. CPU Build (Default - Recommended for Most Users)**

Runs with web UI on port 8080:
```bash
docker compose up --build
# or explicitly:
docker compose --profile cpu up --build
```

Access the web UI at `http://localhost:8080`

**2. GPU Build (Requires NVIDIA GPU)**

GPU-accelerated build with CUDA support:
```bash
docker compose --profile gpu up --build
```

Access the web UI at `http://localhost:8080`

**3. Development Build**

Interactive shell for development (auto-detects GPU):
```bash
docker compose --profile dev up --build
```

This drops you into a bash shell with full access to Kolibrie tools.

#### Running Without Docker Compose

If you prefer using Docker directly:

**CPU Build with Web UI:**

Build:
```bash
docker build \
  --build-arg GPU_VENDOR=none \
  --build-arg ENABLE_WEB_UI=true \
  -t kolibrie:cpu \
  .
```

Run:
```bash
docker run -d \
  --name kolibrie \
  -p 8080:8080 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/models:/app/ml/examples/models \
  kolibrie:cpu
```

**GPU Build with Web UI:**

Build:
```bash
docker build \
  --build-arg GPU_VENDOR=nvidia \
  --build-arg CUDA_VERSION=11.8 \
  --build-arg BASE_IMAGE=nvidia/cuda:11.8-devel-ubuntu22.04 \
  --build-arg ENABLE_WEB_UI=true \
  -t kolibrie:gpu \
  .
```

Run:
```bash
docker run -d \
  --name kolibrie-gpu \
  --gpus all \
  -p 8080:8080 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/models:/app/ml/examples/models \
  kolibrie:gpu
```

**Development Build (Shell Access):**

Build:
```bash
docker build \
  --build-arg GPU_VENDOR=none \
  --build-arg ENABLE_WEB_UI=false \
  -t kolibrie:dev \
  .
```

Run:
```bash
docker run -it \
  --name kolibrie-dev \
  -v $(pwd):/app \
  kolibrie:dev \
  bash
```

For GPU-enabled development shell:
```bash
docker run -it \
  --name kolibrie-gpu-dev \
  --gpus all \
  -v $(pwd):/app \
  kolibrie:gpu \
  bash
```

## Usage

### Initializing the Database

Create a new instance of the `SparqlDatabase`:

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // Your code here
}
```

### Parsing RDF Data

**Kolibrie** supports parsing RDF data from files or strings in various formats. 

#### Parsing RDF/XML from a File

```rust
db.parse_rdf_from_file("data.rdf");
```

#### Parsing RDF/XML from a String

```rust
let rdf_data = r#"
<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF 
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:foaf="http://xmlns.com/foaf/0. 1/">
    
    <rdf:Description rdf:about="http://example.org/alice">
        <foaf:name>Alice</foaf:name>
        <foaf:age>30</foaf:age>
    </rdf:Description>
</rdf:RDF>
"#;

db.parse_rdf(rdf_data);
```

#### Parsing Turtle Data from a String

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob . 
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_turtle(turtle_data);
```

#### Parsing N3 Data from a String

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_n3(n3_data);
```

#### Parsing N-Triples from a String

```rust
let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> . 
<http://example.org/jane> <http://example.org/name> "Jane Doe" . 
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

db.parse_ntriples_and_add(ntriples_data);
```

### Adding Triples Programmatically

Add individual triples directly to the database:

```rust
db.add_triple_parts(
    "http://example.org/alice",
    "http://xmlns.com/foaf/0.1/name",
    "Alice"
);

db.add_triple_parts(
    "http://example.org/alice",
    "http://xmlns.com/foaf/0.1/age",
    "30"
);
```

### Executing SPARQL Queries

Execute SPARQL queries to retrieve and manipulate data. 

#### Basic SELECT Query

```rust
use kolibrie::execute_query::execute_query;

let sparql_query = r#"
PREFIX ex: <http://example.org/>
SELECT ?s ?o
WHERE {
    ?s ex:knows ?o .
}
"#;

let results = execute_query(sparql_query, &mut db);

for row in results {
    println!("Subject: {}, Object: {}", row[0], row[1]);
}
```

#### Query with FILTER

```rust
let sparql = r#"
PREFIX ex: <http://example.org/vocab#>

SELECT ?name ?attendees
WHERE {
    ?event ex:name ?name .
    ?event ex:attendees ?attendees . 
    FILTER (?attendees > 50)
}
"#;

let results = execute_query(sparql, &mut db);

for row in results {
    println! ("Event: {}, Attendees: {}", row[0], row[1]);
}
```

#### Query with OR Operator

```rust
let sparql = r#"
PREFIX ex: <http://example.org/vocab#>

SELECT ?name ?type ?attendees
WHERE {
    ?event ex:name ?name . 
    ?event ex:type ?type .
    ?event ex:attendees ?attendees . 
    FILTER (?type = "Technical" || ?type = "Academic")
}
"#;

let results = execute_query(sparql, &mut db);

for row in results {
    if let [name, type_, attendees] = &row[.. ] {
        println!("Name: {}, Type: {}, Attendees: {}", name, type_, attendees);
    }
}
```

#### Query with LIMIT

```rust
let sparql = r#"
PREFIX ex: <http://example.org/vocab#>

SELECT ?name ?type
WHERE {
    ?event ex:name ?name .
    ?event ex:type ?type .
    FILTER (?type = "Technical" || ?type = "Academic")
}
LIMIT 2
"#;

let results = execute_query(sparql, &mut db);

for row in results {
    println!("Name: {}, Type: {}", row[0], row[1]);
}
```

#### Query with Aggregations

```rust
let sparql = r#"
PREFIX ds: <https://data.cityofchicago.org/resource/xzkq-xp2w/>

SELECT AVG(?salary) AS ?average_salary
WHERE {
    ?employee ds:annual_salary ?salary
}
GROUPBY ?average_salary
"#;

let results = execute_query(sparql, &mut db);

for row in results {
    if let [avg_salary] = &row[.. ] {
        println!("Average Salary: {}", avg_salary);
    }
}
```

**Supported Aggregations:**
- `AVG(? var)` - Calculate average
- `COUNT(?var)` - Count occurrences
- `SUM(?var)` - Sum values
- `MIN(?var)` - Find minimum
- `MAX(? var)` - Find maximum

#### Query with String Functions

```rust
let sparql = r#"
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name
WHERE {
    ?person foaf:givenName ?first .
    ?person foaf:surname ?last
    BIND(CONCAT(?first, " ", ?last) AS ?name)
}
"#;

let results = execute_query(sparql, &mut db);

for row in results {
    println!("Full Name: {}", row[0]);
}
```

#### Nested Queries

```rust
let sparql = r#"
PREFIX ex: <http://example.org/>

SELECT ?friendName
WHERE {
    ?person ex:name "Alice" .
    ?person ex:knows ?friend
    {
        SELECT ?friend ?friendName
        WHERE {
            ?friend ex:name ?friendName . 
        }
    }
}"#;

let results = execute_query(sparql, &mut db);

for row in results {
    println!("Alice's Friend: {}", row[0]);
}
```

### Using the Query Builder API

The Query Builder provides a fluent interface for programmatic query construction. 

#### Basic Query Building

```rust
// Get all objects for a specific predicate
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/name")
    .get_objects();

for object in results {
    println!("Name: {}", object);
}
```

#### Query with Filtering

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/age")
    .filter(|triple| {
        // Custom filter logic
        db.dictionary.decode(triple.object)
            .and_then(|age| age.parse::<i32>().ok())
            .map(|age| age > 25)
            .unwrap_or(false)
    })
    .get_decoded_triples();

for (subject, predicate, object) in results {
    println!("{} is {} years old", subject, object);
}
```

#### Query with Joins

```rust
let other_db = SparqlDatabase::new();
// ...  populate other_db ...

let results = db.query()
    .join(&other_db)
    .join_on_subject()
    .get_triples();
```

#### Query with Sorting, Limiting, and Distinct

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/name")
    .order_by(|triple| {
        db.dictionary.decode(triple.object).unwrap().to_string()
    })
    .distinct()
    .limit(10)
    .offset(5)
    .get_decoded_triples();

for (subject, predicate, object) in results {
    println!("{} - {} - {}", subject, predicate, object);
}
```

### Using the Volcano Optimizer

The **Volcano Optimizer** is integrated within **Kolibrie** to optimize query execution plans based on cost estimation. It transforms logical query plans into efficient physical plans using various join strategies and applies cost-based decisions to select the most performant execution path.

#### Example: Optimized Query Execution

```rust
use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn main() {
    let mut db = SparqlDatabase::new();

    // Parse N-Triples data
    let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> .
<http://example.org/jane> <http://example.org/name> "Jane Doe" .
<http://example.org/john> <http://example.org/name> "John Smith" . 
<http://example.org/jane> <http://example.org/age> "25"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/john> <http://example.org/age> "30"^^<http://www. w3.org/2001/XMLSchema#integer> .
    "#;

    db.parse_ntriples_and_add(ntriples_data);
    
    // Build statistics for the optimizer
    db.get_or_build_stats();

    // Define the SPARQL query
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?friend ?friendName
    WHERE {
        ?person ex:hasFriend ?friend .
        ?friend ex:name ?friendName .
    }
    "#;

    // Execute the query with optimized plan
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Friend: {}, Friend's Name: {}", row[0], row[1], row[2]);
    }
}
```

### Working with the Reasoner

The **Reasoner** component allows you to build and manage semantic networks with instance-level (ABox) information. It supports dynamic rule-based inference using forward chaining, backward chaining, and semi-naive evaluation to derive new knowledge from existing data.

#### Example: Building and Querying a Reasoner

```rust
use datalog::knowledge_graph::Reasoner;
use shared::terms::Term;
use shared::rule::Rule;

fn main() {
    let mut kg = Reasoner::new();

    // Add ABox triples (instance-level data)
    kg.add_abox_triple("Alice", "parentOf", "Bob");
    kg.add_abox_triple("Bob", "parentOf", "Charlie");

    // Define a transitivity rule for ancestorOf relationship
    // Rule: parentOf(X, Y) ∧ parentOf(Y, Z) → ancestorOf(X, Z)
    let ancestor_rule = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("parentOf")),
                Term::Variable("Y".to_string()),
            ),
            (
                Term::Variable("Y". to_string()),
                Term::Constant(kg.dictionary.encode("parentOf")),
                Term::Variable("Z".to_string()),
            ),
        ],
        conclusion: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("ancestorOf")),
                Term::Variable("Z".to_string()),
            )
        ],
        filters: vec![],
    };

    kg.add_rule(ancestor_rule);

    // Infer new facts using forward chaining
    let inferred_facts = kg.infer_new_facts();
    
    println!("Inferred {} new facts", inferred_facts.len());

    // Query the Knowledge Graph for ancestorOf relationships
    let results = kg.query_abox(
        Some("Alice"),
        Some("ancestorOf"),
        None,
    );

    for triple in results {
        println!(
            "{} is ancestor of {}",
            kg.dictionary.decode(triple.subject).unwrap(),
            kg. dictionary.decode(triple.object). unwrap()
        );
    }
}
```

**Output:**
```
Inferred 1 new facts
Alice is ancestor of Charlie
```

### Machine Learning Integration

**All machine learning examples can be found [here](https://github.com/StreamIntelligenceLab/Kolibrie/tree/main/kolibrie/examples/sparql_syntax/combination)**.

## API Documentation

### `SparqlDatabase` Struct

The `SparqlDatabase` struct is the core component representing the RDF store and providing methods for data manipulation and querying.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
    pub udfs: HashMap<String, ClonableFn>,
    pub index_manager: UnifiedIndex,
    pub rule_map: HashMap<String, String>,
    pub cached_stats: Option<Arc<DatabaseStats>>,
}
```

#### Fields

- **triples**: Stores RDF triples in a sorted set for efficient querying. 
- **streams**: Holds timestamped triples for streaming and temporal queries.
- **sliding_window**: Optional sliding window for time-based data analysis.
- **dictionary**: Encodes and decodes RDF terms for storage efficiency.
- **prefixes**: Manages namespace prefixes for resolving prefixed terms.
- **udfs**: User-defined functions registry for custom operations.
- **index_manager**: Unified indexing system for optimized query performance.
- **rule_map**: Maps rule names to their definitions.
- **cached_stats**: Cached database statistics for query optimization.

### `Streamertail` Struct

The `Streamertail` implements a cost-based query optimizer based on the Volcano model.  It transforms logical query plans into efficient physical plans by evaluating different physical operators and selecting the one with the lowest estimated cost.

```rust
pub struct Streamertail<'a> {
    pub stats: Arc<DatabaseStats>,
    pub memo: HashMap<String, (PhysicalOperator, f64)>,
    pub selected_variables: Vec<String>,
    database: &'a SparqlDatabase,
}
```

#### Fields

- **stats**: Shared statistical information about the database to aid in cost estimation.
- **memo**: Caches optimized physical operators with their costs to avoid redundant computations.
- **selected_variables**: Keeps track of variables selected in the query. 
- **database**: Reference to the SPARQL database for query execution.

### `Reasoner` Struct

The `Reasoner` struct manages instance-level (ABox) assertions, supports dynamic rule-based inference, and provides querying capabilities with forward chaining, backward chaining, and semi-naive evaluation.

```rust
pub struct Reasoner {
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>,
    pub index_manager: UnifiedIndex,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
}
```

#### Fields

- **dictionary**: Encodes and decodes RDF terms for storage efficiency. 
- **rules**: Contains dynamic rules for inferencing new knowledge.
- **index_manager**: Unified indexing system for storing and querying triples.
- **rule_index**: Specialized index for efficient rule matching.
- **constraints**: Integrity constraints for inconsistency detection and repair.

### Core Methods

#### `SparqlDatabase::new() -> Self`

Creates a new, empty `SparqlDatabase`.

```rust
let mut db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Parses RDF/XML data from a specified file and populates the database.

```rust
db.parse_rdf_from_file("data. rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Parses RDF/XML data from a string. 

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">... </rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Parses Turtle-formatted RDF data from a string.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> . 

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Parses N3-formatted RDF data from a string.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> . 

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `parse_ntriples_and_add(&mut self, ntriples_data: &str)`

Parses N-Triples data and adds it to the database.

```rust
let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> . 
<http://example.org/jane> <http://example.org/name> "Jane Doe" .
"#;
db.parse_ntriples_and_add(ntriples_data);
```

#### `add_triple_parts(&mut self, subject: &str, predicate: &str, object: &str)`

Adds a triple to the database by encoding its parts.

```rust
db.add_triple_parts(
    "http://example.org/alice",
    "http://xmlns.com/foaf/0. 1/name",
    "Alice"
);
```

#### `delete_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) -> bool`

Deletes a triple from the database and returns whether it was successfully removed.

```rust
let deleted = db.delete_triple_parts(
    "http://example.org/alice",
    "http://xmlns.com/foaf/0.1/age",
    "30"
);
```

#### `build_all_indexes(&mut self)`

Builds all indexes from the current triples for optimized query performance.

```rust
db.build_all_indexes();
```

#### `get_or_build_stats(&mut self) -> Arc<DatabaseStats>`

Gets cached statistics or builds new statistics for query optimization.

```rust
let stats = db.get_or_build_stats();
```

#### `invalidate_stats_cache(&mut self)`

Invalidates the statistics cache after data modifications.

```rust
db.invalidate_stats_cache();
```

#### `query(&self) -> QueryBuilder`

Returns a QueryBuilder instance for programmatic query construction.

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0. 1/name")
    .get_objects();
```

#### `register_udf<F>(&mut self, name: &str, f: F)`

Registers a user-defined function for use in queries. 

```rust
db.register_udf("toUpperCase", |args: Vec<&str>| {
    args[0].to_uppercase()
});
```

#### `generate_rdf_xml(&mut self) -> String`

Generates RDF/XML representation of the database. 

```rust
let rdf_xml = db.generate_rdf_xml();
```

#### `decode_triple(&self, triple: &Triple) -> Option<(&str, &str, &str)>`

Decodes a triple to its string representation.

```rust
if let Some((s, p, o)) = db. decode_triple(&triple) {
    println!("{} - {} - {}", s, p, o);
}
```

### `Streamertail` Methods

#### `new(database: &SparqlDatabase) -> Self`

Creates a new instance of the `Streamertail` with statistical data gathered from the provided database.

```rust
let optimizer = Streamertail::new(&db);
```

#### `with_cached_stats(stats: Arc<DatabaseStats>) -> Self`

Creates a new optimizer with pre-computed statistics for better performance.

```rust
let stats = db.get_or_build_stats();
let optimizer = Streamertail::with_cached_stats(stats);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Determines the most efficient physical execution plan for a given logical query plan.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

#### `execute_plan(&mut self, plan: &PhysicalOperator, database: &mut SparqlDatabase) -> Vec<BTreeMap<String, String>>`

Executes an optimized physical plan and returns the query results.

```rust
let results = optimizer.execute_plan(&physical_plan, &mut db);
```

### `Reasoner` Methods

#### `new() -> Self`

Creates a new, empty `Reasoner`. 

```rust
let mut kg = Reasoner::new();
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Adds an ABox triple (instance-level information) to the knowledge graph.

```rust
kg.add_abox_triple("Alice", "knows", "Bob");
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Queries the ABox for instance-level assertions based on optional subject, predicate, and object filters.

```rust
let results = kg.query_abox(Some("Alice"), Some("knows"), None);
```

#### `add_rule(&mut self, rule: Rule)`

Adds a dynamic rule to the knowledge graph for inferencing.

```rust
let rule = Rule {
    premise: vec![... ],
    conclusion: vec![... ],
    filters: vec![],
};
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Performs naive forward chaining to derive new triples. 

```rust
let inferred = kg.infer_new_facts();
println!("Inferred {} new facts", inferred.len());
```

#### `infer_new_facts_semi_naive(&mut self) -> Vec<Triple>`

Performs semi-naive evaluation for more efficient forward chaining.

```rust
let inferred = kg.infer_new_facts_semi_naive();
```

#### `infer_new_facts_semi_naive_parallel(&mut self) -> Vec<Triple>`

Performs parallel semi-naive evaluation for large-scale inference.

```rust
let inferred = kg.infer_new_facts_semi_naive_parallel();
```

#### `backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>>`

Performs backward chaining to answer queries by deriving solutions from rules.

```rust
let query_pattern = (
    Term::Variable("X".to_string()),
    Term::Constant(kg.dictionary.encode("knows")),
    Term::Variable("Y".to_string())
);

let results = kg.backward_chaining(&query_pattern);
```

#### `add_constraint(&mut self, constraint: Rule)`

Adds an integrity constraint to the knowledge graph. 

```rust
kg.add_constraint(constraint);
```

#### `infer_new_facts_semi_naive_with_repairs(&mut self) -> Vec<Triple>`

Performs inference while handling inconsistencies through automatic repair.

```rust
let inferred = kg.infer_new_facts_semi_naive_with_repairs();
```

#### `query_with_repairs(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>>`

Queries the knowledge graph using inconsistency-tolerant semantics (IAR).

```rust
let results = kg.query_with_repairs(&query_pattern);
```

## Performance

**Kolibrie** is optimized for high performance through:

- **Parallel Parsing and Processing**: Utilizes Rayon and Crossbeam for multi-threaded data parsing and query execution.
- **SIMD Instructions**: Implements SIMD operations to accelerate filtering and aggregation tasks.
- **Volcano Optimizer**: Employs a cost-based query optimizer to generate efficient physical execution plans, minimizing query execution time.
- **Knowledge Graph Inference**: Leverages rule-based inference and backward chaining to derive new knowledge without significant performance overhead. 
- **Efficient Data Structures**: Employs `BTreeSet` for sorted storage and `HashMap` for prefix management, ensuring quick data retrieval and manipulation. 
- **Memory Optimization**: Uses dictionary encoding to minimize memory footprint by reusing repeated terms.

### Benchmarking Results

Our benchmarks demonstrate Kolibrie's superior performance compared to other popular RDF engines.  The following tests were conducted using:
- **Dataset**: [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 10M triples benchmark
- **Oxigraph Configuration**: RocksDB backend for optimal performance
- **Deep Taxonomy Reasoning**: Hierarchy depth testing up to 10K levels

#### WatDiv 10M - Query Performance Comparison (20 runs each)

![WatDiv 10M Query Performance](docs/img/image1.png)

*Figure 1: Query execution times across different SPARQL engines using the WatDiv 10M dataset*

**Key Findings:**
- Kolibrie consistently outperforms competitors across all query types (L1-L5, S1-S7, F1-F3, C1-C3)
- Average query execution time: **sub-millisecond to low millisecond range**
- Blazegraph and QLever show competitive performance on specific query patterns
- Oxigraph (with RocksDB) demonstrates stable performance across all queries

The running example can be found [here](https://github.com/StreamIntelligenceLab/Kolibrie/blob/main/kolibrie/examples/sparql_syntax/n_triples_data/n_triple_10M.rs)

#### Deep Taxonomy - Reasoning over Hierarchy Depth

![Deep Taxonomy Reasoning Performance](docs/img/image2.png)

*Figure 2: Reasoning performance across different hierarchy depths (10, 100, 1K, 10K levels)*

**Key Findings:**
- Kolibrie shows **logarithmic scaling** with hierarchy depth
- At 10K hierarchy levels, Kolibrie maintains sub-second response times
- Superior performance compared to Apache Jena and EYE reasoner
- Efficient handling of complex taxonomic structures

The running example can be found [here](https://github.com/StreamIntelligenceLab/Kolibrie/blob/main/kolibrie/examples/sparql_syntax/knowledge_graph/deep_taxonomy.rs)

## How to Contribute

### Submitting Issues
Use the Issue Tracker to submit bug reports and feature/enhancement requests. Before submitting a new issue, ensure that there is no similar open issue.

### Manual Testing
Anyone manually testing the code and reporting bugs or suggestions for enhancements in the Issue Tracker are very welcome!

### Submitting Pull Requests
Patches/fixes are accepted in form of pull requests (PRs). Make sure the issue the pull request addresses is open in the Issue Tracker.

Submitted pull request is deemed to have agreed to publish under Mozilla Public License Version 2.0.

## Community

Join our [Discord community](https://discord.gg/KcFXrUUyYm) to discuss Kolibrie, ask questions, and share your experiences.

## License

Kolibrie is licensed under the [MPL-2.0 License](LICENSE).
