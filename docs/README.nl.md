# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![GitHub Workflow Status](https://img.shields.io/github/commit-activity/t/ladroid/goku) -->
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Rust Version](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** is een hoogwaardig, concurrerend en rijk aan functies SPARQL-query-engine geïmplementeerd in Rust. Ontworpen voor schaalbaarheid en efficiëntie, maakt het gebruik van het robuuste concurrentiemodel van Rust en geavanceerde optimalisaties, waaronder SIMD (Single Instruction, Multiple Data) en parallelle verwerking met Rayon, om moeiteloos om te gaan met grootschalige RDF (Resource Description Framework) datasets.

Met een uitgebreide API faciliteert **Kolibrie** het parsen, opslaan en opvragen van RDF-gegevens met behulp van SPARQL-, Turtle- en N3-formaten. De geavanceerde filtering, aggregatie, join-operaties en verfijnde optimalisatiestrategieën maken het een geschikte keuze voor toepassingen die complexe semantische dataverwerking vereisen. Bovendien stelt de integratie van de Volcano Optimizer en Knowledge Graph-gebruikers in staat kosteneffectieve queryplanning uit te voeren en regelgebaseerde inferentie te benutten voor verbeterde data-inzichten.

## Onderzoekscontext

**Kolibrie** is ontwikkeld binnen het [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) aan de KU Leuven, onder supervisie van Prof. Pieter Bonte. Het Stream Intelligence Lab richt zich op **Stream Reasoning**, een opkomend onderzoeksveld dat logica-gebaseerde technieken uit kunstmatige intelligentie integreert met data-gedreven machine learning benaderingen om tijdige en bruikbare inzichten te verkrijgen uit continue datastromen. Ons onderzoek benadrukt toepassingen in het Internet of Things (IoT) en Edge processing, wat real-time besluitvorming in dynamische omgevingen mogelijk maakt, zoals autonome voertuigen, robotica en webanalyse.

Voor meer informatie over ons onderzoek en lopende projecten, bezoek de [Stream Intelligence Lab website](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Functies

- **Efficiënt RDF Parsing**: Ondersteunt het parsen van RDF/XML, Turtle en N3-formaten met robuuste foutafhandeling en prefixbeheer.
- **Concurrerende Verwerking**: Maakt gebruik van Rayon en Crossbeam voor parallelle gegevensverwerking, wat optimale prestaties op multi-core systemen waarborgt.
- **SIMD Optimalisaties**: Implementeert SIMD-instructies voor versnelde query-filtering en aggregatie.
- **Flexibele Querying**: Ondersteunt complexe SPARQL-queries, inclusief SELECT, INSERT, FILTER, GROUP BY en VALUES clausules.
- **Volcano Optimizer**: Integreert een kostengebaseerde query optimizer gebaseerd op het Volcano-model om de meest efficiënte uitvoeringsplannen te bepalen.
- **Knowledge Graph**: Biedt robuuste ondersteuning voor het bouwen en opvragen van knowledge graphs, inclusief ABox (instance-niveau) en TBox (schema-niveau) assertions, dynamische regelgebaseerde inferentie en backward chaining.
- **Streaming en Sliding Windows**: Verwerkt getimestampte triples en sliding window operaties voor tijdsgebaseerde data-analyse.
- **Uitbreidbare Dictionary Encoding**: Codeert en decodeert RDF-termen efficiënt met behulp van een aanpasbare dictionary.
- **Uitgebreide API**: Biedt een rijke set methoden voor gegevensmanipulatie, querying en resultaatsverwerking.

> [!WARNING]
> Het gebruik van CUDA is experimenteel en in ontwikkeling.

## Installatie

Zorg ervoor dat je [Rust](https://www.rust-lang.org/tools/install) geïnstalleerd hebt (versie 1.60 of hoger).

Voeg **Kolibrie** toe aan je `Cargo.toml`:

```toml
[dependencies]
kolibrie = "0.1.0"
```

Include het vervolgens in je project:

```rust
use kolibrie::SparqlDatabase;
```

## Gebruik

### Initialiseren van de Database

Maak een nieuw exemplaar van de `SparqlDatabase`:

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // Jouw code hier
}
```

### RDF-gegevens Parsen

**Kolibrie** ondersteunt het parsen van RDF-gegevens uit bestanden of strings in verschillende formaten.

#### RDF/XML Parsen vanuit een Bestand

```rust
db.parse_rdf_from_file("data.rdf");
```

#### Turtle Gegevens Parsen vanuit een String

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_turtle(turtle_data);
```

#### N3 Gegevens Parsen vanuit een String

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_n3(n3_data);
```

### SPARQL-Queries Uitvoeren

Voer SPARQL-queries uit om gegevens op te halen en te manipuleren.

#### Basisquery

```rust
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

#### Gegevens Invoegen

```rust
let insert_query = r#"
PREFIX ex: <http://example.org/>
INSERT {
    ex:Charlie ex:knows ex:David .
}
WHERE {
    ex:Bob ex:knows ex:Charlie .
}
"#;

let results = execute_query(insert_query, &mut db);
// Insert operaties geven geen resultaten terug
```

### Gebruik van de Volcano Optimizer

De **Volcano Optimizer** is geïntegreerd binnen **Kolibrie** om query-uitvoeringsplannen te optimaliseren op basis van kostenschattingen. Het transformeert logische queryplannen naar efficiënte fysieke plannen door verschillende fysieke operators te evalueren en de met de laagste geschatte kost te selecteren.

#### Voorbeeld: Geoptimaliseerde Query-uitvoering

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Parse Turtle data
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definieer de SPARQL-query
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Voer de query uit met een geoptimaliseerd plan
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Location: {}", row[0], row[1]);
    }
}
```

### Werken met de Knowledge Graph

De **Knowledge Graph** component stelt je in staat om semantische netwerken te bouwen en beheren met zowel instance-level (ABox) als schema-level (TBox) informatie. Het ondersteunt dynamische regelgebaseerde inferentie en backward chaining om nieuwe kennis af te leiden uit bestaande gegevens.

#### Voorbeeld: Een Knowledge Graph Bouwen en Queryen

```rust
use kolibrie::{KnowledgeGraph, Triple};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Voeg TBox triples toe (schema)
    kg.add_tbox_triple("http://example.org/Person", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Class");
    kg.add_tbox_triple("http://example.org/knows", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Property");

    // Voeg ABox triples toe (instanties)
    kg.add_abox_triple("http://example.org/Alice", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");

    // Definieer en voeg regels toe
    let rule = Rule {
        premise: vec![
            (
                Term::Variable("x".to_string()),
                Term::Constant(kg.dictionary.encode("http://example.org/knows")),
                Term::Variable("y".to_string()),
            )
        ],
        conclusion: (
            Term::Variable("y".to_string()),
            Term::Constant(kg.dictionary.encode("http://example.org/knownBy")),
            Term::Variable("x".to_string()),
        ),
    };
    kg.add_rule(rule);

    // Leid nieuwe feiten af op basis van regels
    kg.infer_new_facts();

    // Query de Knowledge Graph
    let inferred_facts = kg.query_abox(
        Some("http://example.org/Bob"),
        Some("http://example.org/knownBy"),
        Some("http://example.org/Alice"),
    );

    for triple in inferred_facts {
        println!(
            "<{}> -- <{}> -- <{}> .",
            kg.dictionary.decode(triple.subject).unwrap(),
            kg.dictionary.decode(triple.predicate).unwrap(),
            kg.dictionary.decode(triple.object).unwrap()
        );
    }
}
```

**Output:**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## API Documentatie

### `SparqlDatabase` Struct

De `SparqlDatabase` struct is de kerncomponent die de RDF-store representeert en methoden biedt voor gegevensmanipulatie en querying.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Velden

- **triples**: Slaat RDF-triples op in een gesorteerde set voor efficiënte querying.
- **streams**: Bevat getimestampte triples voor streaming en temporele queries.
- **sliding_window**: Optioneel sliding window voor tijdsgebaseerde data-analyse.
- **dictionary**: Codeert en decodeert RDF-termen voor opslag efficiëntie.
- **prefixes**: Beheert namespace-prefixen voor het oplossen van afgekorte termen.

### `VolcanoOptimizer` Struct

De `VolcanoOptimizer` implementeert een kostengebaseerde query optimizer gebaseerd op het Volcano-model. Het transformeert logische queryplannen naar efficiënte fysieke plannen door verschillende fysieke operators te evalueren en de met de laagste geschatte kost te selecteren.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Velden

- **memo**: Cacheert geoptimaliseerde fysieke operators om redundante berekeningen te vermijden.
- **selected_variables**: Houdt bij welke variabelen in de query zijn geselecteerd.
- **stats**: Bevat statistische informatie over de database om te helpen bij kostenschattingen.

### `KnowledgeGraph` Struct

De `KnowledgeGraph` struct beheert zowel ABox (instance-niveau) als TBox (schema-niveau) assertions, ondersteunt dynamische regelgebaseerde inferentie en biedt querying mogelijkheden met backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Assertions over individuen (instanties)
    pub tbox: BTreeSet<Triple>, // TBox: Concepten en relaties (schema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Lijst van dynamische regels
}
```

#### Velden

- **abox**: Slaat instance-niveau RDF-triples op.
- **tbox**: Slaat schema-niveau RDF-triples op.
- **dictionary**: Codeert en decodeert RDF-termen voor opslag efficiëntie.
- **rules**: Bevat dynamische regels voor inferentie.

### Kernmethoden

#### `new() -> Self`

Maakt een nieuwe, lege `SparqlDatabase`.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Parset RDF/XML gegevens uit een gespecificeerd bestand en vult de database.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Parset RDF/XML gegevens uit een string.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Parset Turtle-geformatteerde RDF-gegevens uit een string.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Parset N3-geformatteerde RDF-gegevens uit een string.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Voert een SPARQL-query uit tegen de database en retourneert de resultaten.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtert triples op basis van een predicaatfunctie.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Voegt een getimestampte triple toe aan de streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Haalt triples op binnen een gespecificeerd tijdsvenster.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer` Methoden

#### `new(database: &SparqlDatabase) -> Self`

Maakt een nieuw exemplaar van de `VolcanoOptimizer` met statistische gegevens verzameld uit de gegeven database.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Bepaalt het meest efficiënte fysieke uitvoeringsplan voor een gegeven logische queryplan.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph` Methoden

#### `new() -> Self`

Maakt een nieuwe, lege `KnowledgeGraph`.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Voegt een TBox triple (schema-niveau informatie) toe aan de knowledge graph.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Voegt een ABox triple (instance-niveau informatie) toe aan de knowledge graph.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Voegt een dynamische regel toe aan de knowledge graph voor inferentie.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Voert regelgebaseerde inferentie uit om nieuwe triples af te leiden en werkt de ABox dienovereenkomstig bij.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Queryt de ABox voor instance-niveau assertions op basis van optionele subject-, predicaat- en objectfilters.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Queryt de TBox voor schema-niveau assertions op basis van optionele subject-, predicaat- en objectfilters.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Voorbeelden

### Basisquery

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Parse Turtle data
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Voer een SPARQL SELECT query uit
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
}
```

**Output:**
```
Subject: http://example.org/Alice, Object: http://example.org/Bob
Subject: http://example.org/Bob, Object: http://example.org/Charlie
```

### Geavanceerde Filtering en Aggregatie

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Parse Turtle data
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Voer een SPARQL SELECT query uit met FILTER en GROUP BY
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT AVG(?age) AS ?averageAge
    WHERE {
        ?s ex:age ?age .
        FILTER (?age > "20")
    }
    GROUPBY ?averageAge
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Gemiddelde Leeftijd: {}", row[0]);
    }
}
```

**Output:**
```
Gemiddelde Leeftijd: 30
```

### Geoptimaliseerde Query-uitvoering met Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Parse Turtle data
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definieer de SPARQL-query
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Voer de query uit met een geoptimaliseerd plan
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Location: {}", row[0], row[1]);
    }
}
```

**Output:**
```
Person: http://example.org/Alice, Location: http://example.org/Kulak
Person: http://example.org/Bob, Location: http://example.org/Kortrijk
Person: http://example.org/Charlie, Location: http://example.org/Ughent
```

### Een Knowledge Graph Bouwen en Queryen

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Voeg TBox triples toe (schema)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Voeg ABox triples toe (instanties)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Definieer en voeg regels toe
    let rule = Rule {
        premise: vec![
            (
                Term::Variable("x".to_string()),
                Term::Constant(kg.dictionary.encode("http://example.org/knows")),
                Term::Variable("y".to_string()),
            )
        ],
        conclusion: (
            Term::Variable("y".to_string()),
            Term::Constant(kg.dictionary.encode("http://example.org/knownBy")),
            Term::Variable("x".to_string()),
        ),
    };
    kg.add_rule(rule);

    // Leid nieuwe feiten af op basis van regels
    let inferred_facts = kg.infer_new_facts();

    // Query de Knowledge Graph
    let queried_facts = kg.query_abox(
        Some("http://example.org/Bob"),
        Some("http://example.org/knownBy"),
        Some("http://example.org/Alice"),
    );

    for triple in queried_facts {
        println!(
            "<{}> -- <{}> -- <{}> .",
            kg.dictionary.decode(triple.subject).unwrap(),
            kg.dictionary.decode(triple.predicate).unwrap(),
            kg.dictionary.decode(triple.object).unwrap()
        );
    }
}
```

**Output:**
```
Inferred new fact: Triple { subject: 2, predicate: 4, object: 1 }
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Prestaties

**Kolibrie** is geoptimaliseerd voor hoge prestaties door middel van:

- **Parallel Parsen en Verwerken**: Maakt gebruik van Rayon en Crossbeam voor multi-threaded data parsing en query-uitvoering.
- **SIMD Instructies**: Implementeert SIMD-operaties om filtering en aggregatie taken te versnellen.
- **Volcano Optimizer**: Gebruikt een kostengebaseerde query optimizer om efficiënte fysieke uitvoeringsplannen te genereren, wat de query-uitvoertijd minimaliseert.
- **Knowledge Graph Inferentie**: Benut regelgebaseerde inferentie en backward chaining om nieuwe kennis af te leiden zonder significante prestatieoverlast.
- **Efficiënte Gegevensstructuren**: Maakt gebruik van `BTreeSet` voor gesorteerde opslag en `HashMap` voor prefixbeheer, wat snelle gegevensopvraging en -manipulatie waarborgt.
- **Geheugenoptimalisatie**: Gebruikt dictionary encoding om het geheugenverbruik te minimaliseren door herhaalde termen te hergebruiken.

Benchmarking toont significante prestatieverbeteringen op grote RDF-datasets in vergelijking met traditionele single-threaded SPARQL-engines.

### Kolibrie vs. Oxigraph vs. RDFlib vs. Apache Jena (RDF/XML triples 100K)
**Tijd genomen om RDF-gegevens te laden**
| Kolibrie  | Oxigraph | RDFlib    | Apache Jena |
|-----------|----------|-----------|-------------|
| 759.02ms  | 10.21s   | 23.41s    | 2.32s       |
| 765.30ms  | 10.32s   | 26.69s    | 2.20s       |
| 767.80ms  | 10.26s   | 28.61s    | 2.15s       |
| 763.89ms  | 10.34s   | 30.36s    | 2.11s       |
| 757.12ms  | 10.34s   | 30.39s    | 2.12s       |
| 755.35ms  | 10.26s   | 30.53s    | 2.17s       |
| 765.97ms  | 10.17s   | 30.65s    | 2.08s       |
| 761.93ms  | 10.27s   | 28.82s    | 2.10s       |
| 771.49ms  | 10.17s   | 28.55s    | 2.10s       |
| 763.96ms  | 10.32s   | 30.29s    | 2.11s       |
| 767.13ms  | 10.23s   | 30.29s    | 2.10s       |
| 772.74ms  | 10.28s   | 30.21s    | 2.07s       |
| 759.49ms  | 10.23s   | 32.39s    | 2.24s       |
| 764.33ms  | 10.37s   | 28.78s    | 2.11s       |
| 765.96ms  | 10.14s   | 28.51s    | 2.21s       |
| 776.03ms  | 10.36s   | 30.28s    | 2.16s       |
| 773.43ms  | 10.17s   | 30.63s    | 2.18s       |
| 763.02ms  | 10.17s   | 28.67s    | 2.18s       |
| 751.50ms  | 10.28s   | 30.42s    | 2.20s       |
| 764.07ms  | 10.32s   | 30.37s    | 2.16s       |

**Tijd genomen om SPARQL-query uit te voeren**
| Kolibrie  | Oxigraph | RDFlib    | Apache Jena |
|-----------|----------|-----------|-------------|
| 218.07ms  | 982.44ms | 502.88 ms | 797.67ms    |
| 215.86ms  | 984.54ms | 2.21 ms   | 796.11ms    |
| 213.93ms  | 983.53ms | 2.31 ms   | 749.51ms    |
| 218.92ms  | 994.63ms | 2.28 ms   | 761.53ms    |
| 218.17ms  | 990.50ms | 1.98 ms   | 740.22ms    |
| 213.32ms  | 996.63ms | 2.38 ms   | 732.42ms    |
| 213.98ms  | 977.26ms | 2.14 ms   | 750.46ms    |
| 214.59ms  | 985.31ms | 2.30 ms   | 753.79ms    |
| 209.54ms  | 985.94ms | 1.98 ms   | 759.01ms    |
| 216.22ms  | 976.10ms | 1.97 ms   | 743.88ms    |
| 211.11ms  | 997.83ms | 1.93 ms   | 740.65ms    |
| 217.72ms  | 978.09ms | 2.28 ms   | 753.59ms    |
| 219.35ms  | 989.44ms | 1.98 ms   | 832.77ms    |
| 211.71ms  | 983.64ms | 2.27 ms   | 761.30ms    |
| 220.93ms  | 978.75ms | 1.90 ms   | 745.90ms    |
| 219.62ms  | 985.96ms | 1.89 ms   | 755.21ms    |
| 209.10ms  | 986.19ms | 2.29 ms   | 793.17ms    |
| 215.82ms  | 986.04ms | 2.62 ms   | 757.18ms    |
| 215.88ms  | 979.21ms | 1.98 ms   | 757.05ms    |
| 212.52ms  | 985.24ms | 1.90 ms   | 753.47ms    |

**Samenvatting van Kolibrie**

- Totale Parseertijd: 15,29 seconden
- Totale Query-uitvoertijd: 4,31 seconden
- Gemiddelde Parseertijd: 0,76 seconden
- Gemiddelde Query-uitvoertijd: 0,22 seconden

**Samenvatting van Oxigraph**

- Totale RDF Laadtijd: 205,21 seconden
- Totale Query-uitvoertijd: 19,71 seconden
- Gemiddelde RDF Laadtijd: 10,26 seconden
- Gemiddelde Query-uitvoertijd: 0,99 seconden

**Samenvatting van RDFlib**

- Totale RDF Laadtijd: 588,86 seconden
- Totale SPARQL Query Uitvoertijd: 0,54 seconden
- Gemiddelde RDF Laadtijd: 29,44 seconden
- Gemiddelde SPARQL Query Uitvoertijd: 27,17 ms

**Samenvatting van Apache Jena**

- Totale RDF Laadtijd: 43,07 seconden
- Totale SPARQL Query Uitvoertijd: 15,23 seconden
- Gemiddelde RDF Laadtijd: 2,15 seconden
- Gemiddelde SPARQL Query Uitvoertijd: 761,74 ms

## Hoe Bij te Draegen

### Problemen Indienen
Gebruik de Issue Tracker om bugrapporten en feature/verbeteringsverzoeken in te dienen. Zorg ervoor dat er geen vergelijkbaar openstaand issue is voordat je een nieuw probleem indient.

### Handmatig Testen
Iedereen die de code handmatig test en bugs of suggesties voor verbeteringen rapporteert in de Issue Tracker is zeer welkom!

### Pull Requests Indienen
Patches/fixes worden geaccepteerd in de vorm van pull requests (PRs). Zorg ervoor dat het issue dat de pull request adresseert, open staat in de Issue Tracker.

Ingediende pull request wordt geacht akkoord te zijn gegaan met publiceren onder de MIT-licentie.

## Licentie

Kolibrie is gelicenseerd onder de [MIT License](LICENSE).