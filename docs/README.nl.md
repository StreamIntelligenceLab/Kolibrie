# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![GitHub Workflow Status](https://img.shields.io/github/commit-activity/t/StreamIntelligenceLab/Kolibrie) -->
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Rust Version](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
<!--![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)-->

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** is een hoogwaardig, concurrerend en rijk aan functies SPARQL-query-engine geïmplementeerd in Rust. Ontworpen voor schaalbaarheid en efficiëntie, maakt het gebruik van het robuuste concurrentiemodel van Rust en geavanceerde optimalisaties, waaronder SIMD (Single Instruction, Multiple Data) en parallelle verwerking met Rayon, om moeiteloos om te gaan met grootschalige RDF (Resource Description Framework) datasets.

Met een uitgebreide API faciliteert **Kolibrie** het parsen, opslaan en opvragen van RDF-gegevens met behulp van SPARQL-, Turtle- en N3-formaten. De geavanceerde filtering, aggregatie, join-operaties en verfijnde optimalisatiestrategieën maken het een geschikte keuze voor toepassingen die complexe semantische dataverwerking vereisen. Bovendien stelt de integratie van de Volcano Optimizer en de **Reasoner**-component gebruikers in staat kosteneffectieve queryplanning uit te voeren en regelgebaseerde inferentie te benutten voor verbeterde data-inzichten.

## Onderzoekscontext

**Kolibrie** is ontwikkeld binnen het [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) aan de KU Leuven, onder supervisie van Prof. Pieter Bonte. Het Stream Intelligence Lab richt zich op **Stream Reasoning**, een opkomend onderzoeksveld dat logica-gebaseerde technieken uit kunstmatige intelligentie integreert met data-gedreven machine learning benaderingen om tijdige en bruikbare inzichten te verkrijgen uit continue datastromen. Ons onderzoek benadrukt toepassingen in het Internet of Things (IoT) en Edge processing, wat real-time besluitvorming in dynamische omgevingen mogelijk maakt, zoals autonome voertuigen, robotica en webanalyse.

Voor meer informatie over ons onderzoek en lopende projecten, bezoek de [Stream Intelligence Lab website](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Functies

- **Efficiënt RDF Parsing**: Ondersteunt het parsen van RDF/XML, Turtle en N3-formaten met robuuste foutafhandeling en prefixbeheer.
- **Concurrerende Verwerking**: Maakt gebruik van Rayon en Crossbeam voor parallelle gegevensverwerking, wat optimale prestaties op multi-core systemen waarborgt.
- **SIMD Optimalisaties**: Implementeert SIMD-instructies voor versnelde query-filtering en aggregatie.
- **Flexibele Querying**: Ondersteunt complexe SPARQL-queries, inclusief SELECT, INSERT, FILTER, GROUP BY en VALUES clausules.
- **Volcano Optimizer**: Integreert een kostengebaseerde query optimizer gebaseerd op het Volcano-model om de meest efficiënte uitvoeringsplannen te bepalen.
- **Reasoner**: Biedt robuuste ondersteuning voor het bouwen en opvragen van knowledge graphs, inclusief ABox (instance-niveau) en TBox (schema-niveau) assertions, dynamische regelgebaseerde inferentie en backward chaining.
- **Streaming en Sliding Windows**: Verwerkt getimestampte triples en sliding window operaties voor tijdsgebaseerde data-analyse.
- **Uitbreidbare Dictionary Encoding**: Codeert en decodeert RDF-termen efficiënt met behulp van een aanpasbare dictionary.
- **Uitgebreide API**: Biedt een rijke set methoden voor gegevensmanipulatie, querying en resultaatsverwerking.

> [!WARNING]
> het gebruik van CUDA is experimenteel en in ontwikkeling

## Installatie

### Native Installatie

Zorg ervoor dat je [Rust](https://www.rust-lang.org/tools/install) geïnstalleerd hebt (versie 1.60 of hoger).

Clone de repository:

```bash
git clone https://github.com/StreamIntelligenceLab/Kolibrie.git
cd Kolibrie
```

Bouw het project:

```bash
cargo build --release
```

Include het vervolgens in je project:

```rust
use kolibrie::SparqlDatabase;
```

### Docker Installatie

**Kolibrie** biedt Docker-ondersteuning met meerdere configuraties voor verschillende gebruikssituaties. De Docker-setup behandelt automatisch alle afhankelijkheden inclusief Rust, CUDA (voor GPU builds), en Python ML frameworks.

#### Vereisten

* [Docker](https://docs.docker.com/get-docker/) geïnstalleerd
* [Docker Compose](https://docs.docker.com/compose/install/) geïnstalleerd
* Voor GPU-ondersteuning: [NVIDIA Docker runtime](https://github.com/NVIDIA/nvidia-docker) geïnstalleerd

#### Snelstart

1. **Alleen CPU build** (aanbevolen voor de meeste gebruikers):

```bash
docker compose --profile cpu up --build
```

2. **GPU-enabled build** (vereist NVIDIA GPU en nvidia-docker):

```bash
docker compose --profile gpu up --build
```

3. **Development build** (detecteert automatisch GPU-beschikbaarheid):

```bash
docker compose --profile dev up --build
```

## Gebruik

### Initialiseren van de Database

Maak een nieuw exemplaar van `SparqlDatabase`:

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

#### RDF/XML Parsen vanuit een String

```rust
let rdf_data = r#"
<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF 
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:foaf="http://xmlns.com/foaf/0.1/">
    
    <rdf:Description rdf:about="http://example.org/alice">
        <foaf:name>Alice</foaf:name>
        <foaf:age>30</foaf:age>
    </rdf:Description>
</rdf:RDF>
"#;

db.parse_rdf(rdf_data);
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

#### N-Triples Parsen vanuit een String

```rust
let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> . 
<http://example.org/jane> <http://example.org/name> "Jane Doe" . 
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

db.parse_ntriples_and_add(ntriples_data);
```

### Triples Programmatig Toevoegen

Voeg individuele triples direct toe:

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

### SPARQL-Queries Uitvoeren

#### Basis SELECT

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

#### Query met FILTER

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
    println!("Event: {}, Attendees: {}", row[0], row[1]);
}
```

#### Query met OR

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
    if let [name, type_, attendees] = &row[..] {
        println!("Name: {}, Type: {}, Attendees: {}", name, type_, attendees);
    }
}
```

#### Query met LIMIT

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

#### Query met Aggregaties

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
    if let [avg_salary] = &row[..] {
        println!("Average Salary: {}", avg_salary);
    }
}
```

**Ondersteunde aggregaties:**

* `AVG(?var)`
* `COUNT(?var)`
* `SUM(?var)`
* `MIN(?var)`
* `MAX(?var)`

#### Query met Stringfuncties

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

#### Geneste Queries

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

### De Query Builder API Gebruiken

De Query Builder biedt een fluente interface om queries programmatisch op te bouwen.

#### Basisgebruik

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/name")
    .get_objects();

for object in results {
    println!("Name: {}", object);
}
```

#### Filtering via Closure

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/age")
    .filter(|triple| {
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

#### Joins

```rust
let other_db = SparqlDatabase::new();
// ... populate other_db ...

let results = db.query()
    .join(&other_db)
    .join_on_subject()
    .get_triples();
```

#### Sorteren, Distinct, Limit en Offset

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

### Gebruik van de Volcano Optimizer

De **Volcano Optimizer** is geïntegreerd binnen **Kolibrie** om query-uitvoeringsplannen te optimaliseren op basis van kostenschattingen. Het transformeert logische plannen naar performante fysieke plannen, evalueert verschillende join-strategieën en selecteert de route met de laagste geschatte kost.

#### Voorbeeld: Geoptimaliseerde Query-uitvoering

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
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
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

### Werken met de Reasoner

De **Reasoner**-component maakt het mogelijk semantische netwerken te bouwen en te beheren op ABox-niveau. De engine ondersteunt dynamische regelgebaseerde inferentie via forward chaining, backward chaining en semi-naive evaluatie.

#### Voorbeeld: Reasoner Bouwen en Queryen

```rust
use datalog::knowledge_graph::Reasoner;
use shared::terms::Term;
use shared::rule::Rule;

fn main() {
    let mut kg = Reasoner::new();

    // Add ABox triples (instance-level data)
    kg.add_abox_triple("Alice", "parentOf", "Bob");
    kg.add_abox_triple("Bob", "parentOf", "Charlie");

    // Rule: parentOf(X, Y) ∧ parentOf(Y, Z) → ancestorOf(X, Z)
    let ancestor_rule = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(kg.dictionary.encode("parentOf")),
                Term::Variable("Y".to_string()),
            ),
            (
                Term::Variable("Y".to_string()),
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
            kg.dictionary.decode(triple.object).unwrap()
        );
    }
}
```

**Output:**

```
Inferred 1 new facts
Alice is ancestor of Charlie
```

## API Documentatie

### `SparqlDatabase` Struct

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

#### Velden

* **triples**: Slaat RDF-triples op in een gesorteerde set voor efficiënte querying.
* **streams**: Bevat getimestampte triples voor streaming en temporele queries.
* **sliding_window**: Optioneel sliding window voor tijdsgebaseerde analyse.
* **dictionary**: Codeert en decodeert RDF-termen voor opslag-efficiëntie.
* **prefixes**: Beheert namespace-prefixen.
* **udfs**: Registry van user-defined functions.
* **index_manager**: Geünificeerd indexsysteem voor optimale performance.
* **rule_map**: Mapping van regelnamen naar definities.
* **cached_stats**: Gecachte database-statistieken voor kostenraming.

### `Streamertail` Struct

```rust
pub struct Streamertail<'a> {
    pub stats: Arc<DatabaseStats>,
    pub memo: HashMap<String, (PhysicalOperator, f64)>,
    pub selected_variables: Vec<String>,
    database: &'a SparqlDatabase,
}
```

#### Velden

* **stats**: Gedeelde statistieken voor kostenschattingen.
* **memo**: Cache van gekozen fysieke operators en hun kosten.
* **selected_variables**: Trackt geselecteerde variabelen.
* **database**: Referentie naar de SPARQL-database.

### `Reasoner` Struct

```rust
pub struct Reasoner {
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>,
    pub index_manager: UnifiedIndex,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
}
```

#### Velden

* **dictionary**: Codering/decodering van termen.
* **rules**: Dynamische regels voor inferentie.
* **index_manager**: Indexering van triples.
* **rule_index**: Snelle rule-matching.
* **constraints**: Integriteitsregels voor inconsistente data.

### Kernmethoden

#### `SparqlDatabase::new() -> Self`

```rust
let mut db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

```rust
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

```rust
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

```rust
db.parse_n3(n3_data);
```

#### `parse_ntriples_and_add(&mut self, ntriples_data: &str)`

```rust
db.parse_ntriples_and_add(ntriples_data);
```

#### `add_triple_parts(&mut self, subject: &str, predicate: &str, object: &str)`

```rust
db.add_triple_parts(subject, predicate, object);
```

#### `delete_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) -> bool`

```rust
let deleted = db.delete_triple_parts(subject, predicate, object);
```

#### `build_all_indexes(&mut self)`

```rust
db.build_all_indexes();
```

#### `get_or_build_stats(&mut self) -> Arc<DatabaseStats>`

```rust
let stats = db.get_or_build_stats();
```

#### `invalidate_stats_cache(&mut self)`

```rust
db.invalidate_stats_cache();
```

#### `query(&self) -> QueryBuilder`

```rust
let results = db.query().with_predicate("...").get_objects();
```

#### `register_udf<F>(&mut self, name: &str, f: F)`

```rust
db.register_udf("toUpperCase", |args: Vec<&str>| {
    args[0].to_uppercase()
});
```

#### `generate_rdf_xml(&mut self) -> String`

```rust
let rdf_xml = db.generate_rdf_xml();
```

#### `decode_triple(&self, triple: &Triple) -> Option<(&str, &str, &str)>`

```rust
if let Some((s, p, o)) = db.decode_triple(&triple) {
    println!("{} - {} - {}", s, p, o);
}
```

### `Streamertail` Methoden

#### `new(database: &SparqlDatabase) -> Self`

```rust
let optimizer = Streamertail::new(&db);
```

#### `with_cached_stats(stats: Arc<DatabaseStats>) -> Self`

```rust
let stats = db.get_or_build_stats();
let optimizer = Streamertail::with_cached_stats(stats);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

#### `execute_plan(&mut self, plan: &PhysicalOperator, database: &mut SparqlDatabase) -> Vec<BTreeMap<String, String>>`

```rust
let results = optimizer.execute_plan(&physical_plan, &mut db);
```

### `Reasoner` Methoden

#### `new() -> Self`

```rust
let mut kg = Reasoner::new();
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

```rust
kg.add_abox_triple("Alice", "knows", "Bob");
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

```rust
let results = kg.query_abox(Some("Alice"), Some("knows"), None);
```

#### `add_rule(&mut self, rule: Rule)`

```rust
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

```rust
let inferred = kg.infer_new_facts();
```

#### `infer_new_facts_semi_naive(&mut self) -> Vec<Triple>`

```rust
let inferred = kg.infer_new_facts_semi_naive();
```

#### `infer_new_facts_semi_naive_parallel(&mut self) -> Vec<Triple>`

```rust
let inferred = kg.infer_new_facts_semi_naive_parallel();
```

#### `backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>>`

```rust
let results = kg.backward_chaining(&query_pattern);
```

#### `add_constraint(&mut self, constraint: Rule)`

```rust
kg.add_constraint(constraint);
```

#### `infer_new_facts_semi_naive_with_repairs(&mut self) -> Vec<Triple>`

```rust
let inferred = kg.infer_new_facts_semi_naive_with_repairs();
```

#### `query_with_repairs(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>>`

```rust
let results = kg.query_with_repairs(&query_pattern);
```

## Prestaties

**Kolibrie** is geoptimaliseerd voor hoge prestaties door middel van:

* **Parallel Parsen en Verwerken**: Rayon en Crossbeam voor multi-threaded parsing en query-uitvoering.
* **SIMD Instructies**: Versnelt filtering en aggregaties.
* **Volcano Optimizer**: Kostengebaseerde selectie van fysieke plannen.
* **Rule-based Inferentie**: Efficiënte forward/backward chaining.
* **Efficiënte Datastructuren**: Geïndexeerde opslag en slimme dictionary encoding.
* **Geheugenoptimalisatie**: Hergebruik van termen via dictionary encoding.

### Benchmarking Resultaten

Onze benchmarks tonen de sterke prestaties van Kolibrie tegenover andere populaire RDF/SPARQL engines. De volgende tests werden uitgevoerd met:

* **Dataset**: [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 10M triples
* **Oxigraph Configuratie**: RocksDB backend voor optimale performance
* **Deep Taxonomy Reasoning**: hiërarchische dieptes tot 10K niveaus

#### WatDiv 10M - Query Performance Vergelijking (20 runs per query)

![WatDiv 10M Query Performance](img/image1.png)

*Figuur 1: Query-uitvoeringstijden tussen verschillende SPARQL engines met WatDiv 10M*

**Belangrijkste observaties:**

* Kolibrie presteert consistent sterk over diverse queryvormen (L-, S-, F-, C-patronen).
* Gemiddelde uitvoeringstijden liggen vaak in het **sub-milliseconde tot lage milliseconde** bereik.
* Andere engines kunnen competitief zijn op specifieke patronen, maar tonen vaker grotere variatie.

#### Deep Taxonomy - Reasoning over Hiërarchische Diepte

![Deep Taxonomy Reasoning Performance](img/image2.png)

*Figuur 2: Reasoning performance over 10, 100, 1K en 10K hiërarchische niveaus*

**Belangrijkste observaties:**

* Kolibrie toont **goede schaalbaarheid** bij toenemende diepte.
* Ook bij 10K niveaus blijven responstijden praktisch bruikbaar.
* Sterke prestaties tegenover klassieke reasoners en algemene SPARQL stacks.

## Hoe Bij te Dragen

### Problemen Indienen

Gebruik de Issue Tracker om bugrapporten en feature/verbeteringsverzoeken in te dienen. Controleer eerst of er geen vergelijkbaar openstaand issue bestaat.

### Handmatig Testen

Iedereen die de code handmatig test en bugs of suggesties voor verbeteringen rapporteert, helpt enorm!

### Pull Requests Indienen

Patches/fixes worden geaccepteerd in de vorm van pull requests (PRs). Zorg ervoor dat het issue dat de pull request adresseert open staat in de Issue Tracker.

Ingediende pull requests worden geacht akkoord te zijn gegaan met publicatie onder de Mozilla Public License Version 2.0.

## Community

Word lid van onze [Discord community](https://discord.gg/KcFXrUUyYm) om Kolibrie te bespreken, vragen te stellen en ervaringen te delen.

## Licentie

Kolibrie is gelicenseerd onder de [MPL-2.0 License](LICENSE).
