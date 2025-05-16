# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![GitHub Workflow Status](https://img.shields.io/github/commit-activity/t/ladroid/goku) -->
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Rust Version](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** ist eine leistungsstarke, nebenläufige und funktionsreiche SPARQL-Abfrage-Engine, die in Rust implementiert ist. Entwickelt für Skalierbarkeit und Effizienz nutzt sie das robuste Nebenläufigkeitsmodell von Rust sowie fortschrittliche Optimierungen, einschließlich SIMD (Single Instruction, Multiple Data) und paralleler Verarbeitung mit Rayon, um nahtlos mit groß angelegten RDF (Resource Description Framework)-Datensätzen umzugehen.

Mit einer umfassenden API erleichtert **Kolibrie** das Parsen, Speichern und Abfragen von RDF-Daten in SPARQL-, Turtle- und N3-Formaten. Ihre fortschrittlichen Filter-, Aggregations-, Join-Operationen und ausgefeilten Optimierungsstrategien machen sie zu einer geeigneten Wahl für Anwendungen, die komplexe semantische Datenverarbeitung erfordern. Darüber hinaus ermöglicht die Integration des Volcano Optimizer und der Knowledge Graph-Funktionen den Benutzern, kosteneffiziente Abfrageplanung durchzuführen und regelbasierte Inferenz für erweiterte Datenanalysen zu nutzen.

## Forschungskontext

**Kolibrie** wird innerhalb des [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) an der KU Leuven unter der Leitung von Prof. Pieter Bonte entwickelt. Das Stream Intelligence Lab konzentriert sich auf **Stream Reasoning**, ein aufstrebendes Forschungsfeld, das logisch-basierte Techniken aus der künstlichen Intelligenz mit datengesteuerten maschinellen Lernansätzen integriert, um zeitnahe und umsetzbare Erkenntnisse aus kontinuierlichen Datenströmen zu gewinnen. Unsere Forschung betont Anwendungen im Internet der Dinge (IoT) und Edge-Processing, was Echtzeit-Entscheidungen in dynamischen Umgebungen wie autonomen Fahrzeugen, Robotik und Webanalyse ermöglicht.

Für weitere Informationen über unsere Forschung und laufende Projekte besuchen Sie bitte die [Stream Intelligence Lab Website](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Funktionen

- **Effizientes RDF-Parsen**: Unterstützt das Parsen von RDF/XML-, Turtle- und N3-Formaten mit robuster Fehlerbehandlung und Prefix-Verwaltung.
- **Nebenläufige Verarbeitung**: Nutzt Rayon und Crossbeam für parallele Datenverarbeitung und gewährleistet optimale Leistung auf Multi-Core-Systemen.
- **SIMD-Optimierungen**: Implementiert SIMD-Instruktionen zur Beschleunigung von Query-Filtering und Aggregation.
- **Flexible Abfragen**: Unterstützt komplexe SPARQL-Abfragen, einschließlich SELECT-, INSERT-, FILTER-, GROUP BY- und VALUES-Klauseln.
- **Volcano Optimizer**: Integriert einen kostenbasierten Query-Optimizer basierend auf dem Volcano-Modell, um die effizientesten Ausführungspläne zu bestimmen.
- **Knowledge Graph**: Bietet robuste Unterstützung für den Aufbau und die Abfrage von Wissensgraphen, einschließlich ABox (Instanzebene) und TBox (Schemaebene) Assertions, dynamische regelbasierte Inferenz und Backward Chaining.
- **Streaming und Sliding Windows**: Verarbeitet zeitgestempelte Triple und Sliding-Window-Operationen für zeitbasierte Datenanalysen.
- **Erweiterbare Dictionary-Encoding**: Codiert und decodiert RDF-Terme effizient mithilfe eines anpassbaren Dictionaries.
- **Umfassende API**: Bietet eine reichhaltige Sammlung von Methoden für Datenmanipulation, Abfragen und Ergebnisverarbeitung.

> [!WARNING]
> Die Nutzung von CUDA ist experimentell und in der Entwicklung.

## Installation

Stellen Sie sicher, dass Sie [Rust](https://www.rust-lang.org/tools/install) installiert haben (Version 1.60 oder höher).

Fügen Sie **Kolibrie** zu Ihrem `Cargo.toml` hinzu:

```toml
[dependencies]
kolibrie = "0.1.0"
```

Dann binden Sie es in Ihr Projekt ein:

```rust
use kolibrie::SparqlDatabase;
```

## Verwendung

### Initialisieren der Datenbank

Erstellen Sie eine neue Instanz der `SparqlDatabase`:

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // Ihr Code hier
}
```

### RDF-Daten Parsen

**Kolibrie** unterstützt das Parsen von RDF-Daten aus Dateien oder Strings in verschiedenen Formaten.

#### RDF/XML aus einer Datei Parsen

```rust
db.parse_rdf_from_file("data.rdf");
```

#### Turtle-Daten aus einem String Parsen

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_turtle(turtle_data);
```

#### N3-Daten aus einem String Parsen

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_n3(n3_data);
```

### SPARQL-Abfragen Ausführen

Führen Sie SPARQL-Abfragen aus, um Daten abzurufen und zu manipulieren.

#### Grundlegende Abfrage

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
    println!("Subjekt: {}, Objekt: {}", row[0], row[1]);
}
```

#### Daten Einfügen

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
// Einfügeoperationen geben keine Ergebnisse zurück
```

### Verwendung des Volcano Optimizers

Der **Volcano Optimizer** ist in **Kolibrie** integriert, um Abfrageausführungspläne basierend auf Kostenschätzungen zu optimieren. Er transformiert logische Abfragepläne in effiziente physische Pläne, indem er verschiedene physische Operatoren bewertet und den mit den niedrigsten geschätzten Kosten auswählt.

#### Beispiel: Optimierte Abfrageausführung

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definieren Sie die SPARQL-Abfrage
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Führen Sie die Abfrage mit einem optimierten Plan aus
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Standort: {}", row[0], row[1]);
    }
}
```

### Arbeiten mit dem Wissensgraph

Die **Knowledge Graph**-Komponente ermöglicht es Ihnen, semantische Netzwerke mit sowohl instanzbezogenen (ABox) als auch schemabezogenen (TBox) Informationen aufzubauen und zu verwalten. Sie unterstützt dynamische regelbasierte Inferenz und Backward Chaining, um neues Wissen aus vorhandenen Daten abzuleiten.

#### Beispiel: Aufbau und Abfrage eines Wissensgraphen

```rust
use kolibrie::{KnowledgeGraph, Triple};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBox-Triple (Schema) hinzufügen
    kg.add_tbox_triple("http://example.org/Person", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Class");
    kg.add_tbox_triple("http://example.org/knows", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Property");

    // ABox-Triple (Instanzen) hinzufügen
    kg.add_abox_triple("http://example.org/Alice", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");

    // Regeln definieren und hinzufügen
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

    // Neue Fakten basierend auf Regeln ableiten
    kg.infer_new_facts();

    // Wissensgraph abfragen
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

**Ausgabe:**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## API-Dokumentation

### `SparqlDatabase` Struktur

Die `SparqlDatabase`-Struktur ist die Kernkomponente, die den RDF-Speicher darstellt und Methoden für Datenmanipulation und Abfragen bereitstellt.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Felder

- **triples**: Speichert RDF-Triple in einer sortierten Menge für effiziente Abfragen.
- **streams**: Enthält zeitgestempelte Triple für Streaming- und zeitbezogene Abfragen.
- **sliding_window**: Optionales Sliding Window für zeitbasierte Datenanalysen.
- **dictionary**: Codiert und decodiert RDF-Terme zur Speicheroptimierung.
- **prefixes**: Verwaltet Namensraum-Prefixes zur Auflösung von abgekürzten Begriffen.

### `VolcanoOptimizer` Struktur

Die `VolcanoOptimizer`-Struktur implementiert einen kostenbasierten Query-Optimizer basierend auf dem Volcano-Modell. Sie transformiert logische Abfragepläne in effiziente physische Pläne, indem sie verschiedene physische Operatoren bewertet und den mit den niedrigsten geschätzten Kosten auswählt.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Felder

- **memo**: Cacht optimierte physische Operatoren, um redundante Berechnungen zu vermeiden.
- **selected_variables**: Verfolgt die in der Abfrage ausgewählten Variablen.
- **stats**: Enthält statistische Informationen über die Datenbank zur Unterstützung der Kostenschätzung.

### `KnowledgeGraph` Struktur

Die `KnowledgeGraph`-Struktur verwaltet sowohl ABox (Instanzebene) als auch TBox (Schemaebene) Assertions, unterstützt dynamische regelbasierte Inferenz und bietet Abfragefunktionen mit Backward Chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Assertions über Individuen (Instanzen)
    pub tbox: BTreeSet<Triple>, // TBox: Konzepte und Beziehungen (Schema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste dynamischer Regeln
}
```

#### Felder

- **abox**: Speichert instanzbezogene RDF-Triple.
- **tbox**: Speichert schemabezogene RDF-Triple.
- **dictionary**: Codiert und decodiert RDF-Terme zur Speicheroptimierung.
- **rules**: Enthält dynamische Regeln zur Inferenz.

### Kernmethoden

#### `new() -> Self`

Erstellt eine neue, leere `SparqlDatabase`.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Parst RDF/XML-Daten aus einer angegebenen Datei und füllt die Datenbank.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Parst RDF/XML-Daten aus einem String.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Parst Turtle-formatierte RDF-Daten aus einem String.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Parst N3-formatierte RDF-Daten aus einem String.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Führt eine SPARQL-Abfrage gegen die Datenbank aus und gibt die Ergebnisse zurück.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtert Triple basierend auf einer Prädikatfunktion.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Fügt ein zeitgestempeltes Triple zu den Streams hinzu.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Holt Triple innerhalb eines angegebenen Zeitfensters.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer` Methoden

#### `new(database: &SparqlDatabase) -> Self`

Erstellt eine neue Instanz des `VolcanoOptimizer` mit statistischen Daten aus der bereitgestellten Datenbank.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Bestimmt den effizientesten physischen Ausführungsplan für einen gegebenen logischen Abfrageplan.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph` Methoden

#### `new() -> Self`

Erstellt einen neuen, leeren `KnowledgeGraph`.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Fügt ein TBox-Tripel (Schemaebene Information) zum Wissensgraphen hinzu.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Fügt ein ABox-Tripel (Instanzebene Information) zum Wissensgraphen hinzu.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Fügt eine dynamische Regel zum Wissensgraphen für die Inferenz hinzu.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Führt regelbasierte Inferenz durch, um neue Triple abzuleiten und aktualisiert die ABox entsprechend.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Fragt die ABox nach instanzbezogenen Assertions basierend auf optionalen Subjekt-, Prädikat- und Objektfiltern ab.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Fragt die TBox nach schemabezogenen Assertions basierend auf optionalen Subjekt-, Prädikat- und Objektfiltern ab.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Beispiele

### Grundlegende Abfrage

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Führen Sie eine SPARQL SELECT-Abfrage aus
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Subjekt: {}, Objekt: {}", row[0], row[1]);
    }
}
```

**Ausgabe:**
```
Subjekt: http://example.org/Alice, Objekt: http://example.org/Bob
Subjekt: http://example.org/Bob, Objekt: http://example.org/Charlie
```

### Fortgeschrittenes Filtern und Aggregation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Führen Sie eine SPARQL SELECT-Abfrage mit FILTER und GROUP BY aus
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT (AVG(?age) AS ?averageAge)
    WHERE {
        ?s ex:age ?age .
        FILTER (?age > "20")
    }
    GROUP BY ?averageAge
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Durchschnittsalter: {}", row[0]);
    }
}
```

**Ausgabe:**
```
Durchschnittsalter: 30
```

### Optimierte Abfrageausführung mit Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definieren Sie die SPARQL-Abfrage
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Führen Sie die Abfrage mit einem optimierten Plan aus
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Standort: {}", row[0], row[1]);
    }
}
```

**Ausgabe:**
```
Person: http://example.org/Alice, Standort: http://example.org/Kulak
Person: http://example.org/Bob, Standort: http://example.org/Kortrijk
Person: http://example.org/Charlie, Standort: http://example.org/Ughent
```

### Aufbau und Abfrage eines Wissensgraphen

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBox-Triple (Schema) hinzufügen
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // ABox-Triple (Instanzen) hinzufügen
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Regeln definieren und hinzufügen
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

    // Neue Fakten basierend auf Regeln ableiten
    let inferred_facts = kg.infer_new_facts();

    // Wissensgraph abfragen
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

**Ausgabe:**
```
Inferred new fact: Triple { subject: 2, predicate: 4, object: 1 }
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## API-Dokumentation

### `SparqlDatabase` Struktur

Die `SparqlDatabase`-Struktur ist die Kernkomponente, die den RDF-Speicher darstellt und Methoden für Datenmanipulation und Abfragen bereitstellt.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Felder

- **triples**: Speichert RDF-Triple in einer sortierten Menge für effiziente Abfragen.
- **streams**: Enthält zeitgestempelte Triple für Streaming- und zeitbezogene Abfragen.
- **sliding_window**: Optionales Sliding Window für zeitbasierte Datenanalysen.
- **dictionary**: Codiert und decodiert RDF-Terme zur Speicheroptimierung.
- **prefixes**: Verwaltet Namensraum-Prefixes zur Auflösung von abgekürzten Begriffen.

### `VolcanoOptimizer` Struktur

Die `VolcanoOptimizer`-Struktur implementiert einen kostenbasierten Query-Optimizer basierend auf dem Volcano-Modell. Sie transformiert logische Abfragepläne in effiziente physische Pläne, indem sie verschiedene physische Operatoren bewertet und den mit den niedrigsten geschätzten Kosten auswählt.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Felder

- **memo**: Cacht optimierte physische Operatoren, um redundante Berechnungen zu vermeiden.
- **selected_variables**: Verfolgt die in der Abfrage ausgewählten Variablen.
- **stats**: Enthält statistische Informationen über die Datenbank zur Unterstützung der Kostenschätzung.

### `KnowledgeGraph` Struktur

Die `KnowledgeGraph`-Struktur verwaltet sowohl ABox (Instanzebene) als auch TBox (Schemaebene) Assertions, unterstützt dynamische regelbasierte Inferenz und bietet Abfragefunktionen mit Backward Chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Assertions über Individuen (Instanzen)
    pub tbox: BTreeSet<Triple>, // TBox: Konzepte und Beziehungen (Schema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste dynamischer Regeln
}
```

#### Felder

- **abox**: Speichert instanzbezogene RDF-Triple.
- **tbox**: Speichert schemabezogene RDF-Triple.
- **dictionary**: Codiert und decodiert RDF-Terme zur Speicheroptimierung.
- **rules**: Enthält dynamische Regeln zur Inferenz.

### Kernmethoden

#### `new() -> Self`

Erstellt eine neue, leere `SparqlDatabase`.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Parst RDF/XML-Daten aus einer angegebenen Datei und füllt die Datenbank.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Parst RDF/XML-Daten aus einem String.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Parst Turtle-formatierte RDF-Daten aus einem String.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Parst N3-formatierte RDF-Daten aus einem String.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Führt eine SPARQL-Abfrage gegen die Datenbank aus und gibt die Ergebnisse zurück.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtert Triple basierend auf einer Prädikatfunktion.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Fügt ein zeitgestempeltes Triple zu den Streams hinzu.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Holt Triple innerhalb eines angegebenen Zeitfensters.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer` Methoden

#### `new(database: &SparqlDatabase) -> Self`

Erstellt eine neue Instanz des `VolcanoOptimizer` mit statistischen Daten aus der bereitgestellten Datenbank.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Bestimmt den effizientesten physischen Ausführungsplan für einen gegebenen logischen Abfrageplan.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph` Methoden

#### `new() -> Self`

Erstellt einen neuen, leeren `KnowledgeGraph`.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Fügt ein TBox-Tripel (Schemaebene Information) zum Wissensgraphen hinzu.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Fügt ein ABox-Tripel (Instanzebene Information) zum Wissensgraphen hinzu.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Fügt eine dynamische Regel zum Wissensgraphen für die Inferenz hinzu.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Führt regelbasierte Inferenz durch, um neue Triple abzuleiten und aktualisiert die ABox entsprechend.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Fragt die ABox nach instanzbezogenen Assertions basierend auf optionalen Subjekt-, Prädikat- und Objektfiltern ab.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Fragt die TBox nach schemabezogenen Assertions basierend auf optionalen Subjekt-, Prädikat- und Objektfiltern ab.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Beispiele

### Grundlegende Abfrage

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Führen Sie eine SPARQL SELECT-Abfrage aus
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Subjekt: {}, Objekt: {}", row[0], row[1]);
    }
}
```

**Ausgabe:**
```
Subjekt: http://example.org/Alice, Objekt: http://example.org/Bob
Subjekt: http://example.org/Bob, Objekt: http://example.org/Charlie
```

### Fortgeschrittenes Filtern und Aggregation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Führen Sie eine SPARQL SELECT-Abfrage mit FILTER und GROUP BY aus
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT (AVG(?age) AS ?averageAge)
    WHERE {
        ?s ex:age ?age .
        FILTER (?age > "20")
    }
    GROUP BY ?averageAge
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Durchschnittsalter: {}", row[0]);
    }
}
```

**Ausgabe:**
```
Durchschnittsalter: 30
```

### Optimierte Abfrageausführung mit Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definieren Sie die SPARQL-Abfrage
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Führen Sie die Abfrage mit einem optimierten Plan aus
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Standort: {}", row[0], row[1]);
    }
}
```

**Ausgabe:**
```
Person: http://example.org/Alice, Standort: http://example.org/Kulak
Person: http://example.org/Bob, Standort: http://example.org/Kortrijk
Person: http://example.org/Charlie, Standort: http://example.org/Ughent
```

### Aufbau und Abfrage eines Wissensgraphen

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBox-Triple (Schema) hinzufügen
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // ABox-Triple (Instanzen) hinzufügen
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Regeln definieren und hinzufügen
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

    // Neue Fakten basierend auf Regeln ableiten
    let inferred_facts = kg.infer_new_facts();

    // Wissensgraph abfragen
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

**Ausgabe:**
```
Inferred new fact: Triple { subject: 2, predicate: 4, object: 1 }
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## API-Dokumentation

### `SparqlDatabase` Struktur

Die `SparqlDatabase`-Struktur ist die Kernkomponente, die den RDF-Speicher darstellt und Methoden für Datenmanipulation und Abfragen bereitstellt.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Felder

- **triples**: Speichert RDF-Triple in einer sortierten Menge für effiziente Abfragen.
- **streams**: Enthält zeitgestempelte Triple für Streaming- und zeitbezogene Abfragen.
- **sliding_window**: Optionales Sliding Window für zeitbasierte Datenanalysen.
- **dictionary**: Codiert und decodiert RDF-Terme zur Speicheroptimierung.
- **prefixes**: Verwaltet Namensraum-Prefixes zur Auflösung von abgekürzten Begriffen.

### `VolcanoOptimizer` Struktur

Die `VolcanoOptimizer`-Struktur implementiert einen kostenbasierten Query-Optimizer basierend auf dem Volcano-Modell. Sie transformiert logische Abfragepläne in effiziente physische Pläne, indem sie verschiedene physische Operatoren bewertet und den mit den niedrigsten geschätzten Kosten auswählt.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Felder

- **memo**: Cacht optimierte physische Operatoren, um redundante Berechnungen zu vermeiden.
- **selected_variables**: Verfolgt die in der Abfrage ausgewählten Variablen.
- **stats**: Enthält statistische Informationen über die Datenbank zur Unterstützung der Kostenschätzung.

### `KnowledgeGraph` Struktur

Die `KnowledgeGraph`-Struktur verwaltet sowohl ABox (Instanzebene) als auch TBox (Schemaebene) Assertions, unterstützt dynamische regelbasierte Inferenz und bietet Abfragefunktionen mit Backward Chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Assertions über Individuen (Instanzen)
    pub tbox: BTreeSet<Triple>, // TBox: Konzepte und Beziehungen (Schema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste dynamischer Regeln
}
```

#### Felder

- **abox**: Speichert instanzbezogene RDF-Triple.
- **tbox**: Speichert schemabezogene RDF-Triple.
- **dictionary**: Codiert und decodiert RDF-Terme zur Speicheroptimierung.
- **rules**: Enthält dynamische Regeln zur Inferenz.

### Kernmethoden

#### `new() -> Self`

Erstellt eine neue, leere `SparqlDatabase`.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Parst RDF/XML-Daten aus einer angegebenen Datei und füllt die Datenbank.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Parst RDF/XML-Daten aus einem String.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Parst Turtle-formatierte RDF-Daten aus einem String.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Parst N3-formatierte RDF-Daten aus einem String.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Führt eine SPARQL-Abfrage gegen die Datenbank aus und gibt die Ergebnisse zurück.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtert Triple basierend auf einer Prädikatfunktion.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Fügt ein zeitgestempeltes Triple zu den Streams hinzu.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Holt Triple innerhalb eines angegebenen Zeitfensters.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer` Methoden

#### `new(database: &SparqlDatabase) -> Self`

Erstellt eine neue Instanz des `VolcanoOptimizer` mit statistischen Daten aus der bereitgestellten Datenbank.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Bestimmt den effizientesten physischen Ausführungsplan für einen gegebenen logischen Abfrageplan.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph` Methoden

#### `new() -> Self`

Erstellt einen neuen, leeren `KnowledgeGraph`.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Fügt ein TBox-Tripel (Schemaebene Information) zum Wissensgraphen hinzu.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Fügt ein ABox-Tripel (Instanzebene Information) zum Wissensgraphen hinzu.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Fügt eine dynamische Regel zum Wissensgraphen für die Inferenz hinzu.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Führt regelbasierte Inferenz durch, um neue Triple abzuleiten und aktualisiert die ABox entsprechend.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Fragt die ABox nach instanzbezogenen Assertions basierend auf optionalen Subjekt-, Prädikat- und Objektfiltern ab.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Fragt die TBox nach schemabezogenen Assertions basierend auf optionalen Subjekt-, Prädikat- und Objektfiltern ab.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Beispiele

### Grundlegende Abfrage

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Führen Sie eine SPARQL SELECT-Abfrage aus
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Subjekt: {}, Objekt: {}", row[0], row[1]);
    }
}
```

**Ausgabe:**
```
Subjekt: http://example.org/Alice, Objekt: http://example.org/Bob
Subjekt: http://example.org/Bob, Objekt: http://example.org/Charlie
```

### Fortgeschrittenes Filtern und Aggregation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Führen Sie eine SPARQL SELECT-Abfrage mit FILTER und GROUP BY aus
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT (AVG(?age) AS ?averageAge)
    WHERE {
        ?s ex:age ?age .
        FILTER (?age > "20")
    }
    GROUP BY ?averageAge
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Durchschnittsalter: {}", row[0]);
    }
}
```

**Ausgabe:**
```
Durchschnittsalter: 30
```

### Optimierte Abfrageausführung mit Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtle-Daten parsen
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definieren Sie die SPARQL-Abfrage
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Führen Sie die Abfrage mit einem optimierten Plan aus
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Standort: {}", row[0], row[1]);
    }
}
```

**Ausgabe:**
```
Person: http://example.org/Alice, Standort: http://example.org/Kulak
Person: http://example.org/Bob, Standort: http://example.org/Kortrijk
Person: http://example.org/Charlie, Standort: http://example.org/Ughent
```

### Aufbau und Abfrage eines Wissensgraphen

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBox-Triple (Schema) hinzufügen
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // ABox-Triple (Instanzen) hinzufügen
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Regeln definieren und hinzufügen
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

    // Neue Fakten basierend auf Regeln ableiten
    let inferred_facts = kg.infer_new_facts();

    // Wissensgraph abfragen
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

**Ausgabe:**
```
Inferred new fact: Triple { subject: 2, predicate: 4, object: 1 }
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Leistung

**Kolibrie** ist für hohe Leistung optimiert durch:

- **Paralleles Parsen und Verarbeiten**: Nutzt Rayon und Crossbeam für Multi-Threaded Datenparsen und Abfrageausführung.
- **SIMD-Instruktionen**: Implementiert SIMD-Operationen zur Beschleunigung von Filter- und Aggregationsaufgaben.
- **Volcano Optimizer**: Verwendet einen kostenbasierten Query-Optimizer, um effiziente physische Ausführungspläne zu generieren und die Abfrageausführungszeit zu minimieren.
- **Wissensgraph-Inferenz**: Nutzt regelbasierte Inferenz und Backward Chaining, um neues Wissen abzuleiten, ohne signifikante Leistungseinbußen.
- **Effiziente Datenstrukturen**: Verwendet `BTreeSet` für sortierte Speicherung und `HashMap` für Prefix-Verwaltung, was schnelle Datenabfrage und -manipulation gewährleistet.
- **Speicheroptimierung**: Verwendet Dictionary-Encoding zur Minimierung des Speicherverbrauchs durch Wiederverwendung wiederholter Terme.

Benchmarking zeigt signifikante Leistungsgewinne bei großen RDF-Datensätzen im Vergleich zu traditionellen Single-Threaded SPARQL-Engines.

### Kolibrie vs. Oxigraph vs. RDFlib vs. Apache Jena (RDF/XML-Triple 100K)
**Zeit zum Laden von RDF-Daten**
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

**Zeit zum Ausführen von SPARQL-Abfragen**
| Kolibrie  | Oxigraph | RDFlib    | Apache Jena |
|-----------|----------|-----------|-------------|
| 218.07ms  | 982.44ms | 502.88ms  | 797.67ms    |
| 215.86ms  | 984.54ms | 2.21ms    | 796.11ms    |
| 213.93ms  | 983.53ms | 2.31ms    | 749.51ms    |
| 218.92ms  | 994.63ms | 2.28ms    | 761.53ms    |
| 218.17ms  | 990.50ms | 1.98ms    | 740.22ms    |
| 213.32ms  | 996.63ms | 2.38ms    | 732.42ms    |
| 213.98ms  | 977.26ms | 2.14ms    | 750.46ms    |
| 214.59ms  | 985.31ms | 2.30ms    | 753.79ms    |
| 209.54ms  | 985.94ms | 1.98ms    | 759.01ms    |
| 216.22ms  | 976.10ms | 1.97ms    | 743.88ms    |
| 211.11ms  | 997.83ms | 1.93ms    | 740.65ms    |
| 217.72ms  | 978.09ms | 2.28ms    | 753.59ms    |
| 219.35ms  | 989.44ms | 1.98ms    | 832.77ms    |
| 211.71ms  | 983.64ms | 2.27ms    | 761.30ms    |
| 220.93ms  | 978.75ms | 1.90ms    | 745.90ms    |
| 219.62ms  | 985.96ms | 1.89ms    | 755.21ms    |
| 209.10ms  | 986.19ms | 2.29ms    | 793.17ms    |
| 215.82ms  | 986.04ms | 2.62ms    | 757.18ms    |
| 215.88ms  | 979.21ms | 1.98ms    | 757.05ms    |
| 212.52ms  | 985.24ms | 1.90ms    | 753.47ms    |

**Zusammenfassung von Kolibrie**

- **Gesamte Parszeit**: 15,29 Sekunden
- **Gesamte Abfrageausführungszeit**: 4,31 Sekunden
- **Durchschnittliche Parszeit**: 0,76 Sekunden
- **Durchschnittliche Abfrageausführungszeit**: 0,22 Sekunden

**Zusammenfassung von Oxigraph**

- **Gesamte RDF-Ladezeit**: 205,21 Sekunden
- **Gesamte Abfrageausführungszeit**: 19,71 Sekunden
- **Durchschnittliche RDF-Ladezeit**: 10,26 Sekunden
- **Durchschnittliche Abfrageausführungszeit**: 0,99 Sekunden

**Zusammenfassung von RDFlib**

- **Gesamte RDF-Ladezeit**: 588,86 Sekunden
- **Gesamte SPARQL-Abfrageausführungszeit**: 0,54 Sekunden
- **Durchschnittliche RDF-Ladezeit**: 29,44 Sekunden
- **Durchschnittliche SPARQL-Abfrageausführungszeit**: 27,17ms

**Zusammenfassung von Apache Jena**

- **Gesamte RDF-Ladezeit**: 43,07 Sekunden
- **Gesamte SPARQL-Abfrageausführungszeit**: 15,23 Sekunden
- **Durchschnittliche RDF-Ladezeit**: 2,15 Sekunden
- **Durchschnittliche SPARQL-Abfrageausführungszeit**: 761,74ms

## Beitrag leisten

### Probleme einreichen
Verwenden Sie den Issue Tracker, um Fehlerberichte sowie Feature- oder Verbesserungsanfragen einzureichen. Stellen Sie vor dem Einreichen eines neuen Problems sicher, dass kein ähnliches offenes Issue existiert.

### Manuelles Testen
Jeder, der den Code manuell testet und Fehler oder Vorschläge für Verbesserungen im Issue Tracker meldet, ist sehr willkommen!

### Pull Requests einreichen
Patches/Fixes werden in Form von Pull Requests (PRs) akzeptiert. Stellen Sie sicher, dass das Issue, das der Pull Request adressiert, im Issue Tracker offen ist.

Eingereichte Pull Requests gelten als zugestimmt, unter der Mozilla Public License Version 2.0 veröffentlicht zu werden.

## Lizenz

Kolibrie ist unter der [MPL-2.0 License](LICENSE) lizenziert.