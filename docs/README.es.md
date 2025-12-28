# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![Estado del Flujo de Trabajo de GitHub](https://img.shields.io/github/commit-activity/t/StreamIntelligenceLab/Kolibri) -->
[![Status](https://img.shields.io/badge/status-stable-blue.svg)](https://github.com/StreamIntelligenceLab/Kolibrie/tree/main)
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Rust Version](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
[![Chat Server](https://img.shields.io/badge/chat-discord-7289da.svg)](https://discord.gg/KcFXrUUyYm)
<!--![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)-->

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** es un motor de consultas SPARQL de alto rendimiento, concurrente y con muchas funcionalidades, implementado en Rust. Diseñado para la escalabilidad y eficiencia, aprovecha el robusto modelo de concurrencia de Rust y optimizaciones avanzadas, incluyendo SIMD (Single Instruction, Multiple Data) y procesamiento paralelo con Rayon, para manejar conjuntos de datos RDF (Resource Description Framework) a gran escala de manera fluida.

Con una API integral, **Kolibrie** facilita el análisis, almacenamiento y consulta de datos RDF utilizando formatos SPARQL, Turtle y N3. Sus avanzados filtros, agregaciones, operaciones de unión y sofisticadas estrategias de optimización lo convierten en una opción adecuada para aplicaciones que requieren un procesamiento de datos semánticos complejo. Además, la integración del Volcano Optimizer y las capacidades de Reasoner permiten a los usuarios realizar una planificación de consultas rentable y aprovechar la inferencia basada en reglas para obtener conocimientos de datos mejorados.

## Contexto de Investigación

**Kolibrie** se desarrolla dentro del [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) en la KU Leuven, bajo la supervisión del Prof. Pieter Bonte. El Stream Intelligence Lab se enfoca en **Stream Reasoning**, un campo de investigación emergente que integra técnicas basadas en lógica de la inteligencia artificial con enfoques de aprendizaje automático basados en datos para derivar conocimientos oportunos y accionables de flujos de datos continuos. Nuestra investigación enfatiza aplicaciones en el Internet de las Cosas (IoT) y procesamiento en el Edge, permitiendo la toma de decisiones en tiempo real en entornos dinámicos como vehículos autónomos, robótica y análisis web.

Para más información sobre nuestra investigación y proyectos en curso, visita el [sitio web del Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Características

- **Análisis RDF Eficiente**: Soporta el análisis de formatos RDF/XML, Turtle y N3 con manejo robusto de errores y gestión de prefijos.
- **Procesamiento Concurrente**: Utiliza Rayon y Crossbeam para el procesamiento de datos en paralelo, asegurando un rendimiento óptimo en sistemas multi-core.
- **Optimización SIMD**: Implementa instrucciones SIMD para acelerar el filtrado y la agregación de consultas.
- **Consultas Flexibles**: Soporta consultas SPARQL complejas, incluyendo cláusulas SELECT, INSERT, FILTER, GROUP BY y VALUES.
- **Volcano Optimizer**: Incorpora un optimizador de consultas basado en costos según el modelo Volcano para determinar los planes de ejecución más eficientes.
- **Reasoner**: Proporciona soporte robusto para construir y consultar grafos de conocimiento, incluyendo afirmaciones ABox (nivel de instancia) y TBox (nivel de esquema), inferencia dinámica basada en reglas y backward chaining.
- **Streaming y Ventanas Deslizantes (Sliding Windows)**: Maneja triples con marca de tiempo y operaciones de ventanas deslizantes para análisis de datos basados en tiempo.
- **Codificación de Diccionario Extensible**: Codifica y decodifica términos RDF de manera eficiente usando un diccionario personalizable.
- **API Completa**: Ofrece un conjunto rico de métodos para la manipulación de datos, consultas y procesamiento de resultados.

> [!WARNING]
> el uso de CUDA es experimental y está en desarrollo

## Instalación

### Instalación Nativa

Asegúrate de tener [Rust](https://www.rust-lang.org/tools/install) instalado (versión 1.60 o superior).

Clona el repositorio:

```bash
git clone https://github.com/StreamIntelligenceLab/Kolibrie.git
cd Kolibrie
```

Compila el proyecto:

```bash
cargo build --release
```

Luego, inclúyelo en tu proyecto:

```rust
use kolibrie::SparqlDatabase;
```

### Instalación con Docker

**Kolibrie** proporciona soporte para Docker con múltiples configuraciones para diferentes casos de uso. La configuración de Docker maneja automáticamente todas las dependencias incluyendo Rust, CUDA (para builds GPU) y frameworks de Python ML.

#### Requisitos Previos

* [Docker](https://docs.docker.com/get-docker/) instalado
* [Docker Compose](https://docs.docker.com/compose/install/) instalado
* Para soporte GPU: [NVIDIA Docker runtime](https://github.com/NVIDIA/nvidia-docker) instalado

#### Inicio Rápido

1. **Build solo CPU** (recomendado para la mayoría de usuarios):

```bash
docker compose --profile cpu up --build
```

2. **Build con GPU habilitado** (requiere GPU NVIDIA y nvidia-docker):

```bash
docker compose --profile gpu up --build
```

3. **Build de desarrollo** (detecta automáticamente disponibilidad de GPU):

```bash
docker compose --profile dev up --build
```

## Uso

### Inicializar la Base de Datos

Crea una nueva instancia de `SparqlDatabase`:

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // Tu código aquí
}
```

### Analizar Datos RDF

**Kolibrie** soporta el análisis de datos RDF desde archivos o cadenas en varios formatos.

#### Analizar RDF/XML desde un Archivo

```rust
db.parse_rdf_from_file("data.rdf");
```

#### Analizar RDF/XML desde una Cadena

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

#### Analizar Datos Turtle desde una Cadena

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob . 
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_turtle(turtle_data);
```

#### Analizar Datos N3 desde una Cadena

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_n3(n3_data);
```

#### Analizar N-Triples desde una Cadena

```rust
let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> . 
<http://example.org/jane> <http://example.org/name> "Jane Doe" . 
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

db.parse_ntriples_and_add(ntriples_data);
```

### Añadir Triples Programáticamente

Añade triples individuales directamente a la base de datos:

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

### Ejecutar Consultas SPARQL

Ejecuta consultas SPARQL para recuperar y manipular datos.

#### Consulta SELECT Básica

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

#### Consulta con FILTER

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

#### Consulta con Operador OR

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

#### Consulta con LIMIT

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

#### Consulta con Agregaciones

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

**Agregaciones Soportadas:**

* `AVG(? var)` - Calcular promedio
* `COUNT(?var)` - Contar ocurrencias
* `SUM(?var)` - Sumar valores
* `MIN(?var)` - Encontrar mínimo
* `MAX(? var)` - Encontrar máximo

#### Consulta con Funciones de String

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

#### Consultas Anidadas

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

### Usar la API del Query Builder

El Query Builder proporciona una interfaz fluida para construir consultas de forma programática.

#### Construcción Básica

```rust
// Get all objects for a specific predicate
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/name")
    .get_objects();

for object in results {
    println!("Name: {}", object);
}
```

#### Consulta con Filtrado

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

#### Consulta con Joins

```rust
let other_db = SparqlDatabase::new();
// ...  populate other_db ...

let results = db.query()
    .join(&other_db)
    .join_on_subject()
    .get_triples();
```

#### Consulta con Ordenación, Límite y Distinct

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

### Uso del Volcano Optimizer

El **Volcano Optimizer** está integrado dentro de **Kolibrie** para optimizar los planes de ejecución de consultas basados en la estimación de costos. Transforma planes lógicos en planes físicos eficientes utilizando diversas estrategias de join y toma decisiones basadas en costos para seleccionar la ruta más performante.

#### Ejemplo: Ejecución de Consulta Optimizada

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

### Trabajar con el Reasoner

El componente **Reasoner** permite construir y gestionar redes semánticas con información a nivel de instancia (ABox). Soporta inferencia dinámica basada en reglas usando forward chaining, backward chaining y evaluación semi-naive para derivar nuevo conocimiento.

#### Ejemplo: Construir y Consultar un Reasoner

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

**Salida:**

```
Inferred 1 new facts
Alice is ancestor of Charlie
```

## Documentación de la API

### Estructura `SparqlDatabase`

La estructura `SparqlDatabase` es el componente central que representa el almacén RDF y proporciona métodos para la manipulación de datos y consultas.

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

#### Campos

* **triples**: Almacena triples RDF en un conjunto ordenado para consultas eficientes.
* **streams**: Contiene triples con marca de tiempo para consultas de streaming y temporales.
* **sliding_window**: Ventana deslizante opcional para análisis de datos basados en tiempo.
* **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
* **prefixes**: Gestiona prefijos de espacios de nombres para resolver términos prefijados.
* **udfs**: Registro de funciones definidas por el usuario para operaciones personalizadas.
* **index_manager**: Sistema de indexación unificado para rendimiento optimizado.
* **rule_map**: Mapea nombres de reglas a sus definiciones.
* **cached_stats**: Estadísticas cacheadas de la base de datos para optimización de consultas.

### Estructura `Streamertail`

El `Streamertail` implementa un optimizador de consultas basado en costos según el modelo Volcano. Transforma planes lógicos en planes físicos eficientes evaluando diferentes operadores físicos y seleccionando el de menor costo estimado.

```rust
pub struct Streamertail<'a> {
    pub stats: Arc<DatabaseStats>,
    pub memo: HashMap<String, (PhysicalOperator, f64)>,
    pub selected_variables: Vec<String>,
    database: &'a SparqlDatabase,
}
```

#### Campos

* **stats**: Información estadística compartida de la base de datos para ayudar en la estimación de costos.
* **memo**: Caches de operadores físicos optimizados junto con sus costos para evitar cálculos redundantes.
* **selected_variables**: Rastrea las variables seleccionadas en la consulta.
* **database**: Referencia a la base de datos SPARQL para la ejecución.

### Estructura `Reasoner`

La estructura `Reasoner` gestiona afirmaciones a nivel de instancia (ABox), soporta inferencia dinámica basada en reglas y proporciona capacidades de consulta con forward chaining, backward chaining y evaluación semi-naive.

```rust
pub struct Reasoner {
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>,
    pub index_manager: UnifiedIndex,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
}
```

#### Campos

* **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
* **rules**: Contiene reglas dinámicas para inferencia de nuevo conocimiento.
* **index_manager**: Sistema de indexación unificado para almacenar y consultar triples.
* **rule_index**: Índice especializado para emparejamiento eficiente de reglas.
* **constraints**: Restricciones de integridad para detección y reparación de inconsistencias.

### Métodos Principales

#### `SparqlDatabase::new() -> Self`

Crea una nueva `SparqlDatabase` vacía.

```rust
let mut db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analiza datos RDF/XML desde un archivo especificado y llena la base de datos.

```rust
db.parse_rdf_from_file("data. rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analiza datos RDF/XML desde una cadena.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">... </rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analiza datos RDF en formato Turtle desde una cadena.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> . 

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analiza datos RDF en formato N3 desde una cadena.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> . 

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `parse_ntriples_and_add(&mut self, ntriples_data: &str)`

Analiza N-Triples y los añade a la base de datos.

```rust
let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> . 
<http://example.org/jane> <http://example.org/name> "Jane Doe" .
"#;
db.parse_ntriples_and_add(ntriples_data);
```

#### `add_triple_parts(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple a la base de datos codificando sus partes.

```rust
db.add_triple_parts(
    "http://example.org/alice",
    "http://xmlns.com/foaf/0. 1/name",
    "Alice"
);
```

#### `delete_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) -> bool`

Elimina un triple y retorna si se removió exitosamente.

```rust
let deleted = db.delete_triple_parts(
    "http://example.org/alice",
    "http://xmlns.com/foaf/0.1/age",
    "30"
);
```

#### `build_all_indexes(&mut self)`

Construye todos los índices a partir de los triples actuales.

```rust
db.build_all_indexes();
```

#### `get_or_build_stats(&mut self) -> Arc<DatabaseStats>`

Obtiene estadísticas cacheadas o construye nuevas estadísticas.

```rust
let stats = db.get_or_build_stats();
```

#### `invalidate_stats_cache(&mut self)`

Invalida el caché de estadísticas tras modificaciones de datos.

```rust
db.invalidate_stats_cache();
```

#### `query(&self) -> QueryBuilder`

Devuelve un QueryBuilder para construir consultas programáticamente.

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0. 1/name")
    .get_objects();
```

#### `register_udf<F>(&mut self, name: &str, f: F)`

Registra una función definida por el usuario para usarla en consultas.

```rust
db.register_udf("toUpperCase", |args: Vec<&str>| {
    args[0].to_uppercase()
});
```

#### `generate_rdf_xml(&mut self) -> String`

Genera una representación RDF/XML de la base de datos.

```rust
let rdf_xml = db.generate_rdf_xml();
```

#### `decode_triple(&self, triple: &Triple) -> Option<(&str, &str, &str)>`

Decodifica un triple a su representación en string.

```rust
if let Some((s, p, o)) = db. decode_triple(&triple) {
    println!("{} - {} - {}", s, p, o);
}
```

### Métodos de `Streamertail`

#### `new(database: &SparqlDatabase) -> Self`

Crea una nueva instancia del `Streamertail` con estadísticas de la base de datos.

```rust
let optimizer = Streamertail::new(&db);
```

#### `with_cached_stats(stats: Arc<DatabaseStats>) -> Self`

Crea un optimizador con estadísticas precomputadas.

```rust
let stats = db.get_or_build_stats();
let optimizer = Streamertail::with_cached_stats(stats);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Determina el plan físico más eficiente para un plan lógico dado.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

#### `execute_plan(&mut self, plan: &PhysicalOperator, database: &mut SparqlDatabase) -> Vec<BTreeMap<String, String>>`

Ejecuta un plan físico optimizado y retorna los resultados.

```rust
let results = optimizer.execute_plan(&physical_plan, &mut db);
```

### Métodos de `Reasoner`

#### `new() -> Self`

Crea un `Reasoner` vacío.

```rust
let mut kg = Reasoner::new();
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple ABox (información a nivel de instancia).

```rust
kg.add_abox_triple("Alice", "knows", "Bob");
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la ABox con filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_abox(Some("Alice"), Some("knows"), None);
```

#### `add_rule(&mut self, rule: Rule)`

Añade una regla dinámica para inferencia.

```rust
let rule = Rule {
    premise: vec![... ],
    conclusion: vec![... ],
    filters: vec![],
};
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Realiza naive forward chaining.

```rust
let inferred = kg.infer_new_facts();
println!("Inferred {} new facts", inferred.len());
```

#### `infer_new_facts_semi_naive(&mut self) -> Vec<Triple>`

Realiza evaluación semi-naive para inferencia más eficiente.

```rust
let inferred = kg.infer_new_facts_semi_naive();
```

#### `infer_new_facts_semi_naive_parallel(&mut self) -> Vec<Triple>`

Realiza evaluación semi-naive en paralelo para inferencia a gran escala.

```rust
let inferred = kg.infer_new_facts_semi_naive_parallel();
```

#### `backward_chaining(&self, query: &TriplePattern) -> Vec<HashMap<String, Term>>`

Realiza backward chaining para responder consultas a partir de reglas.

```rust
let query_pattern = (
    Term::Variable("X".to_string()),
    Term::Constant(kg.dictionary.encode("knows")),
    Term::Variable("Y".to_string())
);

let results = kg.backward_chaining(&query_pattern);
```

#### `add_constraint(&mut self, constraint: Rule)`

Añade una restricción de integridad.

```rust
kg.add_constraint(constraint);
```

#### `infer_new_facts_semi_naive_with_repairs(&mut self) -> Vec<Triple>`

Realiza inferencia manejando inconsistencias mediante reparación automática.

```rust
let inferred = kg.infer_new_facts_semi_naive_with_repairs();
```

#### `query_with_repairs(&self, query: &TriplePattern) -> Vec<HashMap<String, u32>>`

Consulta usando semánticas tolerantes a inconsistencias (IAR).

```rust
let results = kg.query_with_repairs(&query_pattern);
```

## Rendimiento

**Kolibrie** está optimizado para alto rendimiento mediante:

* **Análisis y Procesamiento Paralelo**: Utiliza Rayon y Crossbeam para el análisis de datos multi-threaded y la ejecución de consultas.
* **Instrucciones SIMD**: Implementa operaciones SIMD para acelerar tareas de filtrado y agregación.
* **Volcano Optimizer**: Emplea un optimizador de consultas basado en costos para generar planes de ejecución física eficientes, minimizando el tiempo de ejecución de consultas.
* **Inferencia del Grafo de Conocimiento**: Aprovecha la inferencia basada en reglas y backward chaining para derivar nuevos conocimientos sin una sobrecarga significativa de rendimiento.
* **Estructuras de Datos Eficientes**: Emplea `BTreeSet` para almacenamiento ordenado y `HashMap` para la gestión de prefijos, asegurando una rápida recuperación y manipulación de datos.
* **Optimización de Memoria**: Utiliza codificación de diccionario para minimizar la huella de memoria reutilizando términos repetidos.

### Resultados de Benchmarking

Nuestros benchmarks demuestran el rendimiento superior de Kolibrie frente a otros motores RDF populares. Las siguientes pruebas se realizaron usando:

* **Dataset**: benchmark [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 10M triples
* **Configuración de Oxigraph**: backend RocksDB para rendimiento óptimo
* **Razonamiento de Taxonomía Profunda**: pruebas de profundidad jerárquica hasta 10K niveles

#### WatDiv 10M - Comparación de Rendimiento de Consultas (20 ejecuciones cada una)

![WatDiv 10M Query Performance](img/image1.png)

*Figura 1: Tiempos de ejecución de consultas entre diferentes motores SPARQL usando el dataset WatDiv 10M*

**Hallazgos Clave:**

* Kolibrie supera consistentemente a sus competidores en todos los tipos de consulta (L1-L5, S1-S7, F1-F3, C1-C3)
* Tiempo promedio de ejecución: **rango sub-milisegundo a pocos milisegundos**
* Blazegraph y QLever muestran rendimiento competitivo en patrones específicos
* Oxigraph (con RocksDB) demuestra estabilidad en todas las consultas

#### Taxonomía Profunda - Razonamiento sobre Profundidad Jerárquica

![Deep Taxonomy Reasoning Performance](img/image2.png)

*Figura 2: Rendimiento de razonamiento a través de diferentes profundidades jerárquicas (10, 100, 1K, 10K niveles)*

**Hallazgos Clave:**

* Kolibrie muestra **escalado logarítmico** con la profundidad de la jerarquía
* En 10K niveles, Kolibrie mantiene tiempos de respuesta sub-segundo
* Rendimiento superior frente a Apache Jena y el razonador EYE
* Manejo eficiente de estructuras taxonómicas complejas

## Cómo Contribuir

### Envío de Problemas

Utiliza el Issue Tracker para enviar reportes de errores y solicitudes de nuevas funcionalidades/mejoras. Antes de enviar un nuevo problema, asegúrate de que no exista un issue similar abierto.

### Pruebas Manuales

¡Se agradece enormemente que cualquier persona que pruebe el código manualmente y reporte errores o sugerencias de mejoras en el Issue Tracker contribuya!

### Envío de Pull Requests

Se aceptan parches/correcciones en forma de pull requests (PRs). Asegúrate de que el issue que el pull request aborda esté abierto en el Issue Tracker.

El pull request enviado se considera que ha aceptado publicarse bajo la Licencia Pública de Mozilla, versión 2.0.

## Comunidad

Únete a nuestra [comunidad de Discord](https://discord.gg/KcFXrUUyYm) para discutir sobre Kolibrie, hacer preguntas y compartir experiencias.

## Licencia

Kolibrie está licenciado bajo la [Licencia MPL-2.0](LICENSE).
