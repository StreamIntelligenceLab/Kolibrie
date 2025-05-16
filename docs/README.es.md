# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![Estado del Flujo de Trabajo de GitHub](https://img.shields.io/github/commit-activity/t/ladroid/goku) -->
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Versión de Rust](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Estado de Compilación](https://img.shields.io/badge/build-passing-brightgreen.svg)
![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** es un motor de consultas SPARQL de alto rendimiento, concurrente y con muchas funcionalidades, implementado en Rust. Diseñado para la escalabilidad y eficiencia, aprovecha el robusto modelo de concurrencia de Rust y optimizaciones avanzadas, incluyendo SIMD (Single Instruction, Multiple Data) y procesamiento paralelo con Rayon, para manejar conjuntos de datos RDF (Resource Description Framework) a gran escala de manera fluida.

Con una API integral, **Kolibrie** facilita el análisis, almacenamiento y consulta de datos RDF utilizando formatos SPARQL, Turtle y N3. Sus avanzados filtros, agregaciones, operaciones de unión y sofisticadas estrategias de optimización lo convierten en una opción adecuada para aplicaciones que requieren un procesamiento de datos semánticos complejo. Además, la integración del Volcano Optimizer y las capacidades de Knowledge Graph permiten a los usuarios realizar una planificación de consultas rentable y aprovechar la inferencia basada en reglas para obtener conocimientos de datos mejorados.

## Contexto de Investigación

**Kolibrie** se desarrolla dentro del [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) en la KU Leuven, bajo la supervisión del Prof. Pieter Bonte. El Stream Intelligence Lab se enfoca en **Stream Reasoning**, un campo de investigación emergente que integra técnicas basadas en lógica de la inteligencia artificial con enfoques de aprendizaje automático basados en datos para derivar conocimientos oportunos y accionables de flujos de datos continuos. Nuestra investigación enfatiza aplicaciones en el Internet de las Cosas (IoT) y procesamiento en el Edge, permitiendo la toma de decisiones en tiempo real en entornos dinámicos como vehículos autónomos, robótica y análisis web.

Para más información sobre nuestra investigación y proyectos en curso, visita el [sitio web del Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Características

- **Análisis RDF Eficiente**: Soporta el análisis de formatos RDF/XML, Turtle y N3 con manejo robusto de errores y gestión de prefijos.
- **Procesamiento Concurrente**: Utiliza Rayon y Crossbeam para el procesamiento de datos en paralelo, asegurando un rendimiento óptimo en sistemas multi-core.
- **Optimización SIMD**: Implementa instrucciones SIMD para acelerar el filtrado y la agregación de consultas.
- **Consultas Flexibles**: Soporta consultas SPARQL complejas, incluyendo cláusulas SELECT, INSERT, FILTER, GROUP BY y VALUES.
- **Volcano Optimizer**: Incorpora un optimizador de consultas basado en costos según el modelo Volcano para determinar los planes de ejecución más eficientes.
- **Knowledge Graph**: Proporciona soporte robusto para la construcción y consulta de gráficos de conocimiento, incluyendo afirmaciones ABox (nivel de instancia) y TBox (nivel de esquema), inferencia dinámica basada en reglas y backward chaining.
- **Streaming y Ventanas Deslizantes (Sliding Windows)**: Maneja triples con marca de tiempo y operaciones de ventanas deslizantes para análisis de datos basados en tiempo.
- **Codificación de Diccionario Extensible**: Codifica y decodifica términos RDF de manera eficiente usando un diccionario personalizable.
- **API Completa**: Ofrece un conjunto rico de métodos para la manipulación de datos, consultas y procesamiento de resultados.

> [!WARNING]
> El uso de CUDA es experimental y está en desarrollo.

## Instalación

Asegúrate de tener [Rust](https://www.rust-lang.org/tools/install) instalado (versión 1.60 o superior).

Agrega **Kolibrie** a tu `Cargo.toml`:

```toml
[dependencies]
kolibrie = "0.1.0"
```

Luego, inclúyelo en tu proyecto:

```rust
use kolibrie::SparqlDatabase;
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

### Ejecutar Consultas SPARQL

Ejecuta consultas SPARQL para recuperar y manipular datos.

#### Consulta Básica

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
    println!("Sujeto: {}, Objeto: {}", row[0], row[1]);
}
```

#### Insertar Datos

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
// Las operaciones de inserción no retornan resultados
```

### Uso del Volcano Optimizer

El **Volcano Optimizer** está integrado dentro de **Kolibrie** para optimizar los planes de ejecución de consultas basados en la estimación de costos. Transforma planes de consultas lógicas en planes físicos eficientes evaluando diferentes operadores físicos y seleccionando el que tiene el costo estimado más bajo.

#### Ejemplo: Ejecución de Consulta Optimizada

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definir la consulta SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Ejecutar la consulta con plan optimizado
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Persona: {}, Ubicación: {}", row[0], row[1]);
    }
}
```

### Trabajar con el Knowledge Graph

El componente **Knowledge Graph** te permite construir y gestionar redes semánticas con información tanto a nivel de instancia (ABox) como a nivel de esquema (TBox). Soporta inferencia dinámica basada en reglas y backward chaining para derivar nuevos conocimientos a partir de datos existentes.

#### Ejemplo: Construir y Consultar un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Triple};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Añadir triples TBox (esquema)
    kg.add_tbox_triple("http://example.org/Person", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Class");
    kg.add_tbox_triple("http://example.org/knows", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Property");

    // Añadir triples ABox (instancias)
    kg.add_abox_triple("http://example.org/Alice", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");

    // Definir y añadir reglas
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

    // Inferir nuevos hechos basados en reglas
    kg.infer_new_facts();

    // Consultar el Knowledge Graph
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

**Salida:**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
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
}
```

#### Campos

- **triples**: Almacena triples RDF en un conjunto ordenado para consultas eficientes.
- **streams**: Contiene triples con marca de tiempo para consultas de streaming y temporales.
- **sliding_window**: Ventana deslizante opcional para análisis de datos basados en tiempo.
- **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
- **prefixes**: Gestiona prefijos de espacios de nombres para resolver términos prefijados.

### Estructura `VolcanoOptimizer`

La estructura `VolcanoOptimizer` implementa un optimizador de consultas basado en costos según el modelo Volcano. Transforma planes de consultas lógicas en planes físicos eficientes evaluando diferentes operadores físicos y seleccionando el de menor costo estimado.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Campos

- **memo**: Almacena en caché operadores físicos optimizados para evitar cálculos redundantes.
- **selected_variables**: Rastrea las variables seleccionadas en la consulta.
- **stats**: Contiene información estadística sobre la base de datos para ayudar en la estimación de costos.

### Estructura `KnowledgeGraph`

La estructura `KnowledgeGraph` gestiona tanto las afirmaciones ABox (nivel de instancia) como TBox (nivel de esquema), soporta inferencia dinámica basada en reglas y proporciona capacidades de consulta con backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Afirmaciones sobre individuos (instancias)
    pub tbox: BTreeSet<Triple>, // TBox: Conceptos y relaciones (esquema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Lista de reglas dinámicas
}
```

#### Campos

- **abox**: Almacena triples RDF a nivel de instancia.
- **tbox**: Almacena triples RDF a nivel de esquema.
- **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
- **rules**: Contiene reglas dinámicas para la inferencia.

### Métodos Principales

#### `new() -> Self`

Crea una nueva `SparqlDatabase` vacía.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analiza datos RDF/XML desde un archivo especificado y llena la base de datos.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analiza datos RDF/XML desde una cadena.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
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

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Ejecuta una consulta SPARQL contra la base de datos y retorna los resultados.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtra triples basados en una función predicado.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Añade un triple con marca de tiempo a los streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Recupera triples dentro de una ventana de tiempo especificada.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Métodos de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crea una nueva instancia de `VolcanoOptimizer` con datos estadísticos recopilados de la base de datos proporcionada.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Determina el plan de ejecución física más eficiente para un plan de consulta lógica dado.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Métodos de `KnowledgeGraph`

#### `new() -> Self`

Crea un nuevo `KnowledgeGraph` vacío.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple TBox (información a nivel de esquema) al gráfico de conocimiento.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple ABox (información a nivel de instancia) al gráfico de conocimiento.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Añade una regla dinámica al gráfico de conocimiento para la inferencia.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Realiza inferencia basada en reglas para derivar nuevos triples y actualiza la ABox en consecuencia.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la ABox para afirmaciones a nivel de instancia basadas en filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la TBox para afirmaciones a nivel de esquema basadas en filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Ejemplos

### Consulta Básica

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Ejecutar una consulta SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujeto: {}, Objeto: {}", row[0], row[1]);
    }
}
```

**Salida:**
```
Sujeto: http://example.org/Alice, Objeto: http://example.org/Bob
Sujeto: http://example.org/Bob, Objeto: http://example.org/Charlie
```

### Filtrado Avanzado y Agregación

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Ejecutar una consulta SPARQL SELECT con FILTER y GROUP BY
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
        println!("Edad Promedio: {}", row[0]);
    }
}
```

**Salida:**
```
Edad Promedio: 30
```

### Ejecución de Consulta Optimizada con Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definir la consulta SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Ejecutar la consulta con un plan optimizado
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Persona: {}, Ubicación: {}", row[0], row[1]);
    }
}
```

**Salida:**
```
Persona: http://example.org/Alice, Ubicación: http://example.org/Kulak
Persona: http://example.org/Bob, Ubicación: http://example.org/Kortrijk
Persona: http://example.org/Charlie, Ubicación: http://example.org/Ughent
```

### Construir y Consultar un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Añadir triples TBox (esquema)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Añadir triples ABox (instancias)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Definir y añadir reglas
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

    // Inferir nuevos hechos basados en reglas
    let inferred_facts = kg.infer_new_facts();

    // Consultar el Knowledge Graph
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

**Salida:**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
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
}
```

#### Campos

- **triples**: Almacena triples RDF en un conjunto ordenado para consultas eficientes.
- **streams**: Contiene triples con marca de tiempo para consultas de streaming y temporales.
- **sliding_window**: Ventana deslizante opcional para análisis de datos basados en tiempo.
- **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
- **prefixes**: Gestiona prefijos de espacios de nombres para resolver términos prefijados.

### Estructura `VolcanoOptimizer`

La estructura `VolcanoOptimizer` implementa un optimizador de consultas basado en costos según el modelo Volcano. Transforma planes de consultas lógicas en planes físicos eficientes evaluando diferentes operadores físicos y seleccionando el de menor costo estimado.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Campos

- **memo**: Almacena en caché operadores físicos optimizados para evitar cálculos redundantes.
- **selected_variables**: Rastrea las variables seleccionadas en la consulta.
- **stats**: Contiene información estadística sobre la base de datos para ayudar en la estimación de costos.

### Estructura `KnowledgeGraph`

La estructura `KnowledgeGraph` gestiona tanto las afirmaciones ABox (nivel de instancia) como TBox (nivel de esquema), soporta inferencia dinámica basada en reglas y proporciona capacidades de consulta con backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Afirmaciones sobre individuos (instancias)
    pub tbox: BTreeSet<Triple>, // TBox: Conceptos y relaciones (esquema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Lista de reglas dinámicas
}
```

#### Campos

- **abox**: Almacena triples RDF a nivel de instancia.
- **tbox**: Almacena triples RDF a nivel de esquema.
- **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
- **rules**: Contiene reglas dinámicas para la inferencia.

### Métodos Principales

#### `new() -> Self`

Crea una nueva `SparqlDatabase` vacía.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analiza datos RDF/XML desde un archivo especificado y llena la base de datos.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analiza datos RDF/XML desde una cadena.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
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

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Ejecuta una consulta SPARQL contra la base de datos y retorna los resultados.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtra triples basados en una función predicado.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Añade un triple con marca de tiempo a los streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Recupera triples dentro de una ventana de tiempo especificada.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Métodos de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crea una nueva instancia de `VolcanoOptimizer` con datos estadísticos recopilados de la base de datos proporcionada.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Determina el plan de ejecución física más eficiente para un plan de consulta lógica dado.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Métodos de `KnowledgeGraph`

#### `new() -> Self`

Crea un nuevo `KnowledgeGraph` vacío.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple TBox (información a nivel de esquema) al gráfico de conocimiento.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple ABox (información a nivel de instancia) al gráfico de conocimiento.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Añade una regla dinámica al gráfico de conocimiento para la inferencia.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Realiza inferencia basada en reglas para derivar nuevos triples y actualiza la ABox en consecuencia.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la ABox para afirmaciones a nivel de instancia basadas en filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la TBox para afirmaciones a nivel de esquema basadas en filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Ejemplos

### Consulta Básica

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Ejecutar una consulta SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujeto: {}, Objeto: {}", row[0], row[1]);
    }
}
```

**Salida:**
```
Sujeto: http://example.org/Alice, Objeto: http://example.org/Bob
Sujeto: http://example.org/Bob, Objeto: http://example.org/Charlie
```

### Filtrado Avanzado y Agregación

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Ejecutar una consulta SPARQL SELECT con FILTER y GROUP BY
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
        println!("Edad Promedio: {}", row[0]);
    }
}
```

**Salida:**
```
Edad Promedio: 30
```

### Ejecución de Consulta Optimizada con Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analizar datos Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Definir la consulta SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Ejecutar la consulta con un plan optimizado
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Persona: {}, Ubicación: {}", row[0], row[1]);
    }
}
```

**Salida:**
```
Persona: http://example.org/Alice, Ubicación: http://example.org/Kulak
Persona: http://example.org/Bob, Ubicación: http://example.org/Kortrijk
Persona: http://example.org/Charlie, Ubicación: http://example.org/Ughent
```

### Construir y Consultar un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Añadir triples TBox (esquema)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Añadir triples ABox (instancias)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Definir y añadir reglas
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

    // Inferir nuevos hechos basados en reglas
    let inferred_facts = kg.infer_new_facts();

    // Consultar el Knowledge Graph
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

**Salida:**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
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
}
```

#### Campos

- **triples**: Almacena triples RDF en un conjunto ordenado para consultas eficientes.
- **streams**: Contiene triples con marca de tiempo para consultas de streaming y temporales.
- **sliding_window**: Ventana deslizante opcional para análisis de datos basados en tiempo.
- **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
- **prefixes**: Gestiona prefijos de espacios de nombres para resolver términos prefijados.

### Estructura `VolcanoOptimizer`

La estructura `VolcanoOptimizer` implementa un optimizador de consultas basado en costos según el modelo Volcano. Transforma planes de consultas lógicas en planes físicos eficientes evaluando diferentes operadores físicos y seleccionando el de menor costo estimado.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Campos

- **memo**: Almacena en caché operadores físicos optimizados para evitar cálculos redundantes.
- **selected_variables**: Rastrea las variables seleccionadas en la consulta.
- **stats**: Contiene información estadística sobre la base de datos para ayudar en la estimación de costos.

### Estructura `KnowledgeGraph`

La estructura `KnowledgeGraph` gestiona tanto las afirmaciones ABox (nivel de instancia) como TBox (nivel de esquema), soporta inferencia dinámica basada en reglas y proporciona capacidades de consulta con backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox: Afirmaciones sobre individuos (instancias)
    pub tbox: BTreeSet<Triple>, // TBox: Conceptos y relaciones (esquema)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Lista de reglas dinámicas
}
```

#### Campos

- **abox**: Almacena triples RDF a nivel de instancia.
- **tbox**: Almacena triples RDF a nivel de esquema.
- **dictionary**: Codifica y decodifica términos RDF para eficiencia de almacenamiento.
- **rules**: Contiene reglas dinámicas para la inferencia.

### Métodos Principales

#### `new() -> Self`

Crea una nueva `SparqlDatabase` vacía.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analiza datos RDF/XML desde un archivo especificado y llena la base de datos.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analiza datos RDF/XML desde una cadena.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
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

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Ejecuta una consulta SPARQL contra la base de datos y retorna los resultados.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtra triples basados en una función predicado.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Añade un triple con marca de tiempo a los streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Recupera triples dentro de una ventana de tiempo especificada.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Métodos de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crea una nueva instancia de `VolcanoOptimizer` con datos estadísticos recopilados de la base de datos proporcionada.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Determina el plan de ejecución física más eficiente para un plan de consulta lógica dado.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Métodos de `KnowledgeGraph`

#### `new() -> Self`

Crea un nuevo `KnowledgeGraph` vacío.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple TBox (información a nivel de esquema) al gráfico de conocimiento.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Añade un triple ABox (información a nivel de instancia) al gráfico de conocimiento.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Añade una regla dinámica al gráfico de conocimiento para la inferencia.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Realiza inferencia basada en reglas para derivar nuevos triples y actualiza la ABox en consecuencia.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la ABox para afirmaciones a nivel de instancia basadas en filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Consulta la TBox para afirmaciones a nivel de esquema basadas en filtros opcionales de sujeto, predicado y objeto.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Rendimiento

**Kolibrie** está optimizado para alto rendimiento mediante:

- **Análisis y Procesamiento Paralelo**: Utiliza Rayon y Crossbeam para el análisis de datos multi-threaded y la ejecución de consultas.
- **Instrucciones SIMD**: Implementa operaciones SIMD para acelerar tareas de filtrado y agregación.
- **Volcano Optimizer**: Emplea un optimizador de consultas basado en costos para generar planes de ejecución física eficientes, minimizando el tiempo de ejecución de consultas.
- **Inferencia del Knowledge Graph**: Aprovecha la inferencia basada en reglas y backward chaining para derivar nuevos conocimientos sin una sobrecarga significativa de rendimiento.
- **Estructuras de Datos Eficientes**: Utiliza `BTreeSet` para almacenamiento ordenado y `HashMap` para la gestión de prefijos, asegurando una rápida recuperación y manipulación de datos.
- **Optimización de Memoria**: Utiliza codificación de diccionario para minimizar la huella de memoria reutilizando términos repetidos.

Las pruebas de benchmarking indican ganancias de rendimiento significativas en grandes conjuntos de datos RDF en comparación con motores SPARQL tradicionales de un solo hilo.

### Kolibrie vs. Oxigraph vs. RDFlib vs. Apache Jena (100K triples RDF/XML)
**Tiempo para cargar datos RDF**
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

**Tiempo para ejecutar consultas SPARQL**
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

**Resumen de Kolibrie**

- **Tiempo Total de Análisis**: 15.29 segundos
- **Tiempo Total de Ejecución de Consultas**: 4.31 segundos
- **Tiempo Promedio de Análisis**: 0.76 segundos
- **Tiempo Promedio de Ejecución de Consultas**: 0.22 segundos

**Resumen de Oxigraph**

- **Tiempo Total de Carga RDF**: 205.21 segundos
- **Tiempo Total de Ejecución de Consultas**: 19.71 segundos
- **Tiempo Promedio de Carga RDF**: 10.26 segundos
- **Tiempo Promedio de Ejecución de Consultas**: 0.99 segundos

**Resumen de RDFlib**

- **Tiempo Total de Carga RDF**: 588.86 segundos
- **Tiempo Total de Ejecución de Consultas SPARQL**: 0.54 segundos
- **Tiempo Promedio de Carga RDF**: 29.44 segundos
- **Tiempo Promedio de Ejecución de Consultas SPARQL**: 27.17ms

**Resumen de Apache Jena**

- **Tiempo Total de Carga RDF**: 43.07 segundos
- **Tiempo Total de Ejecución de Consultas SPARQL**: 15.23 segundos
- **Tiempo Promedio de Carga RDF**: 2.15 segundos
- **Tiempo Promedio de Ejecución de Consultas SPARQL**: 761.74ms

## Cómo Contribuir

### Envío de Problemas
Utiliza el Rastreador de Problemas (Issue Tracker) para enviar reportes de errores y solicitudes de nuevas funcionalidades/mejoras. Antes de enviar un nuevo problema, asegúrate de que no exista un issue similar abierto.

### Pruebas Manuales
¡Se agradece enormemente que cualquier persona que pruebe el código manualmente y reporte errores o sugerencias de mejoras en el Rastreador de Problemas contribuya!

### Envío de Pull Requests
Se aceptan parches/correcciones en forma de pull requests (PRs). Asegúrate de que el issue que el pull request aborda esté abierto en el Rastreador de Problemas.

El pull request enviado se considera que ha aceptado publicarse bajo la Licencia Pública de Mozilla, versión 2.0.

## Licencia

Kolibrie está licenciado bajo la [Licencia MPL-2.0](LICENSE).