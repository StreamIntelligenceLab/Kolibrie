# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![État du Flux de Travail GitHub](https://img.shields.io/github/commit-activity/t/ladroid/goku) -->
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Version de Rust](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Statut de Compilation](https://img.shields.io/badge/build-passing-brightgreen.svg)
![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** est un moteur de requêtes SPARQL puissant, concurrent et riche en fonctionnalités, implémenté en Rust. Conçu pour la scalabilité et l'efficacité, il exploite le robuste modèle de concurrence de Rust ainsi que des optimisations avancées, y compris SIMD (Single Instruction, Multiple Data) et le traitement parallèle avec Rayon, pour gérer de manière fluide des ensembles de données RDF (Resource Description Framework) à grande échelle.

Avec une API complète, **Kolibrie** facilite l'analyse, le stockage et la requête de données RDF en utilisant les formats SPARQL, Turtle et N3. Ses filtres avancés, agrégations, opérations de jointure et stratégies d'optimisation sophistiquées en font un choix adapté pour les applications nécessitant un traitement complexe de données sémantiques. De plus, l'intégration du Volcano Optimizer et des fonctionnalités de Knowledge Graph permet aux utilisateurs d'effectuer une planification de requêtes rentable et de tirer parti de l'inférence basée sur des règles pour des analyses de données avancées.

## Contexte de Recherche

**Kolibrie** est développé au sein du [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) à la KU Leuven sous la direction du Prof. Pieter Bonte. Le Stream Intelligence Lab se concentre sur le **Stream Reasoning**, un domaine de recherche émergent qui intègre des techniques logiques de l'intelligence artificielle avec des approches d'apprentissage automatique basées sur les données pour dériver des connaissances opportunes et exploitables à partir de flux de données continus. Notre recherche met l'accent sur des applications dans l'Internet des Objets (IoT) et le traitement en périphérie (Edge Processing), permettant des prises de décision en temps réel dans des environnements dynamiques tels que les véhicules autonomes, la robotique et l'analyse web.

Pour plus d'informations sur notre recherche et nos projets en cours, veuillez visiter le [site web du Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Fonctionnalités

- **Analyse RDF Efficace** : Supporte l'analyse des formats RDF/XML, Turtle et N3 avec une gestion robuste des erreurs et des préfixes.
- **Traitement Concurrent** : Utilise Rayon et Crossbeam pour le traitement parallèle des données, assurant des performances optimales sur les systèmes multi-cœurs.
- **Optimisations SIMD** : Implémente des instructions SIMD pour accélérer le filtrage et l'agrégation des requêtes.
- **Requêtes Flexibles** : Supporte des requêtes SPARQL complexes, y compris les clauses SELECT, INSERT, FILTER, GROUP BY et VALUES.
- **Volcano Optimizer** : Intègre un optimiseur de requêtes basé sur les coûts selon le modèle Volcano pour déterminer les plans d'exécution les plus efficaces.
- **Knowledge Graph** : Fournit un support robuste pour la construction et la requête de graphes de connaissance, incluant les assertions ABox (niveau d'instance) et TBox (niveau de schéma), l'inférence dynamique basée sur des règles et le backward chaining.
- **Streaming et Fenêtres Glissantes (Sliding Windows)** : Gère les triples avec timestamp et les opérations de fenêtres glissantes pour les analyses de données temporelles.
- **Codage de Dictionnaire Extensible** : Encode et décode efficacement les termes RDF à l'aide d'un dictionnaire personnalisable.
- **API Complète** : Offre un ensemble riche de méthodes pour la manipulation des données, les requêtes et le traitement des résultats.

> [!WARNING]
> L'utilisation de CUDA est expérimentale et en développement.

## Installation

Assurez-vous d'avoir [Rust](https://www.rust-lang.org/tools/install) installé (version 1.60 ou supérieure).

Ajoutez **Kolibrie** à votre `Cargo.toml` :

```toml
[dependencies]
kolibrie = "0.1.0"
```

Ensuite, incluez-le dans votre projet :

```rust
use kolibrie::SparqlDatabase;
```

## Utilisation

### Initialiser la Base de Données

Créez une nouvelle instance de `SparqlDatabase` :

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // Votre code ici
}
```

### Analyser des Données RDF

**Kolibrie** supporte l'analyse de données RDF depuis des fichiers ou des chaînes dans divers formats.

#### Analyser RDF/XML depuis un Fichier

```rust
db.parse_rdf_from_file("data.rdf");
```

#### Analyser des Données Turtle depuis une Chaîne

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_turtle(turtle_data);
```

#### Analyser des Données N3 depuis une Chaîne

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_n3(n3_data);
```

### Exécuter des Requêtes SPARQL

Exécutez des requêtes SPARQL pour récupérer et manipuler des données.

#### Requête de Base

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
    println!("Sujet: {}, Objet: {}", row[0], row[1]);
}
```

#### Insertion de Données

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
// Les opérations d'insertion ne retournent pas de résultats
```

### Utilisation du Volcano Optimizer

Le **Volcano Optimizer** est intégré dans **Kolibrie** pour optimiser les plans d'exécution des requêtes basés sur l'estimation des coûts. Il transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

#### Exemple : Exécution de Requête Optimisée

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

### Travailler avec le Knowledge Graph

La composante **Knowledge Graph** vous permet de construire et gérer des réseaux sémantiques avec des informations à la fois au niveau de l'instance (ABox) et du schéma (TBox). Elle supporte l'inférence dynamique basée sur des règles et le backward chaining pour dériver de nouvelles connaissances à partir des données existantes.

#### Exemple : Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Triple};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Class");
    kg.add_tbox_triple("http://example.org/knows", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Documentation de l'API

### Structure `SparqlDatabase`

La structure `SparqlDatabase` est le composant central qui représente le stockage RDF et fournit des méthodes pour la manipulation des données et les requêtes.

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### Champs

- **triples** : Stocke les triples RDF dans un ensemble trié pour des requêtes efficaces.
- **streams** : Contient des triples horodatés pour les requêtes de streaming et temporelles.
- **sliding_window** : Fenêtre glissante optionnelle pour les analyses de données temporelles.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **prefixes** : Gère les préfixes des espaces de noms pour résoudre les termes abrégés.

### Structure `VolcanoOptimizer`

La structure `VolcanoOptimizer` implémente un optimiseur de requêtes basé sur les coûts selon le modèle Volcano. Elle transforme les plans de requêtes logiques en plans physiques efficaces en évaluant différents opérateurs physiques et en sélectionnant celui avec les coûts estimés les plus bas.

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### Champs

- **memo** : Met en cache les opérateurs physiques optimisés pour éviter les calculs redondants.
- **selected_variables** : Suit les variables sélectionnées dans la requête.
- **stats** : Contient des informations statistiques sur la base de données pour aider à l'estimation des coûts.

### Structure `KnowledgeGraph`

La structure `KnowledgeGraph` gère à la fois les assertions ABox (niveau d'instance) et TBox (niveau de schéma), supporte l'inférence dynamique basée sur des règles et offre des fonctionnalités de requête avec backward chaining.

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox : Assertions sur les individus (instances)
    pub tbox: BTreeSet<Triple>, // TBox : Concepts et relations (schéma)
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // Liste des règles dynamiques
}
```

#### Champs

- **abox** : Stocke les triples RDF à niveau d'instance.
- **tbox** : Stocke les triples RDF à niveau de schéma.
- **dictionary** : Encode et décode les termes RDF pour optimiser le stockage.
- **rules** : Contient des règles dynamiques pour l'inférence.

### Méthodes Principales

#### `new() -> Self`

Crée une nouvelle `SparqlDatabase` vide.

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

Analyse des données RDF/XML depuis un fichier spécifié et remplit la base de données.

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

Analyse des données RDF/XML depuis une chaîne.

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

Analyse des données RDF au format Turtle depuis une chaîne.

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

Analyse des données RDF au format N3 depuis une chaîne.

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

Exécute une requête SPARQL contre la base de données et retourne les résultats.

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

Filtre les triples basés sur une fonction prédicat.

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

Ajoute un triple horodaté aux streams.

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

Récupère les triples dans une fenêtre de temps spécifiée.

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### Méthodes de `VolcanoOptimizer`

#### `new(database: &SparqlDatabase) -> Self`

Crée une nouvelle instance de `VolcanoOptimizer` avec des données statistiques collectées à partir de la base de données fournie.

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

Détermine le plan d'exécution physique le plus efficace pour un plan de requête logique donné.

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### Méthodes de `KnowledgeGraph`

#### `new() -> Self`

Crée un nouveau `KnowledgeGraph` vide.

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple TBox (information au niveau du schéma) au graphe de connaissance.

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

Ajoute un triple ABox (information au niveau de l'instance) au graphe de connaissance.

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

Ajoute une règle dynamique au graphe de connaissance pour l'inférence.

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

Effectue une inférence basée sur des règles pour dériver de nouveaux triples et met à jour l'ABox en conséquence.

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge l'ABox pour des assertions au niveau de l'instance basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

Interroge la TBox pour des assertions au niveau du schéma basées sur des filtres optionnels de sujet, prédicat et objet.

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## Exemples

### Requête de Base

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?s ?o
    WHERE {
        ?s ex:knows ?o .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Sujet: {}, Objet: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Sujet: http://example.org/Alice, Objet: http://example.org/Bob
Sujet: http://example.org/Bob, Objet: http://example.org/Charlie
```

### Filtrage Avancé et Agrégation

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // Exécuter une requête SPARQL SELECT avec FILTER et GROUP BY
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
        println!("Âge Moyen: {}", row[0]);
    }
}
```

**Sortie :**
```
Âge Moyen: 30
```

### Exécution de Requête Optimisée avec Volcano Optimizer

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Analyser des données Turtle
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // Définir la requête SPARQL
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // Exécuter la requête avec un plan optimisé
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Emplacement: {}", row[0], row[1]);
    }
}
```

**Sortie :**
```
Personne: http://example.org/Alice, Emplacement: http://example.org/Kulak
Personne: http://example.org/Bob, Emplacement: http://example.org/Kortrijk
Personne: http://example.org/Charlie, Emplacement: http://example.org/Ughent
```

### Construction et Requête d'un Knowledge Graph

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // Ajouter des triples TBox (schéma)
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // Ajouter des triples ABox (instances)
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // Définir et ajouter des règles
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

    // Inférer de nouveaux faits basés sur des règles
    let inferred_facts = kg.infer_new_facts();

    // Requêter le Knowledge Graph
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

**Sortie :**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## Performance

**Kolibrie** est optimisé pour des performances élevées grâce à :

- **Analyse et Traitement Parallèle** : Utilise Rayon et Crossbeam pour l'analyse des données multi-thread et l'exécution des requêtes.
- **Instructions SIMD** : Implémente des opérations SIMD pour accélérer les tâches de filtrage et d'agrégation.
- **Volcano Optimizer** : Utilise un optimiseur de requêtes basé sur les coûts pour générer des plans d'exécution physique efficaces, minimisant le temps d'exécution des requêtes.
- **Inférence du Knowledge Graph** : Exploite l'inférence basée sur des règles et le backward chaining pour dériver de nouvelles connaissances sans surcharge significative des performances.
- **Structures de Données Efficaces** : Utilise `BTreeSet` pour le stockage ordonné et `HashMap` pour la gestion des préfixes, assurant une récupération et une manipulation rapides des données.
- **Optimisation Mémoire** : Utilise le codage de dictionnaire pour minimiser l'empreinte mémoire en réutilisant les termes répétés.

Les benchmarks montrent des gains de performance significatifs sur de grands ensembles de données RDF comparés aux moteurs SPARQL traditionnels à thread unique.

### Kolibrie vs. Oxigraph vs. RDFlib vs. Apache Jena (100K triples RDF/XML)
**Temps pour charger les données RDF**
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

**Temps pour exécuter des requêtes SPARQL**
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

**Résumé de Kolibrie**

- **Temps Total d'Analyse** : 15,29 secondes
- **Temps Total d'Exécution des Requêtes** : 4,31 secondes
- **Temps Moyen d'Analyse** : 0,76 secondes
- **Temps Moyen d'Exécution des Requêtes** : 0,22 secondes

**Résumé d'Oxigraph**

- **Temps Total de Chargement RDF** : 205,21 secondes
- **Temps Total d'Exécution des Requêtes** : 19,71 secondes
- **Temps Moyen de Chargement RDF** : 10,26 secondes
- **Temps Moyen d'Exécution des Requêtes** : 0,99 secondes

**Résumé de RDFlib**

- **Temps Total de Chargement RDF** : 588,86 secondes
- **Temps Total d'Exécution des Requêtes SPARQL** : 0,54 secondes
- **Temps Moyen de Chargement RDF** : 29,44 secondes
- **Temps Moyen d'Exécution des Requêtes SPARQL** : 27,17ms

**Résumé d'Apache Jena**

- **Temps Total de Chargement RDF** : 43,07 secondes
- **Temps Total d'Exécution des Requêtes SPARQL** : 15,23 secondes
- **Temps Moyen de Chargement RDF** : 2,15 secondes
- **Temps Moyen d'Exécution des Requêtes SPARQL** : 761,74ms

## Comment Contribuer

### Soumettre des Problèmes
Utilisez le Gestionnaire de Problèmes (Issue Tracker) pour soumettre des rapports de bugs et des demandes de nouvelles fonctionnalités/améliorations. Assurez-vous qu'il n'existe pas de problème similaire ouvert avant de soumettre un nouveau problème.

### Tests Manuels
Toute personne qui teste manuellement le code et signale des bugs ou des suggestions d'améliorations dans le Gestionnaire de Problèmes est très la bienvenue !

### Soumettre des Pull Requests
Les correctifs/améliorations sont acceptés sous forme de pull requests (PRs). Assurez-vous que le problème que le pull request adresse est ouvert dans le Gestionnaire de Problèmes.

Les pull requests soumises sont considérées comme ayant accepté de publier sous la Mozilla Public License Version 2.0.

## Licence

Kolibrie est licencié sous la [Licence MPL-2.0](LICENSE).