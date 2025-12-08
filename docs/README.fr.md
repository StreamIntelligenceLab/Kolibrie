# Kolibrie

<p align="center">
    <img src="logo/kolibrie.jfif" width="400" height="400" />
</p>

<!-- ![GitHub Workflow Status](https://img.shields.io/github/commit-activity/t/StreamIntelligenceLab/Kolibri) -->
[![Status](https://img.shields.io/badge/status-stable-blue.svg)](https://github.com/StreamIntelligenceLab/Kolibrie/tree/main)
![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)
![Rust Version](https://img.shields.io/badge/Rust-1.60+-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
[![Chat Server](https://img.shields.io/badge/chat-discord-7289da.svg)](https://discord.gg/KcFXrUUyYm)
<!--![Crates.io](https://img.shields.io/crates/v/sparql_database.svg)-->

[ [English](../README.md) | [Nederlands](README.nl.md) | [Deutsch](README.de.md) | [Español](README.es.md) | [Français](README.fr.md) | [日本語](README.ja.md) ]

**Kolibrie** est un moteur de requêtes SPARQL haute performance, concurrent et riche en fonctionnalités, implémenté en Rust. Conçu pour la scalabilité et l’efficacité, il exploite le modèle de concurrence robuste de Rust ainsi que des optimisations avancées, notamment SIMD (Single Instruction, Multiple Data) et le traitement parallèle avec Rayon, afin de gérer efficacement des ensembles de données RDF (Resource Description Framework) à grande échelle.

Avec une API complète, **Kolibrie** facilite l’analyse, le stockage et l’interrogation de données RDF via les formats SPARQL, Turtle et N3. Ses capacités avancées de filtrage, d’agrégation, de jointure et ses stratégies d’optimisation sophistiquées en font un choix adapté aux applications nécessitant un traitement sémantique complexe. L’intégration de l’optimiseur Volcano et des fonctionnalités de raisonnement permet une planification de requêtes rentable et l’exploitation de l’inférence basée sur des règles pour enrichir les analyses.

## Contexte de Recherche

**Kolibrie** est développé au sein du [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) à la KU Leuven, sous la supervision du Prof. Pieter Bonte. Le Stream Intelligence Lab se concentre sur le **Stream Reasoning**, un domaine de recherche émergent qui combine des techniques logiques issues de l’IA avec des approches d’apprentissage automatique guidées par les données afin de produire des connaissances opportunes et exploitables à partir de flux continus. Notre recherche met l’accent sur des applications liées à l’Internet des Objets (IoT) et au traitement en périphérie (Edge), pour la prise de décision en temps réel dans des environnements dynamiques tels que les véhicules autonomes, la robotique ou l’analyse web.

Pour plus d’informations, veuillez consulter le [site web du Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab).

## Fonctionnalités

- **Analyse RDF Efficace** : prise en charge de RDF/XML, Turtle et N3 avec une gestion robuste des erreurs et des préfixes.
- **Traitement Concurrent** : utilisation de Rayon et Crossbeam pour le traitement parallèle, optimisé pour les systèmes multi-cœurs.
- **Optimisations SIMD** : accélération du filtrage et des agrégations grâce aux instructions SIMD.
- **Requêtes Flexibles** : support des requêtes SPARQL complexes : SELECT, INSERT, FILTER, GROUP BY, VALUES, etc.
- **Volcano Optimizer** : optimiseur de requêtes basé sur les coûts suivant le modèle Volcano pour sélectionner des plans d’exécution efficaces.
- **Reasoner** : support robuste du raisonnement sur graphes de connaissance, incluant ABox (niveau instance) et TBox (niveau schéma), inférence dynamique par règles et backward chaining.
- **Streaming et Fenêtres Glissantes** : gestion de triples horodatés et d’opérations de fenêtres pour l’analyse temporelle.
- **Codage de Dictionnaire Extensible** : encodage/décodage efficace des termes RDF via un dictionnaire personnalisable.
- **API Complète** : un ensemble riche de méthodes pour manipuler les données, exécuter des requêtes et traiter les résultats.

> [!WARNING]
> L’utilisation de CUDA est expérimentale et en cours de développement.

## Installation

### Installation Native

Assurez-vous d’avoir [Rust](https://www.rust-lang.org/tools/install) installé (version 1.60 ou supérieure).

Clonez le dépôt :

```bash
git clone https://github.com/StreamIntelligenceLab/Kolibrie.git
cd Kolibrie
```

Construisez le projet :

```bash
cargo build --release
```

Puis, incluez-le dans votre projet :

```rust
use kolibrie::SparqlDatabase;
```

### Installation Docker

**Kolibrie** fournit un support Docker avec plusieurs configurations pour différents cas d’usage. L’environnement Docker gère automatiquement toutes les dépendances, y compris Rust, CUDA (pour les builds GPU) et les frameworks Python ML.

#### Prérequis

* [Docker](https://docs.docker.com/get-docker/) installé
* [Docker Compose](https://docs.docker.com/compose/install/) installé
* Pour le support GPU : [NVIDIA Docker runtime](https://github.com/NVIDIA/nvidia-docker) installé

#### Démarrage Rapide

1. **Build CPU uniquement** (recommandé pour la plupart des utilisateurs) :

```bash
docker compose --profile cpu up --build
```

2. **Build avec GPU activé** (nécessite un GPU NVIDIA et nvidia-docker) :

```bash
docker compose --profile gpu up --build
```

3. **Build de développement** (détecte automatiquement la disponibilité GPU) :

```bash
docker compose --profile dev up --build
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

**Kolibrie** supporte l’analyse de données RDF depuis des fichiers ou des chaînes.

#### Analyser RDF/XML depuis un Fichier

```rust
db.parse_rdf_from_file("data.rdf");
```

#### Analyser RDF/XML depuis une Chaîne

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

#### Analyser des Données N-Triples depuis une Chaîne

```rust
let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> . 
<http://example.org/jane> <http://example.org/name> "Jane Doe" . 
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
"#;

db.parse_ntriples_and_add(ntriples_data);
```

### Ajouter des Triples par Code

Ajoutez des triples individuellement :

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

### Exécuter des Requêtes SPARQL

#### Requête SELECT de Base

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
    println!("Sujet: {}, Objet: {}", row[0], row[1]);
}
```

#### Requête avec FILTER

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
    println! ("Événement: {}, Participants: {}", row[0], row[1]);
}
```

#### Requête avec Opérateur OR

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
        println!("Nom: {}, Type: {}, Participants: {}", name, type_, attendees);
    }
}
```

#### Requête avec LIMIT

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
    println!("Nom: {}, Type: {}", row[0], row[1]);
}
```

#### Requête avec Agrégations

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
        println!("Salaire moyen: {}", avg_salary);
    }
}
```

**Agrégations supportées :**

* `AVG(?var)` - moyenne
* `COUNT(?var)` - comptage
* `SUM(?var)` - somme
* `MIN(?var)` - minimum
* `MAX(?var)` - maximum

#### Requête avec Fonctions de Chaînes

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
    println!("Nom complet: {}", row[0]);
}
```

#### Requêtes Imbriquées

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
    println!("Ami d'Alice: {}", row[0]);
}
```

### Utiliser l’API Query Builder

L’API Query Builder fournit une interface fluide pour construire des requêtes par code.

#### Construction de Requête de Base

```rust
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/name")
    .get_objects();

for object in results {
    println!("Nom: {}", object);
}
```

#### Requête avec Filtrage

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
    println!("{} a {} ans", subject, object);
}
```

#### Requête avec Jointures

```rust
let other_db = SparqlDatabase::new();
// ...  peupler other_db ...

let results = db.query()
    .join(&other_db)
    .join_on_subject()
    .get_triples();
```

#### Tri, Limite, Offset et Distinct

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

### Utiliser le Volcano Optimizer

Le **Volcano Optimizer** transforme des plans logiques en plans physiques efficaces en évaluant différentes stratégies de jointure et en sélectionnant l’exécution la plus performante selon une estimation de coûts.

```rust
use kolibrie::execute_query::*;
use kolibrie::sparql_database::*;

fn main() {
    let mut db = SparqlDatabase::new();

    let ntriples_data = r#"
<http://example.org/john> <http://example.org/hasFriend> <http://example.org/jane> .
<http://example.org/jane> <http://example.org/name> "Jane Doe" .
<http://example.org/john> <http://example.org/name> "John Smith" . 
<http://example.org/jane> <http://example.org/age> "25"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/john> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
    "#;

    db.parse_ntriples_and_add(ntriples_data);
    db.get_or_build_stats();

    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?friend ?friendName
    WHERE {
        ?person ex:hasFriend ?friend .
        ?friend ex:name ?friendName .
    }
    "#;

    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Personne: {}, Ami: {}, Nom de l'ami: {}", row[0], row[1], row[2]);
    }
}
```

### Travailler avec le Reasoner

Le **Reasoner** permet de construire et gérer des réseaux sémantiques avec des assertions ABox et de l’inférence dynamique basée sur des règles, incluant le forward chaining, le backward chaining et l’évaluation semi-naïve.

```rust
use datalog::knowledge_graph::Reasoner;
use shared::terms::Term;
use shared::rule::Rule;

fn main() {
    let mut kg = Reasoner::new();

    kg.add_abox_triple("Alice", "parentOf", "Bob");
    kg.add_abox_triple("Bob", "parentOf", "Charlie");

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

    let inferred_facts = kg.infer_new_facts();
    println!("Inféré {} nouveaux faits", inferred_facts.len());

    let results = kg.query_abox(
        Some("Alice"),
        Some("ancestorOf"),
        None,
    );

    for triple in results {
        println!(
            "{} est un ancêtre de {}",
            kg.dictionary.decode(triple.subject).unwrap(),
            kg.dictionary.decode(triple.object).unwrap()
        );
    }
}
```

## Documentation de l’API

### Structure `SparqlDatabase`

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

#### Champs

* **triples** : stockage des triples RDF dans un ensemble trié.
* **streams** : triples horodatés pour le streaming et les requêtes temporelles.
* **sliding_window** : fenêtre glissante optionnelle.
* **dictionary** : encodage/décodage des termes RDF.
* **prefixes** : gestion des préfixes d’espaces de noms.
* **udfs** : registre de fonctions définies par l’utilisateur.
* **index_manager** : système d’indexation unifié pour optimiser les requêtes.
* **rule_map** : mappage des noms de règles vers leurs définitions.
* **cached_stats** : statistiques mises en cache pour l’optimisation.

### Structure `VolcanoOptimizer`

```rust
pub struct VolcanoOptimizer<'a> {
    pub stats: Arc<DatabaseStats>,
    pub memo: HashMap<String, (PhysicalOperator, f64)>,
    pub selected_variables: Vec<String>,
    database: &'a SparqlDatabase,
}
```

### Structure `Reasoner`

```rust
pub struct Reasoner {
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>,
    pub index_manager: UnifiedIndex,
    pub rule_index: RuleIndex,
    pub constraints: Vec<Rule>,
}
```

## Performance

**Kolibrie** vise des performances élevées grâce à :

* **Analyse et exécution parallèles** via Rayon et Crossbeam.
* **Instructions SIMD** pour accélérer le filtrage et les agrégations.
* **Optimisation basée sur les coûts** avec Volcano.
* **Inférence efficace** sans surcharge excessive.
* **Structures de données adaptées** et **optimisation mémoire** par dictionnaire.

### Résultats de Benchmark

Nos benchmarks montrent que Kolibrie surpasse plusieurs moteurs RDF populaires.

Tests réalisés avec :

* **Dataset** : benchmark [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 10M triples
* **Configuration Oxigraph** : backend RocksDB
* **Raisonnement sur taxonomie profonde** : profondeur hiérarchique jusqu’à 10K niveaux

#### WatDiv 10M - Comparaison des performances (20 exécutions par requête)

![WatDiv 10M Query Performance](img/image1.png)

*Figure 1 : temps d’exécution des requêtes pour différents moteurs SPARQL avec WatDiv 10M*

**Constats clés :**

* Kolibrie surpasse de manière constante plusieurs concurrents sur les requêtes L1-L5, S1-S7, F1-F3, C1-C3.
* Temps d’exécution moyen : **de la sous-milliseconde à quelques millisecondes**.
* Blazegraph et QLever restent compétitifs sur certains motifs.
* Oxigraph (avec RocksDB) présente une performance stable.

#### Taxonomie Profonde - Raisonnement selon la profondeur hiérarchique

![Deep Taxonomy Reasoning Performance](img/image2.png)

*Figure 2 : performances de raisonnement selon la profondeur (10, 100, 1K, 10K niveaux)*

**Constats clés :**

* Kolibrie présente une **croissance logarithmique** avec la profondeur.
* À 10K niveaux, Kolibrie conserve des temps de réponse inférieurs à la seconde.
* Meilleures performances que Apache Jena et le reasoner EYE.

## Comment Contribuer

### Soumettre des Problèmes

Utilisez le gestionnaire d’issues pour signaler des bugs et proposer des fonctionnalités/améliorations. Avant de créer une nouvelle issue, vérifiez qu’un problème similaire n’est pas déjà ouvert.

### Tests Manuels

Toute contribution via tests manuels et retours d’expérience est la bienvenue !

### Soumettre des Pull Requests

Les correctifs/améliorations sont acceptés sous forme de pull requests (PR). Assurez-vous que l’issue correspondante est ouverte.

Toute PR soumise est considérée comme acceptant une publication sous la Mozilla Public License Version 2.0.

## Communauté

Rejoignez notre [communauté Discord](https://discord.gg/KcFXrUUyYm) pour discuter de Kolibrie, poser des questions et partager vos retours.

## Licence

Kolibrie est distribué sous la [Licence MPL-2.0](LICENSE).
