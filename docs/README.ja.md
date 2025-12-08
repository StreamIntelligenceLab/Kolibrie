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

**Kolibrie** は、Rustで実装された高性能・高並行・機能豊富なSPARQLクエリエンジンです。スケーラビリティと効率性を念頭に設計されており、Rustの堅牢な並行モデルに加えて、SIMD（Single Instruction, Multiple Data）やRayonによる並列処理などの最適化を活用し、大規模なRDF（Resource Description Framework）データセットをスムーズに処理します。

包括的なAPIを備えた **Kolibrie** は、SPARQL、Turtle、N3形式のRDFデータの解析・保存・クエリを容易にします。高度なフィルタリング、集約、結合操作、洗練された最適化戦略により、複雑なセマンティックデータ処理が求められるアプリケーションに適しています。さらに、Volcano Optimizerの統合とReasoner（知識グラフ推論）機能によって、コストに基づく効率的なクエリプランニングとルールベース推論を活用した高度なデータ分析が可能になります。

## Research Context（研究の背景）

**Kolibrie** は、KU Leuvenの [Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab) にて、Pieter Bonte教授の指導のもと開発されています。Stream Intelligence Labは **Stream Reasoning（ストリーム推論）** に焦点を当てており、継続的なデータストリームからタイムリーで実用的な知識を導出するために、AIにおける論理的手法とデータ駆動型の機械学習アプローチを統合する新興研究分野です。私たちの研究は、IoTやエッジ処理領域の応用に重点を置き、自動運転車、ロボティクス、ウェブ分析などの動的な環境におけるリアルタイム意思決定を可能にします。

研究や進行中プロジェクトの詳細は、[Stream Intelligence Labのウェブサイト](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab)をご覧ください。

## Features（特徴）

- **Efficient RDF Parsing**: RDF/XML、Turtle、N3形式の解析をサポートし、堅牢なエラーハンドリングとプレフィックス管理を提供します。
- **Concurrent Processing**: RayonとCrossbeamを活用した並列データ処理で、マルチコア環境における最適なパフォーマンスを実現します。
- **SIMD Optimizations**: フィルタリングや集約を高速化するためのSIMD最適化を実装しています。
- **Flexible Querying**: SELECT、INSERT、FILTER、GROUP BY、VALUES などを含む複雑なSPARQLクエリに対応します。
- **Volcano Optimizer**: Volcanoモデルに基づくコストベース最適化で、最も効率的な実行プランを選択します。
- **Reasoner**: ABox（インスタンス）およびTBox（スキーマ）のアサーションを含む知識グラフの構築とクエリに対応し、動的ルール推論とバックワードチェイニングを提供します。
- **Streaming and Sliding Windows**: タイムスタンプ付きトリプルおよびスライディングウィンドウを扱う時間的分析をサポートします。
- **Extensible Dictionary Encoding**: カスタマイズ可能な辞書によりRDF用語を効率的にエンコード/デコードします。
- **Comprehensive API**: データ操作、クエリ、結果処理のための豊富なメソッド群を提供します。

> [!WARNING]
> CUDAの利用は実験的で、現在開発中です。

## Installation（インストール）

### Native Installation（ネイティブ）

[Rust](https://www.rust-lang.org/tools/install)（1.60以上）をインストールしてください。

リポジトリをクローン：

```bash
git clone https://github.com/StreamIntelligenceLab/Kolibrie.git
cd Kolibrie
````

ビルド：

```bash
cargo build --release
```

プロジェクトで使用：

```rust
use kolibrie::SparqlDatabase;
```

### Docker Installation（Docker）

**Kolibrie** は、用途に応じた複数のDockerプロファイルを提供します。Docker設定はRust、CUDA（GPUビルド向け）、PythonのMLフレームワークを含む依存関係を自動的に処理します。

#### Prerequisites（前提）

* [Docker](https://docs.docker.com/get-docker/) がインストールされていること
* [Docker Compose](https://docs.docker.com/compose/install/) がインストールされていること
* GPUサポート用：[NVIDIA Docker runtime](https://github.com/NVIDIA/nvidia-docker) がインストールされていること

#### Quick Start

1. **CPUのみビルド**（多くのユーザーに推奨）：

```bash
docker compose --profile cpu up --build
```

2. **GPU対応ビルド**（NVIDIA GPU + nvidia-dockerが必要）：

```bash
docker compose --profile gpu up --build
```

3. **開発ビルド**（GPUの有無を自動検出）：

```bash
docker compose --profile dev up --build
```

## Usage（使い方）

### Initializing the Database（データベース初期化）

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // Your code here
}
```

### Parsing RDF Data（RDF解析）

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

### Adding Triples Programmatically（トリプルの追加）

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

### Executing SPARQL Queries（SPARQLクエリ実行）

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

* `AVG(?var)` - 平均
* `COUNT(?var)` - 件数
* `SUM(?var)` - 合計
* `MIN(?var)` - 最小
* `MAX(?var)` - 最大

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

### Using the Query Builder API（Query Builder）

```rust
// Get all objects for a specific predicate
let results = db.query()
    .with_predicate("http://xmlns.com/foaf/0.1/name")
    .get_objects();

for object in results {
    println!("Name: {}", object);
}
```

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

```rust
let other_db = SparqlDatabase::new();
// ...  populate other_db ...

let results = db.query()
    .join(&other_db)
    .join_on_subject()
    .get_triples();
```

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

### Using the Volcano Optimizer（Volcano Optimizer）

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

### Working with the Reasoner（推論器）

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

## Performance（性能）

**Kolibrie** は以下の最適化により高性能を実現しています：

* **Parallel Parsing and Processing**: RayonとCrossbeamによるマルチスレッド解析/実行。
* **SIMD Instructions**: フィルタリングや集約の高速化。
* **Volcano Optimizer**: コストベース最適化で効率的な物理プランを生成。
* **Knowledge Graph Inference**: ルールベース推論/バックワードチェイニングを効率的に統合。
* **Efficient Data Structures**: `BTreeSet` や `HashMap` などを用途に応じて活用。
* **Memory Optimization**: 辞書エンコーディングでメモリ使用量を縮小。

### Benchmarking Results

ベンチマークでは、Kolibrieが既存のRDFエンジンに対して優位な性能を示すことを確認しています。

* **Dataset**: [WatDiv](https://dsg.uwaterloo.ca/watdiv/) 10M triples benchmark
* **Oxigraph Configuration**: RocksDB backend
* **Deep Taxonomy Reasoning**: 階層深度は最大10Kレベルまで評価

#### WatDiv 10M - Query Performance Comparison (20 runs each)

![WatDiv 10M Query Performance](img/image1.png)

*Figure 1: WatDiv 10Mにおける各SPARQLエンジンのクエリ実行時間*

**Key Findings:**

* Kolibrieは各クエリ形状（L1-L5, S1-S7, F1-F3, C1-C3）で一貫して高い性能を示します。
* 平均クエリ実行時間は **サブミリ秒〜低ミリ秒** の範囲。
* BlazegraphとQLeverは一部形状で競争力のある性能を示します。
* Oxigraph（RocksDB）は全体的に安定した性能を提供します。

#### Deep Taxonomy - Reasoning over Hierarchy Depth

![Deep Taxonomy Reasoning Performance](img/image2.png)

*Figure 2: 階層深度（10, 100, 1K, 10K）に対する推論性能*

**Key Findings:**

* Kolibrieは階層深度に対して **対数的なスケーリング** を示します。
* 10Kレベルでもサブ秒の応答を維持。
* Apache JenaやEYE reasonerに対して優れた性能。
* 複雑なタクソノミ構造を効率的に処理可能。

## コントリビュート方法

### 問題の提出

Issue Trackerを使用して、バグ報告や新機能/改善のリクエストを提出してください。新しい問題を提出する前に、類似のオープンなIssueがないことを確認してください。

### 手動テスト

コードを手動でテストし、バグや改善提案をIssue Trackerに報告してくださる方は大歓迎です！

### プルリクエストの提出

パッチや修正はプルリクエスト（PR）の形式で受け付けています。プルリクエストが対処するIssueがIssue Trackerにオープンしていることを確認してください。

提出されたプルリクエストは、Mozilla Public License Version 2.0 の下で公開することに同意したとみなされます。

## コミュニティ

Kolibrieについて議論し、質問し、経験を共有するために、[Discordコミュニティ](https://discord.gg/KcFXrUUyYm)に参加してください。

## ライセンス

Kolibrieは[MPL-2.0ライセンス](LICENSE)の下でライセンスされています。
