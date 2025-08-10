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

**Kolibrie** は、Rustで実装された強力で並行性の高い、機能豊富なSPARQLクエリエンジンです。スケーラビリティと効率性を念頭に設計されており、Rustの堅牢な並行モデルと、SIMD（単一命令・複数データ）やRayonによる並列処理などの高度な最適化を活用して、大規模なRDF（Resource Description Framework）データセットをスムーズに処理します。

完全なAPIを備えた**Kolibrie**は、SPARQL、Turtle、N3形式のデータを解析、保存、クエリすることを容易にします。高度なフィルタリング、集約、結合操作、および洗練された最適化戦略により、複雑なセマンティックデータ処理を必要とするアプリケーションに適しています。さらに、Volcano Optimizerの統合とKnowledge Graph機能により、効率的なクエリプランニングとルールベースの推論を活用した高度なデータ分析が可能です。

## 研究の背景

**Kolibrie** は、KU Leuvenの[Stream Intelligence Lab](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab)でPieter Bonte教授の指導の下で開発されています。Stream Intelligence Labは、**ストリーム推論（Stream Reasoning）** に焦点を当てており、これは継続的なデータストリームからタイムリーで実用的な知識を導出するために、人工知能の論理的手法とデータに基づく機械学習アプローチを統合する新興の研究分野です。我々の研究は、インターネット・オブ・シングズ（IoT）やエッジ処理（Edge Processing）におけるアプリケーションに重点を置いており、自動運転車、ロボティクス、ウェブ分析などの動的な環境でリアルタイムの意思決定を可能にします。

研究および進行中のプロジェクトの詳細については、[Stream Intelligence Labのウェブサイト](https://kulak.kuleuven.be/nl/onderzoek/Onderzoeksdomeinen/stream-intelligence-lab)をご覧ください。

## 特徴

- **効率的なRDF解析**：RDF/XML、Turtle、N3形式のデータ解析をサポートし、エラーやプレフィックスの管理を堅牢に行います。
- **並行処理**：RayonとCrossbeamを使用したデータの並列処理により、マルチコアシステムでの最適なパフォーマンスを実現します。
- **SIMD最適化**：フィルタリングや集約クエリのタスクを高速化するためにSIMD命令を実装しています。
- **柔軟なクエリ**：SELECT、INSERT、FILTER、GROUP BY、VALUESなどの複雑なSPARQLクエリをサポートします。
- **Volcano Optimizer**：コストベースのVolcanoモデルに基づくクエリオプティマイザーを統合し、最も効率的な実行プランを決定します。
- **Knowledge Graph**：ABox（インスタンスレベル）およびTBox（スキーマレベル）のアサーションを含む知識グラフの構築とクエリを強力にサポートし、ルールベースの動的推論とバックワードチェイニング機能を提供します。
- **ストリーミングおよびスライディングウィンドウ**：タイムスタンプ付きのトリプルと時間的データ分析のためのスライディングウィンドウ操作を管理します。
- **拡張可能な辞書エンコーディング**：カスタマイズ可能な辞書を使用してRDF用語を効率的にエンコードおよびデコードします。
- **完全なAPI**：データ操作、クエリ、および結果処理のための豊富なメソッドセットを提供します。

> [!WARNING]
> CUDAの使用は実験的で開発中です。

## インストール

### ネイティブインストール

[Rust](https://www.rust-lang.org/tools/install)（バージョン1.60以上）がインストールされていることを確認してください。

`Cargo.toml` に**Kolibrie**を追加します：

```toml
[dependencies]
kolibrie = "0.1.0"
```

次に、プロジェクトに含めます：

```rust
use kolibrie::SparqlDatabase;
```

### Dockerインストール

**Kolibrie**は、さまざまな使用例に対応する複数の設定でDockerサポートを提供します。Docker設定は、Rust、CUDA（GPUビルド用）、Python MLフレームワークを含むすべての依存関係を自動的に処理します。

#### 前提条件

- [Docker](https://docs.docker.com/get-docker/)がインストールされていること
- [Docker Compose](https://docs.docker.com/compose/install/)がインストールされていること
- GPUサポート用：[NVIDIA Docker runtime](https://github.com/NVIDIA/nvidia-docker)がインストールされていること

#### クイックスタート

1. **CPUのみビルド**（ほとんどのユーザーに推奨）：
```bash
docker compose --profile cpu up --build
```

2. **GPU対応ビルド**（NVIDIA GPUとnvidia-dockerが必要）：
```bash
docker compose --profile gpu up --build
```

3. **開発ビルド**（GPU可用性を自動検出）：
```bash
docker compose --profile dev up --build
```

## 使い方

### データベースの初期化

`SparqlDatabase`の新しいインスタンスを作成します：

```rust
use kolibrie::SparqlDatabase;

fn main() {
    let mut db = SparqlDatabase::new();
    // ここにコードを追加
}
```

### RDFデータの解析

**Kolibrie**は、さまざまな形式のファイルや文字列からRDFデータの解析をサポートします。

#### ファイルからRDF/XMLの解析

```rust
db.parse_rdf_from_file("data.rdf");
```

#### 文字列からTurtleデータの解析

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_turtle(turtle_data);
```

#### 文字列からN3データの解析

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
ex:Bob ex:knows ex:Charlie .
"#;

db.parse_n3(n3_data);
```

### SPARQLクエリの実行

データを取得および操作するためにSPARQLクエリを実行します。

#### 基本的なクエリ

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

#### データの挿入

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
// 挿入操作は結果を返しません
```

### Volcano Optimizerの使用

**Volcano Optimizer**は、コストの推定に基づいてクエリ実行プランを最適化するために**Kolibrie**に統合されています。論理的なクエリプランを物理的な効率的なプランに変換し、さまざまな物理オペレーターを評価して最もコストの低いものを選択します。

#### 例：最適化されたクエリの実行

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQLクエリの定義
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // 最適化されたプランでクエリを実行
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Location: {}", row[0], row[1]);
    }
}
```

### Knowledge Graphとの連携

**Knowledge Graph**コンポーネントを使用すると、インスタンスレベル（ABox）およびスキーマレベル（TBox）の情報を含むセマンティックネットワークを構築および管理できます。ルールベースの動的推論とバックワードチェイニングをサポートし、既存のデータから新しい知識を導出します。

#### 例：Knowledge Graphの構築とクエリ

```rust
use kolibrie::{KnowledgeGraph, Triple};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBoxトリプル（スキーマ）の追加
    kg.add_tbox_triple("http://example.org/Person", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Class");
    kg.add_tbox_triple("http://example.org/knows", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.w3.org/2000/01/rdf-schema#Property");

    // ABoxトリプル（インスタンス）の追加
    kg.add_abox_triple("http://example.org/Alice", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");

    // ルールの定義と追加
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

    // ルールに基づいて新しい事実を推論
    kg.infer_new_facts();

    // Knowledge Graphをクエリ
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

**出力：**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## APIドキュメント

### 構造体 `SparqlDatabase`

構造体 `SparqlDatabase` は、RDFストレージを表す中心的なコンポーネントであり、データ操作およびクエリのためのメソッドを提供します。

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### フィールド

- **triples**：効率的なクエリのためにソートされたセットにRDFトリプルを保存します。
- **streams**：ストリーミングおよび時間的なクエリのためにタイムスタンプ付きのトリプルを保持します。
- **sliding_window**：時間ベースのデータ分析のためのオプションのスライディングウィンドウ。
- **dictionary**：ストレージ効率のためにRDF用語をエンコードおよびデコードします。
- **prefixes**：プレフィックスを管理し、省略された用語を解決します。

### 構造体 `VolcanoOptimizer`

構造体 `VolcanoOptimizer` は、Volcanoモデルに基づくコストベースのクエリオプティマイザーを実装しています。論理クエリプランを効率的な物理プランに変換し、さまざまな物理オペレーターを評価して最もコストの低いものを選択します。

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### フィールド

- **memo**：最適化された物理オペレーターをキャッシュし、冗長な計算を避けます。
- **selected_variables**：クエリで選択された変数を追跡します。
- **stats**：コスト推定を助けるためにデータベースの統計情報を保持します。

### 構造体 `KnowledgeGraph`

構造体 `KnowledgeGraph` は、ABox（インスタンスレベル）およびTBox（スキーマレベル）のアサーションを管理し、ルールベースの動的推論とバックワードチェイニングを用いたクエリ機能を提供します。

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox：個人（インスタンス）に関するアサーション
    pub tbox: BTreeSet<Triple>, // TBox：概念および関係（スキーマ）
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // 動的ルールのリスト
}
```

#### フィールド

- **abox**：インスタンスレベルのRDFトリプルを保存します。
- **tbox**：スキーマレベルのRDFトリプルを保存します。
- **dictionary**：ストレージ効率のためにRDF用語をエンコードおよびデコードします。
- **rules**：推論のための動的ルールを保持します。

### 主なメソッド

#### `new() -> Self`

新しい空の `SparqlDatabase` を作成します。

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

指定されたファイルからRDF/XMLデータを解析し、データベースを満たします。

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

文字列からRDF/XMLデータを解析します。

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

文字列からTurtle形式のRDFデータを解析します。

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

文字列からN3形式のRDFデータを解析します。

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

データベースに対してSPARQLクエリを実行し、結果を返します。

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

述語関数に基づいてトリプルをフィルタリングします。

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

タイムスタンプ付きのトリプルをストリームに追加します。

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

指定された時間ウィンドウ内のトリプルを取得します。

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer`のメソッド

#### `new(database: &SparqlDatabase) -> Self`

提供されたデータベースから収集された統計データを使用して、新しい `VolcanoOptimizer` のインスタンスを作成します。

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

与えられた論理クエリプランに対して、最も効率的な物理実行プランを決定します。

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph`のメソッド

#### `new() -> Self`

新しい空の `KnowledgeGraph` を作成します。

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

TBoxトリプル（スキーマレベルの情報）をKnowledge Graphに追加します。

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

ABoxトリプル（インスタンスレベルの情報）をKnowledge Graphに追加します。

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

推論のための動的ルールをKnowledge Graphに追加します。

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

ルールに基づいて新しいトリプルを推論し、ABoxを更新します。

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

オプションのフィルタ（サブジェクト、述語、オブジェクト）に基づいてABoxをクエリします。

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

オプションのフィルタ（サブジェクト、述語、オブジェクト）に基づいてTBoxをクエリします。

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## 例

### 基本的なクエリ

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQL SELECTクエリの実行
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

**出力：**
```
Subject: http://example.org/Alice, Object: http://example.org/Bob
Subject: http://example.org/Bob, Object: http://example.org/Charlie
```

### 高度なフィルタリングと集約

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // FILTERおよびGROUP BYを含むSPARQL SELECTクエリの実行
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
        println!("Average Age: {}", row[0]);
    }
}
```

**出力：**
```
Average Age: 30
```

### Volcano Optimizerを使用した最適化されたクエリの実行

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQLクエリの定義
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // 最適化されたプランでクエリを実行
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Location: {}", row[0], row[1]);
    }
}
```

**出力：**
```
Person: http://example.org/Alice, Location: http://example.org/Kulak
Person: http://example.org/Bob, Location: http://example.org/Kortrijk
Person: http://example.org/Charlie, Location: http://example.org/Ughent
```

### Knowledge Graphの構築とクエリ

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBoxトリプル（スキーマ）の追加
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // ABoxトリプル（インスタンス）の追加
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // ルールの定義と追加
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

    // ルールに基づいて新しい事実を推論
    let inferred_facts = kg.infer_new_facts();

    // Knowledge Graphをクエリ
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

**出力：**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## APIドキュメント

### 構造体 `SparqlDatabase`

構造体 `SparqlDatabase` は、RDFストレージを表す中心的なコンポーネントであり、データ操作およびクエリのためのメソッドを提供します。

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### フィールド

- **triples**：効率的なクエリのためにソートされたセットにRDFトリプルを保存します。
- **streams**：ストリーミングおよび時間的なクエリのためにタイムスタンプ付きのトリプルを保持します。
- **sliding_window**：時間ベースのデータ分析のためのオプションのスライディングウィンドウ。
- **dictionary**：ストレージ効率のためにRDF用語をエンコードおよびデコードします。
- **prefixes**：プレフィックスを管理し、省略された用語を解決します。

### 構造体 `VolcanoOptimizer`

構造体 `VolcanoOptimizer` は、Volcanoモデルに基づくコストベースのクエリオプティマイザーを実装しています。論理クエリプランを効率的な物理プランに変換し、さまざまな物理オペレーターを評価して最もコストの低いものを選択します。

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### フィールド

- **memo**：最適化された物理オペレーターをキャッシュし、冗長な計算を避けます。
- **selected_variables**：クエリで選択された変数を追跡します。
- **stats**：コスト推定を助けるためにデータベースの統計情報を保持します。

### 構造体 `KnowledgeGraph`

構造体 `KnowledgeGraph` は、ABox（インスタンスレベル）およびTBox（スキーマレベル）のアサーションを管理し、ルールベースの動的推論とバックワードチェイニングを用いたクエリ機能を提供します。

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox：個人（インスタンス）に関するアサーション
    pub tbox: BTreeSet<Triple>, // TBox：概念および関係（スキーマ）
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // 動的ルールのリスト
}
```

#### フィールド

- **abox**：インスタンスレベルのRDFトリプルを保存します。
- **tbox**：スキーマレベルのRDFトリプルを保存します。
- **dictionary**：ストレージ効率のためにRDF用語をエンコードおよびデコードします。
- **rules**：推論のための動的ルールを保持します。

### 主なメソッド

#### `new() -> Self`

新しい空の `SparqlDatabase` を作成します。

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

指定されたファイルからRDF/XMLデータを解析し、データベースを満たします。

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

文字列からRDF/XMLデータを解析します。

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

文字列からTurtle形式のRDFデータを解析します。

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

文字列からN3形式のRDFデータを解析します。

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

データベースに対してSPARQLクエリを実行し、結果を返します。

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

述語関数に基づいてトリプルをフィルタリングします。

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

タイムスタンプ付きのトリプルをストリームに追加します。

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

指定された時間ウィンドウ内のトリプルを取得します。

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer`のメソッド

#### `new(database: &SparqlDatabase) -> Self`

提供されたデータベースから収集された統計データを使用して、新しい `VolcanoOptimizer` のインスタンスを作成します。

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

与えられた論理クエリプランに対して、最も効率的な物理実行プランを決定します。

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph`のメソッド

#### `new() -> Self`

新しい空の `KnowledgeGraph` を作成します。

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

TBoxトリプル（スキーマレベルの情報）をKnowledge Graphに追加します。

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

ABoxトリプル（インスタンスレベルの情報）をKnowledge Graphに追加します。

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

推論のための動的ルールをKnowledge Graphに追加します。

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

ルールに基づいて新しいトリプルを推論し、ABoxを更新します。

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

オプションのフィルタ（サブジェクト、述語、オブジェクト）に基づいてABoxをクエリします。

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

オプションのフィルタ（サブジェクト、述語、オブジェクト）に基づいてTBoxをクエリします。

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## 例

### 基本的なクエリ

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQL SELECTクエリの実行
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

**出力：**
```
Subject: http://example.org/Alice, Object: http://example.org/Bob
Subject: http://example.org/Bob, Object: http://example.org/Charlie
```

### 高度なフィルタリングと集約

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // FILTERおよびGROUP BYを含むSPARQL SELECTクエリの実行
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
        println!("Average Age: {}", row[0]);
    }
}
```

**出力：**
```
Average Age: 30
```

### Volcano Optimizerを使用した最適化されたクエリの実行

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQLクエリの定義
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // 最適化されたプランでクエリを実行
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Location: {}", row[0], row[1]);
    }
}
```

**出力：**
```
Person: http://example.org/Alice, Location: http://example.org/Kulak
Person: http://example.org/Bob, Location: http://example.org/Kortrijk
Person: http://example.org/Charlie, Location: http://example.org/Ughent
```

### Knowledge Graphの構築とクエリ

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBoxトリプル（スキーマ）の追加
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // ABoxトリプル（インスタンス）の追加
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // ルールの定義と追加
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

    // ルールに基づいて新しい事実を推論
    let inferred_facts = kg.infer_new_facts();

    // Knowledge Graphをクエリ
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

**出力：**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## APIドキュメント

### 構造体 `SparqlDatabase`

構造体 `SparqlDatabase` は、RDFストレージを表す中心的なコンポーネントであり、データ操作およびクエリのためのメソッドを提供します。

```rust
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
}
```

#### フィールド

- **triples**：効率的なクエリのためにソートされたセットにRDFトリプルを保存します。
- **streams**：ストリーミングおよび時間的なクエリのためにタイムスタンプ付きのトリプルを保持します。
- **sliding_window**：時間ベースのデータ分析のためのオプションのスライディングウィンドウ。
- **dictionary**：ストレージ効率のためにRDF用語をエンコードおよびデコードします。
- **prefixes**：プレフィックスを管理し、省略された用語を解決します。

### 構造体 `VolcanoOptimizer`

構造体 `VolcanoOptimizer` は、Volcanoモデルに基づくコストベースのクエリオプティマイザーを実装しています。論理クエリプランを効率的な物理プランに変換し、さまざまな物理オペレーターを評価して最もコストの低いものを選択します。

```rust
pub struct VolcanoOptimizer {
    pub memo: HashMap<String, PhysicalOperator>,
    pub selected_variables: Vec<String>,
    pub stats: DatabaseStats,
}
```

#### フィールド

- **memo**：最適化された物理オペレーターをキャッシュし、冗長な計算を避けます。
- **selected_variables**：クエリで選択された変数を追跡します。
- **stats**：コスト推定を助けるためにデータベースの統計情報を保持します。

### 構造体 `KnowledgeGraph`

構造体 `KnowledgeGraph` は、ABox（インスタンスレベル）およびTBox（スキーマレベル）のアサーションを管理し、ルールベースの動的推論とバックワードチェイニングを用いたクエリ機能を提供します。

```rust
pub struct KnowledgeGraph {
    pub abox: BTreeSet<Triple>, // ABox：個人（インスタンス）に関するアサーション
    pub tbox: BTreeSet<Triple>, // TBox：概念および関係（スキーマ）
    pub dictionary: Dictionary,
    pub rules: Vec<Rule>, // 動的ルールのリスト
}
```

#### フィールド

- **abox**：インスタンスレベルのRDFトリプルを保存します。
- **tbox**：スキーマレベルのRDFトリプルを保存します。
- **dictionary**：ストレージ効率のためにRDF用語をエンコードおよびデコードします。
- **rules**：推論のための動的ルールを保持します。

### 主なメソッド

#### `new() -> Self`

新しい空の `SparqlDatabase` を作成します。

```rust
let db = SparqlDatabase::new();
```

#### `parse_rdf_from_file(&mut self, filename: &str)`

指定されたファイルからRDF/XMLデータを解析し、データベースを満たします。

```rust
db.parse_rdf_from_file("data.rdf");
```

#### `parse_rdf(&mut self, rdf_xml: &str)`

文字列からRDF/XMLデータを解析します。

```rust
let rdf_xml = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">...</rdf:RDF>"#;
db.parse_rdf(rdf_xml);
```

#### `parse_turtle(&mut self, turtle_data: &str)`

文字列からTurtle形式のRDFデータを解析します。

```rust
let turtle_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_turtle(turtle_data);
```

#### `parse_n3(&mut self, n3_data: &str)`

文字列からN3形式のRDFデータを解析します。

```rust
let n3_data = r#"
@prefix ex: <http://example.org/> .

ex:Alice ex:knows ex:Bob .
"#;
db.parse_n3(n3_data);
```

#### `execute_query(sparql: &str, database: &mut SparqlDatabase) -> Vec<Vec<String>>`

データベースに対してSPARQLクエリを実行し、結果を返します。

```rust
let sparql_query = "SELECT ?s WHERE { ?s ex:knows ex:Bob . }";
let results = execute_query(sparql_query, &mut db);
```

#### `filter<F>(&self, predicate: F) -> Self`

述語関数に基づいてトリプルをフィルタリングします。

```rust
let filtered_db = db.filter(|triple| triple.predicate == some_predicate_id);
```

#### `add_stream_data(&mut self, triple: Triple, timestamp: u64)`

タイムスタンプ付きのトリプルをストリームに追加します。

```rust
let triple = Triple { subject: ..., predicate: ..., object: ... };
db.add_stream_data(triple, 1625097600);
```

#### `time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple>`

指定された時間ウィンドウ内のトリプルを取得します。

```rust
let window_triples = db.time_based_window(1625097600, 1625184000);
```

### `VolcanoOptimizer`のメソッド

#### `new(database: &SparqlDatabase) -> Self`

提供されたデータベースから収集された統計データを使用して、新しい `VolcanoOptimizer` のインスタンスを作成します。

```rust
let optimizer = VolcanoOptimizer::new(&db);
```

#### `find_best_plan(&mut self, logical_plan: &LogicalOperator) -> PhysicalOperator`

与えられた論理クエリプランに対して、最も効率的な物理実行プランを決定します。

```rust
let best_plan = optimizer.find_best_plan(&logical_plan);
```

### `KnowledgeGraph`のメソッド

#### `new() -> Self`

新しい空の `KnowledgeGraph` を作成します。

```rust
let kg = KnowledgeGraph::new();
```

#### `add_tbox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

TBoxトリプル（スキーマレベルの情報）をKnowledge Graphに追加します。

```rust
kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
```

#### `add_abox_triple(&mut self, subject: &str, predicate: &str, object: &str)`

ABoxトリプル（インスタンスレベルの情報）をKnowledge Graphに追加します。

```rust
kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
```

#### `add_rule(&mut self, rule: Rule)`

推論のための動的ルールをKnowledge Graphに追加します。

```rust
let rule = Rule { ... };
kg.add_rule(rule);
```

#### `infer_new_facts(&mut self) -> Vec<Triple>`

ルールに基づいて新しいトリプルを推論し、ABoxを更新します。

```rust
let inferred = kg.infer_new_facts();
```

#### `query_abox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

オプションのフィルタ（サブジェクト、述語、オブジェクト）に基づいてABoxをクエリします。

```rust
let results = kg.query_abox(Some("http://example.org/Alice"), None, None);
```

#### `query_tbox(&mut self, subject: Option<&str>, predicate: Option<&str>, object: Option<&str>) -> Vec<Triple>`

オプションのフィルタ（サブジェクト、述語、オブジェクト）に基づいてTBoxをクエリします。

```rust
let results = kg.query_tbox(Some("http://example.org/Person"), Some("rdf:type"), Some("rdfs:Class"));
```

## 例

### 基本的なクエリ

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQL SELECTクエリの実行
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

**出力：**
```
Subject: http://example.org/Alice, Object: http://example.org/Bob
Subject: http://example.org/Bob, Object: http://example.org/Charlie
```

### 高度なフィルタリングと集約

```rust
use kolibrie::{SparqlDatabase, execute_query};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:age "30" .
    ex:Bob ex:age "25" .
    ex:Charlie ex:age "35" .
    "#;
    db.parse_turtle(turtle_data);

    // FILTERおよびGROUP BYを含むSPARQL SELECTクエリの実行
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
        println!("Average Age: {}", row[0]);
    }
}
```

**出力：**
```
Average Age: 30
```

### Volcano Optimizerを使用した最適化されたクエリの実行

```rust
use kolibrie::{SparqlDatabase, execute_query, VolcanoOptimizer};

fn main() {
    let mut db = SparqlDatabase::new();

    // Turtleデータの解析
    let turtle_data = r#"
    @prefix ex: <http://example.org/> .

    ex:Alice ex:knows ex:Bob .
    ex:Bob ex:knows ex:Charlie .
    ex:Charlie ex:knows ex:David .
    "#;
    db.parse_turtle(turtle_data);

    // SPARQLクエリの定義
    let sparql_query = r#"
    PREFIX ex: <http://example.org/>
    SELECT ?person ?location
    WHERE {
        ?person ex:knows ?org .
        ?org ex:located ?location .
    }
    "#;

    // 最適化されたプランでクエリを実行
    let results = execute_query(sparql_query, &mut db);

    for row in results {
        println!("Person: {}, Location: {}", row[0], row[1]);
    }
}
```

**出力：**
```
Person: http://example.org/Alice, Location: http://example.org/Kulak
Person: http://example.org/Bob, Location: http://example.org/Kortrijk
Person: http://example.org/Charlie, Location: http://example.org/Ughent
```

### Knowledge Graphの構築とクエリ

```rust
use kolibrie::{KnowledgeGraph, Rule, Term};

fn main() {
    let mut kg = KnowledgeGraph::new();

    // TBoxトリプル（スキーマ）の追加
    kg.add_tbox_triple("http://example.org/Person", "rdf:type", "rdfs:Class");
    kg.add_tbox_triple("http://example.org/knows", "rdf:type", "rdf:Property");
    kg.add_tbox_triple("http://example.org/knownBy", "rdf:type", "rdf:Property");

    // ABoxトリプル（インスタンス）の追加
    kg.add_abox_triple("http://example.org/Alice", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Alice", "http://example.org/knows", "http://example.org/Bob");
    kg.add_abox_triple("http://example.org/Bob", "rdf:type", "http://example.org/Person");
    kg.add_abox_triple("http://example.org/Bob", "http://example.org/knows", "http://example.org/Charlie");

    // ルールの定義と追加
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

    // ルールに基づいて新しい事実を推論
    let inferred_facts = kg.infer_new_facts();

    // Knowledge Graphをクエリ
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

**出力：**
```
<http://example.org/Bob> -- <http://example.org/knownBy> -- <http://example.org/Alice> .
```

## パフォーマンス

**Kolibrie** は以下の方法で高性能に最適化されています：

- **並列解析と処理**：RayonおよびCrossbeamを使用して、マルチスレッドのデータ解析とクエリ実行を行います。
- **SIMD命令**：フィルタリングや集約タスクを高速化するためにSIMD操作を実装しています。
- **Volcano Optimizer**：コストベースのクエリオプティマイザーを使用して、効率的な物理実行プランを生成し、クエリ実行時間を最小化します。
- **Knowledge Graphの推論**：ルールベースの推論とバックワードチェイニングを活用して、新しい知識を導出しながら、パフォーマンスのオーバーヘッドを抑えます。
- **効率的なデータ構造**：高速なデータ取得と操作を確保するために、`BTreeSet`をソートされたストレージに、`HashMap`をプレフィックス管理に使用します。
- **メモリ最適化**：辞書エンコーディングを使用してメモリフットプリントを最小限に抑え、繰り返し使用される用語を再利用します。

ベンチマークテストでは、**Kolibrie** が従来のシングルスレッドSPARQLエンジンと比較して、大規模なRDFデータセットで顕著なパフォーマンス向上を示しています。

### Kolibrie vs. Oxigraph vs. RDFlib vs. Apache Jena（100KトリプルRDF/XML）

**RDFデータのロード時間**
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

**SPARQLクエリの実行時間**
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

**Kolibrieの概要**

- **解析の総時間**：15.29秒
- **クエリ実行の総時間**：4.31秒
- **解析の平均時間**：0.76秒
- **クエリ実行の平均時間**：0.22秒

**Oxigraphの概要**

- **RDFロードの総時間**：205.21秒
- **クエリ実行の総時間**：19.71秒
- **RDFロードの平均時間**：10.26秒
- **クエリ実行の平均時間**：0.99秒

**RDFlibの概要**

- **RDFロードの総時間**：588.86秒
- **SPARQLクエリ実行の総時間**：0.54秒
- **RDFロードの平均時間**：29.44秒
- **SPARQLクエリ実行の平均時間**：27.17ms

**Apache Jenaの概要**

- **RDFロードの総時間**：43.07秒
- **SPARQLクエリ実行の総時間**：15.23秒
- **RDFロードの平均時間**：2.15秒
- **SPARQLクエリ実行の平均時間**：761.74ms

## コントリビュート方法

### 問題の提出

Issue Trackerを使用して、バグ報告や新機能/改善のリクエストを提出してください。新しい問題を提出する前に、類似のオープンなIssueがないことを確認してください。

### 手動テスト

コードを手動でテストし、バグや改善提案をIssue Trackerに報告してくださる方は大歓迎です！

### プルリクエストの提出

パッチや修正はプルリクエスト（PR）の形式で受け付けています。プルリクエストが対処するIssueがIssue Trackerにオープンしていることを確認してください。

提出されたプルリクエストは、Mozilla Public License Version 2.0 の下で公開することに同意したとみなされます。

## ライセンス

Kolibrieは[MPL-2.0ライセンス](LICENSE)の下でライセンスされています。