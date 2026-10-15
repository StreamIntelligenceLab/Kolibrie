use datalog::reasoning::Reasoner;
use kolibrie::sparql_database::SparqlDatabase;
use shared::rule::Rule;
use shared::terms::Term;
use std::env;
use std::fs;
use std::time::Instant;

/// Depth of the generated chain, when no corpus directory is supplied.
///
/// Each level is one `rdfs:subClassOf` step, so the closure derives this many
/// `rdf:type` facts per instance. Kept modest because the benchmark below reasons 20
/// times over it.
const GENERATED_DEPTH: usize = 1_000;

/// `A1 subClassOf A2 … A(n-1) subClassOf An`, plus one instance of `A1`.
///
/// The same shape as the EYE deep-taxonomy corpus this benchmark was written
/// against: a single chain whose transitive closure is the whole thing, which is what
/// makes it a reasoning benchmark rather than a data-volume one.
fn generated_corpus(depth: usize) -> (String, String) {
    let mut taxonomy = String::from("@prefix : <http://example.org/deep#> .\n");
    taxonomy.push_str("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n");
    for level in 1..depth {
        taxonomy.push_str(&format!(":A{} rdfs:subClassOf :A{} .\n", level, level + 1));
    }
    let facts = String::from(
        "@prefix : <http://example.org/deep#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         :i rdf:type :A1 .\n",
    );
    (taxonomy, facts)
}

/// Where to read `test-dl.ttl` and `test-facts.ttl` from, if anywhere.
///
/// First argument, then `KOLIBRIE_DEEP_TAXONOMY_DIR`, then the EYE checkout this was
/// originally written against. Returns `None` when none of them holds both files, in
/// which case the corpus is generated instead — the example used to panic here, which
/// made it unrunnable for anyone without that checkout.
fn corpus_dir() -> Option<String> {
    let mut candidates = env::args()
        .nth(1)
        .into_iter()
        .chain(env::var("KOLIBRIE_DEEP_TAXONOMY_DIR").ok())
        .chain(
            env::var("HOME")
                .ok()
                .map(|home| format!("{}/eye/reasoning/deep-taxonomy", home)),
        );
    candidates.find(|dir| {
        fs::metadata(format!("{}/test-dl.ttl", dir)).is_ok()
            && fs::metadata(format!("{}/test-facts.ttl", dir)).is_ok()
    })
}

fn main() {
    println!("=== Loading Data (One-time) ===");
    let mut database = SparqlDatabase::new();

    let load_start = Instant::now();
    let (taxonomy_data, facts_data) = match corpus_dir() {
        Some(dir) => {
            println!("Loading corpus from: {}", dir);
            let read = |name: &str| {
                let path = format!("{}/{}", dir, name);
                fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("failed to read {}: {}", path, error))
            };
            (read("test-dl.ttl"), read("test-facts.ttl"))
        }
        None => {
            println!(
                "No corpus directory found, so generating a {}-level chain instead.",
                GENERATED_DEPTH
            );
            println!(
                "Pass a directory holding test-dl.ttl and test-facts.ttl, or set \
                 KOLIBRIE_DEEP_TAXONOMY_DIR, to benchmark against the EYE corpus."
            );
            generated_corpus(GENERATED_DEPTH)
        }
    };

    // Step 1: the taxonomy (subClassOf relationships)
    database.parse_turtle(&taxonomy_data);
    let load_time = load_start.elapsed();
    println!(
        "Loaded {} taxonomy triples in {:?}",
        database.default_triple_count(),
        load_time
    );

    // Step 2: the facts (initial instances)
    database.parse_turtle(&facts_data);
    println!(
        "Total triples after facts: {}",
        database.default_triple_count()
    );

    // Step 3: Create KnowledgeGraph and populate it (One-time)
    println!("\n=== Populating KnowledgeGraph (One-time) ===");
    let mut kg = Reasoner::new();
    kg.dictionary = database.dictionary_handle();

    let populate_start = Instant::now();
    let triples = database.query_default_triples(None, None, None);
    for triple in &triples {
        kg.insert_ground_triple(triple.clone());
    }
    let populate_time = populate_start.elapsed();
    println!("Populated KnowledgeGraph in {:?}", populate_time);

    // Step 4: Find predicate IDs
    let mut rdf_type_id = None;
    let mut rdfs_subclass_id = None;

    let mut predicate_counts = std::collections::HashMap::new();
    for triple in &triples {
        *predicate_counts.entry(triple.predicate).or_insert(0) += 1;
    }

    for (pred_id, _) in &predicate_counts {
        let dict = kg.dictionary.read().unwrap();
        let pred_str = dict.decode(*pred_id).unwrap_or("");
        if pred_str.contains("rdf-syntax-ns#type") || pred_str.ends_with("type") {
            rdf_type_id = Some(*pred_id);
        }
        if pred_str.contains("rdf-schema#subClassOf") || pred_str.ends_with("subClassOf") {
            rdfs_subclass_id = Some(*pred_id);
        }
    }

    let rdf_type = rdf_type_id.expect("rdf:type not found in data!");
    let rdfs_subclass = rdfs_subclass_id.expect("rdfs:subClassOf not found in data!");

    // Step 5: Add reasoning rule
    let type_inference_rule = Rule {
        premise: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(rdf_type),
                Term::Variable("C".to_string()),
            ),
            (
                Term::Variable("C".to_string()),
                Term::Constant(rdfs_subclass),
                Term::Variable("D".to_string()),
            ),
        ],
        negative_premise: vec![],
        conclusion: vec![(
            Term::Variable("X".to_string()),
            Term::Constant(rdf_type),
            Term::Variable("D".to_string()),
        )],
        filters: vec![],
    };

    kg.add_rule(type_inference_rule);

    // Step 6: Run reasoning 20 times
    println!("\n=== Running Reasoning 20 Times ===");
    const ITERATIONS: usize = 20;
    let mut reasoning_times = Vec::new();
    let mut inferred_counts = Vec::new();

    for i in 1..=ITERATIONS {
        // Clone the KG to reset for each iteration
        let mut kg_clone = kg.clone();

        let reasoning_start = Instant::now();
        let inferred_facts = kg_clone.infer_new_facts_semi_naive();
        let reasoning_time = reasoning_start.elapsed();

        reasoning_times.push(reasoning_time.as_secs_f64());
        inferred_counts.push(inferred_facts.len());

        println!(
            "  Iteration {}: {:.6}s ({} inferences)",
            i,
            reasoning_time.as_secs_f64(),
            inferred_facts.len()
        );
    }

    // Calculate statistics
    let avg_time: f64 = reasoning_times.iter().sum::<f64>() / ITERATIONS as f64;
    let min_time = reasoning_times
        .iter()
        .cloned()
        .fold(f64::INFINITY, f64::min);
    let max_time = reasoning_times
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);

    let avg_inferred = inferred_counts.iter().sum::<usize>() / ITERATIONS;

    // Calculate standard deviation
    let variance: f64 = reasoning_times
        .iter()
        .map(|&t| (t - avg_time).powi(2))
        .sum::<f64>()
        / ITERATIONS as f64;
    let std_dev = variance.sqrt();

    println!("\n=== Kolibrie Performance Statistics ===");
    println!("Iterations: {}", ITERATIONS);
    println!("Average reasoning time: {:.6}s", avg_time);
    println!("Min reasoning time: {:.6}s", min_time);
    println!("Max reasoning time: {:.6}s", max_time);
    println!("Standard deviation: {:.6}s", std_dev);
    println!("Average inferred facts: {}", avg_inferred);
    println!(
        "Average throughput: {:.2} inferences/second",
        avg_inferred as f64 / avg_time
    );

    println!("\n=== Summary ===");
    println!("Load time (one-time): {:?}", load_time);
    println!("Populate time (one-time): {:?}", populate_time);
    println!("Average reasoning time: {:.6}s", avg_time);
}
