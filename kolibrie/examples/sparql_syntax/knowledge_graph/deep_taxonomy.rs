use datalog::reasoning::Reasoner;
use shared::terms::Term;
use shared::rule::Rule;
use kolibrie::sparql_database::SparqlDatabase;
use std::fs;
use std::time::Instant;
use std::env;

fn main() {
    let home_dir = env::var("HOME").expect("HOME environment variable not set");
    let base_path = format!("{}/eye/reasoning/deep-taxonomy", home_dir);
    
    println!("=== Loading Data (One-time) ===");
    let mut database = SparqlDatabase::new();
    
    // Step 1: Load the taxonomy (subClassOf relationships)
    let taxonomy_path = format!("{}/test-dl.ttl", base_path);
    println!("Loading taxonomy from: {}", taxonomy_path);
    let load_start = Instant::now();
    let taxonomy_data = fs::read_to_string(&taxonomy_path)
        .expect(&format!("Failed to read {}", taxonomy_path));
    database.parse_turtle(&taxonomy_data);
    let load_time = load_start.elapsed();
    println!("Loaded {} taxonomy triples in {:?}", database.triples.len(), load_time);
    
    // Step 2: Load the facts (initial instances)
    let facts_path = format!("{}/test-facts.ttl", base_path);
    println!("\nLoading facts from: {}", facts_path);
    let facts_data = fs::read_to_string(&facts_path)
        .expect(&format!("Failed to read {}", facts_path));
    database.parse_turtle(&facts_data);
    println!("Total triples after facts: {}", database.triples.len());
    
    // Step 3: Create KnowledgeGraph and populate it (One-time)
    println!("\n=== Populating KnowledgeGraph (One-time) ===");
    let mut kg = Reasoner::new();
    kg.dictionary = database.dictionary.clone();
    
    let populate_start = Instant::now();
    for triple in database.triples.iter() {
        kg.index_manager.insert(triple);
    }
    let populate_time = populate_start. elapsed();
    println!("Populated KnowledgeGraph in {:?}", populate_time);
    
    // Step 4: Find predicate IDs
    let mut rdf_type_id = None;
    let mut rdfs_subclass_id = None;
    
    let mut predicate_counts = std::collections::HashMap::new();
    for triple in database.triples.iter() {
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
                Term::Variable("C".to_string())
            ),
            (
                Term::Variable("C".to_string()),
                Term::Constant(rdfs_subclass),
                Term::Variable("D".to_string())
            ),
        ],
        conclusion: vec![
            (
                Term::Variable("X".to_string()),
                Term::Constant(rdf_type),
                Term::Variable("D".to_string())
            )
        ],
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
        
        println!("  Iteration {}: {:.6}s ({} inferences)", 
                 i, reasoning_time.as_secs_f64(), inferred_facts.len());
    }
    
    // Calculate statistics
    let avg_time: f64 = reasoning_times.iter().sum::<f64>() / ITERATIONS as f64;
    let min_time = reasoning_times.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_time = reasoning_times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    
    let avg_inferred = inferred_counts.iter().sum::<usize>() / ITERATIONS;
    
    // Calculate standard deviation
    let variance: f64 = reasoning_times.iter()
        .map(|&t| (t - avg_time).powi(2))
        .sum::<f64>() / ITERATIONS as f64;
    let std_dev = variance.sqrt();
    
    println!("\n=== Kolibrie Performance Statistics ===");
    println!("Iterations: {}", ITERATIONS);
    println!("Average reasoning time: {:.6}s", avg_time);
    println!("Min reasoning time: {:.6}s", min_time);
    println!("Max reasoning time: {:.6}s", max_time);
    println!("Standard deviation: {:.6}s", std_dev);
    println!("Average inferred facts: {}", avg_inferred);
    println!("Average throughput: {:.2} inferences/second", avg_inferred as f64 / avg_time);
    
    println!("\n=== Summary ===");
    println!("Load time (one-time): {:?}", load_time);
    println!("Populate time (one-time): {:?}", populate_time);
    println!("Average reasoning time: {:.6}s", avg_time);
}
