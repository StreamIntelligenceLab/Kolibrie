/*
 * Small MNIST example for Kolibrie's SDD-based training path
 *
 * The example loads a subset of MNIST into the RDF store, trains a simple
 * classifier through the first-class neural syntax, and then runs a few
 * exact-probability checks with SDD tags
 */

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::Instant;

use datalog::reasoning::materialisation::sdd_seed_materialise::infer_new_facts_with_sdd_seed_specs;
use datalog::reasoning::Reasoner;
use kolibrie::execute_ml_train::execute_ml_training_owned;
use kolibrie::neural_relations::{lower_train_decl_to_owned, register_neural_declarations};
use kolibrie::parser::parse_combined_query;
use kolibrie::sparql_database::SparqlDatabase;
use ml::MlpNeuralPredicate;
use shared::provenance::Provenance;
use shared::sdd::{SddId, SddProvenance};
use shared::seed_spec::{ExclusiveChoice, SeedSpec};
use shared::triple::Triple;

// Dataset and model settings
const DATASET_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/sparql_syntax/ml_train/mnist-dataset"
);

const TRAIN_COUNT: usize = 300;
const TEST_COUNT: usize = 100;
const PIXELS: usize = 784;
const NUM_DIGITS: usize = 10;
const NS: &str = "http://example.org/mnist#";

// Read MNIST IDX files
fn read_u32_be(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]])
}

fn load_images(filename: &str, max: usize) -> Vec<Vec<f64>> {
    let path = Path::new(DATASET_DIR).join(filename);
    let mut file = File::open(&path).unwrap_or_else(|e| panic!("Cannot open {}: {}", path.display(), e));
    let mut raw = Vec::new();
    file.read_to_end(&mut raw).unwrap();
    assert_eq!(read_u32_be(&raw, 0), 2051, "Wrong magic for images file");
    let n = read_u32_be(&raw, 4) as usize;
    let count = n.min(max);
    (0..count)
        .map(|i| {
            let offset = 16 + i * PIXELS;
            raw[offset..offset + PIXELS]
                .iter()
                .map(|&px| px as f64 / 255.0)
                .collect()
        })
        .collect()
}

fn load_labels(filename: &str, max: usize) -> Vec<u8> {
    let path = Path::new(DATASET_DIR).join(filename);
    let mut file = File::open(&path).unwrap_or_else(|e| panic!("Cannot open {}: {}", path.display(), e));
    let mut raw = Vec::new();
    file.read_to_end(&mut raw).unwrap();
    assert_eq!(read_u32_be(&raw, 0), 2049, "Wrong magic for labels file");
    let n = read_u32_be(&raw, 4) as usize;
    raw[8..8 + n.min(max)].to_vec()
}

// Load the dataset into the RDF store
fn populate_rdf_store(db: &mut SparqlDatabase, images: &[Vec<f64>], labels: &[u8]) {
    let label_pred = format!("{}label", NS);
    for (i, (pixels, &label)) in images.iter().zip(labels.iter()).enumerate() {
        let subject = format!("{}train_{}", NS, i);
        db.add_triple_parts(&subject, &label_pred, &label.to_string());
        for (j, &px) in pixels.iter().enumerate() {
            let pred = format!("{}pixel_{}", NS, j);
            db.add_triple_parts(&subject, &pred, &format!("{:.6}", px));
        }
    }
}

// Build the first-class neural program used by this example.
// The query is generated because spelling out 784 feature variables by hand
// would make the file hard to read
fn build_first_class_program() -> String {
    let feature_vars: Vec<String> = (0..PIXELS).map(|i| format!("?p{}", i)).collect();
    let features_csv = feature_vars.join(", ");
    let output_labels = (0..NUM_DIGITS)
        .map(|d| format!("\"{}\"", d))
        .collect::<Vec<_>>()
        .join(", ");

    // One input triple per pixel
    let where_patterns: String = (0..PIXELS)
        .map(|i| format!("?sample <{}pixel_{}> ?p{} .", NS, i, i))
        .collect::<Vec<_>>()
        .join("\n        ");

    format!(
        r#"MODEL "mnist_classifier" {{
    ARCH MLP {{ HIDDEN [64, 32] }}
    OUTPUT EXCLUSIVE {{ {output_labels} }}
}}

NEURAL RELATION <{NS}predicted> USING MODEL "mnist_classifier" {{
    INPUT {{
        {where_patterns}
    }}
    FEATURES {{ {features_csv} }}
}}

TRAIN NEURAL RELATION <{NS}predicted> {{
    DATA {{
        ?sample <{NS}label> ?label .
    }}
    LABEL ?label
    TARGET {{ ?sample <{NS}predicted> ?label }}
    LOSS cross_entropy
    OPTIMIZER adam
    LEARNING_RATE 0.001
    EPOCHS 3
    BATCH_SIZE 16
    SAVE_TO "mnist_digit_model.bin"
}}"#
    )
}

/// Print a short excerpt instead of dumping the full generated query
fn print_program_excerpt(q: &str) {
    let head_lines: Vec<&str> = q.lines().take(12).collect();
    println!("    First-class neural program excerpt:");
    for line in &head_lines {
        println!("      {}", line);
    }
    println!("      ... ({} output labels, {} INPUT patterns) ...", NUM_DIGITS, PIXELS);

    if q.contains("TRAIN NEURAL RELATION") {
        println!("      TRAIN NEURAL RELATION <{}predicted> {{ DATA {{ ... }} ... }}", NS);
    }

    let tail_lines: Vec<&str> = q.lines().rev().take(10).collect::<Vec<_>>().into_iter().rev().collect();
    for line in &tail_lines {
        println!("      {}", line);
    }
}

// Evaluate on the held-out test set
fn evaluate_accuracy(model: &MlpNeuralPredicate, images: &[Vec<f64>], labels: &[u8]) -> f64 {
    let mut correct = 0usize;
    for (pixels, &true_label) in images.iter().zip(labels.iter()) {
        let (_, probs) = model
            .forward_with_grads(&[pixels.clone()])
            .expect("forward pass failed");
        let predicted = probs[0]
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        if predicted == true_label as usize {
            correct += 1;
        }
    }
    100.0 * correct as f64 / images.len() as f64
}

// Show how the predicted class distribution can be reused in SDD reasoning
fn or_tags(prov: &SddProvenance, tags: &[SddId]) -> SddId {
    tags.iter()
        .skip(1)
        .fold(tags[0], |acc, &t| prov.disjunction(&acc, &t))
}

fn neurosymbolic_demo(model: &MlpNeuralPredicate, test_images: &[Vec<f64>], test_labels: &[u8]) {
    println!("\nNeurosymbolic reasoning demo");
    println!("Neural predictions are turned into SDD seeds and queried with exact WMC.\n");

    let prime_set: &[usize] = &[2, 3, 5, 7];
    let even_set: &[usize] = &[0, 2, 4, 6, 8];
    // Used for a simple partition sanity check
    let even_non_prime_set: &[usize] = &[0, 4, 6, 8];
    let odd_non_prime_set: &[usize] = &[1, 9];

    for demo_idx in 0..5.min(test_images.len()) {
        let pixels = &test_images[demo_idx];
        let true_label = test_labels[demo_idx] as usize;

        // Neural forward pass
        let (_, probs) = model
            .forward_with_grads(&[pixels.clone()])
            .expect("forward pass failed");
        let prob_vec = &probs[0];

        let predicted = prob_vec
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i)
            .unwrap_or(0);

        // Turn the predicted digit distribution into an exclusive SDD seed group
        let sample_uri = format!("{}test_{}", NS, demo_idx);
        let pred_uri = format!("{}predicted", NS);

        let mini_db = SparqlDatabase::new();
        let sample_enc = mini_db.encode_term_star(&sample_uri);
        let pred_enc = mini_db.encode_term_star(&pred_uri);

        let choices: Vec<ExclusiveChoice> = (0..NUM_DIGITS)
            .map(|d| ExclusiveChoice {
                triple: Triple {
                    subject: sample_enc,
                    predicate: pred_enc,
                    object: mini_db.encode_term_star(&d.to_string()),
                },
                prob: prob_vec[d],
                choice_id: d as u32,
            })
            .collect();

        // Build the exact-one constraint and seed tags
        let mut local_reasoner = Reasoner::new();
        let (_derived, tag_store) = infer_new_facts_with_sdd_seed_specs(
            &mut local_reasoner,
            vec![SeedSpec::ExclusiveGroup { group_id: 0, choices }],
        );

        let prov = tag_store.provenance().clone();

        // Collect the tag for each digit
        let digit_tags: Vec<SddId> = (0..NUM_DIGITS)
            .map(|d| {
                let t = Triple {
                    subject: sample_enc,
                    predicate: pred_enc,
                    object: mini_db.encode_term_star(&d.to_string()),
                };
                tag_store.get_tag(&t)
            })
            .collect();

        // Ask a few simple queries over the class tags
        let prime_tags: Vec<SddId> = prime_set.iter().map(|&d| digit_tags[d]).collect();
        let even_tags: Vec<SddId> = even_set.iter().map(|&d| digit_tags[d]).collect();

        let p_prime = prov.recover_probability(&or_tags(&prov, &prime_tags));
        let p_even = prov.recover_probability(&or_tags(&prov, &even_tags));
        let p_predicted = prov.recover_probability(&digit_tags[predicted]);

        // prime ∪ even_non_prime ∪ odd_non_prime covers all digits without overlap
        let enp_tags: Vec<SddId> = even_non_prime_set.iter().map(|&d| digit_tags[d]).collect();
        let onp_tags: Vec<SddId> = odd_non_prime_set.iter().map(|&d| digit_tags[d]).collect();
        let p_even_non_prime = prov.recover_probability(&or_tags(&prov, &enp_tags));
        let p_odd_non_prime  = prov.recover_probability(&or_tags(&prov, &onp_tags));

        let result_mark = if predicted == true_label { "✓" } else { "✗" };
        println!("Sample {:2}: true={} predicted={} {}", demo_idx, true_label, predicted, result_mark);

        let mut ranked: Vec<(usize, f64)> = prob_vec.iter().cloned().enumerate().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top3: Vec<String> = ranked[..3].iter().map(|(d, p)| format!("{}:{:.3}", d, p)).collect();
        println!("  Neural top-3: [{}]", top3.join(", "));
        println!("  P(digit={})              via SDD WMC = {:.4}", predicted, p_predicted);
        println!("  P(prime  ∈ {{2,3,5,7}})   via SDD WMC = {:.4}", p_prime);
        println!("  P(even   ∈ {{0,2,4,6,8}}) via SDD WMC = {:.4}", p_even);
        // This sum should be exactly 1.0
        println!("  Partition check  prime({:.3}) + even∩¬prime({:.3}) + odd∩¬prime({:.3}) = {:.4}",
                 p_prime, p_even_non_prime, p_odd_non_prime,
                 p_prime + p_even_non_prime + p_odd_non_prime);
        println!();
    }

    println!("The partition check sums to 1.0000 because the SDD encodes exactly one digit.");
    println!("P(prime) + P(even) can still exceed 1.0 because those sets overlap at digit 2.");
}

// Run the end-to-end example
fn main() {
    println!("MNIST training example");
    println!("Using first-class neural syntax and SDD-based training.\n");

    // Load the dataset
    println!("[1/5] Loading MNIST data...");
    let t0 = Instant::now();
    let train_images = load_images("train-images.idx3-ubyte", TRAIN_COUNT);
    let train_labels = load_labels("train-labels.idx1-ubyte", TRAIN_COUNT);
    let test_images = load_images("t10k-images.idx3-ubyte", TEST_COUNT);
    let test_labels = load_labels("t10k-labels.idx1-ubyte", TEST_COUNT);
    println!(
        "    {} training images, {} test images  ({:.1}s)",
        train_images.len(),
        test_images.len(),
        t0.elapsed().as_secs_f64()
    );

    // Insert the training samples as RDF triples
    println!("\n[2/5] Populating RDF triple store...");
    println!(
        "    {} samples × {} triples/sample = {} triples total",
        TRAIN_COUNT,
        PIXELS + 1,
        TRAIN_COUNT * (PIXELS + 1)
    );
    let t1 = Instant::now();
    let mut db = SparqlDatabase::new();
    populate_rdf_store(&mut db, &train_images, &train_labels);
    println!("    Done ({:.1}s)", t1.elapsed().as_secs_f64());

    // Build and parse the training program
    println!("\n[3/5] Building & parsing first-class neural program ({} feature variables)...", PIXELS);
    let neural_program = build_first_class_program();
    print_program_excerpt(&neural_program);

    let (_, combined) = parse_combined_query(&neural_program).expect("first-class neural program parse failed");
    register_neural_declarations(
        &mut db,
        &combined.prefixes,
        &combined.model_decls,
        &combined.neural_relation_decls,
        &combined.train_neural_relation_decls,
    );
    let train_decl = combined
        .train_neural_relation_decls
        .first()
        .expect("expected one TRAIN NEURAL RELATION block");
    let owned_clause = lower_train_decl_to_owned(&db, train_decl).expect("TRAIN lowering failed");
    let n_choices = NUM_DIGITS;
    println!(
        "\n    Parsed OK → model={:?}  features={}  classes={}  epochs={}  batch={}",
        owned_clause.model_name,
        PIXELS,
        n_choices,
        owned_clause.epochs,
        owned_clause.batch_size
    );
    println!("    Architecture: {}→64→32→{} (ReLU hidden, softmax output)", PIXELS, NUM_DIGITS);

    // Train the model
    println!("\n[4/5] Training (neural outputs → SDD seeds → WMC gradients → backprop)...");
    println!("    Training with neural outputs, SDD seeds, and WMC gradients.\n");

    // Training data comes from the SPARQL query over `db`, so no rules are needed here
    let empty_reasoner = Reasoner::new();
    let t_train = Instant::now();

    let model = execute_ml_training_owned(&owned_clause, &empty_reasoner, &mut db)
        .expect("first-class TRAIN NEURAL RELATION execution failed");

    println!(
        "\n    Training completed in {:.1}s",
        t_train.elapsed().as_secs_f64()
    );

    // Evaluate on held-out samples and print a small reasoning demo
    println!("\n[5/5] Evaluating on {} test samples...", TEST_COUNT);
    let accuracy = evaluate_accuracy(&model, &test_images, &test_labels);
    println!("    Test accuracy: {:.1}%", accuracy);
    println!(
        "    (Small 784→64→32→10 network, {} samples, {} epochs — expect ~{}%+ with more data/epochs)",
        TRAIN_COUNT, owned_clause.epochs,
        if TRAIN_COUNT >= 1000 { 50 } else { 30 }
    );

    neurosymbolic_demo(&model, &test_images, &test_labels);

    println!("\nDone.");
    println!("Model saved to mnist_digit_model.bin");
    println!("The current example saves weights, but automatic resume/loading is still a later step.");
}
