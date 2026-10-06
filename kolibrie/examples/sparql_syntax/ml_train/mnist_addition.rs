use std::{error::Error, fs, path::PathBuf, time::Instant};

use kolibrie::{
    ml_policy::MlExecutionContext,
    program::{compile_program, execute_program, query_program_result, ProgramOptions},
    sparql_database::SparqlDatabase,
};

include!("mnist_program.rs");
use mnist_program::*;

const TRAIN_COUNT: usize = 300;
const TEST_COUNT: usize = 20;
const PIXELS: usize = 784;

fn dataset_dir() -> PathBuf {
    std::env::args()
        .nth(1)
        .or_else(|| std::env::var("MNIST_DATASET_DIR").ok())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/examples/sparql_syntax/ml_train/mnist-dataset"
            ))
        })
}

fn read_idx(dir: &PathBuf, stem: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    for name in [stem.replace("-idx", ".idx"), stem.to_string()] {
        let path = dir.join(&name);
        if path.is_file() {
            return Ok(fs::read(path)?);
        }
    }
    Err(format!("missing {stem} in {}", dir.display()).into())
}

fn be_u32(bytes: &[u8], offset: usize) -> usize {
    u32::from_be_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]]) as usize
}

fn load_images(dir: &PathBuf, stem: &str, count: usize) -> Result<Vec<Vec<f64>>, Box<dyn Error>> {
    let raw = read_idx(dir, stem)?;
    if be_u32(&raw, 0) != 2051 || be_u32(&raw, 4) < count {
        return Err(format!("{stem} is not an IDX image file with {count} images").into());
    }
    Ok((0..count)
        .map(|i| raw[16 + i * PIXELS..16 + (i + 1) * PIXELS].iter().map(|&p| p as f64 / 255.0).collect())
        .collect())
}

fn load_labels(dir: &PathBuf, stem: &str, count: usize) -> Result<Vec<usize>, Box<dyn Error>> {
    let raw = read_idx(dir, stem)?;
    if be_u32(&raw, 0) != 2049 || be_u32(&raw, 4) < count {
        return Err(format!("{stem} is not an IDX label file with {count} labels").into());
    }
    Ok(raw[8..8 + count].iter().map(|&label| label as usize).collect())
}

fn excerpt(program: &str) {
    let mut shown = 0;
    let mut in_block = false;
    for line in program.lines() {
        if line.starts_with("MODEL") || line.starts_with("NEURAL") || line.starts_with("TRAIN") || line.starts_with("ML.PREDICT") || line.starts_with("RULE") {
            println!("  {line}");
            in_block = true;
            shown += 1;
        } else if in_block && (line.contains("OUTPUT") || line.starts_with("CONSTRUCT") || line.contains("TARGET") || line.contains("LOSS")) {
            println!("      {}", line.trim());
        } else if line.trim().is_empty() {
            in_block = false;
        }
        if shown > 16 {
            println!("  ...");
            break;
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let dir = dataset_dir();
    let train_images = load_images(&dir, "train-images-idx3-ubyte", TRAIN_COUNT)?;
    let train_labels = load_labels(&dir, "train-labels-idx1-ubyte", TRAIN_COUNT)?;
    let test_images = load_images(&dir, "t10k-images-idx3-ubyte", TEST_COUNT)?;
    let test_labels = load_labels(&dir, "t10k-labels-idx1-ubyte", TEST_COUNT)?;

    let output = std::env::temp_dir().join(format!(
        "kolibrie-mnist-addition-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
    ));
    let mut db = SparqlDatabase::with_ml_context(MlExecutionContext::trusted_local(&output)?);
    for (index, (image, label)) in train_images.iter().zip(&train_labels).enumerate() {
        load_image(&mut db, &term(&format!("train/{index}")), image, "train", Some(*label));
    }
    for (index, image) in test_images.iter().enumerate() {
        load_image(&mut db, &image_term(index), image, "test", None);
    }
    load_knowledge(&mut db)?;
    let pairs: Vec<(usize, usize)> = (0..TEST_COUNT / 2).map(|k| (2 * k, 2 * k + 1)).collect();
    for (k, (left, right)) in pairs.iter().enumerate() {
        load_pair(&mut db, &pair_term(k), &image_term(*left), &image_term(*right));
    }

    let text = full_program(0.001, 3, 16, "digit_net.bin", "test");
    println!("Program (RULE syntax + TRAIN NEURAL RELATION + ML.PREDICT):");
    excerpt(&text);

    let started = Instant::now();
    let program = compile_program(&text, &db.ml_context)?;
    let options = ProgramOptions {
        training_seed: Some(0),
        required_choices: Some((0..TEST_COUNT).map(|i| (DIGIT.to_string(), image_term(i))).collect()),
        ..Default::default()
    };
    let mut result = execute_program(&mut db, &program, &options)?;
    let timings = result.timings().clone();
    println!(
        "\nExecuted in {:.2}s: training {:.2}s, ML.PREDICT {:.3}s (+{:.3}s input), rules {:.3}s, {} derived facts",
        started.elapsed().as_secs_f64(),
        timings.training.as_secs_f64(),
        timings.neural_inference.as_secs_f64(),
        timings.prediction_input.as_secs_f64(),
        timings.logical_inference.as_secs_f64(),
        result.derived_facts()
    );

    let mut digit_hits = 0;
    for (index, label) in test_labels.iter().enumerate() {
        let probabilities = result.snapshot().distribution(DIGIT, &image_term(index)).ok_or("missing prediction")?;
        let best = (0..10).max_by(|a, b| probabilities[*a].total_cmp(&probabilities[*b])).unwrap();
        digit_hits += usize::from(best == *label);
    }
    println!("Digit accuracy on {TEST_COUNT} test images: {digit_hits}/{TEST_COUNT}");

    println!("\nPer pair: true digits, most likely sum, P(true sum), total P(sum), checks of the left image");
    let mut sum_hits = 0;
    for (k, (left, right)) in pairs.iter().enumerate() {
        let query = pair_term(k);
        let sums: Vec<f64> = (0..19)
            .map(|s| result.answer_probability(&query, SUM, &sum_term(s)))
            .collect::<Result<_, _>>()?;
        let best = (0..19).max_by(|a, b| sums[*a].total_cmp(&sums[*b])).unwrap();
        let truth = test_labels[*left] + test_labels[*right];
        sum_hits += usize::from(best == truth);
        let checks: Vec<String> = ANSWER_KINDS
            .iter()
            .map(|kind| Ok(format!("{kind}={:.3}", result.answer_probability(&query, ANSWER, &term(kind))?)))
            .collect::<Result<_, kolibrie::program::ProgramError>>()?;
        println!(
            "  q{k}: {}+{}  argmax sum {best:>2}  P(true)={:.3}  total={:.6}  {}",
            test_labels[*left],
            test_labels[*right],
            sums[truth],
            sums.iter().sum::<f64>(),
            checks.join(" ")
        );
    }
    println!("Addition accuracy: {sum_hits}/{}", pairs.len());

    let rows = query_program_result(&mut result, SUM_ANNOTATIONS)?;
    println!("\nSPARQL-star answer query returned {} << ?q mnist:sum ?s >> prob:value ?p rows", rows.len());
    println!("Model artifact: {}", output.join("digit_net.bin").display());
    Ok(())
}
