//! JSON adapter for the standalone mnist_deepproblog benchmark. All timings exclude JSON I/O.
use std::{collections::BTreeMap, error::Error, fs, path::Path, time::Instant};

use kolibrie::{
    ml_policy::MlExecutionContext,
    program::{
        compile_program, execute_program, parse_probability_literal, query_program_result,
        PredictionSnapshot, ProgramOptions, ProgramTimings,
    },
    sparql_database::SparqlDatabase,
};
use ml::{MlpNeuralPredicate, OutputType};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

include!("mnist_program.rs");
include!("mnist_oracle.rs");
use mnist_program::*;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const PROTOCOL_VERSION: u32 = 3;
const ARTIFACT: &str = "digit_net.bin";
const ORACLE_TOLERANCE: f64 = 1e-12;

#[derive(Deserialize)]
struct Request {
    action: String,
    #[serde(default)]
    train_images: Vec<Vec<f64>>,
    #[serde(default)]
    train_labels: Vec<usize>,
    #[serde(default)]
    eval_images: BTreeMap<String, Vec<Vec<f64>>>,
    #[serde(default)]
    probabilities: Vec<Vec<f64>>,
    #[serde(default)]
    pairs: Vec<[usize; 2]>,
    #[serde(default)]
    seed: u64,
    #[serde(default = "one")]
    epochs: usize,
    #[serde(default = "one")]
    batch_size: usize,
    #[serde(default = "rate")]
    learning_rate: f64,
    #[serde(default = "one")]
    repeats: usize,
}
fn one() -> usize {
    1
}
fn rate() -> f64 {
    0.001
}

fn seconds(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64()
}

fn threads() -> Value {
    let get = |key: &str| std::env::var(key).ok();
    json!({"RAYON_NUM_THREADS": get("RAYON_NUM_THREADS"), "OMP_NUM_THREADS": get("OMP_NUM_THREADS")})
}

fn data_sha256(rows: &[&[f64]], labels: &[usize]) -> String {
    let mut hash = Sha256::new();
    for row in rows {
        for value in *row {
            hash.update(value.to_le_bytes());
        }
    }
    for label in labels {
        hash.update((*label as u64).to_le_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn program_timings(parse: f64, timings: &ProgramTimings) -> Value {
    json!({
        "parse_s": parse,
        "validation_s": seconds(timings.validation),
        "training_s": seconds(timings.training),
        "prediction_input_s": seconds(timings.prediction_input),
        "neural_inference_s": seconds(timings.neural_inference),
        "prediction_publication_s": seconds(timings.prediction_publication),
        "data_preparation_s": seconds(timings.data_preparation),
        "logical_inference_s": seconds(timings.logical_inference),
        "probability_recovery_s": seconds(timings.probability_recovery),
        "result_publication_s": seconds(timings.result_publication),
        "artifact_publication_s": seconds(timings.artifact_publication),
        "total_s": seconds(timings.total)
    })
}

fn train(req: &Request, output_dir: &Path) -> Result<Value> {
    if req.train_images.is_empty()
        || req.train_images.len() != req.train_labels.len()
        || req.train_labels.iter().any(|&x| x > 9)
        || req.batch_size == 0
        || req.epochs == 0
        || !req.learning_rate.is_finite()
        || req.learning_rate <= 0.0
    {
        return Err("invalid training configuration".into());
    }
    for row in req
        .train_images
        .iter()
        .chain(req.eval_images.values().flatten())
    {
        if row.len() != 784
            || row
                .iter()
                .any(|v| !v.is_finite() || !(0.0..=1.0).contains(v))
        {
            return Err("expected 784 finite pixels in [0,1]".into());
        }
    }
    let start = Instant::now();
    let mut db = SparqlDatabase::with_ml_context(MlExecutionContext::trusted_local(output_dir)?);
    for (i, (image, label)) in req.train_images.iter().zip(&req.train_labels).enumerate() {
        load_image(&mut db, &term(&format!("train/{i:08}")), image, "train", Some(*label));
    }
    let train_population = start.elapsed();
    let eval_start = Instant::now();
    let mut anchors: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (name, images) in &req.eval_images {
        if images.is_empty() {
            return Err("empty evaluation set".into());
        }
        for (i, image) in images.iter().enumerate() {
            let anchor = term(&format!("eval/{name}/{i:05}"));
            load_image(&mut db, &anchor, image, "eval", None);
            anchors.entry(name).or_default().push(anchor);
        }
    }
    let eval_population_s = seconds(eval_start.elapsed());
    let parse_start = Instant::now();
    let program = compile_program(
        &neural_program(req.learning_rate, req.epochs, req.batch_size, ARTIFACT, "eval"),
        &db.ml_context,
    )?;
    let parse_s = seconds(parse_start.elapsed());
    let prepare_s = seconds(train_population) + parse_s;

    let options = ProgramOptions {
        training_seed: Some(req.seed),
        required_choices: Some(
            anchors
                .values()
                .flatten()
                .map(|anchor| (DIGIT.to_string(), anchor.clone()))
                .collect(),
        ),
        prediction_batch_size: Some(req.batch_size),
        ..Default::default()
    };
    let result = execute_program(&mut db, &program, &options)?;
    let timings = result.timings().clone();
    let report = result.training().first().ok_or("the program did not train")?.clone();

    let mut predictions = BTreeMap::new();
    for (name, set) in &anchors {
        let rows = set
            .iter()
            .map(|anchor| {
                result
                    .snapshot()
                    .distribution(DIGIT, anchor)
                    .map(<[f64]>::to_vec)
                    .ok_or_else(|| format!("no prediction for {anchor}"))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        predictions.insert(name.to_string(), rows);
    }

    // Direct prediction with the published artifact, the same rows and batching.
    let bytes = fs::read(output_dir.join(ARTIFACT))?;
    if format!("{:x}", Sha256::digest(&bytes)) != report.artifact_sha256 {
        return Err("published artifact differs from the trained model".into());
    }
    let model = MlpNeuralPredicate::from_bytes(784, &[64, 32], OutputType::Categorical(10), &bytes)?;
    let mut latency = BTreeMap::new();
    let mut inference_s = BTreeMap::new();
    let mut max_difference: f64 = 0.0;
    for (name, images) in &req.eval_images {
        model.predict(&images[..images.len().min(req.batch_size)])?; // untimed warm-up
        let mut probs = Vec::new();
        let mut times = Vec::new();
        let set_start = Instant::now();
        for batch in images.chunks(req.batch_size) {
            let start = Instant::now();
            let out = model.predict(batch)?;
            times.push(start.elapsed().as_secs_f64());
            probs.extend(out);
        }
        inference_s.insert(name.clone(), set_start.elapsed().as_secs_f64());
        latency.insert(name.clone(), times);
        for (direct, syntax) in probs.iter().flatten().zip(predictions[name].iter().flatten()) {
            max_difference = max_difference.max((direct - syntax).abs());
        }
    }
    if max_difference != 0.0 {
        return Err(format!("syntax-driven and direct predictions differ by {max_difference}").into());
    }

    let train_rows: Vec<&[f64]> = req.train_images.iter().map(Vec::as_slice).collect();
    let eval_rows: Vec<&[f64]> = req.eval_images.values().flatten().map(Vec::as_slice).collect();
    Ok(
        json!({"backend":"kolibrie", "prepare_s":prepare_s,
        "train_s":seconds(timings.validation + timings.training),
        "data_fetch_s":seconds(report.data_fetch), "learning_s":seconds(report.learning),
        "phase_labels":{
            "data_fetch":"RDF/SPARQL training-row query and row conversion, inside train_s",
            "learning":"epoch loop: shuffling, feature parsing, circuit compilation, gradients and optimizer updates",
            "other_fit":"RDF population of training images, program parsing, static validation, TRAIN lowering, row sorting, model initialization and artifact staging",
            "eval_population":"RDF population of evaluation images for ML.PREDICT, outside fit"},
        "probabilities":predictions, "inference_batch_s":latency, "inference_s":inference_s,
        "program_timings":program_timings(parse_s, &timings),
        "eval_population_s":eval_population_s,
        "syntax_vs_direct_max_abs_diff":max_difference,
        "metadata":{
            "protocol_version":PROTOCOL_VERSION,
            "supervision":"individual digit labels via TRAIN NEURAL RELATION (LOSS cross_entropy, mean over every supervised example)",
            "execution_scope":"one program: TRAIN NEURAL RELATION, then ML.PREDICT OUTPUT ?digit DISTRIBUTION over every evaluation image",
            "prediction_protocol":"probabilities are the program's validated snapshot; neural inference timing is direct prediction with the same published artifact, rows and batching",
            "preprocessing":"pixels/255 as RDF literals; features ?p0..?p783 in row-major order; labels mnist:d0..mnist:d9",
            "seed":req.seed, "model_sha256":report.artifact_sha256,
            "train_data_sha256":data_sha256(&train_rows, &req.train_labels),
            "eval_data_sha256":data_sha256(&eval_rows, &[]),
            "threads":threads()},
        "dtype":"float64 (Candle return tensor float32; native probabilities float64)"}),
    )
}

fn reason(req: &Request) -> Result<Value> {
    if req.pairs.is_empty() || req.repeats == 0 {
        return Err("pairs and repeats must be nonempty".into());
    }
    let parse_start = Instant::now();
    let program = compile_program(REASONING_RULES, &MlExecutionContext::disabled())?;
    let parse_s = seconds(parse_start.elapsed());
    let labels = digit_labels();
    let mut answers = Vec::new();
    let mut cold = Vec::new();
    let mut warm = Vec::new();
    let mut annotation_reads = Vec::new();
    let mut phases = Vec::new();
    let mut oracle_error: f64 = 0.0;
    let reasoning_start = Instant::now();
    for (index, &[a, b]) in req.pairs.iter().enumerate() {
        if a >= req.probabilities.len() || b >= req.probabilities.len() {
            return Err("invalid pair index".into());
        }
        let start = Instant::now();
        let query = pair_term(index);
        let (left, right) = (image_term(a), image_term(b));
        let mut db = SparqlDatabase::new();
        load_knowledge(&mut db)?;
        load_pair(&mut db, &query, &left, &right);
        let mut snapshot = PredictionSnapshot::new();
        snapshot.declare_exclusive_relation(DIGIT, &labels)?;
        snapshot.add_distribution(DIGIT, &left, &req.probabilities[a], None)?;
        snapshot.add_distribution(DIGIT, &right, &req.probabilities[b], None)?;
        let options = ProgramOptions {
            frozen_predictions: Some(snapshot),
            required_choices: Some(vec![
                (DIGIT.to_string(), left.clone()),
                (DIGIT.to_string(), right.clone()),
            ]),
            retain_provenance: true,
            ..Default::default()
        };
        let input_s = seconds(start.elapsed());
        let mut result = execute_program(&mut db, &program, &options)?;
        let answer_start = Instant::now();
        let prepared = result.prepare_answers(&answer_targets(&query))?;
        let values = prepared.recover();
        let answer_s = seconds(answer_start.elapsed());
        cold.push(seconds(start.elapsed()));
        for _ in 0..req.repeats {
            let start = Instant::now();
            std::hint::black_box(prepared.recover());
            warm.push(start.elapsed().as_secs_f64());
        }

        let read_start = Instant::now();
        let mut annotated = BTreeMap::new();
        for select in [SUM_ANNOTATIONS, ANSWER_ANNOTATIONS] {
            for row in query_program_result(&mut result, select)? {
                let probability = parse_probability_literal(&row[2]).ok_or("malformed prob:value literal")?;
                annotated.insert(row[1].clone(), probability);
            }
        }
        annotation_reads.push(seconds(read_start.elapsed()));
        for ((_, _, object), value) in answer_targets(&query).iter().zip(&values) {
            let read = annotated.get(object).copied().unwrap_or(0.0);
            if read.to_bits() != value.to_bits() {
                return Err(format!("annotation for {object} reads {read}, expected {value}").into());
            }
        }

        let expected = mnist_oracle::answers(&req.probabilities[a], &req.probabilities[b], a == b);
        for (actual, oracle) in values.iter().zip(&expected) {
            oracle_error = oracle_error.max((actual - oracle).abs());
        }
        if oracle_error > ORACLE_TOLERANCE {
            return Err(format!("RULE program differs from the retained oracle by {oracle_error}").into());
        }
        let timings = result.timings();
        phases.push(json!({
            "input_preparation_s": input_s,
            "validation_s": seconds(timings.validation),
            "data_preparation_s": seconds(timings.data_preparation),
            "logical_inference_s": seconds(timings.logical_inference),
            "probability_recovery_s": seconds(timings.probability_recovery),
            "result_publication_s": seconds(timings.result_publication),
            "answer_preparation_s": answer_s,
        }));
        answers.push(
            json!({"sum":values[..19], "even":values[19], "prime":values[20],
            "even_or_prime":values[21], "even_and_prime":values[22],
            "repeat_even":values[23], "contradiction":values[24]}),
        );
    }
    let reasoning_s = seconds(reasoning_start.elapsed());
    Ok(
        json!({"backend":"kolibrie", "answers":answers, "cold_bundle_s":cold, "warm_bundle_s":warm,
        "reasoning_s":reasoning_s, "parse_s":parse_s, "annotation_query_bundle_s":annotation_reads,
        "reasoning_phases":phases, "rust_oracle_max_abs_error":oracle_error,
        "metadata":{
            "protocol_version":PROTOCOL_VERSION,
            "execution_scope":"reference RULE program, one execution per pair over frozen digit distributions",
            "cold_operation":"asserted input construction, program execution (joint SDD fixpoint, probability recovery, result publication) and answer preparation",
            "warm_operation":"probability recovery from retained SDD provenance of the prepared 25 answers",
            "annotation_operation":"SPARQL-star rereading of published prob:value annotations, reported separately",
            "oracle":"retained Rust-built rules, maximum absolute difference 1e-12",
            "threads":threads()}}),
    )
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: mnist_compare REQUEST.json RESULT.json".into());
    }
    let req: Request = serde_json::from_slice(&fs::read(&args[1])?)?;
    let output = Path::new(&args[2]);
    let dir = output.parent().ok_or("result needs a parent directory")?;
    fs::create_dir_all(dir)?;
    let dir = fs::canonicalize(dir)?;
    let result = match req.action.as_str() {
        "train" => train(&req, &dir)?,
        "reason" => reason(&req)?,
        _ => return Err("unknown action".into()),
    };
    fs::write(output, serde_json::to_vec_pretty(&result)?)?;
    Ok(())
}
