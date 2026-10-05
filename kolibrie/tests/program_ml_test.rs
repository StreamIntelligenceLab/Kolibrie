#![cfg(feature = "ml")]

mod ml_local {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/ml_local.rs"));
}

use kolibrie::program::*;
use kolibrie::sparql_database::SparqlDatabase;
use ml::{MlpNeuralPredicate, OutputType};

const EX: &str = "http://example.org/";

fn ex(local: &str) -> String {
    format!("{EX}{local}")
}

fn samples() -> Vec<(&'static str, &'static str, [f64; 3], Option<&'static str>)> {
    vec![
        ("s0", "train", [1.0, 0.0, 0.0], Some("A")),
        ("s1", "train", [0.9, 0.1, 0.0], Some("A")),
        ("s2", "train", [0.0, 1.0, 0.0], Some("B")),
        ("s3", "train", [0.1, 0.9, 0.0], Some("B")),
        ("s4", "train", [0.0, 0.0, 1.0], Some("C")),
        ("s5", "train", [0.0, 0.1, 0.9], Some("C")),
        ("e0", "eval", [0.8, 0.2, 0.0], None),
        ("e1", "eval", [0.0, 0.2, 0.8], None),
        ("e2", "eval", [0.8, 0.2, 0.0], None),
    ]
}

fn populate(db: &mut SparqlDatabase, eval_labels: bool) {
    for (name, split, features, label) in samples() {
        let subject = ex(name);
        db.add_triple_parts(&subject, &ex("split"), &ex(split));
        for (index, value) in features.iter().enumerate() {
            db.add_triple_parts(&subject, &ex(&format!("x{index}")), &value.to_string());
        }
        if let Some(label) = label {
            db.add_triple_parts(&subject, &ex("gold"), &ex(label));
        } else if eval_labels {
            db.add_triple_parts(&subject, &ex("gold"), &ex("C"));
        }
    }
    db.add_triple_parts(&ex("pair"), &ex("left"), &ex("e0"));
    db.add_triple_parts(&ex("pair"), &ex("right"), &ex("e1"));
}

fn program(save_to: &str, predict_select: &str, rules: &str) -> String {
    format!(
        r#"PREFIX ex: <http://example.org/>
MODEL "abc" {{ ARCH MLP {{ HIDDEN [8] }} OUTPUT EXCLUSIVE {{ ex:A, ex:B, ex:C }} }}
NEURAL RELATION ex:cls USING MODEL "abc" {{
    INPUT {{ ?item ex:x0 ?x0 . ?item ex:x1 ?x1 . ?item ex:x2 ?x2 . }}
    FEATURES {{ ?x0, ?x1, ?x2 }}
}}
TRAIN NEURAL RELATION ex:cls {{
    DATA {{ ?item ex:split ex:train . ?item ex:gold ?label . }}
    LABEL ?label
    TARGET {{ ?item ex:cls ?label }}
    LOSS cross_entropy
    OPTIMIZER adam
    LEARNING_RATE 0.05
    EPOCHS 40
    BATCH_SIZE 4
    SAVE_TO "{save_to}"
}}
ML.PREDICT(MODEL "abc",
    INPUT {{ SELECT {predict_select} WHERE {{ ?item ex:split ex:eval . ?item ex:x0 ?x0 . ?item ex:x1 ?x1 . ?item ex:x2 ?x2 . }} }},
    OUTPUT ?c DISTRIBUTION)
{rules}"#
    )
}

const SAME_CLASS: &str = r#"RULE :Same PROB(combination=sdd) :-
CONSTRUCT { ?p ex:same ex:yes . }
WHERE { ?p ex:left ?a . ?p ex:right ?b . ?a ex:cls ?k . ?b ex:cls ?k . }
"#;

fn eval_anchors() -> Vec<(String, String)> {
    ["e0", "e1", "e2"]
        .iter()
        .map(|name| (ex("cls"), ex(name)))
        .collect()
}

fn options(seed: u64) -> ProgramOptions {
    ProgramOptions {
        training_seed: Some(seed),
        required_choices: Some(eval_anchors()),
        retain_provenance: true,
        ..Default::default()
    }
}

#[test]
fn trained_distribution_prediction_feeds_joint_rules() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    let text = program("abc_joint.bin", "?item ?x0 ?x1 ?x2", SAME_CLASS);
    let compiled = compile_program(&text, &db.ml_context).unwrap();
    let result = execute_program(&mut db, &compiled, &options(3)).unwrap();

    let labels = vec![ex("A"), ex("B"), ex("C")];
    assert_eq!(result.snapshot().labels(&ex("cls")).unwrap(), labels.as_slice());
    let left = result.snapshot().distribution(&ex("cls"), &ex("e0")).unwrap().to_vec();
    let right = result.snapshot().distribution(&ex("cls"), &ex("e1")).unwrap().to_vec();
    let expected: f64 = left.iter().zip(&right).map(|(a, b)| a * b).sum();
    let same = result.answer_probability(&ex("pair"), &ex("same"), &ex("yes")).unwrap();
    assert!((same - expected).abs() <= 1e-12);
    assert!(left[0] > 0.5 && right[2] > 0.5, "{left:?} {right:?}");

    let metadata = &result.snapshot().neural_metadata()[0];
    assert_eq!(metadata.feature_vars, vec!["?x0", "?x1", "?x2"]);
    assert_eq!(metadata.labels, labels);
    assert_eq!(metadata.rows, 3);
    assert_eq!(result.training()[0].artifact_sha256, metadata.artifact_sha256);
    assert_eq!(result.coverage().prediction_rows, 3);
    assert!(result.coverage().confirmed);

    let path = db.ml_context.local_artifact("abc_joint.bin").unwrap();
    let bytes = std::fs::read(path).unwrap();
    let model = MlpNeuralPredicate::from_bytes(3, &[8], OutputType::Categorical(3), &bytes).unwrap();
    for (name, _, features, _) in samples().into_iter().filter(|s| s.1 == "eval") {
        let direct = model.predict(&[features.to_vec()]).unwrap().remove(0);
        let syntax = result.snapshot().distribution(&ex("cls"), &ex(name)).unwrap();
        assert_eq!(direct.as_slice(), syntax, "direct and syntax-driven predictions differ for {name}");
    }
    assert_eq!(db.neural_model_artifacts.get("abc").map(String::as_str), Some("abc_joint.bin"));
    assert!(db.neural_relation_decls.contains_key(&ex("cls")));
}

#[test]
fn prediction_from_a_published_artifact_matches_the_training_run() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    let trained = compile_program(&program("abc_reuse.bin", "?item ?x0 ?x1 ?x2", ""), &db.ml_context).unwrap();
    let first = execute_program(&mut db, &trained, &options(5)).unwrap();

    let predict_only = r#"PREFIX ex: <http://example.org/>
ML.PREDICT(MODEL "abc",
    INPUT { SELECT ?item ?x0 ?x1 ?x2 WHERE { ?item ex:split ex:eval . ?item ex:x0 ?x0 . ?item ex:x1 ?x1 . ?item ex:x2 ?x2 . } },
    OUTPUT ?c DISTRIBUTION)
"#;
    let reused = compile_program(predict_only, &db.ml_context).unwrap();
    let mut batched = options(5);
    batched.prediction_batch_size = Some(2);
    let second = execute_program(&mut db, &reused, &batched).unwrap();
    for (_, anchor) in eval_anchors() {
        assert_eq!(
            first.snapshot().distribution(&ex("cls"), &anchor),
            second.snapshot().distribution(&ex("cls"), &anchor)
        );
    }
    assert_eq!(
        first.snapshot().neural_metadata()[0].artifact_sha256,
        second.snapshot().neural_metadata()[0].artifact_sha256
    );
}

#[test]
fn evaluation_labels_do_not_reach_training() {
    let mut plain = ml_local::database();
    populate(&mut plain, false);
    let mut labelled = ml_local::database();
    populate(&mut labelled, true);
    let text = program("abc_leak.bin", "?item ?x0 ?x1 ?x2", "");
    let compiled = compile_program(&text, &plain.ml_context).unwrap();
    let a = execute_program(&mut plain, &compiled, &options(9)).unwrap();
    let b = execute_program(&mut labelled, &compiled, &options(9)).unwrap();
    assert_eq!(a.training()[0].artifact_sha256, b.training()[0].artifact_sha256);
    for (_, anchor) in eval_anchors() {
        assert_eq!(
            a.snapshot().distribution(&ex("cls"), &anchor),
            b.snapshot().distribution(&ex("cls"), &anchor)
        );
    }
}

#[test]
fn identical_features_keep_distinct_anchors_as_distinct_choices() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    let compiled = compile_program(&program("abc_ident.bin", "?item ?x0 ?x1 ?x2", ""), &db.ml_context).unwrap();
    let result = execute_program(&mut db, &compiled, &options(1)).unwrap();
    assert_eq!(result.snapshot().choice_groups(), 3);
    assert_eq!(
        result.snapshot().distribution(&ex("cls"), &ex("e0")),
        result.snapshot().distribution(&ex("cls"), &ex("e2"))
    );
}

#[test]
fn invalid_programs_fail_before_training_or_publication() {
    let mut db = ml_local::database();
    populate(&mut db, false);

    let missing_anchor = program("abc_anchor.bin", "?x0 ?x1 ?x2", "");
    let compiled = compile_program(&missing_anchor, &db.ml_context).unwrap();
    assert!(matches!(
        execute_program(&mut db, &compiled, &options(1)),
        Err(ProgramError::Unsupported(_))
    ));
    assert!(!db.ml_context.local_artifact("abc_anchor.bin").unwrap().exists());

    let argmax = program("abc_argmax.bin", "?item ?x0 ?x1 ?x2", "").replace(" DISTRIBUTION", "");
    assert!(matches!(
        compile_program(&argmax, &db.ml_context),
        Err(ProgramError::Unsupported(_))
    ));
    assert!(matches!(
        compile_program(&program("abc_policy.bin", "?item ?x0 ?x1 ?x2", ""), &kolibrie::ml_policy::MlExecutionContext::disabled()),
        Err(ProgramError::Policy(_))
    ));
    assert!(db.neural_model_artifacts.is_empty());
    assert!(db.neural_relation_decls.is_empty());
}

#[test]
fn failed_prediction_keeps_the_previous_publication_and_writes_nothing() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    let mut published = PublishedProgramResult::new();
    let good = compile_program(&program("abc_good.bin", "?item ?x0 ?x1 ?x2", SAME_CLASS), &db.ml_context).unwrap();
    published.publish(execute_program(&mut db, &good, &options(2))).unwrap();
    let before = published
        .current()
        .unwrap()
        .answer_probability(&ex("pair"), &ex("same"), &ex("yes"))
        .unwrap();

    db.add_triple_parts(&ex("e1"), &ex("x0"), "not-a-number");
    let failing = compile_program(&program("abc_failed.bin", "?item ?x0 ?x1 ?x2", SAME_CLASS), &db.ml_context).unwrap();
    let outcome = published.publish(execute_program(&mut db, &failing, &options(2)));
    assert!(outcome.is_err());
    assert!(!db.ml_context.local_artifact("abc_failed.bin").unwrap().exists());
    assert_eq!(db.neural_model_artifacts.get("abc").map(String::as_str), Some("abc_good.bin"));
    assert_eq!(
        published
            .current()
            .unwrap()
            .answer_probability(&ex("pair"), &ex("same"), &ex("yes"))
            .unwrap(),
        before
    );
}

#[test]
fn result_queries_never_trigger_prediction() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    let compiled = compile_program(&program("abc_query.bin", "?item ?x0 ?x1 ?x2", SAME_CLASS), &db.ml_context).unwrap();
    let mut result = execute_program(&mut db, &compiled, &options(4)).unwrap();
    let rows = query_program_result(
        &mut result,
        "PREFIX ex: <http://example.org/>\nSELECT ?item ?k WHERE { ?item ex:cls ?k . }",
    )
    .unwrap();
    assert_eq!(rows.len(), 9);
    assert!(db.neural_materialized_triples.is_empty());
    assert!(db.query_default_triples(None, None, None).iter().all(|triple| {
        db.decode_any(triple.predicate).as_deref() != Some(ex("cls").as_str())
    }));
}

#[test]
fn incomplete_prediction_coverage_is_an_error() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    let compiled = compile_program(&program("abc_cover.bin", "?item ?x0 ?x1 ?x2", SAME_CLASS), &db.ml_context).unwrap();
    let mut missing = options(1);
    missing
        .required_choices
        .as_mut()
        .unwrap()
        .push((ex("cls"), ex("s0")));
    assert!(matches!(
        execute_program(&mut db, &compiled, &missing),
        Err(ProgramError::Coverage(_))
    ));
    assert!(!db.ml_context.local_artifact("abc_cover.bin").unwrap().exists());
}

#[test]
fn conflicting_features_for_one_anchor_are_rejected() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    db.add_triple_parts(&ex("e0"), &ex("x0"), "0.3");
    let compiled = compile_program(&program("abc_conflict.bin", "?item ?x0 ?x1 ?x2", ""), &db.ml_context).unwrap();
    assert!(matches!(
        execute_program(&mut db, &compiled, &options(1)),
        Err(ProgramError::Snapshot(_))
    ));
}

#[test]
fn predictions_cannot_overlap_asserted_facts_even_without_rules() {
    let mut db = ml_local::database();
    populate(&mut db, false);
    db.add_triple_parts(&ex("e0"), &ex("cls"), &ex("A"));
    let compiled = compile_program(&program("abc_overlap.bin", "?item ?x0 ?x1 ?x2", ""), &db.ml_context).unwrap();
    assert!(matches!(
        execute_program(&mut db, &compiled, &options(1)),
        Err(ProgramError::Snapshot(_))
    ));
    assert!(!db.ml_context.local_artifact("abc_overlap.bin").unwrap().exists());
}

#[test]
fn binary_distributions_are_independent_bernoulli_facts() {
    let mut db = ml_local::database();
    for (name, split, x, label) in [
        ("t0", "train", "1.0", "1"),
        ("t1", "train", "0.9", "1"),
        ("t2", "train", "0.0", "0"),
        ("t3", "train", "0.1", "0"),
        ("u0", "eval", "0.95", ""),
        ("u1", "eval", "0.05", ""),
    ] {
        db.add_triple_parts(&ex(name), &ex("split"), &ex(split));
        db.add_triple_parts(&ex(name), &ex("x"), x);
        if !label.is_empty() {
            db.add_triple_parts(&ex(name), &ex("gold"), label);
        }
        db.add_triple_parts(&ex(name), &ex("in"), &ex("zone"));
    }
    let text = r#"PREFIX ex: <http://example.org/>
MODEL "risk" { ARCH MLP { HIDDEN [] } OUTPUT BINARY { ex:yes } }
NEURAL RELATION ex:risky USING MODEL "risk" { INPUT { ?item ex:x ?x . } FEATURES { ?x } }
TRAIN NEURAL RELATION ex:risky {
    DATA { ?item ex:split ex:train . ?item ex:gold ?label . }
    LABEL ?label
    TARGET { ?item ex:risky ex:yes }
    LOSS binary_cross_entropy
    OPTIMIZER adam
    LEARNING_RATE 0.1
    EPOCHS 60
    BATCH_SIZE 2
    SAVE_TO "risk_binary.bin"
}
ML.PREDICT(MODEL "risk", INPUT { SELECT ?item ?x WHERE { ?item ex:split ex:eval . ?item ex:x ?x . } }, OUTPUT ?r DISTRIBUTION)
RULE :Alert PROB(combination=sdd) :- CONSTRUCT { ex:zone ex:alert ex:on . } WHERE { ?item ex:in ex:zone . ?item ex:risky ex:yes . }
"#;
    let compiled = compile_program(text, &db.ml_context).unwrap();
    let result = execute_program(
        &mut db,
        &compiled,
        &ProgramOptions {
            training_seed: Some(7),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(result.snapshot().independent_facts(), 2);
    let p0 = result.fact_probability(&ex("u0"), &ex("risky"), &ex("yes")).unwrap();
    let p1 = result.fact_probability(&ex("u1"), &ex("risky"), &ex("yes")).unwrap();
    assert!(p0 > 0.5 && p1 < 0.5, "{p0} {p1}");
    let alert = result.fact_probability(&ex("zone"), &ex("alert"), &ex("on")).unwrap();
    assert!((alert - (1.0 - (1.0 - p0) * (1.0 - p1))).abs() <= 1e-12);
}
