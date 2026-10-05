use std::sync::Arc;
use std::time::Duration;

use datalog::reasoning::materialisation::hybrid_materialisation::validate_hybrid_rules;
use kolibrie::ml_policy::MlExecutionContext;
use kolibrie::parser::{parse_combined_query, process_rule_definition};
use kolibrie::program::*;
use kolibrie::sparql_database::SparqlDatabase;
use shared::dictionary::Dictionary;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/sparql_syntax/ml_train/mnist_program.rs"
));
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/sparql_syntax/ml_train/mnist_oracle.rs"
));
use mnist_program::*;

const EX: &str = "http://example.org/";

fn compile(text: &str) -> Result<CompiledProgram, ProgramError> {
    compile_program(text, &MlExecutionContext::disabled())
}

fn reference() -> CompiledProgram {
    compile(REASONING_RULES).unwrap()
}

fn ex(local: &str) -> String {
    format!("{EX}{local}")
}

fn one_hot(digit: usize) -> Vec<f64> {
    let mut p = vec![0.0; 10];
    p[digit] = 1.0;
    p
}

fn fixtures() -> Vec<Vec<f64>> {
    let mut spread = vec![0.0; 10];
    let mut total = 0.0;
    for (i, value) in spread.iter_mut().enumerate() {
        *value = ((i * 7 + 3) % 11) as f64 + 0.5;
        total += *value;
    }
    for value in &mut spread {
        *value /= total;
    }
    vec![
        one_hot(2),
        one_hot(7),
        vec![0.1; 10],
        spread,
        vec![0.4, 1e-300, 0.1, 0.2, 0.0, 0.3, 0.0, 0.0, 0.0, 0.0],
        [vec![0.999996, 0.000004], vec![0.0; 8]].concat(),
    ]
}

fn pair_options(left: &[f64], right: Option<&[f64]>) -> ProgramOptions {
    let left_image = image_term(0);
    let right_image = if right.is_some() { image_term(1) } else { image_term(0) };
    let mut snapshot = PredictionSnapshot::new();
    snapshot.declare_exclusive_relation(DIGIT, &digit_labels()).unwrap();
    snapshot.add_distribution(DIGIT, &left_image, left, None).unwrap();
    if let Some(right) = right {
        snapshot.add_distribution(DIGIT, &right_image, right, None).unwrap();
    }
    ProgramOptions {
        frozen_predictions: Some(snapshot),
        required_choices: Some(vec![
            (DIGIT.to_string(), left_image),
            (DIGIT.to_string(), right_image),
        ]),
        retain_provenance: true,
        ..Default::default()
    }
}

fn pair_input(same_image: bool) -> SparqlDatabase {
    let mut db = SparqlDatabase::new();
    load_knowledge(&mut db).unwrap();
    let right = if same_image { image_term(0) } else { image_term(1) };
    load_pair(&mut db, &pair_term(0), &image_term(0), &right);
    db
}

fn run_pair(program: &CompiledProgram, left: &[f64], right: Option<&[f64]>) -> ProgramResult {
    let mut db = pair_input(right.is_none());
    execute_program(&mut db, program, &pair_options(left, right)).unwrap()
}

fn answers(result: &ProgramResult) -> Vec<f64> {
    answer_targets(&pair_term(0))
        .iter()
        .map(|(s, p, o)| result.answer_probability(s, p, o).unwrap())
        .collect()
}

fn enumerate(left: &[f64], right: Option<&[f64]>) -> Vec<f64> {
    let mut expected = vec![0.0; 25];
    match right {
        None => {
            for d in 0..10 {
                expected[2 * d] += left[d];
            }
        }
        Some(right) => {
            for a in 0..10 {
                for b in 0..10 {
                    expected[a + b] += left[a] * right[b];
                }
            }
        }
    }
    let even: f64 = [0, 2, 4, 6, 8].iter().map(|d| left[*d]).sum();
    let prime: f64 = [2, 3, 5, 7].iter().map(|d| left[*d]).sum();
    expected[19] = even;
    expected[20] = prime;
    expected[21] = even + prime - left[2];
    expected[22] = left[2];
    expected[23] = even;
    expected[24] = 0.0;
    expected
}

fn assert_close(actual: &[f64], expected: &[f64], tolerance: f64) {
    assert_eq!(actual.len(), expected.len());
    for (index, (a, e)) in actual.iter().zip(expected).enumerate() {
        assert!((a - e).abs() <= tolerance, "answer {index}: {a} vs {e}");
    }
}

#[test]
fn multiple_rule_blocks_parse_completely() {
    let (rest, combined) = parse_combined_query(REASONING_RULES).unwrap();
    assert!(rest.trim().is_empty());
    assert_eq!(combined.rules.len(), 10);
    assert!(combined.rules.iter().all(|rule| rule.ml_predict.is_none()));
    assert!(combined.single_rule().is_err());
}

#[test]
fn top_level_prediction_before_rules_and_attached_prediction_keep_their_meaning() {
    let leading = r#"PREFIX ex: <http://example.org/>
ML.PREDICT(MODEL "m", INPUT { SELECT ?s ?x WHERE { ?s ex:x ?x . } }, OUTPUT ?d DISTRIBUTION)
RULE :R PROB(combination=sdd) :- CONSTRUCT { ?s ex:y ex:z . } WHERE { ?s ex:d ex:z . }
"#;
    let (rest, combined) = parse_combined_query(leading).unwrap();
    assert!(rest.trim().is_empty());
    let top = combined.ml_predict.as_ref().unwrap();
    assert!(top.distribution);
    assert_eq!(combined.rules.len(), 1);
    assert!(combined.rules[0].ml_predict.is_none());

    let attached = r#"PREFIX ex: <http://example.org/>
RULE :R :- CONSTRUCT { ?s ex:level ?d . } WHERE { ?s ex:x ?x . }
ML.PREDICT(MODEL "m", INPUT { SELECT ?s ?x WHERE { ?s ex:x ?x . } }, OUTPUT ?d)
"#;
    let (rest, combined) = parse_combined_query(attached).unwrap();
    assert!(rest.trim().is_empty());
    assert!(combined.ml_predict.is_none());
    let attached = combined.rules[0].ml_predict.as_ref().unwrap();
    assert!(!attached.distribution);
}

#[test]
fn unsupported_trailing_statements_fail_before_side_effects() {
    let with_select = format!("{REASONING_RULES}\nSELECT ?s WHERE {{ ?s ?p ?o . }}");
    assert!(matches!(compile(&with_select), Err(ProgramError::Unsupported(_))));
    let with_garbage = format!("{REASONING_RULES}\nNOT A STATEMENT");
    assert!(matches!(compile(&with_garbage), Err(ProgramError::Parse(_))));

    let mut db = SparqlDatabase::new();
    db.add_triple_parts(&ex("s"), &ex("a"), &ex("o"));
    let two_rules = r#"PREFIX ex: <http://example.org/>
RULE :A :- CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }
RULE :B :- CONSTRUCT { ?x ex:c ?y . } WHERE { ?x ex:b ?y . }
"#;
    assert!(process_rule_definition(two_rules, &mut db).is_err());
    assert_eq!(db.query_default_triples(None, None, None).len(), 1);
    assert!(kolibrie::execute_query::execute_sparql_query(
        &format!("{two_rules}\nSELECT ?x WHERE {{ ?x ex:a ?y . }}"),
        &mut db
    )
    .is_err());
    assert_eq!(db.query_default_triples(None, None, None).len(), 1);
}

#[test]
fn checked_lowering_rejects_constructs_it_cannot_represent() {
    let rule = |body: &str, head: &str| {
        format!(
            "PREFIX ex: <http://example.org/>\nRULE :R PROB(combination=sdd) :- CONSTRUCT {{ {head} }} WHERE {{ {body} }}"
        )
    };
    let cases = [
        rule("?x ex:a ?y . BIND(STR(?y) AS ?z)", "?x ex:b ?z ."),
        rule("?x ex:a ?y . NOT ?x ex:c ?y .", "?x ex:b ?y ."),
        rule("?x ex:a ?y . VALUES ?y { ex:o }", "?x ex:b ?y ."),
        rule("?x ex:a ?y . FILTER(?y > 1 || ?y < 0)", "?x ex:b ?y ."),
        rule("?x ex:a ?y . FILTER(?w > 1)", "?x ex:b ?y ."),
        rule("?x ex:a ?y . { SELECT ?x WHERE { ?x ex:c ?z . } }", "?x ex:b ?y ."),
        rule("?x ex:a ?y .", "?x ex:b ?fresh ."),
        rule("?x ex:a ?y . WINDOW :w { ?x ex:c ?y . }", "?x ex:b ?y ."),
        rule("?x ex:a ?y . GRAPH ex:g { ?x ex:c ?y . }", "?x ex:b ?y ."),
        rule("?x ex:a ?y .", "?x ex:b undeclared:thing ."),
        "PREFIX ex: <http://example.org/>\nRULE :R PROB(combination=wmc) :- CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }".to_string(),
        "PREFIX ex: <http://example.org/>\nRULE :R PROB(combination=sdd, threshold=0.3) :- CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }".to_string(),
        "PREFIX ex: <http://example.org/>\nRULE :R :- RSTREAM FROM NAMED WINDOW <http://example.org/w> ON <http://example.org/s> [TUMBLING 5 REPORT NON_EMPTY_CONTENT TICK TUPLE_DRIVEN] CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }".to_string(),
        "PREFIX ex: <http://example.org/>\nRULE :R :- CONSTRUCT { ?s ex:level ?d . } WHERE { ?s ex:x ?x . }\nML.PREDICT(MODEL \"m\", INPUT { SELECT ?s ?x WHERE { ?s ex:x ?x . } }, OUTPUT ?d DISTRIBUTION)".to_string(),
        "PREFIX ex: <http://example.org/>\nRULE :A :- CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }\nRULE :B PROB(combination=sdd) :- CONSTRUCT { ?x ex:c ?y . } WHERE { ?x ex:b ?y . }".to_string(),
    ];
    for (index, program) in cases.iter().enumerate() {
        let error = compile(program).err();
        match (index, &error) {
            (8, Some(ProgramError::Parse(_))) => {}
            (_, Some(ProgramError::Unsupported(_))) => {}
            _ => panic!("case {index} must be rejected as unsupported, got {error:?}:\n{program}"),
        }
    }

    let numeric = rule("?x ex:score ?v . FILTER(?v >= 2)", "?x ex:high ex:yes .");
    let program = compile(&numeric).unwrap();
    let mut db = SparqlDatabase::new();
    db.add_tagged_triple(&ex("a"), &ex("score"), "3", 0.5);
    db.add_tagged_triple(&ex("b"), &ex("score"), "1", 0.5);
    let result = execute_program(&mut db, &program, &ProgramOptions::default()).unwrap();
    assert_eq!(result.fact_probability(&ex("a"), &ex("high"), &ex("yes")), Some(0.5));
    assert_eq!(result.fact_probability(&ex("b"), &ex("high"), &ex("yes")), None);
}

#[test]
fn plain_rules_reject_uncertain_inputs() {
    let program = compile(
        "PREFIX ex: <http://example.org/>\nRULE :A :- CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }",
    )
    .unwrap();
    let mut db = SparqlDatabase::new();
    db.add_tagged_triple(&ex("s"), &ex("a"), &ex("o"), 0.4);
    assert!(matches!(
        execute_program(&mut db, &program, &ProgramOptions::default()),
        Err(ProgramError::Unsupported(_))
    ));
}

#[test]
fn deterministic_chains_reach_one_fixpoint_in_any_rule_order() {
    let rules = [
        "RULE :A :- CONSTRUCT { ?x ex:b ?y . } WHERE { ?x ex:a ?y . }",
        "RULE :B :- CONSTRUCT { ?x ex:c ?y . } WHERE { ?x ex:b ?y . }",
        "RULE :C :- CONSTRUCT { ?x ex:d ?z . } WHERE { ?x ex:c ?y . ?y ex:next ?z . }",
    ];
    let mut outcomes = Vec::new();
    for order in [[0, 1, 2], [2, 1, 0]] {
        let text = format!(
            "PREFIX ex: <http://example.org/>\n{}",
            order.map(|i| rules[i]).join("\n")
        );
        let program = compile(&text).unwrap();
        assert_eq!(program.mode(), Some(RuleMode::Deterministic));
        let mut db = SparqlDatabase::new();
        db.add_triple_parts(&ex("s"), &ex("a"), &ex("m"));
        db.add_triple_parts(&ex("m"), &ex("next"), &ex("t"));
        let mut result = execute_program(&mut db, &program, &ProgramOptions::default()).unwrap();
        assert_eq!(result.fact_probability(&ex("s"), &ex("d"), &ex("t")), Some(1.0));
        assert_eq!(result.derived_facts(), 3);
        let rows = query_program_result(
            &mut result,
            "PREFIX ex: <http://example.org/>\nSELECT ?x ?z WHERE { ?x ex:d ?z . }",
        )
        .unwrap();
        outcomes.push(rows);
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0], vec![vec![ex("s"), ex("t")]]);
}

#[test]
fn probabilistic_rules_combine_every_proof_in_any_order() {
    let rules = [
        "RULE :Smoke PROB(combination=sdd) :- CONSTRUCT { ?x ex:alarm ex:on . } WHERE { ?x ex:smoke ex:yes . }",
        "RULE :Heat PROB(combination=sdd) :- CONSTRUCT { ?x ex:alarm ex:on . } WHERE { ?x ex:heat ex:yes . }",
        "RULE :Call PROB(combination=sdd) :- CONSTRUCT { ?x ex:call ex:fire . } WHERE { ?x ex:alarm ex:on . ?x ex:staffed ex:yes . }",
    ];
    let mut outcomes = Vec::new();
    for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
        let text = format!(
            "PREFIX ex: <http://example.org/>\n{}",
            order.map(|i| rules[i]).join("\n")
        );
        let program = compile(&text).unwrap();
        let mut db = SparqlDatabase::new();
        db.add_tagged_triple(&ex("room"), &ex("smoke"), &ex("yes"), 0.6);
        db.add_tagged_triple(&ex("room"), &ex("heat"), &ex("yes"), 0.5);
        db.add_triple_parts(&ex("room"), &ex("staffed"), &ex("yes"));
        let result = execute_program(&mut db, &program, &ProgramOptions::default()).unwrap();
        let alarm = result.fact_probability(&ex("room"), &ex("alarm"), &ex("on")).unwrap();
        let call = result.fact_probability(&ex("room"), &ex("call"), &ex("fire")).unwrap();
        assert!((alarm - 0.8).abs() <= 1e-12);
        assert!((call - 0.8).abs() <= 1e-12);
        assert_eq!(result.fact_probability(&ex("room"), &ex("staffed"), &ex("yes")), Some(1.0));
        outcomes.push((alarm.to_bits(), call.to_bits()));
    }
    assert!(outcomes.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn reference_rules_are_order_invariant() {
    let (_, combined) = parse_combined_query(REASONING_RULES).unwrap();
    assert_eq!(combined.rules.len(), 10);
    let blocks: Vec<&str> = REASONING_RULES
        .split("\nRULE ")
        .skip(1)
        .collect();
    let reversed = format!(
        "PREFIX mnist: <http://mnist/>\nPREFIX : <http://mnist/rules/>\n{}",
        blocks
            .iter()
            .rev()
            .map(|block| format!("RULE {block}\n"))
            .collect::<String>()
    );
    let forward = reference();
    let backward = compile(&reversed).unwrap();
    for left in fixtures() {
        for right in [one_hot(4), vec![0.1; 10]] {
            let a = answers(&run_pair(&forward, &left, Some(&right)));
            let b = answers(&run_pair(&backward, &left, Some(&right)));
            assert_close(&a, &b, 1e-15);
        }
    }
}

const OVERLOADED_VOCABULARY: &str = r#"PREFIX mnist: <http://mnist/>
PREFIX : <http://mnist/rules/>
RULE :Even PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:even . } WHERE { ?x mnist:digit ?d . ?d mnist:parity mnist:even . }
RULE :Prime PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:prime . } WHERE { ?x mnist:digit ?d . ?d mnist:kind mnist:prime . }
RULE :EvenOrPrimeA PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:evenOrPrime . } WHERE { ?x mnist:is mnist:even . }
RULE :EvenOrPrimeB PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:evenOrPrime . } WHERE { ?x mnist:is mnist:prime . }
RULE :EvenAndPrime PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:evenAndPrime . } WHERE { ?x mnist:is mnist:even . ?x mnist:is mnist:prime . }
RULE :RepeatEven PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:repeatEven . } WHERE { ?x mnist:is mnist:even . ?x mnist:is mnist:even . }
RULE :Contradiction PROB(combination=sdd) :- CONSTRUCT { ?x mnist:is mnist:contradiction . } WHERE { ?x mnist:digit mnist:d2 . ?x mnist:digit mnist:d3 . }
RULE :Answer PROB(combination=sdd) :- CONSTRUCT { ?q mnist:answer ?kind . } WHERE { ?q mnist:left ?x . ?x mnist:is ?kind . }
"#;

#[test]
fn overloaded_vocabulary_fails_hybrid_validation_but_direct_sdd_does_not_use_it() {
    let overloaded = compile(OVERLOADED_VOCABULARY).unwrap();
    let revised = reference();
    let mut dictionary = Dictionary::new();
    assert!(validate_hybrid_rules(&overloaded.datalog_rules(&mut dictionary)).is_err());
    assert!(validate_hybrid_rules(&revised.datalog_rules(&mut dictionary)).is_ok());

    let left = fixtures()[3].clone();
    let result = run_pair(&overloaded, &left, Some(&one_hot(1)));
    let expected = enumerate(&left, Some(&one_hot(1)));
    let checks: Vec<f64> = ANSWER_KINDS
        .iter()
        .map(|kind| result.answer_probability(&pair_term(0), ANSWER, &term(kind)).unwrap())
        .collect();
    assert_close(&checks, &expected[19..], 1e-12);
}

#[test]
fn distinct_images_convolve_and_checks_match_enumeration_and_oracle() {
    let program = reference();
    let rows = fixtures();
    for left in &rows {
        for right in &rows {
            let result = run_pair(&program, left, Some(right));
            let actual = answers(&result);
            assert_close(&actual, &enumerate(left, Some(right)), 1e-12);
            assert_close(&actual, &mnist_oracle::answers(left, right, false), 1e-12);
        }
    }
}

#[test]
fn one_image_used_twice_doubles_its_digit() {
    let program = reference();
    for left in fixtures() {
        let result = run_pair(&program, &left, None);
        let actual = answers(&result);
        assert_close(&actual, &enumerate(&left, None), 1e-12);
        assert_close(&actual, &mnist_oracle::answers(&left, &left, true), 1e-12);
        for odd in (1..19).step_by(2) {
            assert_eq!(result.fact_probability(&pair_term(0), SUM, &sum_term(odd)), None);
            assert_eq!(result.answer_probability(&pair_term(0), SUM, &sum_term(odd)), Ok(0.0));
        }
    }
}

#[test]
fn independent_seeds_mix_with_exclusive_groups() {
    let text = format!(
        "{REASONING_RULES}\nRULE :BrightEven PROB(combination=sdd) :- CONSTRUCT {{ ?x mnist:is mnist:brightEven . }} WHERE {{ ?x mnist:base mnist:even . ?x mnist:bright mnist:yes . }}\n"
    );
    let program = compile(&text).unwrap();
    let left = fixtures()[3].clone();
    let mut options = pair_options(&left, Some(&one_hot(0)));
    options
        .frozen_predictions
        .as_mut()
        .unwrap()
        .add_independent(&image_term(0), &term("bright"), &term("yes"), 0.3)
        .unwrap();
    let mut db = pair_input(false);
    let result = execute_program(&mut db, &program, &options).unwrap();
    let even: f64 = [0, 2, 4, 6, 8].iter().map(|d| left[*d]).sum();
    let bright_even = result
        .answer_probability(&image_term(0), &term("is"), &term("brightEven"))
        .unwrap();
    assert!((bright_even - 0.3 * even).abs() <= 1e-12);
}

#[test]
fn contradiction_is_absent_from_select_and_zero_through_the_api() {
    let program = reference();
    let mut result = run_pair(&program, &vec![0.1; 10], Some(&vec![0.1; 10]));
    let direct = "PREFIX mnist: <http://mnist/>\nSELECT ?x WHERE { ?x mnist:is mnist:contradiction . }";
    let projected = "PREFIX mnist: <http://mnist/>\nSELECT ?q WHERE { ?q mnist:answer mnist:contradiction . }";
    assert!(query_program_result(&mut result, direct).unwrap().is_empty());
    assert!(query_program_result(&mut result, projected).unwrap().is_empty());
    assert_eq!(result.fact_probability(&image_term(0), &term("is"), &term("contradiction")), None);
    assert_eq!(result.answer_probability(&pair_term(0), ANSWER, &term("contradiction")), Ok(0.0));
    let annotated = query_program_result(&mut result, ANSWER_ANNOTATIONS).unwrap();
    assert!(annotated.iter().all(|row| row[1] != term("contradiction")));
    assert_eq!(annotated.len(), 5);
}

#[test]
fn structural_false_zero_weight_tiny_certain_and_absent_facts_are_distinct() {
    let program = reference();
    let tiny = fixtures()[4].clone();
    let mut result = run_pair(&program, &tiny, Some(&one_hot(0)));

    assert_eq!(result.fact_probability(&image_term(0), &term("is"), &term("contradiction")), None);
    assert_eq!(result.fact_probability(&image_term(0), DIGIT, &digit_label(4)), Some(0.0));
    assert_eq!(result.fact_probability(&pair_term(0), SUM, &sum_term(4)), Some(0.0));
    assert_eq!(result.fact_probability(&pair_term(0), SUM, &sum_term(1)), Some(1e-300));
    assert_eq!(result.fact_probability(&image_term(1), DIGIT, &digit_label(0)), Some(1.0));
    assert_eq!(result.fact_probability(&pair_term(0), LEFT, &image_term(0)), Some(1.0));
    assert_eq!(result.fact_probability(&pair_term(0), SUM, &term("unknown")), None);

    let sums = query_program_result(&mut result, SUM_ANNOTATIONS).unwrap();
    assert_eq!(sums.len(), 19);
    let tiny_sum = sums.iter().find(|row| row[1] == sum_term(1)).unwrap();
    assert_eq!(parse_probability_literal(&tiny_sum[2]), Some(1e-300));
    let certain = query_program_result(
        &mut result,
        "PREFIX mnist: <http://mnist/>\nPREFIX prob: <http://www.w3.org/ns/prob#>\nSELECT ?p WHERE { << <http://mnist/img/1> mnist:digit mnist:d0 >> prob:value ?p . }",
    )
    .unwrap();
    assert_eq!(certain.len(), 1);
    assert_eq!(parse_probability_literal(&certain[0][0]), Some(1.0));
}

#[test]
fn answer_sets_include_certain_asserted_facts() {
    let program = compile(
        "PREFIX ex: <http://example.org/>\nRULE :R PROB(combination=sdd) :- CONSTRUCT { ?x ex:flag ex:on . } WHERE { ?x ex:risk ex:yes . }",
    )
    .unwrap();
    let mut db = SparqlDatabase::new();
    db.add_tagged_triple(&ex("a"), &ex("risk"), &ex("yes"), 0.25);
    db.add_triple_parts(&ex("b"), &ex("risk"), &ex("yes"));
    let result = execute_program(&mut db, &program, &ProgramOptions::default()).unwrap();
    assert_eq!(
        result.answer_set(&ex("flag")),
        vec![(ex("a"), ex("on"), 0.25), (ex("b"), ex("on"), 1.0)]
    );
    assert_eq!(
        result.answer_set(&ex("risk")),
        vec![(ex("a"), ex("yes"), 0.25), (ex("b"), ex("yes"), 1.0)]
    );
}

#[test]
fn reruns_read_only_asserted_input() {
    let program = reference();
    let left = fixtures()[3].clone();
    let mut db = pair_input(false);
    let before = db.query_default_triples(None, None, None).len();
    let options = pair_options(&left, Some(&one_hot(5)));
    let first = execute_program(&mut db, &program, &options).unwrap();
    let second = execute_program(&mut db, &program, &options).unwrap();
    assert_eq!(db.query_default_triples(None, None, None).len(), before);
    assert_eq!(answers(&first), answers(&second));
    assert_eq!(first.hypothesis_count(), second.hypothesis_count());
    assert!(!Arc::ptr_eq(&db.dictionary, &first.dataset().dictionary));
    assert!(!Arc::ptr_eq(&first.dataset().dictionary, &second.dataset().dictionary));
    assert!(!Arc::ptr_eq(
        &db.quoted_triple_store,
        &first.dataset().quoted_triple_store
    ));
}

#[test]
fn failed_runs_preserve_the_published_result() {
    let program = reference();
    let left = fixtures()[3].clone();
    let mut published = PublishedProgramResult::new();
    let mut db = pair_input(false);
    published
        .publish(execute_program(&mut db, &program, &pair_options(&left, Some(&one_hot(1)))))
        .unwrap();
    let expected = answers(published.current().unwrap());

    let mut missing = pair_options(&left, Some(&one_hot(1)));
    missing
        .required_choices
        .as_mut()
        .unwrap()
        .push((DIGIT.to_string(), image_term(9)));
    let failure = published.publish(execute_program(&mut db, &program, &missing));
    assert!(matches!(failure, Err(ProgramError::Coverage(_))));

    let mut tight = pair_options(&left, Some(&one_hot(1)));
    tight.limits.max_sdd_nodes = 8;
    let failure = published.publish(execute_program(&mut db, &program, &tight));
    assert_eq!(
        failure.err(),
        Some(ProgramError::Resource(ResourceError::NodeBudgetExceeded))
    );
    assert_eq!(answers(published.current().unwrap()), expected);
}

#[test]
fn result_queries_run_select_only() {
    let program = reference();
    let mut result = run_pair(&program, &one_hot(3), Some(&one_hot(4)));
    let predict = r#"PREFIX ex: <http://example.org/>
ML.PREDICT(MODEL "m", INPUT { SELECT ?s ?x WHERE { ?s ex:x ?x . } }, OUTPUT ?d DISTRIBUTION)"#;
    assert!(matches!(
        query_program_result(&mut result, predict),
        Err(ProgramError::Unsupported(_))
    ));
    assert!(matches!(
        query_program_result(&mut result, "INSERT DATA { <a> <b> <c> . }"),
        Err(ProgramError::Unsupported(_))
    ));
    let rows = query_program_result(
        &mut result,
        "PREFIX mnist: <http://mnist/>\nSELECT ?s WHERE { <http://mnist/q/0> mnist:sum ?s . }",
    )
    .unwrap();
    assert_eq!(rows.len(), 19);
}

#[test]
fn coverage_must_be_complete_and_confirmed_before_absent_means_zero() {
    let program = reference();
    let left = fixtures()[3].clone();
    let mut db = pair_input(false);

    let mut only_left = pair_options(&left, None);
    only_left.required_choices = Some(vec![
        (DIGIT.to_string(), image_term(0)),
        (DIGIT.to_string(), image_term(1)),
    ]);
    assert!(matches!(
        execute_program(&mut db, &program, &only_left),
        Err(ProgramError::Coverage(_))
    ));

    let mut unconfirmed = pair_options(&left, Some(&one_hot(2)));
    unconfirmed.required_choices = None;
    let result = execute_program(&mut db, &program, &unconfirmed).unwrap();
    assert!(!result.coverage().confirmed);
    assert!(result.answer_probability(&pair_term(0), SUM, &sum_term(2)).is_ok());
    assert!(matches!(
        result.answer_probability(&pair_term(0), ANSWER, &term("contradiction")),
        Err(ProgramError::Coverage(_))
    ));
    assert!(result.prepare_answers(&answer_targets(&pair_term(0))).is_err());
}

#[test]
fn prefixed_and_full_iris_compile_to_the_same_program() {
    let prefixed = "PREFIX m: <http://mnist/>\nRULE :E PROB(combination=sdd) :- CONSTRUCT { ?x m:base m:even . } WHERE { ?x m:digit ?d . ?d m:parity m:even . }";
    let iris = "RULE <http://mnist/rules/E> PROB(combination=sdd) :- CONSTRUCT { ?x <http://mnist/base> <http://mnist/even> . } WHERE { ?x <http://mnist/digit> ?d . ?d <http://mnist/parity> <http://mnist/even> . }";
    let mut outcomes = Vec::new();
    for text in [prefixed, iris] {
        let program = compile(text).unwrap();
        let result = run_pair(&program, &fixtures()[3], Some(&one_hot(0)));
        outcomes.push(result.fact_probability(&image_term(0), &term("base"), &term("even")));
    }
    assert!(outcomes[0].is_some());
    assert_eq!(outcomes[0], outcomes[1]);
    assert!(matches!(
        compile("RULE :E PROB(combination=sdd) :- CONSTRUCT { ?x m:base m:even . } WHERE { ?x m:digit ?d . }"),
        Err(ProgramError::Unsupported(_))
    ));
}

#[test]
fn snapshot_validation_is_shared_and_strict() {
    let labels = digit_labels();
    let mut snapshot = PredictionSnapshot::new();
    snapshot.declare_exclusive_relation(DIGIT, &labels).unwrap();
    assert!(snapshot.declare_exclusive_relation(DIGIT, &labels).is_ok());
    let mut reordered = labels.clone();
    reordered.swap(0, 1);
    assert!(snapshot.declare_exclusive_relation(DIGIT, &reordered).is_err());
    assert!(snapshot
        .declare_exclusive_relation("http://mnist/other", &[labels[0].clone(), labels[0].clone()])
        .is_err());

    let uniform = vec![0.1; 10];
    snapshot.add_distribution(DIGIT, &image_term(0), &uniform, Some(&[0.5])).unwrap();
    snapshot.add_distribution(DIGIT, &image_term(0), &uniform, Some(&[0.5])).unwrap();
    assert_eq!(snapshot.choice_groups(), 1);
    assert!(snapshot.add_distribution(DIGIT, &image_term(0), &uniform, Some(&[0.25])).is_err());
    assert!(snapshot.add_distribution(DIGIT, &image_term(0), &one_hot(1), Some(&[0.5])).is_err());
    snapshot.add_distribution(DIGIT, &image_term(1), &uniform, Some(&[0.5])).unwrap();
    assert_eq!(snapshot.choice_groups(), 2);

    assert!(snapshot.add_distribution(DIGIT, &image_term(2), &[0.5, 0.5], None).is_err());
    assert!(snapshot.add_distribution(DIGIT, &image_term(2), &[f64::NAN; 10], None).is_err());
    let mut negative = one_hot(0);
    negative[0] = 1.5;
    negative[1] = -0.5;
    assert!(snapshot.add_distribution(DIGIT, &image_term(2), &negative, None).is_err());

    let accepted = [0.5, 0.5 + 5e-8, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    snapshot.add_distribution(DIGIT, &image_term(4), &accepted, None).unwrap();
    assert_eq!(snapshot.distribution(DIGIT, &image_term(4)).unwrap(), &accepted);
    let rejected = [0.5, 0.5 + 2e-7, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    assert!(snapshot.add_distribution(DIGIT, &image_term(5), &rejected, None).is_err());

    assert!(snapshot.add_independent(&image_term(0), DIGIT, &labels[3], 0.5).is_err());
    snapshot.add_independent(&image_term(0), &term("bright"), &term("yes"), 0.5).unwrap();
    assert!(snapshot.add_independent(&image_term(0), &term("bright"), &term("yes"), 0.6).is_err());
    assert!(snapshot.add_independent(&image_term(0), &term("bright"), &term("no"), 1.5).is_err());
    assert!(snapshot.add_distribution("http://mnist/undeclared", &image_term(0), &uniform, None).is_err());
}

#[test]
fn seeds_cannot_overlap_asserted_certain_facts() {
    let program = reference();
    let mut db = pair_input(false);
    db.add_triple_parts(&image_term(0), DIGIT, &digit_label(3));
    let result = execute_program(&mut db, &program, &pair_options(&one_hot(3), Some(&one_hot(4))));
    assert!(matches!(result, Err(ProgramError::Snapshot(_))));
}

#[test]
fn distinct_anchors_with_equal_distributions_stay_independent() {
    let program = reference();
    let uniform = vec![0.1; 10];
    let result = run_pair(&program, &uniform, Some(&uniform));
    let sum_zero = result.answer_probability(&pair_term(0), SUM, &sum_term(0)).unwrap();
    assert!((sum_zero - 0.01).abs() <= 1e-15);
}

#[test]
fn result_dictionary_remaps_input_ids_and_literals_round_trip() {
    let program = reference();
    let mut db = SparqlDatabase::new();
    for index in 0..50 {
        db.add_triple_parts(&ex(&format!("noise{index}")), &ex("p"), &ex("o"));
    }
    load_knowledge(&mut db).unwrap();
    load_pair(&mut db, &pair_term(0), &image_term(0), &image_term(1));
    let left = fixtures()[3].clone();
    let mut result = execute_program(&mut db, &program, &pair_options(&left, Some(&one_hot(6)))).unwrap();

    let input_terms = db.dictionary.read().unwrap().string_to_id.len();
    let result_terms = result.dataset().dictionary.read().unwrap().string_to_id.len();
    assert!(result_terms < input_terms);
    assert!(result
        .dataset()
        .dictionary
        .read()
        .unwrap()
        .string_to_id
        .get(&ex("noise0"))
        .is_none());

    for row in query_program_result(&mut result, SUM_ANNOTATIONS).unwrap() {
        let parsed = parse_probability_literal(&row[2]).unwrap();
        let direct = result.fact_probability(&row[0], SUM, &row[1]).unwrap();
        assert_eq!(parsed.to_bits(), direct.to_bits());
    }
}

#[test]
fn resource_limits_fail_with_structured_errors() {
    let program = reference();
    let left = fixtures()[3].clone();
    let mut db = pair_input(false);

    let mut seeds = pair_options(&left, Some(&one_hot(1)));
    seeds.limits.max_seed_variables = 5;
    assert_eq!(
        execute_program(&mut db, &program, &seeds).err(),
        Some(ProgramError::Resource(ResourceError::SeedLimit {
            requested: 20,
            limit: 5
        }))
    );

    let mut nodes = pair_options(&left, Some(&one_hot(1)));
    nodes.limits.max_sdd_nodes = 8;
    assert_eq!(
        execute_program(&mut db, &program, &nodes).err(),
        Some(ProgramError::Resource(ResourceError::NodeBudgetExceeded))
    );

    let mut deadline = pair_options(&left, Some(&one_hot(1)));
    deadline.limits.deadline = Some(Duration::ZERO);
    assert_eq!(
        execute_program(&mut db, &program, &deadline).err(),
        Some(ProgramError::Resource(ResourceError::DeadlineExceeded))
    );
}

#[test]
fn warm_recovery_matches_published_probabilities() {
    let program = reference();
    let left = fixtures()[3].clone();
    let result = run_pair(&program, &left, Some(&fixtures()[2]));
    let prepared = result.prepare_answers(&answer_targets(&pair_term(0))).unwrap();
    assert_eq!(prepared.recover(), answers(&result));
}

#[test]
fn addition_table_is_validated() {
    let mut rows = addition_table();
    assert!(validate_addition_table(&rows).is_ok());
    rows.push((term("add/3_4"), term("total"), sum_term(8)));
    assert!(validate_addition_table(&rows).is_err());
    let mut rows = addition_table();
    rows.retain(|(subject, _, _)| subject != &term("add/9_9"));
    assert!(validate_addition_table(&rows).is_err());
    let mut rows = addition_table();
    let last = rows.len() - 1;
    rows[last].2 = sum_term(17);
    assert!(validate_addition_table(&rows).is_err());
}
