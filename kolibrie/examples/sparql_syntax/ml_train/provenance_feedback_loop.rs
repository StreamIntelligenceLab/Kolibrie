/*
 * Small feedback-loop example for provenance, RDF-star, and neural relations
 *
 * The flow is:
 *   probabilistic evidence -> provenance-aware RULE syntax -> RDF-star annotations
 *   -> SPARQL-star feature query -> train a neural relation -> feed predictions back into RULE syntax
 */

use kolibrie::execute_query::execute_query;
use kolibrie::execute_ml_train::build_ground_reasoner_from_db;
use kolibrie::neural_relations::execute_neural_program;
use kolibrie::parser::{convert_combined_rule, parse_combined_query, process_rule_definition};
use kolibrie::sparql_database::SparqlDatabase;
use shared::sdd::SddProvenance;

const EX: &str = "http://example.org/";
const PROB: &str = "http://www.w3.org/ns/prob#";
const MODEL_PATH: &str = "/tmp/kolibrie_provenance_feedback_model.bin";
const SENSOR_TYPE: &str = "http://example.org/Sensor";
const LABEL_MONITOR: &str = "http://example.org/monitor";
const LABEL_DISPATCH: &str = "http://example.org/dispatch";

#[derive(Clone, Copy)]
struct SensorCase {
    id: &'static str,
    temp_prob: Option<f64>,
    hr_prob: Option<f64>,
    pressure_prob: f64,
    gold_response: &'static str,
}

fn iri(local: &str) -> String {
    format!("{EX}{local}")
}

fn sensor_iri(id: &str) -> String {
    format!("{EX}sensor/{id}")
}

fn populate_static_data(db: &mut SparqlDatabase, sensors: &[SensorCase]) {
    let sensor_type = iri("type");
    let gold_response = iri("goldResponse");
    let dispatch_policy = iri("dispatchPolicy");
    let required_risk = iri("requiredRisk");
    let required_response = iri("requiredResponse");

    for case in sensors {
        let sensor = sensor_iri(case.id);
        db.add_triple_parts(&sensor, &sensor_type, SENSOR_TYPE);
        db.add_triple_parts(&sensor, &gold_response, case.gold_response);
    }

    db.add_triple_parts(&dispatch_policy, &required_risk, &iri("high"));
    db.add_triple_parts(&dispatch_policy, &required_response, LABEL_DISPATCH);
}

fn add_probabilistic_fact(
    db: &mut SparqlDatabase,
    subject: &str,
    predicate: &str,
    probability: f64,
) {
    db.add_tagged_triple(subject, predicate, "true", probability);
}

fn rule_risk_from_temp_pressure() -> String {
    format!(
        r#"PREFIX ex: <{EX}>

RULE :RiskFromTempPressure PROB(combination=sdd) :-
CONSTRUCT {{
    ?sensor ex:riskSignal ex:high .
}}
WHERE {{
    ?sensor ex:tempEvidence true .
    ?sensor ex:pressureEvidence true .
}}"#
    )
}

fn rule_risk_from_hr_pressure() -> String {
    format!(
        r#"PREFIX ex: <{EX}>

RULE :RiskFromHrPressure PROB(combination=sdd) :-
CONSTRUCT {{
    ?sensor ex:riskSignal ex:high .
}}
WHERE {{
    ?sensor ex:hrEvidence true .
    ?sensor ex:pressureEvidence true .
}}"#
    )
}

fn provenance_feature_query() -> String {
    format!(
        r#"PREFIX ex: <{EX}>
PREFIX prob: <{PROB}>

SELECT ?sensor ?riskProb ?proofCount WHERE {{
    << ?sensor ex:riskSignal ex:high >> prob:value ?riskProb .
    << ?sensor ex:riskSignal ex:high >> prob:proofCount ?proofCount .
}}
ORDER BY ?sensor"#
    )
}

fn build_feedback_program() -> String {
    [
        format!("PREFIX ex: <{}>", EX),
        format!("PREFIX prob: <{}>", PROB),
        String::new(),
        "MODEL \"response_model\" {".to_string(),
        "    ARCH MLP { HIDDEN [8, 4] }".to_string(),
        format!("    OUTPUT EXCLUSIVE {{ <{}monitor>, <{}dispatch> }}", EX, EX),
        "}".to_string(),
        String::new(),
        "NEURAL RELATION ex:predictedResponse USING MODEL \"response_model\" {".to_string(),
        "    INPUT {".to_string(),
        "        ?sensor ex:type ex:Sensor .".to_string(),
        "        ?sensor ex:riskProbFeature ?riskProb .".to_string(),
        "        ?sensor ex:proofCountFeature ?proofCount .".to_string(),
        "    }".to_string(),
        "    FEATURES { ?riskProb, ?proofCount }".to_string(),
        "}".to_string(),
        String::new(),
        "TRAIN NEURAL RELATION ex:predictedResponse {".to_string(),
        "    DATA {".to_string(),
        "        ?sensor ex:goldResponse ?label .".to_string(),
        "    }".to_string(),
        "    LABEL ?label".to_string(),
        "    TARGET { ?sensor ex:predictedResponse ?label }".to_string(),
        "    LOSS cross_entropy".to_string(),
        "    OPTIMIZER adam".to_string(),
        "    LEARNING_RATE 0.1".to_string(),
        "    EPOCHS 120".to_string(),
        "    BATCH_SIZE 3".to_string(),
        format!("    SAVE_TO \"{}\"", MODEL_PATH),
        "}".to_string(),
    ]
    .join("\n")
}

fn prediction_query() -> String {
    format!(
        r#"PREFIX ex: <{EX}>

SELECT ?sensor ?predicted ?gold WHERE {{
    ?sensor ex:predictedResponse ?predicted .
    ?sensor ex:goldResponse ?gold .
}}
ORDER BY ?sensor"#
    )
}

fn execute_sdd_rule_batch(db: &mut SparqlDatabase, rule_inputs: &[String]) -> usize {
    let mut reasoner = build_ground_reasoner_from_db(db, None);
    reasoner.probability_seeds = db.probability_seeds.clone();

    for rule_input in rule_inputs {
        db.register_prefixes_from_query(rule_input);
        let (_rest, combined) = parse_combined_query(rule_input)
            .expect("failed to parse provenance rule");

        for (prefix, uri) in &combined.prefixes {
            db.prefixes.insert(prefix.clone(), uri.clone());
        }

        let mut prefixes = combined.prefixes.clone();
        db.share_prefixes_with(&mut prefixes);
        let parsed_rule = combined.rule.expect("expected RULE block");

        let mut dict = reasoner.dictionary.write().unwrap();
        let dynamic_rule = convert_combined_rule(parsed_rule, &mut dict, &prefixes);
        drop(dict);
        reasoner.add_rule(dynamic_rule);
    }

    let (derived_facts, tag_store) = reasoner.infer_new_facts_with_provenance(SddProvenance::new());

    let mut dict = reasoner.dictionary.write().unwrap();
    let mut qt_store = db.quoted_triple_store.write().unwrap();
    let rdf_star = tag_store.encode_as_rdf_star_with_explanation(&mut dict, &mut qt_store);
    drop(qt_store);
    drop(dict);

    for triple in rdf_star {
        db.triples.insert(triple);
    }

    for triple in &derived_facts {
        db.triples.insert(triple.clone());
    }

    derived_facts.len()
}

fn feedback_rule() -> String {
    format!(
        r#"PREFIX ex: <{EX}>

RULE :OpenDispatchCase :-
CONSTRUCT {{
    ?sensor ex:dispatchCase ex:open .
}}
WHERE {{
    ?sensor ex:riskSignal ?risk .
    ?sensor ex:predictedResponse ?response .
    ex:dispatchPolicy ex:requiredRisk ?risk .
    ex:dispatchPolicy ex:requiredResponse ?response .
}}"#
    )
}

fn dispatch_case_query() -> String {
    format!(
        r#"PREFIX ex: <{EX}>

SELECT ?sensor WHERE {{
    ?sensor ex:dispatchCase ex:open .
}}
ORDER BY ?sensor"#
    )
}

fn risk_signal_query() -> String {
    format!(
        r#"PREFIX ex: <{EX}>

SELECT ?sensor WHERE {{
    ?sensor ex:riskSignal ex:high .
}}
ORDER BY ?sensor"#
    )
}

fn numeric_lexical(term: &str) -> String {
    let trimmed = term.trim();
    if let Some(rest) = trimmed.strip_prefix('"') {
        if let Some(end_quote) = rest.find('"') {
            return rest[..end_quote].to_string();
        }
    }
    trimmed.to_string()
}

fn provenance_rows_from_rdf_star(db: &SparqlDatabase) -> Vec<Vec<String>> {
    let prob_value = format!("{PROB}value");
    let proof_count = format!("{PROB}proofCount");
    let risk_signal = iri("riskSignal");
    let high = iri("high");
    let mut rows: std::collections::BTreeMap<String, (Option<String>, Option<String>)> =
        std::collections::BTreeMap::new();

    for triple in &db.triples {
        let predicate = db.decode_any(triple.predicate).unwrap_or_default();
        if predicate != prob_value && predicate != proof_count {
            continue;
        }

        let subject = db.decode_any(triple.subject).unwrap_or_default();
        if !(subject.starts_with("<<") && subject.ends_with(">>")) {
            continue;
        }

        let inner = subject[2..subject.len() - 2].trim();
        let (sensor, pred, obj) = SparqlDatabase::split_quoted_triple_content(inner);
        if pred != risk_signal || obj != high {
            continue;
        }

        let entry = rows.entry(sensor).or_insert((None, None));
        let value = db.decode_any(triple.object).unwrap_or_default();
        if predicate == prob_value {
            entry.0 = Some(value);
        } else {
            entry.1 = Some(value);
        }
    }

    rows.into_iter()
        .filter_map(|(sensor, (risk_prob, proof_count))| {
            Some(vec![sensor, risk_prob?, proof_count?])
        })
        .collect()
}

fn materialize_feature_triples_from_rows(db: &mut SparqlDatabase, rows: &[Vec<String>]) -> usize {
    let risk_prob_feature = iri("riskProbFeature");
    let proof_count_feature = iri("proofCountFeature");
    let mut count = 0usize;

    for row in rows {
        if row.len() < 3 {
            continue;
        }

        let sensor = &row[0];
        let risk_prob = numeric_lexical(&row[1]);
        let proof_count = numeric_lexical(&row[2]);

        db.add_triple_parts(sensor, &risk_prob_feature, &risk_prob);
        db.add_triple_parts(sensor, &proof_count_feature, &proof_count);
        count += 1;
    }

    count
}

fn print_block(title: &str, text: &str) {
    println!("{title}");
    for line in text.lines() {
        println!("  {}", line);
    }
}

fn print_rows(title: &str, rows: &[Vec<String>]) {
    println!("{title}");
    for row in rows {
        let rendered = row.iter().map(|value| shorten(value)).collect::<Vec<_>>();
        println!("  {}", rendered.join(" | "));
    }
    if rows.is_empty() {
        println!("  <no rows>");
    }
}

fn shorten(value: &str) -> String {
    if value.starts_with(EX) {
        value
            .rsplit(['/', '#'])
            .next()
            .unwrap_or(value)
            .to_string()
    } else {
        value.to_string()
    }
}

fn main() {
    let sensors = [
        SensorCase {
            id: "s1",
            temp_prob: Some(0.90),
            hr_prob: Some(0.85),
            pressure_prob: 0.95,
            gold_response: LABEL_DISPATCH,
        },
        SensorCase {
            id: "s2",
            temp_prob: Some(0.88),
            hr_prob: Some(0.82),
            pressure_prob: 0.92,
            gold_response: LABEL_DISPATCH,
        },
        SensorCase {
            id: "s3",
            temp_prob: Some(0.86),
            hr_prob: Some(0.78),
            pressure_prob: 0.94,
            gold_response: LABEL_DISPATCH,
        },
        SensorCase {
            id: "s4",
            temp_prob: Some(0.58),
            hr_prob: None,
            pressure_prob: 0.75,
            gold_response: LABEL_MONITOR,
        },
        SensorCase {
            id: "s5",
            temp_prob: None,
            hr_prob: Some(0.63),
            pressure_prob: 0.72,
            gold_response: LABEL_MONITOR,
        },
        SensorCase {
            id: "s6",
            temp_prob: Some(0.52),
            hr_prob: None,
            pressure_prob: 0.78,
            gold_response: LABEL_MONITOR,
        },
    ];

    let mut db = SparqlDatabase::new();

    println!("Syntax-first provenance -> neural relation -> reasoning feedback loop");
    println!("Probabilistic base facts are still loaded through a Rust helper; rules, queries, and neural declarations are shown in syntax\n");

    println!("[1/5] Loading static sensor facts and probabilistic evidence");
    populate_static_data(&mut db, &sensors);
    for case in sensors {
        let sensor = sensor_iri(case.id);
        if let Some(prob) = case.temp_prob {
            add_probabilistic_fact(&mut db, &sensor, &iri("tempEvidence"), prob);
        }
        if let Some(prob) = case.hr_prob {
            add_probabilistic_fact(&mut db, &sensor, &iri("hrEvidence"), prob);
        }
        add_probabilistic_fact(&mut db, &sensor, &iri("pressureEvidence"), case.pressure_prob);
    }
    println!("  Added {} labeled sensors and seeded uncertain evidence facts", sensors.len());

    println!("\n[2/5] Running provenance-aware RULE syntax");
    let temp_rule = rule_risk_from_temp_pressure();
    print_block("  RULE :RiskFromTempPressure", &temp_rule);

    let hr_rule = rule_risk_from_hr_pressure();
    println!();
    print_block("  RULE :RiskFromHrPressure", &hr_rule);
    let provenance_inferred = execute_sdd_rule_batch(&mut db, &[temp_rule, hr_rule]);
    println!("\n  Shared SDD inference produced {} new provenance-tagged facts", provenance_inferred);

    let risk_rows = execute_query(&risk_signal_query(), &mut db);
    println!("\n  Derived {} riskSignal facts", risk_rows.len());

    println!("\n[3/5] Inspecting provenance with SPARQL-star and building neural features");
    let star_query = provenance_feature_query();
    print_block("  SPARQL-star query", &star_query);
    let provenance_rows = provenance_rows_from_rdf_star(&db);
    print_rows("  Provenance rows", &provenance_rows);
    let feature_count = materialize_feature_triples_from_rows(&mut db, &provenance_rows);
    println!("  Materialized {} numeric feature rows for training", feature_count);

    println!("\n[4/5] Training and materializing the neural relation");
    let neural_program = build_feedback_program();
    print_block("  Neural program", &neural_program);
    execute_neural_program(&mut db, &neural_program).expect("first-class neural program failed");

    let prediction_query = prediction_query();
    print_block("  Prediction query", &prediction_query);
    let prediction_rows = execute_query(&prediction_query, &mut db);
    print_rows("  Predicted responses", &prediction_rows);

    println!("\n[5/5] Feeding predictions back into RULE syntax");
    let feedback_rule = feedback_rule();
    print_block("  RULE :OpenDispatchCase", &feedback_rule);
    let (_rule, inferred) = process_rule_definition(&feedback_rule, &mut db)
        .expect("feedback rule execution failed");
    println!("  Rule inference produced {} new dispatchCase facts", inferred.len());

    let case_query = dispatch_case_query();
    print_block("  Final SELECT query", &case_query);
    let case_rows = execute_query(&case_query, &mut db);
    print_rows("  Dispatch cases opened by the rule", &case_rows);

    println!("\nModel saved to {MODEL_PATH}");
}
