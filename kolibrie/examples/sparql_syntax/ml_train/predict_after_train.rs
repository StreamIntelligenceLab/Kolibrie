/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::neural_relations::execute_neural_program;
use kolibrie::parser::process_rule_definition;
use kolibrie::sparql_database::SparqlDatabase;

fn tmp_model_path(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("/tmp/kolibrie_example_{}_{}_{}.bin", name, std::process::id(), nanos)
}

fn main() {
    let mut database = SparqlDatabase::new();

    for (sample, label, x0, x1, x2) in [
        ("s0", "A", "1", "0", "0"),
        ("s1", "A", "1", "0", "0"),
        ("s2", "B", "0", "1", "0"),
        ("s3", "B", "0", "1", "0"),
        ("s4", "C", "0", "0", "1"),
        ("s5", "C", "0", "0", "1"),
    ] {
        database.add_triple_parts(sample, "http://example.org/x0", x0);
        database.add_triple_parts(sample, "http://example.org/x1", x1);
        database.add_triple_parts(sample, "http://example.org/x2", x2);
        database.add_triple_parts(sample, "http://example.org/gold", label);
    }

    let save_path = tmp_model_path("predict_after_train");
    let program = format!(
        r#"
PREFIX ex: <http://example.org/>

MODEL "digit_model" {{
    ARCH MLP {{ HIDDEN [16, 8] }}
    OUTPUT EXCLUSIVE {{ "A", "B", "C" }}
}}

NEURAL RELATION ex:predictedDigit USING MODEL "digit_model" {{
    INPUT {{
        ?sample ex:x0 ?x0 .
        ?sample ex:x1 ?x1 .
        ?sample ex:x2 ?x2 .
    }}
    FEATURES {{ ?x0, ?x1, ?x2 }}
}}

TRAIN NEURAL RELATION ex:predictedDigit {{
    DATA {{ ?sample ex:gold ?label . }}
    LABEL ?label
    TARGET {{ ?sample ex:predictedDigit ?label }}
    LOSS cross_entropy
    OPTIMIZER adam
    LEARNING_RATE 0.1
    EPOCHS 80
    BATCH_SIZE 4
    SAVE_TO "{save_path}"
}}
"#
    );
    execute_neural_program(&mut database, &program).expect("training failed");
    if let Some(triples) = database
        .neural_materialized_triples
        .remove("http://example.org/predictedDigit")
    {
        for triple in triples {
            database.delete_triple(&triple);
        }
    }

    let rule = r#"
PREFIX ex: <http://example.org/>

RULE :PredictAfterTrain :-
CONSTRUCT {
    ?sample ex:predictedDigit ?label .
}
WHERE {
    ?sample ex:gold ?gold .
}
ML.PREDICT(MODEL "digit_model",
    INPUT {
        SELECT ?sample ?x0 ?x1 ?x2
        WHERE {
            ?sample ex:x0 ?x0 .
            ?sample ex:x1 ?x1 .
            ?sample ex:x2 ?x2 .
            FILTER (?x0 > 0)
        }
    },
    OUTPUT ?label
)
"#;

    process_rule_definition(rule, &mut database).expect("ML.PREDICT rule failed");

    let dict = database.dictionary.read().unwrap();
    let pred_id = dict
        .string_to_id
        .get("http://example.org/predictedDigit")
        .copied()
        .expect("predictedDigit predicate missing");

    println!("Predictions materialized by ML.PREDICT:");
    for triple in &database.triples {
        if triple.predicate != pred_id {
            continue;
        }
        let subject = dict.decode(triple.subject).unwrap_or("<unknown>");
        let object = dict.decode(triple.object).unwrap_or("<unknown>");
        println!("  {subject} -> {object}");
    }
}
