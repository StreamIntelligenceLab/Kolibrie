#[allow(dead_code)]
mod mnist_program {
    use std::collections::BTreeMap;

    use kolibrie::sparql_database::SparqlDatabase;

    pub const MNIST: &str = "http://mnist/";
    pub const DIGIT: &str = "http://mnist/digit";
    pub const SUM: &str = "http://mnist/sum";
    pub const ANSWER: &str = "http://mnist/answer";
    pub const LEFT: &str = "http://mnist/left";
    pub const RIGHT: &str = "http://mnist/right";
    pub const LABEL: &str = "http://mnist/label";
    pub const SPLIT: &str = "http://mnist/split";
    pub const ANSWER_KINDS: [&str; 6] = [
        "even",
        "prime",
        "evenOrPrime",
        "evenAndPrime",
        "repeatEven",
        "contradiction",
    ];

    pub const PREFIXES: &str = "PREFIX mnist: <http://mnist/>\nPREFIX : <http://mnist/rules/>\n";

    pub const REASONING_RULES: &str = r#"PREFIX mnist: <http://mnist/>
PREFIX : <http://mnist/rules/>

RULE :Addition PROB(combination=sdd) :-
CONSTRUCT { ?q mnist:sum ?s . }
WHERE {
    ?q mnist:left ?x .
    ?q mnist:right ?y .
    ?x mnist:digit ?a .
    ?y mnist:digit ?b .
    ?t mnist:addend1 ?a .
    ?t mnist:addend2 ?b .
    ?t mnist:total ?s .
}

RULE :Even PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:base mnist:even . }
WHERE {
    ?x mnist:digit ?d .
    ?d mnist:parity mnist:even .
}

RULE :Prime PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:base mnist:prime . }
WHERE {
    ?x mnist:digit ?d .
    ?d mnist:kind mnist:prime .
}

RULE :EvenOrPrimeA PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:is mnist:evenOrPrime . }
WHERE { ?x mnist:base mnist:even . }

RULE :EvenOrPrimeB PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:is mnist:evenOrPrime . }
WHERE { ?x mnist:base mnist:prime . }

RULE :EvenAndPrime PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:is mnist:evenAndPrime . }
WHERE {
    ?x mnist:base mnist:even .
    ?x mnist:base mnist:prime .
}

RULE :RepeatEven PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:is mnist:repeatEven . }
WHERE {
    ?x mnist:base mnist:even .
    ?x mnist:base mnist:even .
}

RULE :Contradiction PROB(combination=sdd) :-
CONSTRUCT { ?x mnist:is mnist:contradiction . }
WHERE {
    ?x mnist:digit mnist:d2 .
    ?x mnist:digit mnist:d3 .
}

RULE :AnswerBase PROB(combination=sdd) :-
CONSTRUCT { ?q mnist:answer ?kind . }
WHERE {
    ?q mnist:left ?x .
    ?x mnist:base ?kind .
}

RULE :AnswerComposite PROB(combination=sdd) :-
CONSTRUCT { ?q mnist:answer ?kind . }
WHERE {
    ?q mnist:left ?x .
    ?x mnist:is ?kind .
}
"#;

    pub const SUM_ANNOTATIONS: &str = "PREFIX mnist: <http://mnist/>\nPREFIX prob: <http://www.w3.org/ns/prob#>\nSELECT ?q ?s ?p WHERE { << ?q mnist:sum ?s >> prob:value ?p . }";
    pub const ANSWER_ANNOTATIONS: &str = "PREFIX mnist: <http://mnist/>\nPREFIX prob: <http://www.w3.org/ns/prob#>\nSELECT ?q ?k ?p WHERE { << ?q mnist:answer ?k >> prob:value ?p . }";

    pub fn term(local: &str) -> String {
        format!("{MNIST}{local}")
    }

    pub fn digit_label(digit: usize) -> String {
        term(&format!("d{digit}"))
    }

    pub fn digit_labels() -> Vec<String> {
        (0..10).map(digit_label).collect()
    }

    pub fn sum_term(sum: usize) -> String {
        term(&format!("s{sum}"))
    }

    pub fn image_term(index: usize) -> String {
        term(&format!("img/{index}"))
    }

    pub fn pair_term(index: usize) -> String {
        term(&format!("q/{index}"))
    }

    pub fn addition_table() -> Vec<(String, String, String)> {
        let mut rows = Vec::with_capacity(300);
        for a in 0..10 {
            for b in 0..10 {
                let fact = term(&format!("add/{a}_{b}"));
                rows.push((fact.clone(), term("addend1"), digit_label(a)));
                rows.push((fact.clone(), term("addend2"), digit_label(b)));
                rows.push((fact, term("total"), sum_term(a + b)));
            }
        }
        rows
    }

    pub fn validate_addition_table(rows: &[(String, String, String)]) -> Result<(), String> {
        let mut facts: BTreeMap<&str, (Vec<&str>, Vec<&str>, Vec<&str>)> = BTreeMap::new();
        for (subject, predicate, object) in rows {
            let entry = facts.entry(subject.as_str()).or_default();
            match predicate.strip_prefix(MNIST) {
                Some("addend1") => entry.0.push(object),
                Some("addend2") => entry.1.push(object),
                Some("total") => entry.2.push(object),
                _ => return Err(format!("unexpected addition-table predicate {predicate}")),
            }
        }
        let labels = digit_labels();
        let digit = |value: &str| labels.iter().position(|label| label == value);
        let mut totals: BTreeMap<(usize, usize), Vec<&str>> = BTreeMap::new();
        for (fact, (left, right, total)) in &facts {
            let ([left], [right], [total]) = (left.as_slice(), right.as_slice(), total.as_slice()) else {
                return Err(format!("{fact} needs exactly one addend1, addend2 and total"));
            };
            let (Some(a), Some(b)) = (digit(left), digit(right)) else {
                return Err(format!("{fact} has an addend outside the digit domain"));
            };
            totals.entry((a, b)).or_default().push(total);
        }
        for a in 0..10 {
            for b in 0..10 {
                match totals.get(&(a, b)).map(Vec::as_slice) {
                    Some([total]) if *total == sum_term(a + b) => {}
                    Some([total]) => return Err(format!("{a}+{b} has total {total}")),
                    Some(many) => return Err(format!("{a}+{b} has {} totals", many.len())),
                    None => return Err(format!("{a}+{b} has no total")),
                }
            }
        }
        if totals.len() != 100 {
            return Err(format!("expected 100 ordered pairs, found {}", totals.len()));
        }
        Ok(())
    }

    pub fn knowledge_triples() -> Vec<(String, String, String)> {
        let mut rows = addition_table();
        for digit in 0..10 {
            let parity = if digit % 2 == 0 { "even" } else { "odd" };
            rows.push((digit_label(digit), term("parity"), term(parity)));
        }
        for digit in [2, 3, 5, 7] {
            rows.push((digit_label(digit), term("kind"), term("prime")));
        }
        rows
    }

    pub fn load_knowledge(db: &mut SparqlDatabase) -> Result<(), String> {
        let rows = knowledge_triples();
        validate_addition_table(&rows[..300])?;
        for (subject, predicate, object) in &rows {
            db.add_triple_parts(subject, predicate, object);
        }
        Ok(())
    }

    pub fn load_pair(db: &mut SparqlDatabase, query: &str, left: &str, right: &str) {
        db.add_triple_parts(query, LEFT, left);
        db.add_triple_parts(query, RIGHT, right);
    }

    pub fn answer_targets(query: &str) -> Vec<(String, String, String)> {
        (0..19)
            .map(|sum| (query.to_string(), SUM.to_string(), sum_term(sum)))
            .chain(
                ANSWER_KINDS
                    .iter()
                    .map(|kind| (query.to_string(), ANSWER.to_string(), term(kind))),
            )
            .collect()
    }

    pub fn load_image(
        db: &mut SparqlDatabase,
        image: &str,
        pixels: &[f64],
        split: &str,
        label: Option<usize>,
    ) {
        db.add_triple_parts(image, SPLIT, &term(split));
        if let Some(label) = label {
            db.add_triple_parts(image, LABEL, &digit_label(label));
        }
        for (index, pixel) in pixels.iter().enumerate() {
            db.add_triple_parts(image, &term(&format!("p{index}")), &pixel.to_string());
        }
    }

    pub fn rule_statements() -> &'static str {
        let start = REASONING_RULES.find("RULE ").unwrap();
        &REASONING_RULES[start..]
    }

    pub fn neural_program(
        learning_rate: f64,
        epochs: usize,
        batch_size: usize,
        save_to: &str,
        predict_split: &str,
    ) -> String {
        format!(
            "{PREFIXES}\n{}",
            neural_statements(learning_rate, epochs, batch_size, save_to, predict_split)
        )
    }

    pub fn full_program(
        learning_rate: f64,
        epochs: usize,
        batch_size: usize,
        save_to: &str,
        predict_split: &str,
    ) -> String {
        format!(
            "{PREFIXES}\n{}\n{}",
            neural_statements(learning_rate, epochs, batch_size, save_to, predict_split),
            rule_statements()
        )
    }

    pub fn neural_statements(
        learning_rate: f64,
        epochs: usize,
        batch_size: usize,
        save_to: &str,
        predict_split: &str,
    ) -> String {
        let patterns = (0..784)
            .map(|j| format!("?sample mnist:p{j} ?p{j} ."))
            .collect::<Vec<_>>()
            .join("\n        ");
        let features = (0..784).map(|j| format!("?p{j}")).collect::<Vec<_>>();
        let labels = (0..10).map(|d| format!("mnist:d{d}")).collect::<Vec<_>>().join(", ");
        format!(
            r#"MODEL "digit_net" {{
    ARCH MLP {{ HIDDEN [64, 32] }}
    OUTPUT EXCLUSIVE {{ {labels} }}
}}

NEURAL RELATION mnist:digit USING MODEL "digit_net" {{
    INPUT {{
        {patterns}
    }}
    FEATURES {{ {feature_list} }}
}}

TRAIN NEURAL RELATION mnist:digit {{
    DATA {{
        ?sample mnist:split mnist:train .
        ?sample mnist:label ?label .
    }}
    LABEL ?label
    TARGET {{ ?sample mnist:digit ?label }}
    LOSS cross_entropy
    OPTIMIZER adam
    LEARNING_RATE {learning_rate}
    EPOCHS {epochs}
    BATCH_SIZE {batch_size}
    SAVE_TO "{save_to}"
}}

ML.PREDICT(MODEL "digit_net",
    INPUT {{
        SELECT ?sample {feature_select}
        WHERE {{
        ?sample mnist:split mnist:{predict_split} .
        {patterns}
        }}
    }},
    OUTPUT ?digit DISTRIBUTION
)
"#,
            feature_list = features.join(", "),
            feature_select = features.join(" "),
        )
    }
}
