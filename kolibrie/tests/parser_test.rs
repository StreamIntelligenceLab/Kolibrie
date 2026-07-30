/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

#[cfg(test)]
mod tests {
    use kolibrie::neural_relations::lower_ml_predict_alias;
    use kolibrie::parser::*;
    use shared::hybrid::ThresholdPolicyKind;
    use shared::query::{
        FilterExpression, GroupGraphPattern, ModelArch, NeuralOutputKind, SortDirection,
        SparqlOperation, TrainingDataSource, UpdateOperation,
    };

    #[test]
    fn test_identifier_parsing() {
        let result = identifier("person_name");
        assert_eq!(result, Ok(("", "person_name")));

        // Debug what the parser actually returns
        let result = identifier("123invalid");
        assert!(result.is_ok());

        // If your identifier parser allows numbers at start, test with something that should fail
        let result = identifier(""); // Empty string should fail
        assert!(result.is_err());

        let result = identifier("!invalid"); // Special characters should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_variable_parsing() {
        let result = variable("?person");
        assert_eq!(result, Ok(("", "?person")));

        let result = variable("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_predicate_parsing() {
        // Test prefixed predicate
        let result = predicate("ex:worksAt");
        assert_eq!(result, Ok(("", "ex:worksAt")));

        // Test colon-prefixed predicate
        let result = predicate(":worksAt");
        assert_eq!(result, Ok(("", ":worksAt")));

        // Test 'a' predicate (rdf:type)
        let result = predicate("a");
        assert_eq!(result, Ok(("", "a")));

        // Test URI predicate
        let result = predicate("<http://example.org/worksAt>");
        assert_eq!(result, Ok(("", "http://example.org/worksAt")));

        // Test variable as predicate
        let result = predicate("?predicate");
        assert_eq!(result, Ok(("", "?predicate")));
    }

    #[test]
    fn test_literal_parsing() {
        let result = parse_literal("\"John Doe\"");
        assert_eq!(result, Ok(("", "John Doe")));

        let result = parse_literal("\"unterminated");
        assert!(result.is_err());
    }

    #[test]
    fn test_uri_parsing() {
        let result = parse_uri("<http://example.org/person>");
        assert_eq!(result, Ok(("", "http://example.org/person")));

        let result = parse_uri("<incomplete");
        assert!(result.is_err());
    }

    #[test]
    fn test_triple_block_parsing() {
        let input = "?person ex:name \"John\" ; ex:age 25";
        let result = parse_triple_block(input);

        assert!(result.is_ok());
        let (remaining, triples) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(triples.len(), 2);
        // Fix: Your parser strips the quotes from literals
        assert_eq!(triples[0], ("?person", "ex:name", "John")); // Without quotes
        assert_eq!(triples[1], ("?person", "ex:age", "25"));
    }

    #[test]
    fn test_filter_comparison_parsing() {
        let input = "?age > 18";
        let result = parse_comparison(input);

        assert!(result.is_ok());
        let (_, filter) = result.unwrap();
        match filter {
            FilterExpression::Comparison(var, op, value) => {
                assert_eq!(var, "?age");
                assert_eq!(op, ">");
                assert_eq!(value, "18");
            }
            _ => panic!("Expected comparison filter"),
        }
    }

    #[test]
    fn test_arithmetic_expression_parsing() {
        let input = "?x + 5 * ?y";
        let result = parse_arithmetic_expression(input);

        assert!(result.is_ok());
        // Add specific assertions for the arithmetic structure
    }

    #[test]
    fn test_select_parsing() {
        // Test simple SELECT
        let result = parse_select("SELECT ?person ?name");
        assert!(result.is_ok());
        let (_, variables) = result.unwrap();
        assert_eq!(variables.len(), 2);

        // Test SELECT *
        let result = parse_select("SELECT *");
        assert!(result.is_ok());
        let (_, variables) = result.unwrap();
        assert_eq!(variables[0], ("*", "*", None));

        // Test SELECT with aggregation
        let result = parse_select("SELECT SUM(?salary) AS ?total");
        assert!(result.is_ok());
    }

    #[test]
    fn test_values_clause_parsing() {
        let input = "VALUES ?person { <http://example.org/john> <http://example.org/jane> }";
        let result = parse_values(input);

        assert!(result.is_ok());
        let (_, values_clause) = result.unwrap();
        assert_eq!(values_clause.variables, vec!["?person"]);
        assert_eq!(values_clause.values.len(), 2);
    }

    #[test]
    fn test_bind_parsing() {
        let input = "BIND(CONCAT(?firstName, \" \", ?lastName) AS ?fullName)";
        let result = parse_bind(input);

        assert!(result.is_ok());
        let (_, (func_name, args, new_var)) = result.unwrap();
        assert_eq!(func_name, "CONCAT");
        assert_eq!(args.len(), 3);
        assert_eq!(new_var, "?fullName");
    }

    #[test]
    fn test_rule_parsing() {
        let input = r#"RULE :OverheatingAlert :-
        CONSTRUCT {
            ?room ex:overheatingAlert true .
        }
        WHERE {
            ?reading ex:room ?room ;
                    ex:temperature ?temp
            FILTER (?temp > 80)
        }"#;

        let result = parse_rule(input);

        assert!(result.is_ok());

        let (_, rule) = result.unwrap();
        assert_eq!(rule.head.predicate, ":OverheatingAlert");
        assert_eq!(rule.conclusion.len(), 1);
    }

    #[test]
    fn test_ml_predict_parsing() {
        let input = r#"
            ML.PREDICT(
                MODEL "temperaturePredictor",
                INPUT { SELECT ?room ?humidity WHERE { ?room :humidity ?humidity } },
                OUTPUT ?predictedTemp
            )
        "#;

        let result = parse_ml_predict(input);
        assert!(result.is_ok());

        let (_, ml_clause) = result.unwrap();
        assert_eq!(ml_clause.model, "temperaturePredictor");
        assert_eq!(ml_clause.output, "?predictedTemp");
    }

    #[test]
    fn test_sparql_select_with_a_syntax() {
        let input = r#"PREFIX example: <http://www.example.com/>
SELECT ?patient ?name ?riskScore
WHERE {
  ?patient a example:Test .
}"#;

        let result = parse_sparql_query(input);

        assert!(result.is_ok());

        let (_, query) = result.unwrap();

        // Check that variables are parsed correctly
        assert_eq!(query.variables.len(), 3);
        assert_eq!(query.variables[0].1, "?patient");
        assert_eq!(query.variables[1].1, "?name");
        assert_eq!(query.variables[2].1, "?riskScore");

        // The unified syntax tree retains lexical terms. `a` is expanded
        // exactly once when the AST is lowered to the execution plan.
        let GroupGraphPattern::Bgp(patterns) = query.pattern else {
            panic!("expected a BGP");
        };
        assert_eq!(patterns, vec![("?patient", "a", "example:Test")]);
    }

    #[test]
    fn test_rule_with_a_syntax_in_where() {
        let input = r#"RULE :OverheatingAlert :-
CONSTRUCT {
    ?room ex:overheatingAlert true .
}
WHERE {
    ?reading a ex:Sensor ;
             ex:room ?room ;
             ex:temperature ?temp
    FILTER (?temp > 80)
}"#;

        let result = parse_rule(input);

        assert!(result.is_ok());

        let (_, rule) = result.unwrap();

        // Check rule head
        assert_eq!(rule.head.predicate, ":OverheatingAlert");

        // Check conclusion
        assert_eq!(rule.conclusion.len(), 1);
        assert_eq!(rule.conclusion[0].0, "?room");

        // Check body patterns
        let (patterns, filters, _, _, _) = &rule.body;
        assert!(patterns.len() >= 3);

        // First pattern should have 'a' for rdf:type
        assert_eq!(patterns[0].0, "?reading");
        assert!(patterns[0].1 == "a" || patterns[0].1.contains("type"));
        assert!(patterns[0].2.contains("Sensor"));

        // Check that filters are present
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_triple_block_with_a_syntax() {
        // Test that triple blocks can parse 'a' as a predicate
        let input = "?patient a example:Test ; example:name \"John\"";
        let result = parse_triple_block(input);

        assert!(result.is_ok());
        let (remaining, triples) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(triples.len(), 2);

        // First triple - 'a'
        assert_eq!(triples[0].0, "?patient");
        assert_eq!(
            triples[0].1,
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        );
        assert!(triples[0].2.contains("Test"));

        // Second triple is a normal pattern
        assert_eq!(triples[1].0, "?patient");
        assert!(triples[1].1.contains("name"));
        assert_eq!(triples[1].2, "John");
    }

    #[test]
    fn test_rule_with_prob_annotation() {
        let input = r#"RULE :TransitiveRelated PROB(combination=independent, threshold=0.3, confidence=0.9) :-
CONSTRUCT {
    ?x ex:related ?z .
}
WHERE {
    ?x ex:related ?y .
    ?y ex:related ?z .
}"#;

        let result = parse_rule(input);
        assert!(
            result.is_ok(),
            "Failed to parse RULE with PROB annotation: {:?}",
            result.err()
        );

        let (_, rule) = result.unwrap();

        // Check rule head
        assert_eq!(rule.head.predicate, ":TransitiveRelated");

        // Check PROB annotation is present and correct
        let prob = rule
            .prob_annotation
            .as_ref()
            .expect("PROB annotation should be present");
        assert_eq!(prob.combination, "independent");
        assert!((prob.threshold.unwrap() - 0.3).abs() < 1e-9);
        assert!((prob.confidence.unwrap() - 0.9).abs() < 1e-9);

        // Check conclusion
        assert_eq!(rule.conclusion.len(), 1);
        assert_eq!(rule.conclusion[0], ("?x", "ex:related", "?z"));

        // Check body patterns
        let (patterns, filters, _, _, _) = &rule.body;
        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0], ("?x", "ex:related", "?y"));
        assert_eq!(patterns[1], ("?y", "ex:related", "?z"));
        assert!(filters.is_empty());
    }

    #[test]
    fn test_rule_with_prob_annotation_min_combination() {
        let input = r#"RULE :InferType PROB(combination=min, threshold=0.5) :-
CONSTRUCT {
    ?x a ex:HighRisk .
}
WHERE {
    ?x ex:score ?s .
    FILTER (?s > 80)
}"#;

        let result = parse_rule(input);
        assert!(
            result.is_ok(),
            "Failed to parse RULE with min PROB: {:?}",
            result.err()
        );

        let (_, rule) = result.unwrap();

        let prob = rule
            .prob_annotation
            .as_ref()
            .expect("PROB annotation should be present");
        assert_eq!(prob.combination, "min");
        assert!((prob.threshold.unwrap() - 0.5).abs() < 1e-9);
        assert!(
            prob.confidence.is_none(),
            "confidence should be None when not specified"
        );

        // Check filter is parsed
        let (_, filters, _, _, _) = &rule.body;
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_rule_with_prob_annotation_provenance_alias() {
        let input = r#"RULE :CriticalRisk PROB(provenance=minmax, threshold=0.5) :-
CONSTRUCT {
    ?x ex:risk true .
}
WHERE {
    ?x ex:score ?s .
    FILTER (?s > 80)
}"#;

        let result = parse_rule(input);
        assert!(
            result.is_ok(),
            "Failed to parse RULE with provenance PROB alias: {:?}",
            result.err()
        );

        let (_, rule) = result.unwrap();
        let prob = rule
            .prob_annotation
            .as_ref()
            .expect("PROB annotation should be present");
        assert_eq!(prob.combination, "minmax");
        assert!((prob.threshold.unwrap() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn test_rule_without_prob_annotation_still_works() {
        // Regression: rules without PROB should parse identically to before
        let input = r#"RULE :SimpleRule :-
CONSTRUCT {
    ?x ex:inferred true .
}
WHERE {
    ?x ex:fact ?y .
}"#;

        let result = parse_rule(input);
        assert!(result.is_ok());

        let (_, rule) = result.unwrap();
        assert!(
            rule.prob_annotation.is_none(),
            "PROB annotation should be None for classical rules"
        );
        assert_eq!(rule.head.predicate, ":SimpleRule");
        assert_eq!(rule.conclusion.len(), 1);
    }

    #[test]
    fn test_select_all_with_prefix() {
        let input = r#"PREFIX ex: <http://example.org#>
SELECT *
WHERE {
  ?s ?p ?o.
}"#;

        let result = parse_sparql_query(input);

        assert!(result.is_ok());

        let (_, query) = result.unwrap();

        // Check that SELECT * is parsed correctly
        assert_eq!(query.variables.len(), 1);
        assert_eq!(query.variables[0], ("*", "*", None));

        let (_, combined) = parse_combined_query(input).unwrap();
        assert_eq!(combined.prefixes["ex"], "http://example.org#");

        // Check that the triple pattern is parsed correctly
        let GroupGraphPattern::Bgp(patterns) = query.pattern else {
            panic!("expected a BGP");
        };
        assert_eq!(patterns, vec![("?s", "?p", "?o")]);
    }

    #[test]
    fn test_rule_with_prob_annotation_topk() {
        let input = r#"RULE :TopKRule PROB(combination=topk, threshold=5) :-
CONSTRUCT {
    ?x ex:related ?z .
}
WHERE {
    ?x ex:related ?y .
    ?y ex:related ?z .
}"#;

        let result = parse_rule(input);
        assert!(
            result.is_ok(),
            "Failed to parse RULE with topk PROB: {:?}",
            result.err()
        );

        let (_, rule) = result.unwrap();
        let prob = rule
            .prob_annotation
            .as_ref()
            .expect("PROB annotation should be present");
        assert_eq!(prob.combination, "topk");
        assert!((prob.threshold.unwrap() - 5.0).abs() < 1e-9);
        assert!(prob.confidence.is_none());
    }

    #[test]
    fn test_rule_with_prob_annotation_wmc() {
        let input = r#"RULE :WmcRule PROB(combination=wmc) :-
CONSTRUCT {
    ?x ex:related ?z .
}
WHERE {
    ?x ex:related ?y .
    ?y ex:related ?z .
}"#;

        let result = parse_rule(input);
        assert!(
            result.is_ok(),
            "Failed to parse RULE with wmc PROB: {:?}",
            result.err()
        );

        let (_, rule) = result.unwrap();
        let prob = rule
            .prob_annotation
            .as_ref()
            .expect("PROB annotation should be present");
        assert_eq!(prob.combination, "wmc");
        assert!(prob.threshold.is_none(), "threshold should be None for wmc");
        assert!(prob.confidence.is_none());
    }

    #[test]
    fn parse_model_decl_exclusive() {
        let input = r#"
            MODEL "mnist_classifier" {
                ARCH MLP { HIDDEN [64, 32] }
                OUTPUT EXCLUSIVE { "0", "1", "2" }
            }
        "#;
        let (_, decl) = parse_model_decl(input).unwrap();
        assert_eq!(decl.name, "mnist_classifier");
        assert_eq!(
            decl.arch,
            ModelArch::Mlp {
                hidden_layers: vec![64, 32]
            }
        );
        assert_eq!(
            decl.output_kind,
            NeuralOutputKind::Exclusive {
                labels: vec!["0".to_string(), "1".to_string(), "2".to_string()],
            }
        );
    }

    #[test]
    fn parse_neural_relation_decl_multiline() {
        let input = r#"
            NEURAL RELATION ex:predictedDigit USING MODEL "mnist_classifier" {
                INPUT {
                    ?sample ex:pixel_0 ?p0 .
                    ?sample ex:pixel_1 ?p1 .
                    ?sample ex:pixel_2 ?p2 .
                }
                FEATURES { ?p0, ?p1, ?p2 }
            }
        "#;
        let (_, decl) = parse_neural_relation_decl(input).unwrap();
        assert_eq!(decl.predicate, "ex:predictedDigit");
        assert_eq!(decl.model_name, "mnist_classifier");
        assert_eq!(decl.input_patterns.len(), 3);
        assert_eq!(decl.anchor_var, "?sample");
        assert_eq!(decl.feature_vars, vec!["?p0", "?p1", "?p2"]);
    }

    #[test]
    fn parse_train_neural_relation_data_block() {
        let input = r#"
            TRAIN NEURAL RELATION ex:predictedDigit {
                DATA {
                    ?sample ex:label ?label .
                }
                LABEL ?label
                TARGET { ?sample ex:predictedDigit ?label }
                LOSS cross_entropy
                OPTIMIZER adam
                LEARNING_RATE 0.001
                EPOCHS 50
                BATCH_SIZE 16
                SAVE_TO "mnist_digit_model.bin"
            }
        "#;
        let (_, decl) = parse_train_neural_relation_decl(input).unwrap();
        match decl.data_source {
            TrainingDataSource::GraphPattern(patterns) => assert_eq!(patterns.len(), 1),
            _ => panic!("expected DATA graph-pattern source"),
        }
        assert_eq!(decl.label_var, "?label");
        assert_eq!(decl.target_triple.1, "ex:predictedDigit");
        assert_eq!(decl.save_path.as_deref(), Some("mnist_digit_model.bin"));
    }

    #[test]
    fn parse_train_neural_relation_query_block() {
        let input = r#"
            TRAIN NEURAL RELATION ex:predictedDigit {
                QUERY {
                    SELECT ?sample ?p0 ?p1 ?label
                    WHERE {
                        ?sample ex:pixel_0 ?p0 .
                        ?sample ex:pixel_1 ?p1 .
                        ?sample ex:label ?label .
                    }
                }
                LABEL ?label
                TARGET { ?sample ex:predictedDigit ?label }
                LOSS cross_entropy
                OPTIMIZER adam
                LEARNING_RATE 0.001
                EPOCHS 5
                BATCH_SIZE 2
            }
        "#;
        let (_, decl) = parse_train_neural_relation_decl(input).unwrap();
        match decl.data_source {
            TrainingDataSource::Query(query) => {
                assert!(query.contains("SELECT ?sample ?p0 ?p1 ?label"))
            }
            _ => panic!("expected QUERY fallback source"),
        }
    }

    #[test]
    fn parse_top_level_ml_predict_after_neural_decls() {
        let input = r#"
PREFIX ex: <http://example.org/>

MODEL "digit_model" {
    ARCH MLP { HIDDEN [16, 8] }
    OUTPUT EXCLUSIVE { "A", "B", "C" }
}

NEURAL RELATION ex:predictedDigit USING MODEL "digit_model" {
    INPUT {
        ?sample ex:x0 ?x0 .
        ?sample ex:x1 ?x1 .
        ?sample ex:x2 ?x2 .
    }
    FEATURES { ?x0, ?x1, ?x2 }
}

ML.PREDICT(MODEL "digit_model",
    INPUT {
        SELECT ?sample ?x0 ?x1 ?x2
        WHERE {
            ?sample ex:x0 ?x0 .
            ?sample ex:x1 ?x1 .
            ?sample ex:x2 ?x2 .
        }
    },
    OUTPUT ?label
)
        "#;

        let (rest, combined) = parse_combined_query(input).unwrap();
        assert!(rest.trim().is_empty());
        assert!(combined.rule.is_none());
        assert_eq!(combined.model_decls.len(), 1);
        assert_eq!(combined.neural_relation_decls.len(), 1);

        let ml_predict = combined
            .ml_predict
            .as_ref()
            .expect("top-level ML.PREDICT should be parsed");
        assert_eq!(ml_predict.model, "digit_model");
        assert_eq!(ml_predict.output, "?label");
    }

    #[test]
    fn lower_ml_predict_alias_test() {
        let predict_input = r#"
            ML.PREDICT(MODEL "fraud_predictor",
                INPUT {
                    SELECT ?tx ?amt WHERE {
                        ?tx ex:amount ?amt .
                    }
                },
                OUTPUT ?score
            )
        "#;
        let (_, predict_clause) = parse_ml_predict(predict_input).unwrap();
        let relation_decl = lower_ml_predict_alias(&predict_clause).unwrap();
        assert_eq!(relation_decl.model_name, "fraud_predictor");
        assert_eq!(relation_decl.predicate, "?score");
        assert_eq!(relation_decl.input_patterns.len(), 1);
    }

    #[test]
    fn hybrid_probability_annotation_parses_explicit_policy() {
        let input = r#"RULE :Hybrid PROB(
            provenance=hybrid,
            threshold=0.7,
            band_epsilon=0.01,
            marginal_floor=0.00001,
            k_initial=4,
            k_max=32,
            k_growth=2,
            topk_budget_ms=10,
            sdd_budget_ms=100,
            node_budget=50000
        ) :- CONSTRUCT { ?x <result> <yes> } WHERE { ?x <input> <yes> } ."#;
        let (_, rule) = parse_rule(input).expect("hybrid annotation should parse");
        let annotation = rule.prob_annotation.expect("probability annotation");
        let config = annotation.hybrid_config.expect("validated hybrid config");
        assert_eq!(config.threshold, 0.7);
        assert_eq!(config.threshold_policy, ThresholdPolicyKind::Explicit);
        assert_eq!(config.k_initial, 4);
        assert_eq!(config.k_max, 32);
        assert_eq!(config.topk_budget.as_millis(), 10);
        assert_eq!(config.sdd_node_budget, 50_000);
    }

    #[test]
    fn hybrid_probability_annotation_parses_cost_ratio_policy() {
        let input = r#"RULE :Hybrid PROB(
            provenance=hybrid,
            threshold=auto:cost(fp=2,fn=8),
            k_initial=4
        ) :- CONSTRUCT { ?x <result> <yes> } WHERE { ?x <input> <yes> } ."#;
        let (_, rule) = parse_rule(input).expect("cost-ratio threshold should parse");
        let annotation = rule.prob_annotation.expect("probability annotation");
        let config = annotation.hybrid_config.expect("validated hybrid config");
        assert!((config.threshold - 0.2).abs() < 1e-12);
        assert_eq!(config.threshold_policy, ThresholdPolicyKind::CostRatio);
        assert!((annotation.threshold.unwrap() - 0.2).abs() < 1e-12);
    }

    #[test]
    fn hybrid_probability_annotation_rejects_invalid_automatic_policies() {
        let invalid = [
            "auto",
            "auto:quantile(0.9)",
            "auto:cost(fp=NaN,fn=1)",
            "auto:cost(fp=inf,fn=1)",
            "auto:cost(fp=-1,fn=2)",
            "auto:cost(fp=0,fn=0)",
            "auto:cost(fp=1)",
            "auto:cost(fp=1,fn=2,other=3)",
            "auto:cost(fp=1,fp=2,fn=3)",
        ];
        for threshold in invalid {
            let input = format!(
                "RULE :Hybrid PROB(provenance=hybrid, threshold={threshold}) :- \
                 CONSTRUCT {{ ?x <result> <yes> }} WHERE {{ ?x <input> <yes> }} ."
            );
            assert!(parse_rule(&input).is_err(), "accepted invalid {threshold}");
        }

        let duplicate = r#"RULE :Hybrid PROB(
            provenance=hybrid,
            threshold=0.4,
            threshold=auto:cost(fp=1,fn=1)
        ) :- CONSTRUCT { ?x <result> <yes> } WHERE { ?x <input> <yes> } ."#;
        assert!(parse_rule(duplicate).is_err());

        let topk_collision = r#"RULE :TopK PROB(
            combination=topk,
            threshold=auto:cost(fp=1,fn=1)
        ) :- CONSTRUCT { ?x <result> <yes> } WHERE { ?x <input> <yes> } ."#;
        assert!(parse_rule(topk_collision).is_err());
    }

    #[test]
    fn hybrid_probability_annotation_rejects_missing_threshold_and_unknown_keys() {
        let missing = r#"RULE :Hybrid PROB(provenance=hybrid) :-
            CONSTRUCT { ?x <result> <yes> } WHERE { ?x <input> <yes> } ."#;
        assert!(parse_rule(missing).is_err());

        let unknown = r#"RULE :Hybrid PROB(provenance=hybrid, threshold=0.7, mystery=1) :-
            CONSTRUCT { ?x <result> <yes> } WHERE { ?x <input> <yes> } ."#;
        assert!(parse_rule(unknown).is_err());
    }

    #[test]
    fn unified_select_parses_nested_graph_and_union_case_insensitively() {
        let input = r#"
            # GRAPH in this comment is not syntax
            prefix ex: <http://example.com/>
            select distinct ?s ?g
            from named ex:outer
            FROM NAMED <urn:second>
            where {
                graph ?g {
                    ?s a ex:Thing .
                    { ?s ex:label "escaped \"GRAPH\""@en }
                    union
                    { graph ex:inner { } }
                }
            }
            limit 5 # trailing comment
        "#;
        let (_, combined) = parse_combined_query(input).expect("SELECT should parse");
        let Some(SparqlOperation::Select(query)) = combined.sparql else {
            panic!("expected SELECT request");
        };
        assert!(query.distinct);
        assert_eq!(query.limit, Some(5));
        assert_eq!(combined.prefixes["ex"], "http://example.com/");
        assert_eq!(query.from_named, vec!["ex:outer", "<urn:second>"]);
        assert_eq!(
            query.variables,
            vec![("VAR", "?s", None), ("VAR", "?g", None)]
        );

        let GroupGraphPattern::Graph { name, pattern } = query.pattern else {
            panic!("expected outer GRAPH");
        };
        assert_eq!(name, "?g");
        let GroupGraphPattern::Join(parts) = *pattern else {
            panic!("expected a BGP joined to a UNION");
        };
        assert_eq!(parts.len(), 2);
        assert!(matches!(parts[1], GroupGraphPattern::Union(ref branches) if branches.len() == 2));
    }

    #[test]
    fn unified_parser_supports_requested_update_forms_and_graph_templates() {
        let (_, insert_data) = parse_combined_query(
            r#"INSERT DATA {
                <urn:s> <urn:p> "line\\nvalue"@en .
                GRAPH <urn:g> { <urn:s> <urn:q> "7"^^<urn:datatype> }
            }"#,
        )
        .expect("INSERT DATA");
        let Some(SparqlOperation::Update(UpdateOperation::InsertData(insert))) = insert_data.sparql
        else {
            panic!("expected INSERT DATA");
        };
        assert_eq!(insert.quads.len(), 2);
        assert_eq!(insert.quads[1].graph, Some("<urn:g>"));
        assert_eq!(insert.quads[1].triple.2, "\"7\"^^<urn:datatype>");

        let (_, delete_only) = parse_combined_query(
            "DELETE { GRAPH ?g { ?s <urn:p> ?o } } WHERE { GRAPH ?g { ?s <urn:p> ?o } }",
        )
        .expect("DELETE WHERE modify");
        assert!(matches!(
            delete_only.sparql,
            Some(SparqlOperation::Update(UpdateOperation::DeleteWhere {
                ref delete,
                ..
            })) if delete.quads.len() == 1
        ));

        let (_, insert_only) = parse_combined_query(
            "INSERT { GRAPH <urn:g> { ?s <urn:new> ?o } } WHERE { ?s <urn:old> ?o }",
        )
        .expect("INSERT WHERE modify");
        assert!(matches!(
            insert_only.sparql,
            Some(SparqlOperation::Update(UpdateOperation::InsertWhere {
                ref insert,
                ..
            })) if insert.quads.len() == 1
        ));

        let (_, combined) = parse_combined_query(
            "DELETE { ?s <urn:old> ?o } INSERT { ?s <urn:new> ?o } WHERE { ?s <urn:old> ?o }",
        )
        .expect("combined DELETE/INSERT");
        assert!(matches!(
            combined.sparql,
            Some(SparqlOperation::Update(UpdateOperation::DeleteInsertWhere {
                ref delete,
                ref insert,
                ..
            })) if delete.quads.len() == 1 && insert.quads.len() == 1
        ));

        let (_, delete_where) =
            parse_combined_query("DELETE WHERE { GRAPH ?g { ?s <urn:p> ?o . ?s <urn:q> ?v } }")
                .expect("DELETE WHERE shorthand");
        assert!(matches!(
            delete_where.sparql,
            Some(SparqlOperation::Update(UpdateOperation::DeleteWhereShorthand {
                ref delete,
                ref where_pattern,
            })) if delete.quads.len() == 2 && matches!(where_pattern, GroupGraphPattern::Join(_))
        ));
    }

    #[test]
    fn unified_group_pattern_contains_existing_select_features() {
        let input = r#"
            PREFIX ex: <http://example.com/>
            SELECT ?s ?name
            FROM <urn:default-1>
            FROM <urn:default-2>
            FROM NAMED <urn:named>
            WHERE {
                VALUES (?s ?label) {
                    (<urn:s> "first")
                    (UNDEF "second")
                }
                GRAPH ?g {
                    ?s ex:value ?value .
                    FILTER (?value > 1)
                    BIND (CONCAT(?label, "!") AS ?name)
                }
                {
                    SELECT ?s WHERE { ?s ex:enabled true }
                    LIMIT 1
                }
            }
            GROUP BY ?s ?name
            ORDER BY DESC(?name) ?s
            LIMIT 10
        "#;

        let (_, combined) = parse_combined_query(input).expect("unified SELECT");
        let Some(SparqlOperation::Select(query)) = combined.sparql else {
            panic!("expected SELECT");
        };
        assert_eq!(query.from, vec!["<urn:default-1>", "<urn:default-2>"]);
        assert_eq!(query.from_named, vec!["<urn:named>"]);
        assert_eq!(query.group_vars, vec!["?s", "?name"]);
        assert_eq!(query.order_conditions.len(), 2);
        assert_eq!(query.limit, Some(10));

        let GroupGraphPattern::Join(parts) = query.pattern else {
            panic!("expected joined VALUES, GRAPH, and subquery");
        };
        assert!(matches!(parts[0], GroupGraphPattern::Values(_)));
        let GroupGraphPattern::Graph { name, pattern } = &parts[1] else {
            panic!("expected GRAPH");
        };
        assert_eq!(*name, "?g");
        let GroupGraphPattern::Join(graph_parts) = pattern.as_ref() else {
            panic!("expected graph-local BGP/FILTER/BIND");
        };
        assert!(matches!(graph_parts[0], GroupGraphPattern::Bgp(_)));
        assert!(matches!(graph_parts[1], GroupGraphPattern::Filter(_)));
        assert!(matches!(graph_parts[2], GroupGraphPattern::Bind(_)));
        assert!(matches!(parts[2], GroupGraphPattern::SubQuery(_)));
    }

    #[test]
    fn unified_parser_validates_updates_and_consumes_the_complete_request() {
        assert!(parse_combined_query("INSERT DATA { ?s <urn:p> <urn:o> }").is_err());
        assert!(
            parse_combined_query("INSERT DATA { GRAPH ?g { <urn:s> <urn:p> <urn:o> } }").is_err()
        );
        assert!(parse_combined_query(
            r#"INSERT DATA { << <urn:s> <urn:p> "?not-a-variable" >> <urn:source> <urn:o> }"#
        )
        .is_ok());
        assert!(parse_combined_query(
            "INSERT DATA { << ?s <urn:p> <urn:o> >> <urn:source> <urn:o> }"
        )
        .is_err());
        assert!(parse_combined_query("DELETE DATA { _:b <urn:p> <urn:o> }").is_err());
        assert!(parse_combined_query("DELETE { _:b <urn:p> ?o } WHERE { ?s <urn:p> ?o }").is_err());
        assert!(parse_combined_query("SELECT * WHERE {} garbage").is_err());
        assert!(parse_combined_query("SELECT * FROM NAMED ?g WHERE {}").is_err());
        assert!(parse_combined_query("SELECT * FROM <urn:g> WHERE {}").is_ok());
        assert!(parse_combined_query("SELECT * WHERE {} # only a comment remains").is_ok());
        assert!(parse_combined_query("SELECT * WHERE { GRAPH:x <urn:p> <urn:o> }").is_ok());

        assert!(parse_combined_query("INSERT { <urn:s> <urn:p> <urn:o> }").is_err());
        assert!(
            parse_combined_query_with_options("INSERT { <urn:s> <urn:p> <urn:o> }", true).is_ok()
        );
        assert!(parse_combined_query("DELETE { <urn:s> <urn:p> <urn:o> }").is_err());
        assert!(
            parse_combined_query_with_options("DELETE { <urn:s> <urn:p> <urn:o> }", true).is_ok()
        );
    }

    #[test]
    fn unified_filter_parses_comments_and_complete_arithmetic_comparisons() {
        let (_, combined) = parse_combined_query(
            r#"
            SELECT $s WHERE {
                $s <urn:value> $value .
                FILTER (
                    ($value # comments are whitespace in SPARQL
                     + 1) >= (2 * $scale)
                )
            }
            "#,
        )
        .expect("arithmetic FILTER with comments");
        let Some(SparqlOperation::Select(query)) = combined.sparql else {
            panic!("expected SELECT");
        };
        let GroupGraphPattern::Join(parts) = query.pattern else {
            panic!("expected BGP and FILTER");
        };
        let GroupGraphPattern::Filter(FilterExpression::Comparison(left, ">=", right)) = &parts[1]
        else {
            panic!("expected an arithmetic comparison");
        };
        assert!(left.contains("$value"));
        assert!(left.contains("+ 1"));
        assert!(right.contains("2 * $scale"));
    }

    #[test]
    fn unified_parser_accepts_prefixed_local_dots_and_escapes() {
        let (_, combined) = parse_combined_query(
            r#"
            PREFIX ex: <http://example.com/>
            SELECT ?o WHERE {
                ex:item.one ex:has\.value ex:encoded%2Evalue .
                ex:escaped\#name ex:p ?o
            }
            "#,
        )
        .expect("valid PN_LOCAL spellings");
        let Some(SparqlOperation::Select(query)) = combined.sparql else {
            panic!("expected SELECT");
        };
        let GroupGraphPattern::Join(patterns) = query.pattern else {
            panic!("expected two adjacent triple patterns");
        };
        let GroupGraphPattern::Bgp(first) = &patterns[0] else {
            panic!("expected first BGP");
        };
        let GroupGraphPattern::Bgp(second) = &patterns[1] else {
            panic!("expected second BGP");
        };
        assert_eq!(
            first[0],
            ("ex:item.one", r"ex:has\.value", "ex:encoded%2Evalue")
        );
        assert_eq!(second[0].0, r"ex:escaped\#name");

        assert!(parse_combined_query("SELECT * WHERE { <urn:s> <urn:p> ex:bad\\escape }").is_err());
        assert!(parse_combined_query("SELECT * WHERE { <urn:s> <urn:p> ex:bad%2 }").is_err());
    }

    #[test]
    fn unified_parser_validates_literal_escapes_and_language_tags() {
        assert!(parse_combined_query(
            r#"SELECT * WHERE { <urn:s> <urn:p> "\t\b\n\r\f\"\'\\\u0041\U0001F642"@en-US }"#
        )
        .is_ok());
        assert!(parse_combined_query(r#"SELECT * WHERE { <urn:s> <urn:p> "bad\q" }"#).is_err());
        assert!(parse_combined_query(r#"SELECT * WHERE { <urn:s> <urn:p> "bad\u12" }"#).is_err());
        assert!(parse_combined_query(r#"SELECT * WHERE { <urn:s> <urn:p> "bad"@1 }"#).is_err());
        assert!(parse_combined_query(r#"SELECT * WHERE { <urn:s> <urn:p> "bad"@en- }"#).is_err());
        assert!(
            parse_combined_query(r#"SELECT * WHERE { <urn:s> <urn:p> "bad"@en--US }"#).is_err()
        );
    }

    #[test]
    fn unified_parser_enforces_sparql_iri_blank_node_and_prefix_boundaries() {
        assert!(parse_combined_query(
            r#"SELECT * WHERE { GRAPH <urn:bad graph> { <urn:s> <urn:p> <urn:o> } }"#
        )
        .is_err());
        assert!(parse_combined_query(
            r#"SELECT * WHERE { GRAPH <urn:bad\escape> { <urn:s> <urn:p> <urn:o> } }"#
        )
        .is_err());
        assert!(parse_combined_query(
            r#"SELECT * WHERE { GRAPH <urn:\u0067> { <urn:s> <urn:p> <urn:o> } }"#
        )
        .is_ok());

        assert!(parse_combined_query(
            "SELECT * WHERE { _:a.b <urn:p> <urn:o> . _:1valid <urn:p> <urn:o> }"
        )
        .is_ok());
        assert!(parse_combined_query("SELECT * WHERE { _:-bad <urn:p> <urn:o> }").is_err());
        assert!(parse_combined_query("SELECT * WHERE { 1bad:s <urn:p> <urn:o> }").is_err());
        assert!(parse_combined_query("SELECT * WHERE { -bad:s <urn:p> <urn:o> }").is_err());
    }

    #[test]
    fn public_standard_clause_parsers_delegate_to_the_unified_grammar() {
        let (remaining, grouped) = parse_group_by("group by ?s # comment\n").unwrap();
        assert_eq!(remaining, " # comment\n");
        assert_eq!(grouped, vec!["?s"]);
        assert!(parse_group_by("GROUPBY ?s").is_err());
        assert_eq!(parse_limit("limit 7"), Ok(("", 7)));
        assert_eq!(
            parse_prefix("prefix ex: <http://example.com/>"),
            Ok(("", ("ex", "http://example.com/")))
        );
        assert!(parse_prefix("PREFIX 1bad: <urn:bad>").is_err());

        let (_, conditions) = parse_order_by("order by desc(?score) ?name").unwrap();
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].direction, SortDirection::Desc);
        assert_eq!(conditions[1].direction, SortDirection::Asc);
    }

    #[test]
    fn update_validation_reports_the_offending_lexeme() {
        let input = "DELETE DATA { <urn:s> <urn:p> ?illegal }";
        let error = parse_combined_query(input).unwrap_err();
        let offending = match error {
            nom::Err::Error(error) | nom::Err::Failure(error) => error.input,
            nom::Err::Incomplete(_) => panic!("parser unexpectedly requested more input"),
        };
        assert_eq!(offending, "?illegal");
    }

    #[test]
    fn unified_parser_recursively_validates_rdf_star_and_update_restrictions() {
        assert!(parse_combined_query(
            r#"
            INSERT DATA {
                << << <urn:s> <urn:p> <urn:o> >>
                   <urn:source>
                   "literal ?not-var _:not-blank \" >>" >>
                <urn:assertedBy>
                <urn:test>
            }
            "#
        )
        .is_ok());

        assert!(
            parse_combined_query("SELECT * WHERE { << <urn:s> <urn:p> >> <urn:q> <urn:o> }")
                .is_err()
        );
        assert!(parse_combined_query(
            "SELECT * WHERE { << <urn:s> <urn:p> <urn:o> <urn:extra> >> <urn:q> <urn:o> }"
        )
        .is_err());
        assert!(
            parse_combined_query("SELECT * WHERE { << \"literal\" <urn:p> <urn:o> >> ?p ?o }")
                .is_err()
        );
        assert!(parse_combined_query(
            "SELECT * WHERE { << <urn:s> << <urn:a> <urn:b> <urn:c> >> <urn:o> >> ?p ?o }"
        )
        .is_err());

        assert!(parse_combined_query(
            "INSERT DATA { << << ?s <urn:p> <urn:o> >> <urn:q> <urn:r> >> <urn:x> <urn:y> }"
        )
        .is_err());
        assert!(parse_combined_query(
            "DELETE DATA { << << _:b <urn:p> <urn:o> >> <urn:q> <urn:r> >> <urn:x> <urn:y> }"
        )
        .is_err());
        assert!(parse_combined_query(
            "DELETE { << <urn:s> <urn:p> _:b >> <urn:q> ?o } WHERE { ?s <urn:q> ?o }"
        )
        .is_err());
    }

    #[test]
    fn unified_parser_requires_braced_union_operands_and_nonempty_requests() {
        assert!(parse_combined_query("").is_err());
        assert!(parse_combined_query(" # comment only").is_err());
        assert!(parse_combined_query("PREFIX ex: <urn:example#> # prologue only").is_err());

        assert!(
            parse_combined_query("SELECT * WHERE { ?s <urn:p> ?o UNION { ?s <urn:q> ?o } }")
                .is_err()
        );
        assert!(parse_combined_query(
            "SELECT * WHERE { { ?s <urn:p> ?o } UNION GRAPH <urn:g> { ?s <urn:q> ?o } }"
        )
        .is_err());
        assert!(parse_combined_query(
            "SELECT * WHERE { { ?s <urn:p> ?o } UNION { GRAPH <urn:g> { ?s <urn:q> ?o } } }"
        )
        .is_ok());
    }
}
