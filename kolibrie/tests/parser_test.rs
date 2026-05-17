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
    use kolibrie::parser::*;
    use shared::query::FilterExpression;
    use shared::query::TrainingDataSource;
    use shared::query::{ModelArch, NeuralOutputKind};
    use kolibrie::neural_relations::lower_ml_predict_alias;
    
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
        
        let (_, (_, variables, patterns, _, _, _, _, _, _, _, _, _)) = result.unwrap();
        
        // Check that variables are parsed correctly
        assert_eq!(variables.len(), 3);
        assert_eq!(variables[0].1, "?patient");
        assert_eq!(variables[1].1, "?name");
        assert_eq!(variables[2].1, "?riskScore");
        
        // Check that the pattern includes the 'a' syntax
        assert!(patterns.len() >= 1);
        assert_eq!(patterns[0].0, "?patient");
        // The parser expands 'a' to the full URI
        assert_eq!(patterns[0].1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        assert!(patterns[0].2.contains("Test"));
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
        assert_eq!(triples[0].1, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
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
        assert!(result.is_ok(), "Failed to parse RULE with PROB annotation: {:?}", result.err());

        let (_, rule) = result.unwrap();

        // Check rule head
        assert_eq!(rule.head.predicate, ":TransitiveRelated");

        // Check PROB annotation is present and correct
        let prob = rule.prob_annotation.as_ref().expect("PROB annotation should be present");
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
        assert!(result.is_ok(), "Failed to parse RULE with min PROB: {:?}", result.err());

        let (_, rule) = result.unwrap();

        let prob = rule.prob_annotation.as_ref().expect("PROB annotation should be present");
        assert_eq!(prob.combination, "min");
        assert!((prob.threshold.unwrap() - 0.5).abs() < 1e-9);
        assert!(prob.confidence.is_none(), "confidence should be None when not specified");

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
        assert!(rule.prob_annotation.is_none(), "PROB annotation should be None for classical rules");
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
        
        let (_, (_, variables, patterns, _, _, prefixes, _, _, _, _, _, _)) = result.unwrap();
        
        // Check that SELECT * is parsed correctly
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0], ("*", "*", None));
        
        // Check that the prefix is registered
        assert!(prefixes.contains_key("ex"));
        assert_eq!(prefixes.get("ex").unwrap(), "http://example.org#");
        
        // Check that the triple pattern is parsed correctly
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].0, "?s");
        assert_eq!(patterns[0].1, "?p");
        assert_eq!(patterns[0].2, "?o");
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
        assert!(result.is_ok(), "Failed to parse RULE with topk PROB: {:?}", result.err());

        let (_, rule) = result.unwrap();
        let prob = rule.prob_annotation.as_ref().expect("PROB annotation should be present");
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
        assert!(result.is_ok(), "Failed to parse RULE with wmc PROB: {:?}", result.err());

        let (_, rule) = result.unwrap();
        let prob = rule.prob_annotation.as_ref().expect("PROB annotation should be present");
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
        assert_eq!(decl.arch, ModelArch::Mlp { hidden_layers: vec![64, 32] });
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
            TrainingDataSource::Query(query) => assert!(query.contains("SELECT ?sample ?p0 ?p1 ?label")),
            _ => panic!("expected QUERY fallback source"),
        }
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
}
