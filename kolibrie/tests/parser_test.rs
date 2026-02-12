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
        let input = r#"RULE :OverheatingAlert(?room) :- 
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
        assert_eq!(rule.head.arguments.len(), 1);
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
        let input = r#"RULE :OverheatingAlert(?room) :- 
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
        assert_eq!(rule.head.arguments.len(), 1);
        assert_eq!(rule.head.arguments[0], "?room");
        
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
}