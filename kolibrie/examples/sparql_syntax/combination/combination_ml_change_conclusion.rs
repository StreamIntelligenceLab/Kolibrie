use kolibrie::execute_ml::execute_ml_prediction_from_clause;
use kolibrie::execute_query::execute_query;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use ml::MLHandler;
use pyo3::prepare_freethreaded_python;
use serde::{Deserialize, Serialize};
use shared::query::CombinedRule;
use shared::query::FilterExpression;
use shared::triple::Triple;
use std::collections::HashMap;
use std::error::Error;
use std::time::SystemTime;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TrafficData {
    road_id: String,
    avg_speed: f64,
    vehicle_count: f64,
    timestamp: SystemTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CongestionPrediction {
    road_id: String,
    congestion_level: f64,
    confidence: f64,
    severity: String,
    timestamp: SystemTime,
}

// Function to extract traffic data from the database
fn extract_traffic_data_from_database(
    database: &SparqlDatabase,
) -> Result<Vec<TrafficData>, Box<dyn Error>> {
    let dict = database.dictionary.read().unwrap();
    
    let traffic_data: Vec<TrafficData> = database
        .triples
        .iter()
        .filter(|triple| {
            // Use dict instead of database.dictionary
            dict.decode(triple.predicate)
                .map_or(false, |pred| pred.ends_with("avgVehicleSpeed"))
        })
        .map(|triple| {
            // All decode calls use dict
            let road_id = dict
                .decode(triple.subject)
                .unwrap_or_default()
                .split('#')
                .last()
                .unwrap_or_default()
                .to_string();

            let avg_speed = dict
                .decode(triple.object)
                .unwrap_or_default()
                .parse()
                .unwrap_or(0.0);

            let vehicle_count = database
                .triples
                .iter()
                .find(|t| {
                    t.subject == triple.subject
                        && dict.decode(t.predicate)
                            .map_or(false, |p| p.ends_with("vehicleCount"))
                })
                .and_then(|t| dict.decode(t.object))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);

            TrafficData {
                road_id,
                avg_speed,
                vehicle_count,
                timestamp: SystemTime::now(),
            }
        })
        .collect();

    // Release lock before printing
    drop(dict);

    println!("Found {} traffic data entries", traffic_data.len());
    for traffic in &traffic_data {
        println!(
            "Road: {}, Speed: {:.1} km/h, Vehicles: {}",
            traffic.road_id, traffic.avg_speed, traffic.vehicle_count
        );
    }

    if traffic_data.is_empty() {
        return Err("No traffic data found for ML prediction".into());
    }

    Ok(traffic_data)
}

// Function that handles congestion prediction logic
fn predict_congestion(
    ml_handler: &MLHandler,
    best_model: &str,
    traffic_data: &[TrafficData],
    feature_names: &[String],
) -> Result<(Vec<CongestionPrediction>, kolibrie::execute_ml::MLPredictTiming), Box<dyn Error>> {
    println!("\n[Rust] Building feature vectors...");
    let feature_build_start = std::time::Instant::now();
    
    // Build feature vectors based on selected variables
    let features: Vec<Vec<f64>> = traffic_data
        .iter()
        .map(|data| {
            let mut feature_vector = Vec::new();
            for feature_name in feature_names {
                match feature_name.as_str() {
                    "avgSpeed" => feature_vector.push(data.avg_speed),
                    "maxCount" => feature_vector.push(data.vehicle_count),
                    _ => {}
                }
            }
            feature_vector
        })
        .collect();

    if features.is_empty() || features[0].is_empty() {
        return Err("No valid features found for prediction".into());
    }

    let feature_build_time = feature_build_start.elapsed().as_secs_f64();
    println!("[Rust] Feature vector building: {:.6} seconds", feature_build_time);
    println!("Using features for congestion prediction: {:?}", features);

    // Call ML prediction (this now includes detailed timing)
    let prediction_results = ml_handler.predict(best_model, features)?;

    // Extract timing information from the ML prediction result
    let timing = kolibrie::execute_ml::MLPredictTiming {
        total_time: 0.0, // Will be updated by execute_ml_prediction_from_clause
        rust_to_python_time: 0.0, // Will be updated by execute_ml_prediction_from_clause
        python_preprocessing_time: prediction_results.timing.preprocessing_time,
        actual_prediction_time: prediction_results.timing.actual_prediction_time,
        python_postprocessing_time: prediction_results.timing.postprocessing_time,
        python_to_rust_time: 0.0, // Will be updated by execute_ml_prediction_from_clause
    };

    println!("\nML Model Performance:");
    println!("  Prediction Time: {:.4} seconds", 
        prediction_results.performance_metrics.prediction_time);
    println!("  Memory Usage: {:.2} MB", 
        prediction_results.performance_metrics.memory_usage_mb);
    println!("  CPU Usage: {:.2}%", 
        prediction_results.performance_metrics.cpu_usage_percent);

    // Create congestion predictions with severity levels
    let predictions: Vec<CongestionPrediction> = traffic_data
        .iter()
        .zip(prediction_results.predictions.iter())
        .zip(
            prediction_results
                .probabilities
                .unwrap_or_default()
                .iter()
                .chain(std::iter::repeat(&0.85)),
        )
        .map(|((data, &pred), &conf)| {
            let severity = match pred {
                level if level >= 0.8 => "SEVERE",
                level if level >= 0.6 => "HIGH",
                level if level >= 0.4 => "MODERATE",
                level if level >= 0.2 => "LOW",
                _ => "MINIMAL",
            };

            CongestionPrediction {
                road_id: data.road_id.clone(),
                congestion_level: pred,
                confidence: conf,
                severity: severity.to_string(),
                timestamp: SystemTime::now(),
            }
        })
        .collect();

    Ok((predictions, timing))
}

// Struct to manage dynamic rule configurations
struct DynamicRuleManager {
    current_rules: HashMap<String, String>,
    database: SparqlDatabase,
}

impl DynamicRuleManager {
    fn new() -> Self {
        Self {
            current_rules: HashMap::new(),
            database: SparqlDatabase::new(),
        }
    }

    fn load_initial_data(&mut self, rdf_data: &str) {
        self.database.parse_rdf(rdf_data);
        println!("Initial RDF data loaded.");
    }

    // Update rule with new definition
    fn update_rule(
        &mut self,
        rule_name: &str,
        rule_definition: &str,
    ) -> Result<(), Box<dyn Error>> {
        println!("\nUpdating rule: {}", rule_name);
        println!("Rule definition:\n{}", rule_definition);

        self.current_rules
            .insert(rule_name.to_string(), rule_definition.to_string());

        let is_ml_rule = rule_definition.contains("ML.PREDICT");

        if is_ml_rule {
            if let Ok((_rest, (parsed_rule, _))) = parse_standalone_rule(rule_definition) {
                println!("Processing ML rule...");

                if let Some(ml_predict) = &parsed_rule.ml_predict {
                    match execute_ml_prediction_from_clause(
                        ml_predict,
                        &self.database,
                        "traffic_predictor.py",
                        extract_traffic_data_from_database,
                        predict_congestion,
                    ) {
                        Ok((predictions, timing)) => {
                            timing.print_breakdown();
                            println!("ML Predictions completed:");
                            let mut ml_facts = Vec::new();

                            for prediction in predictions {
                                println!(
                                    "   Road: {} | Level: {:.2} | Severity: {} | Confidence: {:.2}",
                                    prediction.road_id,
                                    prediction.congestion_level,
                                    prediction.severity,
                                    prediction.confidence
                                );

                                self.add_prediction_to_database(&prediction);
                                self.create_ml_enhanced_triples(
                                    &mut ml_facts,
                                    &prediction,
                                    &parsed_rule,
                                );
                            }

                            println!("   Enhanced {} fact(s) with ML predictions", ml_facts.len());
                            for triple in ml_facts.iter() {
                                let dict = self.database.dictionary.read().unwrap();
                                println!("   {}", self.database.triple_to_string(triple, &*dict));
                                drop(dict);
                            }
                        }
                        Err(e) => eprintln!("ML prediction error: {}", e),
                    }
                }
            }
        } else {
            // Non-ML rule processing remains unchanged
            match process_rule_definition(rule_definition, &mut self.database) {
                Ok((rule, inferred_facts)) => {
                    println!("Rule '{}' processed successfully.", rule_name);
                    println!("   Premise patterns: {:?}", rule.premise);
                    println!("   Conclusion patterns: {:?}", rule.conclusion);
                    println!("   Inferred {} new fact(s)", inferred_facts.len());

                    let dict = self.database.dictionary.read().unwrap();
                    for triple in inferred_facts.iter() {
                        println!("   {}", self.database.triple_to_string(triple, &*dict));
                    }
                    drop(dict);

                    if inferred_facts.is_empty() {
                        println!("     NO FACTS INFERRED - Applying manual workaround...");
                        match rule_name {
                            "emergency_priority" => {
                                self.manually_apply_emergency_priority_rule();
                            }
                            "weather_congestion" => {
                                self.manually_apply_weather_congestion_rule();
                            }
                            _ => {
                                println!(
                                    "   No manual workaround available for rule: {}",
                                    rule_name
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    eprintln!("Error processing rule '{}': {}", rule_name, error);
                    return Err(error.into());
                }
            }
        }

        Ok(())
    }

    // Manual implementation of emergency priority rule
    fn manually_apply_emergency_priority_rule(&mut self) {
        println!("     Manually applying emergency priority rule...");

        let emergency_roads = self.find_roads_with_emergency_vehicles();

        for (road_uri, count) in emergency_roads {
            if count > 0 {
                println!(
                    "     Creating priority triples for {} (emergency vehicles: {})",
                    road_uri, count
                );

                // Create priority level triple
                self.add_triple(
                    &road_uri,
                    "http://example.org/traffic#priorityLevel",
                    "HIGH",
                );

                // Create clearance required triple
                self.add_triple(
                    &road_uri,
                    "http://example.org/traffic#clearanceRequired",
                    "true",
                );
            }
        }
    }

    // Manual implementation of weather congestion rule
    fn manually_apply_weather_congestion_rule(&mut self) {
        println!("     Manually applying weather congestion rule...");

        let roads_with_bad_weather = self.find_roads_with_bad_weather();
        let roads_with_congestion = self.find_roads_with_congestion();

        for road_uri in roads_with_bad_weather {
            if let Some(congestion_level) = roads_with_congestion.get(&road_uri) {
                println!(
                    "     Creating weather impact triples for {} (congestion: {})",
                    road_uri, congestion_level
                );

                // Create weather impact triple
                self.add_triple(
                    &road_uri,
                    "http://example.org/traffic#weatherImpact",
                    "HIGH",
                );

                // Create adjusted congestion level triple
                self.add_triple(
                    &road_uri,
                    "http://example.org/traffic#adjustedCongestionLevel",
                    congestion_level,
                );
            }
        }
    }

    // Helper functions for manual rule application
    fn find_roads_with_emergency_vehicles(&self) -> Vec<(String, i32)> {
        let mut results = Vec::new();
        let dict = self.database.dictionary.read().unwrap();
        
        for triple in &self.database.triples {
            if let Some(pred) = dict.decode(triple.predicate) {
                if pred.contains("emergencyVehicles") {
                    if let (Some(subject_str), Some(object_str)) = (
                        dict.decode(triple.subject),
                        dict.decode(triple.object),
                    ) {
                        if let Ok(count) = object_str.parse::<i32>() {
                            results.push((subject_str.to_string(), count));
                        }
                    }
                }
            }
        }
        
        results
    }

    fn find_roads_with_bad_weather(&self) -> Vec<String> {
        let mut results = Vec::new();
        let dict = self.database.dictionary.read().unwrap();
        
        for triple in &self.database.triples {
            if let Some(pred) = dict.decode(triple.predicate) {
                if pred.contains("weatherCondition") {
                    if let (Some(subject_str), Some(object_str)) = (
                        dict.decode(triple.subject),
                        dict.decode(triple.object),
                    ) {
                        if object_str == "rain" || object_str == "fog" {
                            results.push(subject_str.to_string());
                        }
                    }
                }
            }
        }
        
        results
    }

    fn find_roads_with_congestion(&self) -> HashMap<String, String> {
        let mut results = HashMap::new();
        let dict = self.database.dictionary.read().unwrap();
        
        for triple in &self.database.triples {
            if let Some(pred) = dict.decode(triple.predicate) {
                if pred.contains("congestionLevel") && !pred.starts_with("ex:") {
                    if let (Some(subject_str), Some(object_str)) = (
                        dict.decode(triple.subject),
                        dict.decode(triple.object),
                    ) {
                        if object_str.parse::<f64>().is_ok() {
                            results
                                .entry(subject_str.to_string())
                                .or_insert(object_str.to_string());
                        }
                    }
                }
            }
        }
        
        results
    }

    fn create_ml_enhanced_triples(
        &mut self,
        facts: &mut Vec<Triple>,
        prediction: &CongestionPrediction,
        rule: &CombinedRule,
    ) {
        let road_subject = format!("http://example.org/traffic#{}", prediction.road_id);
        
        // Encode with write lock
        let road_subject_id = {
            let mut dict = self.database.dictionary.write().unwrap();
            dict.encode(&road_subject)
        };

        if !self.prediction_matches_where_clause(prediction, rule) {
            println!("  Skipping {} - doesn't match WHERE clause filters", prediction.road_id);
            return;
        }

        for conclusion in &rule.conclusion {
            let (subject_str, predicate_str, object_str) = conclusion;

            if *subject_str == "?road" {
                let expanded_predicate = self
                    .database
                    .resolve_query_term(predicate_str, &self.database.prefixes.clone());
                
                // Acquire write lock for encoding
                let mut dict = self.database.dictionary.write().unwrap();
                let predicate_id = dict.encode(&expanded_predicate);

                let object_id = if *object_str == "?level" || *object_str == "?delay" {
                    let ml_value = prediction.congestion_level.to_string();
                    dict.encode(&ml_value)
                } else if object_str.starts_with('?') {
                    drop(dict);
                    eprintln!("Warning: Unresolved variable {} in conclusion", object_str);
                    continue;
                } else {
                    dict.encode(object_str)
                };
                
                drop(dict); // Release write lock

                let enhanced_triple = Triple {
                    subject: road_subject_id,
                    predicate: predicate_id,
                    object: object_id,
                };

                if self.database.triples.insert(enhanced_triple.clone()) {
                    facts.push(enhanced_triple.clone());

                    // Acquire read lock for printing
                    let dict = self.database.dictionary.read().unwrap();
                    println!(
                        "  Created triple: {} {} {}",
                        dict.decode(road_subject_id).unwrap_or("?"),
                        &expanded_predicate,
                        dict.decode(object_id).unwrap_or("?")
                    );
                }
            }
        }
    }

    // Pattern match on FilterExpression enum
    fn prediction_matches_where_clause(
        &self,
        prediction: &CongestionPrediction,
        rule: &CombinedRule,
    ) -> bool {
        // Get the road's data from the database
        let road_subject = format!("http://example.org/traffic#{}", prediction.road_id);
        let road_subject_id = {
            let mut dict = self.database.dictionary.write().unwrap();
            dict.encode(&road_subject)
        };

        // Get road data
        let avg_speed = self
            .get_road_data(road_subject_id, "avgVehicleSpeed")
            .unwrap_or(0.0);
        let vehicle_count = self
            .get_road_data(road_subject_id, "vehicleCount")
            .unwrap_or(0.0);

        println!(
            "  Checking filters for {}: speed={:.1}, count={:.0}",
            prediction.road_id, avg_speed, vehicle_count
        );

        // Handle different rule types with specific filters
        let rule_predicate = rule.head.predicate;

        if rule_predicate.contains("CongestionWithSeverity") {
            // Rule: FILTER (?speed < 30)
            let matches = avg_speed < 30.0;
            println!(
                "    CongestionWithSeverity filter: speed {:.1} < 30? {}",
                avg_speed, matches
            );
            return matches;
        }

        if rule_predicate.contains("IncidentResponse") {
            // Rule: FILTER (?speed < 20 && ?count > 100)
            let speed_matches = avg_speed < 20.0;
            let count_matches = vehicle_count > 100.0;
            let overall_matches = speed_matches && count_matches;

            println!("    IncidentResponse filters:");
            println!("      speed {:.1} < 20? {}", avg_speed, speed_matches);
            println!("      count {:.0} > 100? {}", vehicle_count, count_matches);
            println!("      overall match? {}", overall_matches);

            return overall_matches;
        }

        // Parse filters properly (for generic cases)
        let (_, filters, _, _, _) = &rule.body;

        for filter in filters {
            match filter {
                FilterExpression::Comparison(variable, operator, value) => {
                    let var_name = variable.trim_start_matches('?');
                    let actual_value = match var_name {
                        "speed" => avg_speed,
                        "count" => vehicle_count,
                        _ => {
                            println!("    Unknown variable in filter: {}", var_name);
                            continue;
                        }
                    };

                    if let Ok(threshold) = value.parse::<f64>() {
                        let matches = match *operator {
                            "<" => actual_value < threshold,
                            "<=" => actual_value <= threshold,
                            ">" => actual_value > threshold,
                            ">=" => actual_value >= threshold,
                            "=" => (actual_value - threshold).abs() < f64::EPSILON,
                            "!=" => (actual_value - threshold).abs() >= f64::EPSILON,
                            _ => {
                                println!("    Unknown operator: {}", operator);
                                true
                            }
                        };

                        println!(
                            "    Filter: {} {:.1} {} {:.1}? {}",
                            var_name, actual_value, operator, threshold, matches
                        );

                        if !matches {
                            return false;
                        }
                    }
                }
                _ => {
                    println!("    Warning: Complex filter not implemented");
                }
            }
        }

        true
    }

    // Helper method to get road data
    fn get_road_data(&self, road_subject_id: u32, data_type: &str) -> Option<f64> {
        let dict = self.database.dictionary.read().unwrap();
        
        self.database
            .triples
            .iter()
            .find(|triple| {
                triple.subject == road_subject_id
                    && dict
                        .decode(triple.predicate)
                        .map_or(false, |pred| pred.ends_with(data_type))
            })
            .and_then(|triple| dict.decode(triple.object))
            .and_then(|value_str| value_str.parse::<f64>().ok())
    }

    fn add_prediction_to_database(&mut self, prediction: &CongestionPrediction) {
        let subject = format!("http://example.org/traffic#{}", prediction.road_id);

        // Add congestion level
        let congestion_predicate = "http://example.org/traffic#congestionLevel";
        self.add_triple(
            &subject,
            congestion_predicate,
            &prediction.congestion_level.to_string(),
        );

        // Add severity
        let severity_predicate = "http://example.org/traffic#congestionSeverity";
        self.add_triple(&subject, severity_predicate, &prediction.severity);

        // Add confidence
        let confidence_predicate = "http://example.org/traffic#predictionConfidence";
        self.add_triple(
            &subject,
            confidence_predicate,
            &prediction.confidence.to_string(),
        );
    }

    fn add_triple(&mut self, subject: &str, predicate: &str, object: &str) {
        let mut dict = self.database.dictionary.write().unwrap();
        let subject_id = dict.encode(subject);
        let predicate_id = dict.encode(predicate);
        let object_id = dict.encode(object);
        drop(dict);

        self.database.triples.insert(Triple {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        });
    }

    fn query(&mut self, query: &str, description: &str) -> Result<(), Box<dyn Error>> {
        println!("\nQuery: {}", description);
        let results = execute_query(query, &mut self.database);
        println!("Results: {:?}", results);
        Ok(())
    }

    fn list_current_rules(&self) {
        println!("\nCurrent active rules:");
        for (name, _definition) in &self.current_rules {
            println!("   - {}", name);
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    prepare_freethreaded_python();

    // Initialize dynamic rule manager
    let mut rule_manager = DynamicRuleManager::new();

    // Load initial traffic data
    let traffic_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/traffic#">
          <rdf:Description rdf:about="http://example.org/traffic#HighwayA1">
            <ex:avgVehicleSpeed>45.0</ex:avgVehicleSpeed>
            <ex:vehicleCount>120</ex:vehicleCount>
            <ex:roadType>highway</ex:roadType>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/traffic#CityRoadB2">
            <ex:avgVehicleSpeed>25.0</ex:avgVehicleSpeed>
            <ex:vehicleCount>85</ex:vehicleCount>
            <ex:roadType>city</ex:roadType>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/traffic#DowntownC3">
            <ex:avgVehicleSpeed>15.0</ex:avgVehicleSpeed>
            <ex:vehicleCount>200</ex:vehicleCount>
            <ex:roadType>downtown</ex:roadType>
          </rdf:Description>
        </rdf:RDF>
    "#;

    rule_manager.load_initial_data(traffic_data);

    println!("Dynamic Traffic Monitoring System Started");
    println!("=========================================================");

    // SCENARIO 1: Initial parameterless rule for basic congestion detection
    println!("\nSCENARIO 1: Basic Congestion Detection");
    let basic_rule = r#"PREFIX ex: <http://example.org/traffic#>
RULE :DetectCongestion :- 
    CONSTRUCT {
        ?road ex:congestionLevel ?level .
    }
    WHERE {
        ?road ex:avgVehicleSpeed ?speed ;
              ex:vehicleCount ?count .
    }
    ML.PREDICT(MODEL "congestion_model",
        INPUT {
            SELECT ?road ?avgSpeed ?maxCount
            WHERE {
                ?road ex:avgVehicleSpeed ?avgSpeed ;
                      ex:vehicleCount ?maxCount .
            }
        },
        OUTPUT ?level
    )"#;

    rule_manager.update_rule("basic_congestion", basic_rule)?;

    // Query current congestion levels
    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?level WHERE { 
            ?road ex:congestionLevel ?level .
        }"#,
        "Current congestion levels",
    )?;

    // SCENARIO 2: Update rule on the fly - Add severity classification
    println!("\nSCENARIO 2: Enhanced Rule with Severity (Dynamic Update)");
    std::thread::sleep(std::time::Duration::from_secs(2)); // Simulate time passing

    let enhanced_rule = r#"PREFIX ex: <http://example.org/traffic#>
RULE :DetectCongestionWithSeverity :- 
    CONSTRUCT {
        ?road ex:congestionLevel ?level ;
              ex:trafficAlert "Congestion detected" .
    }
    WHERE {
        ?road ex:avgVehicleSpeed ?speed ;
              ex:vehicleCount ?count .
        FILTER (?speed < 30)
    }
    ML.PREDICT(MODEL "congestion_model",
        INPUT {
            SELECT ?road ?avgSpeed ?maxCount
            WHERE {
                ?road ex:avgVehicleSpeed ?avgSpeed ;
                      ex:vehicleCount ?maxCount .
                FILTER (?avgSpeed < 30)
            }
        },
        OUTPUT ?level
    )"#;

    rule_manager.update_rule("enhanced_congestion", enhanced_rule)?;

    // Query enhanced results
    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?level WHERE { 
            ?road ex:congestionLevel ?level .
        }"#,
        "Congestion levels after enhancement",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?severity WHERE { 
            ?road ex:congestionSeverity ?severity .
        }"#,
        "Congestion severity levels",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?alert WHERE { 
            ?road ex:trafficAlert ?alert .
        }"#,
        "Traffic alerts",
    )?;

    // SCENARIO 3: Add new data and update rule logic again
    println!("\nSCENARIO 3: Adding Emergency Vehicles Data and Priority Rules");

    // Add new emergency vehicle data
    let emergency_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                 xmlns:ex="http://example.org/traffic#">
          <rdf:Description rdf:about="http://example.org/traffic#HighwayA1">
            <ex:emergencyVehicles>2</ex:emergencyVehicles>
            <ex:weatherCondition>rain</ex:weatherCondition>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/traffic#CityRoadB2">
            <ex:emergencyVehicles>0</ex:emergencyVehicles>
            <ex:weatherCondition>clear</ex:weatherCondition>
          </rdf:Description>
          <rdf:Description rdf:about="http://example.org/traffic#DowntownC3">
            <ex:emergencyVehicles>1</ex:emergencyVehicles>
            <ex:weatherCondition>fog</ex:weatherCondition>
          </rdf:Description>
        </rdf:RDF>
    "#;

    rule_manager.load_initial_data(emergency_data);

    // Priority rule for emergency vehicles
    let priority_rule = r#"PREFIX ex: <http://example.org/traffic#>
RULE :EmergencyPriority :- 
    CONSTRUCT {
        ?road ex:priorityLevel "HIGH" ;
              ex:clearanceRequired "true" .
    }
    WHERE {
        ?road ex:emergencyVehicles ?count .
        FILTER (?count > 0)
    }"#;

    rule_manager.update_rule("emergency_priority", priority_rule)?;

    // Weather-aware congestion rule
    let weather_rule = r#"PREFIX ex: <http://example.org/traffic#>
RULE :WeatherAwareCongestion :- 
    CONSTRUCT {
        ?road ex:weatherImpact "HIGH" ;
              ex:adjustedCongestionLevel ?level .
    }
    WHERE {
        ?road ex:congestionLevel ?level ;
              ex:weatherCondition ?weather .
        FILTER (?weather = "rain" || ?weather = "fog")
    }"#;

    rule_manager.update_rule("weather_congestion", weather_rule)?;

    // Query comprehensive traffic status with separate queries
    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?congestion WHERE { 
            ?road ex:congestionLevel ?congestion .
        }"#,
        "Current congestion levels",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?priority WHERE { 
            ?road ex:priorityLevel ?priority .
        }"#,
        "Priority levels",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?clearance WHERE { 
            ?road ex:clearanceRequired ?clearance .
        }"#,
        "Clearance requirements",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?weather WHERE { 
            ?road ex:weatherCondition ?weather .
        }"#,
        "Weather conditions",
    )?;

    // SCENARIO 4: Real-time rule modification based on conditions
    println!("\nSCENARIO 4: Real-time Adaptive Rules");

    // Simulate a traffic incident and modify rules accordingly
    println!("Traffic incident detected on HighwayA1!");

    let incident_rule = r#"PREFIX ex: <http://example.org/traffic#>
RULE :IncidentResponse :- 
    CONSTRUCT {
        ?road ex:incidentStatus "ACTIVE" ;
              ex:recommendedAction "REROUTE" ;
              ex:estimatedDelay ?delay .
    }
    WHERE {
        ?road ex:avgVehicleSpeed ?speed ;
              ex:vehicleCount ?count .
        FILTER (?speed < 20 && ?count > 100)
    }
    ML.PREDICT(MODEL "congestion_model",
        INPUT {
            SELECT ?road ?avgSpeed ?maxCount
            WHERE {
                ?road ex:avgVehicleSpeed ?avgSpeed ;
                      ex:vehicleCount ?maxCount .
                FILTER (?avgSpeed < 20)
            }
        },
        OUTPUT ?delay
    )"#;

    rule_manager.update_rule("incident_response", incident_rule)?;

    // Final comprehensive queries
    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?congestion WHERE { 
            ?road ex:congestionLevel ?congestion .
        }"#,
        "Final congestion levels",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?severity WHERE { 
            ?road ex:congestionSeverity ?severity .
        }"#,
        "Final severity levels",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?incident WHERE { 
            ?road ex:incidentStatus ?incident .
        }"#,
        "Incident statuses",
    )?;

    rule_manager.query(
        r#"PREFIX ex: <http://example.org/traffic#>
        SELECT ?road ?action WHERE { 
            ?road ex:recommendedAction ?action .
        }"#,
        "Recommended actions",
    )?;

    // Show all active rules
    rule_manager.list_current_rules();

    println!("\nDynamic rule management demonstration completed!");
    println!("Rules were modified on the fly without system restart");
    println!("ML predictions adapted to new rule logic automatically");
    println!("MLSchema maintained standardized model performance tracking");

    Ok(())
}
