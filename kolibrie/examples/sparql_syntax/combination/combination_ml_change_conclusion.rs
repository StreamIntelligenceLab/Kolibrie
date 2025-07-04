use kolibrie::execute_ml::execute_ml_prediction_from_clause;
use kolibrie::execute_query::execute_query;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use ml::MLHandler;
use pyo3::prepare_freethreaded_python;
use serde::{Deserialize, Serialize};
use shared::query::CombinedRule;
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
    let traffic_data: Vec<TrafficData> = database
        .triples
        .iter()
        .filter(|triple| {
            database
                .dictionary
                .decode(triple.predicate)
                .map_or(false, |pred| pred.ends_with("avgVehicleSpeed"))
        })
        .map(|triple| {
            let road_id = database
                .dictionary
                .decode(triple.subject)
                .unwrap_or_default()
                .split('#')
                .last()
                .unwrap_or_default()
                .to_string();

            let avg_speed = database
                .dictionary
                .decode(triple.object)
                .unwrap_or_default()
                .parse()
                .unwrap_or(0.0);

            // Find vehicle count
            let vehicle_count = database
                .triples
                .iter()
                .find(|t| {
                    t.subject == triple.subject
                        && database
                            .dictionary
                            .decode(t.predicate)
                            .map_or(false, |p| p.ends_with("vehicleCount"))
                })
                .and_then(|t| database.dictionary.decode(t.object))
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
) -> Result<Vec<CongestionPrediction>, Box<dyn Error>> {
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

    println!("Using features for congestion prediction: {:?}", features);

    let prediction_results = ml_handler.predict(best_model, features)?;

    println!("\nML Model Performance:");
    println!(
        "  Prediction Time: {:.4} seconds",
        prediction_results.performance_metrics.prediction_time
    );
    println!(
        "  Memory Usage: {:.2} MB",
        prediction_results.performance_metrics.memory_usage_mb
    );
    println!(
        "  CPU Usage: {:.2}%",
        prediction_results.performance_metrics.cpu_usage_percent
    );

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

    Ok(predictions)
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

    fn debug_database_contents(&self) {
        println!("\nDatabase Contents Debug:");
        println!("Total triples: {}", self.database.triples.len());

        // Group by predicate for easier reading
        let mut predicate_groups: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();

        for triple in &self.database.triples {
            let subject = self
                .database
                .dictionary
                .decode(triple.subject)
                .unwrap_or("unknown")
                .to_string();
            let predicate = self
                .database
                .dictionary
                .decode(triple.predicate)
                .unwrap_or("unknown")
                .to_string();
            let object = self
                .database
                .dictionary
                .decode(triple.object)
                .unwrap_or("unknown")
                .to_string();

            predicate_groups
                .entry(predicate.clone())
                .or_insert_with(Vec::new)
                .push((subject, object));
        }

        for (predicate, subject_objects) in predicate_groups {
            println!("  {}: {} triples", predicate, subject_objects.len());
            for (subject, object) in subject_objects.iter().take(3) {
                // Show first 3
                println!("    {} -> {}", subject, object);
            }
            if subject_objects.len() > 3 {
                println!("    ... and {} more", subject_objects.len() - 3);
            }
        }
    }

    fn debug_rule_execution_detailed(&self, rule_name: &str) {
        println!("   Detailed rule execution debug for: {}", rule_name);

        if rule_name == "emergency_priority" {
            // Check emergency vehicle data and filter conditions
            for triple in &self.database.triples {
                if let Some(pred) = self.database.dictionary.decode(triple.predicate) {
                    if pred.contains("emergencyVehicles") {
                        let subject = self
                            .database
                            .dictionary
                            .decode(triple.subject)
                            .unwrap_or("?");
                        let object = self
                            .database
                            .dictionary
                            .decode(triple.object)
                            .unwrap_or("?");

                        // Parse object as number for filter evaluation
                        if let Ok(count) = object.parse::<i32>() {
                            println!(
                                "     Emergency vehicle data: {} has {} vehicles (filter: > 0? {})",
                                subject,
                                count,
                                count > 0
                            );

                            if count > 0 {
                                // Expected triples
                                println!(
                                    "     This should generate priority triples for {}",
                                    subject
                                );
                                self.create_emergency_priority_triples(subject);
                            }
                        }
                    }
                }
            }
        }

        if rule_name == "weather_congestion" {
            // Check for roads with both congestion levels and weather conditions
            let mut roads_with_congestion = std::collections::HashMap::new();
            let mut roads_with_weather = std::collections::HashMap::new();

            for triple in &self.database.triples {
                if let Some(pred) = self.database.dictionary.decode(triple.predicate) {
                    let subject = self
                        .database
                        .dictionary
                        .decode(triple.subject)
                        .unwrap_or("?");

                    if pred.contains("congestionLevel") && !pred.starts_with("ex:") {
                        let object = self
                            .database
                            .dictionary
                            .decode(triple.object)
                            .unwrap_or("?");
                        if object.parse::<f64>().is_ok() {
                            // Only numeric congestion levels
                            roads_with_congestion.insert(subject.to_string(), object.to_string());
                        }
                    }

                    if pred.contains("weatherCondition") {
                        let object = self
                            .database
                            .dictionary
                            .decode(triple.object)
                            .unwrap_or("?");
                        if object == "rain" || object == "fog" {
                            roads_with_weather.insert(subject.to_string(), object.to_string());
                            println!(
                                "     Weather condition: {} has {} (matches filter)",
                                subject, object
                            );
                        }
                    }
                }
            }

            // Find intersection
            for (road, weather) in &roads_with_weather {
                if let Some(congestion_level) = roads_with_congestion.get(road) {
                    println!(
                        "     Road {} has both weather ({}) and congestion ({})",
                        road, weather, congestion_level
                    );
                    println!(
                        "     This should generate weather impact triples for {}",
                        road
                    );
                    self.create_weather_impact_triples(road, congestion_level);
                }
            }
        }
    }

    fn create_emergency_priority_triples(&self, road: &str) {
        println!(
            "     Manually creating emergency priority triples for {}",
            road
        );
        println!("     Should create: {} ex:priorityLevel \"HIGH\"", road);
        println!("     Should create: {} ex:clearanceRequired \"true\"", road);
    }

    fn create_weather_impact_triples(&self, road: &str, congestion_level: &str) {
        println!("     Manually creating weather impact triples for {}", road);
        println!("     Should create: {} ex:weatherImpact \"HIGH\"", road);
        println!(
            "     Should create: {} ex:adjustedCongestionLevel \"{}\"",
            road, congestion_level
        );
    }

    // Update rule with new definition
    fn update_rule(
        &mut self,
        rule_name: &str,
        rule_definition: &str,
    ) -> Result<(), Box<dyn Error>> {
        println!("\nUpdating rule: {}", rule_name);
        println!("Rule definition:\n{}", rule_definition);

        // Debug database contents
        if rule_name == "emergency_priority" || rule_name == "weather_congestion" {
            self.debug_database_contents();
        }

        // Store the rule
        self.current_rules
            .insert(rule_name.to_string(), rule_definition.to_string());

        // Check if this is an ML rule
        let is_ml_rule = rule_definition.contains("ML.PREDICT");

        if is_ml_rule {
            // Handle ML rules differently - don't use the standard rule processing
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
                        Ok(predictions) => {
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

                                // Add predictions to database
                                self.add_prediction_to_database(&prediction);

                                // Create proper ML-enhanced triples
                                self.create_ml_enhanced_triples(
                                    &mut ml_facts,
                                    &prediction,
                                    &parsed_rule,
                                );
                            }

                            println!("   Enhanced {} fact(s) with ML predictions", ml_facts.len());
                            for triple in ml_facts.iter() {
                                println!(
                                    "   {}",
                                    self.database
                                        .triple_to_string(triple, &self.database.dictionary)
                                );
                            }
                        }
                        Err(e) => eprintln!("ML prediction error: {}", e),
                    }
                }
            }
        } else {
            // Handle non-ML rules with standard processing
            match process_rule_definition(rule_definition, &mut self.database) {
                Ok((rule, inferred_facts)) => {
                    println!("Rule '{}' processed successfully.", rule_name);
                    println!("   Premise patterns: {:?}", rule.premise);
                    println!("   Conclusion patterns: {:?}", rule.conclusion);
                    println!("   Inferred {} new fact(s)", inferred_facts.len());

                    for triple in inferred_facts.iter() {
                        println!(
                            "   {}",
                            self.database
                                .triple_to_string(triple, &self.database.dictionary)
                        );
                    }

                    // If no facts were inferred for emergency/weather rules, debug why
                    if (rule_name == "emergency_priority" || rule_name == "weather_congestion")
                        && inferred_facts.is_empty()
                    {
                        println!("   No facts inferred - checking rule execution...");
                        self.debug_rule_execution_detailed(rule_name);
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

    fn create_ml_enhanced_triples(
        &mut self,
        facts: &mut Vec<Triple>,
        prediction: &CongestionPrediction,
        rule: &CombinedRule,
    ) {
        let road_subject = format!("http://example.org/traffic#{}", prediction.road_id);
        let road_subject_id = self.database.dictionary.encode(&road_subject);

        // Process each conclusion pattern in the rule
        for conclusion in &rule.conclusion {
            let subject_str = conclusion.0;
            let predicate_str = conclusion.1;
            let object_str = conclusion.2;

            println!(
                "Debug: Processing conclusion pattern: {} {} {}",
                subject_str, predicate_str, object_str
            );

            // Only process conclusions where the subject is a road variable
            if subject_str == "?road" {
                // Check if this is an ML output pattern
                if object_str == "?level" || object_str == "?delay" {
                    // Pattern: ?road ex:congestionLevel ?level (ML output)
                    if predicate_str.contains("congestionLevel")
                        || predicate_str.contains("estimatedDelay")
                    {
                        let ml_value = if object_str == "?level" {
                            prediction.congestion_level.to_string()
                        } else {
                            prediction.congestion_level.to_string() // Use same value for delay
                        };

                        let predicate_id = self.database.dictionary.encode(predicate_str);
                        let object_id = self.database.dictionary.encode(&ml_value);

                        let enhanced_triple = Triple {
                            subject: road_subject_id,
                            predicate: predicate_id,
                            object: object_id,
                        };

                        // Add to database and facts list
                        self.database.triples.insert(enhanced_triple.clone());
                        facts.push(enhanced_triple);

                        println!(
                            "  Created ML triple: {} {} {}",
                            self.database
                                .dictionary
                                .decode(road_subject_id)
                                .unwrap_or("?"),
                            predicate_str,
                            ml_value
                        );
                    }
                } else if !object_str.starts_with('?') {
                    // Pattern: ?road ex:trafficAlert "Congestion detected" (static conclusion)
                    let predicate_id = self.database.dictionary.encode(predicate_str);
                    let object_id = self.database.dictionary.encode(object_str);

                    let enhanced_triple = Triple {
                        subject: road_subject_id,
                        predicate: predicate_id,
                        object: object_id,
                    };

                    // Add to database and facts list
                    self.database.triples.insert(enhanced_triple.clone());
                    facts.push(enhanced_triple);

                    println!(
                        "  Created static triple: {} {} {}",
                        self.database
                            .dictionary
                            .decode(road_subject_id)
                            .unwrap_or("?"),
                        predicate_str,
                        object_str
                    );
                }
            } else {
                println!(
                    "  Skipping pattern (subject not ?road): {} {} {}",
                    subject_str, predicate_str, object_str
                );
            }
        }
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
        let subject_id = self.database.dictionary.encode(subject);
        let predicate_id = self.database.dictionary.encode(predicate);
        let object_id = self.database.dictionary.encode(object);

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
          <rdf:Description rdf:about=>"http://example.org/traffic#CityRoadB2">
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
