/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::execute_query::execute_query;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use datalog::reasoning::Reasoner;
use shared::terms::Term;
use rand::Rng;
use chrono::Utc;

/// Generates RDF/XML data with random sensor values but fixed grid positions
fn generate_rdf_xml() -> String {
    // Create random number generator
    let mut rng = rand::thread_rng();
    
    // Get current timestamp
    let now = Utc::now();
    let current_datetime = now.format("%Y-%m-%d %H:%M:%S").to_string();
    
    // Random light and noise levels for room
    let room_light_level = rng.gen_range(60..95);
    let room_noise_level = rng.gen_range(20..35);
    
    // Generate random motion detection status and other sensor attributes
    let camera1_motion = rng.gen_bool(0.7);
    let camera2_motion = rng.gen_bool(0.4);
    let motion1_detection = rng.gen_bool(0.8);
    let motion2_detection = rng.gen_bool(0.5);
    let motion3_detection = rng.gen_bool(0.7);
    let noise1_detection = rng.gen_bool(0.3);
    let noise2_detection = rng.gen_bool(0.4);
    
    // Random noise levels for noise sensors
    let noise1_level = rng.gen_range(5..20);
    let noise2_level = rng.gen_range(8..25);
    
    // Random camera attributes
    let camera1_coverage = match rng.gen_range(0..3) {
        0 => "Narrow",
        1 => "Medium",
        2 => "Wide",
        _ => "Standard"
    };
    
    let camera2_angle = rng.gen_range(0..360);
    
    // Generate random time for an event
    let random_hour = rng.gen_range(0..24);
    let random_minute = rng.gen_range(0..60);
    let random_event_time = format!("{:02}:{:02}", random_hour, random_minute);
    
    // System user identifier (anonymous)
    let system_user = "system";
    
    // Generate random time slots for allowed access
    let cat_a_slot1_start = format!("{:02}:00", rng.gen_range(7..10));
    let cat_a_slot1_end = format!("{:02}:00", rng.gen_range(11..14));
    let cat_a_slot2_start = format!("{:02}:00", rng.gen_range(13..16));
    let cat_a_slot2_end = format!("{:02}:00", rng.gen_range(17..20));
    
    let cat_b_slot1_start = format!("{:02}:00", rng.gen_range(8..11));
    let cat_b_slot1_end = format!("{:02}:00", rng.gen_range(12..15));
    let cat_b_slot2_start = format!("{:02}:00", rng.gen_range(14..17));
    let cat_b_slot2_end = format!("{:02}:00", rng.gen_range(18..21));
    
    // Shared time slot
    let shared_slot_start = "10:00";
    let shared_slot_end = "10:30";
    
    // Build the RDF/XML content
    format!(r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org#">
          
          <!-- Room definition -->
          <rdf:Description rdf:about="http://example.org#VirtualRoom">
            <ex:lightLevel>{}</ex:lightLevel>
            <ex:noiseLevel>{}</ex:noiseLevel>
            <ex:gridWidth>150</ex:gridWidth>
            <ex:gridHeight>150</ex:gridHeight>
            <ex:gridUnit>cm</ex:gridUnit>
            <ex:lastUpdated>{}</ex:lastUpdated>
            <ex:lastUpdatedBy>{}</ex:lastUpdatedBy>
          </rdf:Description>
          
          <!-- Camera in the left up corner (SensorA at 0,0) -->
          <rdf:Description rdf:about="http://example.org#Camera1">
            <ex:type>Camera</ex:type>
            <ex:sensorId>SensorA</ex:sensorId>
            <ex:gridX>0</ex:gridX>
            <ex:gridY>0</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:coverage>{}</ex:coverage>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>
          
          <!-- Movable camera (SensorG) -->
          <rdf:Description rdf:about="http://example.org#Camera2">
            <ex:type>RotatingCamera</ex:type>
            <ex:sensorId>SensorG</ex:sensorId>
            <ex:gridX>150</ex:gridX>
            <ex:gridY>100</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:coverage>Rotating</ex:coverage>
            <ex:currentAngle>{}</ex:currentAngle>
            <ex:isActive>true</ex:isActive>
            <ex:isMovable>true</ex:isMovable>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>
          
          <!-- Motion sensor (SensorC) -->
          <rdf:Description rdf:about="http://example.org#MotionSensor1">
            <ex:type>MotionSensor</ex:type>
            <ex:sensorId>SensorC</ex:sensorId>
            <ex:gridX>0</ex:gridX>
            <ex:gridY>150</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:sensitivity>{}</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>
          
          <!-- Motion sensor (SensorB) -->
          <rdf:Description rdf:about="http://example.org#MotionSensor2">
            <ex:type>MotionSensor</ex:type>
            <ex:sensorId>SensorB</ex:sensorId>
            <ex:gridX>150</ex:gridX>
            <ex:gridY>0</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:sensitivity>{}</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>
          
          <!-- Motion sensor (SensorF) -->
          <rdf:Description rdf:about="http://example.org#MotionSensor3">
            <ex:type>MotionSensor</ex:type>
            <ex:sensorId>SensorF</ex:sensorId>
            <ex:gridX>150</ex:gridX>
            <ex:gridY>150</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:sensitivity>{}</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>
          
          <!-- Noise sensor (SensorE) -->
          <rdf:Description rdf:about="http://example.org#NoiseSensor1">
            <ex:type>NoiseSensor</ex:type>
            <ex:sensorId>SensorE</ex:sensorId>
            <ex:gridX>50</ex:gridX>
            <ex:gridY>150</ex:gridY>
            <ex:detectedNoise>{}</ex:detectedNoise>
            <ex:noiseLevel>{}</ex:noiseLevel>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>
          
          <!-- Noise sensor (SensorD) -->
          <rdf:Description rdf:about="http://example.org#NoiseSensor2">
            <ex:type>NoiseSensor</ex:type>
            <ex:sensorId>SensorD</ex:sensorId>
            <ex:gridX>50</ex:gridX>
            <ex:gridY>50</ex:gridY>
            <ex:detectedNoise>{}</ex:detectedNoise>
            <ex:noiseLevel>{}</ex:noiseLevel>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>

          <!-- Category A with allowed time windows -->
          <rdf:Description rdf:about="http://example.org#CategoryA">
            <ex:allowedTimeSlots rdf:parseType="Collection">
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
              <!-- Shared time slot for both A and B -->
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
            </ex:allowedTimeSlots>
          </rdf:Description>

          <!-- Category B with allowed time windows -->
          <rdf:Description rdf:about="http://example.org#CategoryB">
            <ex:allowedTimeSlots rdf:parseType="Collection">
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
              <!-- Shared time slot for both A and B -->
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
            </ex:allowedTimeSlots>
          </rdf:Description>

          <!-- Example detection event: Category A detected -->
          <rdf:Description rdf:about="http://example.org#DetectionEvent1">
            <ex:detectedCategory rdf:resource="http://example.org#CategoryA"/>
            <ex:timeOfDetection>{}</ex:timeOfDetection>
            <!-- Link it to a sensor or a room if needed -->
            <ex:detectedBy rdf:resource="http://example.org#Camera1"/>
            <ex:room rdf:resource="http://example.org#VirtualRoom"/>
            <ex:recordedAt>{}</ex:recordedAt>
          </rdf:Description>

        </rdf:RDF>
    "#, 
    // Room attributes
    room_light_level, room_noise_level, current_datetime, system_user,
    
    // Camera1 attributes
    camera1_motion, camera1_coverage, current_datetime,
    
    // Camera2 attributes
    camera2_motion, camera2_angle, current_datetime,
    
    // MotionSensor1 attributes
    motion1_detection, 
    if rng.gen_bool(0.5) { "High" } else { "Medium" }, 
    current_datetime,
    
    // MotionSensor2 attributes
    motion2_detection, 
    if rng.gen_bool(0.5) { "High" } else { "Medium" }, 
    current_datetime,
    
    // MotionSensor3 attributes
    motion3_detection, 
    if rng.gen_bool(0.5) { "High" } else { "Medium" }, 
    current_datetime,
    
    // NoiseSensor1 attributes
    noise1_detection, noise1_level, current_datetime,
    
    // NoiseSensor2 attributes
    noise2_detection, noise2_level, current_datetime,
    
    // CategoryA time slots
    cat_a_slot1_start, cat_a_slot1_end,
    cat_a_slot2_start, cat_a_slot2_end,
    shared_slot_start, shared_slot_end,
    
    // CategoryB time slots
    cat_b_slot1_start, cat_b_slot1_end,
    cat_b_slot2_start, cat_b_slot2_end,
    shared_slot_start, shared_slot_end,
    
    // Detection event time and timestamp
    random_event_time, current_datetime
    )
}

fn main() {
    // Generate RDF/XML data
    let rdf_xml_data = generate_rdf_xml();

    // Create and populate the database
    let mut database = SparqlDatabase::new();
    database.parse_rdf(&rdf_xml_data);
    println!("Database RDF triples: {:#?}", database.triples);

    // Load data into knowledge graph
    let mut kg = Reasoner::new();
    for triple in database.triples.iter() {
        let subject = database.dictionary.decode(triple.subject);
        let predicate = database.dictionary.decode(triple.predicate);
        let object = database.dictionary.decode(triple.object);
        kg.add_abox_triple(&subject.unwrap(), &predicate.unwrap(), &object.unwrap());
    }
    println!("KnowledgeGraph ABox loaded.");

    // Rule 1: If it's quiet (noise level < 30), use noise for detection
    let rule1 = r#"PREFIX ex: <http://example.org#>
RULE :UseNoiseSensor(?room) :- 
    WHERE { 
        ?room ex:noiseLevel ?level .
        FILTER (?level < 30)
    } 
    => 
    { 
        ?room ex:detectionStrategy "NoiseBased" .
    }.
SELECT ?room 
WHERE { 
    :UseNoiseSensor(?room)  
}"#;

    // Rule 2: Universally define motion sensor for all rooms (will be superseded by noise-based if quiet)
    let rule2 = r#"PREFIX ex: <http://example.org#>
RULE :DefaultMotionSensor(?room) :- 
    WHERE { 
        ?room ex:noiseLevel ?level .
    } 
    => 
    { 
        ?room ex:fallbackDetectionStrategy "MotionBased" .
    }.
SELECT ?room 
WHERE { 
    :DefaultMotionSensor(?room)  
}"#;

    // Rule 3a: If it's not dark (light level > 50), use cameras for detection
    let rule3a = r#"PREFIX ex: <http://example.org#>
RULE :UseCameraDetection(?room) :- 
    WHERE { 
        ?room ex:lightLevel ?level .
        FILTER (?level > 50)
    } 
    => 
    { 
        ?room ex:detectionStrategy "CameraBased" .
    }.
SELECT ?room 
WHERE { 
    :UseCameraDetection(?room) 
}"#;

    // Rule 3b: If it's not dark (light level > 50), use cameras for identification
    let rule3b = r#"PREFIX ex: <http://example.org#>
RULE :UseCameraIdentification(?room) :- 
    WHERE { 
        ?room ex:lightLevel ?level .
        FILTER (?level > 50)
    } 
    => 
    { 
        ?room ex:identificationMethod "CameraIdentification" .
    }.
SELECT ?room 
WHERE { 
    :UseCameraIdentification(?room) 
}"#;

    // Mark all detection events as unauthorized:
    let rule_mark_all_unauthorized = r#"PREFIX ex: <http://example.org#>
RULE :MarkAllEventsUnauthorized(?event) :-
    WHERE {
        ?event ex:detectedCategory ?person .
    }
    =>
    {
        ?event ex:unauthorized "true" .
    }.
SELECT ?event
WHERE {
    :MarkAllEventsUnauthorized(?event)
}"#;

    // Override to authorized if event is within the allowed time slot:
    let rule_mark_authorized_if_within_schedule = r#"PREFIX ex: <http://example.org#>
RULE :MarkAllEventsUnauthorized(?event) :-
    WHERE {
        ?event ex:detectedCategory ?person .
    }
    =>
    {
        ?event ex:unauthorized "true" .
    }.
SELECT ?event
WHERE {
    :MarkAllEventsUnauthorized(?event)
}"#;

    // Execute rules
    let rules = [
        rule1, 
        rule2, 
        rule3a, 
        rule3b, 
        rule_mark_all_unauthorized,
        rule_mark_authorized_if_within_schedule
    ];
    
    for rule in rules.iter() {
        let (_rest, combined_query) = parse_combined_query(rule)
            .expect("Failed to parse combined query");
        
        if let Some(rule_def) = combined_query.rule {
            let dynamic_rule = convert_combined_rule(rule_def, &mut database.dictionary, &combined_query.prefixes);
            println!("Dynamic rule: {:#?}", dynamic_rule);
            kg.add_rule(dynamic_rule.clone());
            
            if let Some(first_conclusion) = &dynamic_rule.conclusion.first() {
                if let Term::Constant(code) = first_conclusion.1 {
                    if let Some(expanded) = database.dictionary.decode(code) {
                        if let Some(idx) = expanded.rfind('#') {
                            let local = &expanded[idx + 1..];
                            database.rule_map.insert(local.to_lowercase(), expanded.to_string());
                        }
                    }
                }
            }
        }
    }
    
    // Execute inference
    let inferred_facts = kg.infer_new_facts_semi_naive();
    println!("Inferred {} new fact(s):", inferred_facts.len());
    for triple in inferred_facts.iter() {
        println!("{}", database.triple_to_string(triple, &database.dictionary));
        database.triples.insert(triple.clone());
    }

    // Query for sensors by grid position
    let query_grid_sensors = r#"PREFIX ex: <http://example.org#>
    SELECT ?sensor ?type ?x ?y
    WHERE {
        ?sensor ex:type ?type ;
                ex:gridX ?x ;
                ex:gridY ?y .
    }"#;

    let grid_sensor_results = execute_query(query_grid_sensors, &mut database);
    println!("\n==> Sensors in grid coordinates:");
    for row in grid_sensor_results {
        println!("{:?}", row);
    }

    // Original unauthorized query
    let query_unauthorized = r#"PREFIX ex: <http://example.org#>
    SELECT ?event ?time
    WHERE {
        ?event ex:unauthorized "true" ;
                ex:timeOfDetection ?time .
    }"#;

    let unauthorized_results = execute_query(query_unauthorized, &mut database);
    println!("\n==> Unauthorized detection events:");
    for row in unauthorized_results {
        println!("{:?}", row);
    }
}
