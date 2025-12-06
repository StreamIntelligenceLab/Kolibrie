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
use chrono::Utc;
use std::time::{Duration, SystemTime};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use rumqttc::{Client, MqttOptions, QoS};
use std::thread;
use std::time::Instant;

// Define the MQTT message structures to match Python output
#[derive(Debug, Deserialize, Serialize, Clone)]
struct SensorData {
    id: String,
    t: u64,
    #[serde(rename = "type")]
    sensor_type: String,
    intensity: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Detection {
    #[serde(rename = "type")]
    detection_type: String,
    confidence: f64,
    bbox: Vec<f64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct CameraData {
    id: String,
    #[serde(rename = "type")]
    data_type: String,
    timestamp: u64,
    detections: Vec<Detection>,
}

// This structure will store sensor data as it arrives from MQTT
struct MqttSensorState {
    room_data: HashMap<String, f64>,
    sensors: HashMap<String, SensorData>,
    cameras: HashMap<String, CameraData>,
    last_updated: SystemTime,
    last_processed: SystemTime,
    new_data_available: bool,
}

impl MqttSensorState {
    fn new() -> Self {
        MqttSensorState {
            room_data: HashMap::new(),
            sensors: HashMap::new(),
            cameras: HashMap::new(),
            last_updated: SystemTime::now(),
            last_processed: SystemTime::now(),
            new_data_available: false,
        }
    }
    
    // Helper method to update room-level aggregated data
    fn update_aggregated_data(&mut self) {
        // Initialize with default values
        self.room_data.insert("noiseLevel".to_string(), 30.0); // Default noise level
        self.room_data.insert("lightLevel".to_string(), 70.0); // Default light level
        
        // Calculate average noise level from noise sensors
        let mut total_noise = 0.0;
        let mut noise_count = 0;
        
        for (_, sensor) in &self.sensors {
            if sensor.sensor_type == "NOISE" {
                total_noise += sensor.intensity;
                noise_count += 1;
            }
        }
        
        if noise_count > 0 {
            self.room_data.insert("noiseLevel".to_string(), total_noise / noise_count as f64 * 100.0);
        }
        
        // Calculate light level as an aggregate of PIR sensors (as an approximation)
        let mut total_light = 0.0;
        let mut light_count = 0;
        
        for (_, sensor) in &self.sensors {
            if sensor.sensor_type == "PIR" {
                total_light += sensor.intensity;
                light_count += 1;
            }
        }
        
        if light_count > 0 {
            self.room_data.insert("lightLevel".to_string(), total_light / light_count as f64 * 100.0);
        }
    }
    
    // Helper method to find the highest confidence object type from camera detections
    fn get_highest_confidence_object_type(&self) -> Option<(String, f64)> {
        let mut highest_conf_type = None;
        let mut highest_conf = 0.0;
        
        for (_, camera) in &self.cameras {
            for detection in &camera.detections {
                if detection.confidence > highest_conf {
                    highest_conf = detection.confidence;
                    highest_conf_type = Some(detection.detection_type.clone());
                }
            }
        }
        
        highest_conf_type.map(|t| (t, highest_conf))
    }
}

// Function to start MQTT subscription in a background thread
fn start_mqtt_collection(sensor_state: Arc<Mutex<MqttSensorState>>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut mqttoptions = MqttOptions::new("rust_kolibrie_client", "localhost", 1883);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        mqttoptions.set_clean_session(true);

        let (mut client, mut connection) = Client::new(mqttoptions, 10);
        
        // Subscribe to the topics that our Python script is publishing to
        client.subscribe("sensors/pir/#", QoS::AtLeastOnce).unwrap();
        client.subscribe("sensors/noise/#", QoS::AtLeastOnce).unwrap();
        client.subscribe("cameras/detections/#", QoS::AtLeastOnce).unwrap();
        
        println!("MQTT subscriptions active. Waiting for messages...");
        
        loop {
            if let Ok(notification) = connection.recv_timeout(Duration::from_millis(100)) {
                if let Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg))) = notification {
                    let topic = msg.topic.clone();
                    if let Ok(payload) = String::from_utf8(msg.payload.to_vec()) {
                        
                        // Parse the topic to determine message type
                        let topic_parts: Vec<&str> = topic.split('/').collect();
                        
                        if topic_parts.len() >= 2 {
                            let mut sensor_state = sensor_state.lock().unwrap();
                            
                            if topic_parts[0] == "sensors" {
                                if topic_parts[1] == "pir" || topic_parts[1] == "noise" {
                                    if let Ok(sensor_data) = serde_json::from_str::<SensorData>(&payload) {
                                        println!("Received {} sensor data: {}", sensor_data.sensor_type, payload);
                                        sensor_state.sensors.insert(sensor_data.id.clone(), sensor_data);
                                        sensor_state.last_updated = SystemTime::now();
                                        sensor_state.new_data_available = true;
                                    }
                                }
                            } else if topic_parts[0] == "cameras" {
                                if let Ok(camera_data) = serde_json::from_str::<CameraData>(&payload) {
                                    println!("Received camera detection data: {}", payload);
                                    sensor_state.cameras.insert(camera_data.id.clone(), camera_data);
                                    sensor_state.last_updated = SystemTime::now();
                                    sensor_state.new_data_available = true;
                                }
                            }
                            
                            // Update room-level aggregated data
                            sensor_state.update_aggregated_data();
                        }
                    }
                }
            }
            
            // Small sleep to prevent CPU spinning
            thread::sleep(Duration::from_millis(10));
        }
    })
}

// Convert MQTT data to RDF triples
fn mqtt_data_to_rdf(sensor_state: &MqttSensorState) -> String {
    let mut rdf_parts = Vec::new();
    
    // Add XML header
    rdf_parts.push(r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org#">"#.to_string());
    
    // Add room definition
    let current_datetime = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let room_light_level = sensor_state.room_data.get("lightLevel").cloned().unwrap_or(70.0);
    let room_noise_level = sensor_state.room_data.get("noiseLevel").cloned().unwrap_or(30.0);
    
    println!("Room light level: {}, noise level: {}", room_light_level, room_noise_level);
    
    rdf_parts.push(format!(r#"
          <!-- Room definition -->
          <rdf:Description rdf:about="http://example.org#VirtualRoom">
            <ex:lightLevel>{}</ex:lightLevel>
            <ex:noiseLevel>{}</ex:noiseLevel>
            <ex:gridWidth>150</ex:gridWidth>
            <ex:gridHeight>150</ex:gridHeight>
            <ex:gridUnit>cm</ex:gridUnit>
            <ex:lastUpdated>{}</ex:lastUpdated>
            <ex:lastUpdatedBy>system</ex:lastUpdatedBy>
          </rdf:Description>"#, room_light_level, room_noise_level, current_datetime));
    
    // Define a mapping from MQTT sensor IDs to grid positions and descriptions
    let sensor_positions: HashMap<&str, (u32, u32, &str)> = [
        ("sensor_nw_pir", (0, 0, "MotionSensor1")),
        ("sensor_ne_pir", (150, 0, "MotionSensor2")),
        ("sensor_sw_pir", (0, 150, "MotionSensor3")),
        ("sensor_se_pir", (150, 150, "MotionSensor4")),
        ("sensor_s_noise", (50, 150, "NoiseSensor1")),
        ("sensor_n_noise", (50, 50, "NoiseSensor2")),
        ("static_cam", (0, 0, "Camera1")),
        ("rotating_cam1", (150, 100, "Camera2")),
    ].iter().cloned().collect();
    
    // Add motion and noise sensors
    for (id, sensor) in &sensor_state.sensors {
        if let Some(&(grid_x, grid_y, description)) = sensor_positions.get(id.as_str()) {
            match sensor.sensor_type.as_str() {
                "PIR" => {
                    let detected_motion = sensor.intensity > 0.5;
                    let sensitivity = if sensor.intensity > 0.7 { "High" } else { "Medium" };
                    
                    rdf_parts.push(format!(r#"
          <!-- Motion sensor ({}) -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:type>MotionSensor</ex:type>
            <ex:sensorId>{}</ex:sensorId>
            <ex:gridX>{}</ex:gridX>
            <ex:gridY>{}</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:sensitivity>{}</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>"#, 
                    id, description, id, grid_x, grid_y, detected_motion, 
                    sensitivity, current_datetime));
                },
                "NOISE" => {
                    let detected_noise = sensor.intensity > 0.5;
                    let noise_level = (sensor.intensity * 25.0) as u32;
                    
                    rdf_parts.push(format!(r#"
          <!-- Noise sensor ({}) -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:type>NoiseSensor</ex:type>
            <ex:sensorId>{}</ex:sensorId>
            <ex:gridX>{}</ex:gridX>
            <ex:gridY>{}</ex:gridY>
            <ex:detectedNoise>{}</ex:detectedNoise>
            <ex:noiseLevel>{}</ex:noiseLevel>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>"#, 
                    id, description, id, grid_x, grid_y, detected_noise, 
                    noise_level, current_datetime));
                },
                _ => {}
            }
        }
    }
    
    // Get the highest confidence object type from detections (if any)
    let highest_conf_obj = sensor_state.get_highest_confidence_object_type();
    println!("Highest confidence object: {:?}", highest_conf_obj);
    
    // Add camera data
    for (id, camera) in &sensor_state.cameras {
        if let Some(&(grid_x, grid_y, description)) = sensor_positions.get(id.as_str()) {
            // Check if any person was detected by this camera
            let person_detected = camera.detections.iter().any(|d| d.detection_type == "person");
            
            // If it's the static camera
            if id == "static_cam" {
                rdf_parts.push(format!(r#"
          <!-- Camera in the left up corner ({}) -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:type>Camera</ex:type>
            <ex:sensorId>{}</ex:sensorId>
            <ex:gridX>{}</ex:gridX>
            <ex:gridY>{}</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:coverage>Wide</ex:coverage>
            <ex:isActive>true</ex:isActive>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>"#, 
                id, description, id, grid_x, grid_y, person_detected, current_datetime));
            } 
            // If it's the rotating camera
            else if id == "rotating_cam1" {
                // Calculate a deterministic rotation angle based on the timestamp
                let angle = (camera.timestamp % 360) as u32;
                
                rdf_parts.push(format!(r#"
          <!-- Movable camera ({}) -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:type>RotatingCamera</ex:type>
            <ex:sensorId>{}</ex:sensorId>
            <ex:gridX>{}</ex:gridX>
            <ex:gridY>{}</ex:gridY>
            <ex:detectedMotion>{}</ex:detectedMotion>
            <ex:coverage>Rotating</ex:coverage>
            <ex:currentAngle>{}</ex:currentAngle>
            <ex:isActive>true</ex:isActive>
            <ex:isMovable>true</ex:isMovable>
            <ex:lastChecked>{}</ex:lastChecked>
          </rdf:Description>"#, 
                id, description, id, grid_x, grid_y, person_detected, angle, current_datetime));
            }
            
            // Process all detections to create detection events
            for (i, detection) in camera.detections.iter().enumerate() {
                // Calculate a time for the detection based on the camera timestamp
                let detection_time = {
                    let timestamp_seconds = camera.timestamp / 1000;
                    let hour = (timestamp_seconds / 3600) % 24;
                    let minute = (timestamp_seconds / 60) % 60;
                    format!("{:02}:{:02}", hour, minute)
                };
                
                // Use the detection's object type directly
                rdf_parts.push(format!(r#"
          <!-- Detection event: {} detected -->
          <rdf:Description rdf:about="http://example.org#DetectionEvent{}">
            <ex:detectedCategory rdf:resource="http://example.org#{}"/>
            <ex:confidence>{}</ex:confidence>
            <ex:timeOfDetection>{}</ex:timeOfDetection>
            <ex:detectedBy rdf:resource="http://example.org#{}"/>
            <ex:room rdf:resource="http://example.org#VirtualRoom"/>
            <ex:recordedAt>{}</ex:recordedAt>
          </rdf:Description>"#, 
                detection.detection_type, i, 
                detection.detection_type, detection.confidence,
                detection_time, description, current_datetime));
            }
        }
    }
    
    // Define time window definitions for different categories
    let object_types = ["car", "bus", "truck", "person"];
    
    for object_type in object_types.iter() {
        // Different time slots for different object types
        let (start_time1, end_time1, start_time2, end_time2) = match *object_type {
            "car" => ("08:00", "12:00", "14:00", "18:00"),
            "bus" => ("07:00", "10:00", "15:00", "20:00"),
            "truck" => ("10:00", "14:00", "16:00", "19:00"),
            "person" => ("07:00", "22:00", "00:00", "00:00"), // Person allowed most of the day
            _ => ("09:00", "17:00", "00:00", "00:00"),  // Default for other types
        };
        
        rdf_parts.push(format!(r#"
          <!-- Category {} with allowed time windows -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:allowedTimeSlots rdf:parseType="Collection">
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
              <rdf:Description>
                <ex:startTime>{}</ex:startTime>
                <ex:endTime>{}</ex:endTime>
              </rdf:Description>
            </ex:allowedTimeSlots>
          </rdf:Description>"#, object_type, object_type, start_time1, end_time1, start_time2, end_time2));
    }
    
    // Close the RDF
    rdf_parts.push("</rdf:RDF>".to_string());
    
    // Join all parts and return
    rdf_parts.join("\n")
}

fn main() {
    println!("Starting continuous MQTT data monitoring...");
    
    // Create a shared state for our MQTT sensor data
    let sensor_state = Arc::new(Mutex::new(MqttSensorState::new()));
    
    // Start MQTT collection in background
    let _mqtt_thread = start_mqtt_collection(sensor_state.clone());
    
    // Wait a bit for initial data to arrive
    println!("Waiting for initial data...");
    thread::sleep(Duration::from_secs(10)); // Increased to 10 seconds to collect more initial data
    
    // Create database and knowledge graph
    let mut database;
    let mut kg;
    
    // Main processing loop
    let mut last_update_time = Instant::now();
    let update_interval = Duration::from_secs(10); // Process updates every 10 seconds
    
    loop {
        // Check if it's time to process updates
        if last_update_time.elapsed() >= update_interval {
            // Check if we have new data to process
            let should_process;
            
            {
                let mut state = sensor_state.lock().unwrap();
                should_process = state.new_data_available;
                if should_process {
                    state.new_data_available = false;
                    state.last_processed = SystemTime::now();
                }
            }
            
            if should_process {
                println!("\n=== Processing updated sensor data ===");
                
                // Convert the collected MQTT data to RDF/XML
                let rdf_xml_data = mqtt_data_to_rdf(&sensor_state.lock().unwrap());
                println!("Generated RDF/XML data from MQTT messages");
                
                // Create fresh database and knowledge graph for this update
                database = SparqlDatabase::new();
                kg = Reasoner::new();
                
                // Parse the RDF data into the database
                database.parse_rdf(&rdf_xml_data);
                println!("Updated RDF triples: {} triples", database.triples.len());
                
                // Load data into knowledge graph
                for triple in database.triples.iter() {
                    let subject = database.dictionary.decode(triple.subject);
                    let predicate = database.dictionary.decode(triple.predicate);
                    let object = database.dictionary.decode(triple.object);
                    kg.add_abox_triple(&subject.unwrap(), &predicate.unwrap(), &object.unwrap());
                }
                
                println!("KnowledgeGraph ABox loaded with {} triples", database.triples.len());
                
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

                // Mark all detection events as unauthorized unless they are within their allowed time windows
                let rule_mark_all_unauthorized = r#"PREFIX ex: <http://example.org#>
RULE :MarkAllEventsUnauthorized(?event) :-
    WHERE {
        ?event ex:detectedCategory ?category .
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
                    rule_mark_all_unauthorized
                ];
                
                println!("Processing {} rules:", rules.len());
                for (idx, rule) in rules.iter().enumerate() {
                    println!("Rule #{} - Parsing rule...", idx+1);
                    match parse_combined_query(rule) {
                        Ok((_rest, combined_query)) => {
                            if let Some(rule_def) = combined_query.rule {
                                println!("Rule #{} - Successfully parsed", idx+1);
                                let dynamic_rule = convert_combined_rule(rule_def, &mut database.dictionary, &combined_query.prefixes);
                                println!("Rule #{} - Adding to knowledge graph", idx+1);
                                kg.add_rule(dynamic_rule.clone());
                                
                                // Handle mapping the rule name
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
                            } else {
                                println!("Rule #{} - WARNING: No rule definition in combined query", idx+1);
                            }
                        },
                        Err(err) => {
                            println!("Rule #{} - ERROR parsing rule: {:?}", idx+1, err);
                        }
                    }
                }
                
                // Execute inference
                println!("Running inference engine...");
                let inferred_facts = kg.infer_new_facts_semi_naive();
                println!("Inferred {} new fact(s):", inferred_facts.len());
                
                for triple in inferred_facts.iter() {
                    let s = database.dictionary.decode(triple.subject).unwrap_or_default();
                    let p = database.dictionary.decode(triple.predicate).unwrap_or_default();
                    let o = database.dictionary.decode(triple.object).unwrap_or_default();
                    println!("  Inferred: {} {} {}", s, p, o);
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
                
                // Query for detection strategies
                let query_strategies = r#"PREFIX ex: <http://example.org#>
                SELECT ?room ?strategy
                WHERE {
                    ?room ex:detectionStrategy ?strategy .
                }"#;
            
                let strategy_results = execute_query(query_strategies, &mut database);
                println!("\n==> Detection strategies:");
                for row in strategy_results {
                    println!("{:?}", row);
                }
                
                // Query for fallback strategies
                let query_fallbacks = r#"PREFIX ex: <http://example.org#>
                SELECT ?room ?strategy
                WHERE {
                    ?room ex:fallbackDetectionStrategy ?strategy .
                }"#;
            
                let fallback_results = execute_query(query_fallbacks, &mut database);
                println!("\n==> Fallback strategies:");
                for row in fallback_results {
                    println!("{:?}", row);
                }
            
                // Check for active motion detection
                let query_motion = r#"PREFIX ex: <http://example.org#>
                SELECT ?sensor ?detected
                WHERE {
                    ?sensor ex:type ?type ;
                            ex:detectedMotion ?detected .
                    FILTER(?detected = "true")
                }"#;
            
                let motion_results = execute_query(query_motion, &mut database);
                println!("\n==> Active motion detection:");
                for row in motion_results {
                    println!("{:?}", row);
                }
            
                // Check for detected objects by type
                let query_detections = r#"PREFIX ex: <http://example.org#>
                SELECT ?event ?category ?confidence ?time
                WHERE {
                    ?event ex:detectedCategory ?category ;
                           ex:confidence ?confidence ;
                           ex:timeOfDetection ?time .
                }"#;
            
                let detection_results = execute_query(query_detections, &mut database);
                println!("\n==> Detected objects");
                for row in detection_results {
                    println!("{:?}", row);
                }
            
                // Check for unauthorized events
                let query_unauthorized = r#"PREFIX ex: <http://example.org#>
                SELECT ?event ?category ?time
                WHERE {
                    ?event ex:unauthorized "true" ;
                           ex:detectedCategory ?category ;
                           ex:timeOfDetection ?time .
                }"#;
            
                let unauthorized_results = execute_query(query_unauthorized, &mut database);
                println!("\n==> Unauthorized detection events:");
                for row in unauthorized_results {
                    println!("{:?}", row);
                }
                
                println!("============================================\n");
            }
            
            last_update_time = Instant::now();
        }
        
        // Sleep a bit to prevent busy-waiting
        thread::sleep(Duration::from_millis(100));
    }
}
