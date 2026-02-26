/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::{Local, Timelike};
use datalog::reasoning::Reasoner;
use kolibrie::execute_query::execute_query;
use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use rumqttc::{Client, MqttOptions, QoS, RecvTimeoutError};
use serde::{Deserialize, Serialize};
use shared::terms::Term;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::time::{Duration, SystemTime};

// Add PIR sensor data structure
#[derive(Debug, Deserialize, Serialize, Clone)]
struct PirSensorData {
    id: String,
    t: u64,
    #[serde(rename = "type")]
    sensor_type: String,
    intensity: u32,
}

// Define the MQTT message structure for object detection
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
    timestamp: u64,
    #[serde(rename = "type")]
    data_type: String,
    status: String,
    detections: Vec<Detection>,
    triggered_by: Vec<String>,
}

// New struct for schedule data
#[derive(Debug, Deserialize, Serialize, Clone)]
struct ScheduleData {
    hour: u32,
    time: String,
}

// Store the detection data and state
struct SecurityState {
    cameras: HashMap<String, CameraData>,
    schedule: Option<ScheduleData>,
    last_updated: SystemTime,
    last_processed: SystemTime,
    last_alarm_sent: SystemTime,
    new_data_available: bool,
    current_alarm_state: bool,
    // Throttling parameters
    pir_sensors: HashMap<String, PirSensorData>,
    alarm_cooldown: Duration,
    pir_intensity_threshold: u32,
}

impl SecurityState {
    fn new() -> Self {
        SecurityState {
            cameras: HashMap::new(),
            schedule: None,
            last_updated: SystemTime::now(),
            last_processed: SystemTime::now(),
            last_alarm_sent: SystemTime::now(),
            new_data_available: false,
            current_alarm_state: false,
            // Set throttling cooldown to 5 seconds
            pir_sensors: HashMap::new(),
            alarm_cooldown: Duration::from_secs(5),
            pir_intensity_threshold: 1,
        }
    }

    // Check if any detection matches specified vehicle types
    fn has_vehicle_detection(&self) -> bool {
        for (_, camera) in &self.cameras {
            for detection in &camera.detections {
                let obj_type = detection.detection_type.to_lowercase();
                if obj_type == "car"
                    || obj_type == "bus"
                    || obj_type == "truck"
                    || obj_type == "train"
                {
                    return true;
                }
            }
        }
        false
    }

    // Check if any motion is detected (person object)
    fn has_motion_detection(&self) -> bool {
        for (_, camera) in &self.cameras {
            for detection in &camera.detections {
                let obj_type = detection.detection_type.to_lowercase();
                if obj_type == "person" || obj_type == "teddy bear" {
                    return true;
                }
            }
        }
        false
    }

    fn has_pir_motion(&self) -> bool {
        for (_, sensor) in &self.pir_sensors {
            if sensor.intensity >= self.pir_intensity_threshold {
                return true;
            }
        }
        false
    }

    // Check if enough time has passed to send a new alarm
    fn can_send_alarm(&self) -> bool {
        match self.last_alarm_sent.elapsed() {
            Ok(elapsed) => elapsed >= self.alarm_cooldown,
            Err(_) => true, // If there's an error getting elapsed time, allow sending
        }
    }

    // Generate an alarm message with specific field order
    fn generate_alarm_message(
        &self,
        reason: &str,
        status: &str,
        detection_types: &[&str],
    ) -> String {
        // Find the relevant detections to include in the alarm
        let mut detections_json = Vec::new();
        let mut camera_ids = Vec::new();

        for (camera_id, camera) in &self.cameras {
            for detection in &camera.detections {
                // Only include detections that match the specified types
                let obj_type = detection.detection_type.to_lowercase();
                let type_matches = detection_types.is_empty()
                    || detection_types
                        .iter()
                        .any(|t| obj_type.contains(&t.to_lowercase()));

                if type_matches {
                    // Create detection JSON with specific field order
                    let mut detection_obj = serde_json::Map::new();
                    detection_obj.insert(
                        "type".to_string(),
                        serde_json::json!(detection.detection_type),
                    );
                    detection_obj.insert(
                        "confidence".to_string(),
                        serde_json::json!(detection.confidence),
                    );
                    detection_obj.insert("bbox".to_string(), serde_json::json!(detection.bbox));

                    detections_json.push(serde_json::Value::Object(detection_obj));

                    if !camera_ids.contains(camera_id) {
                        camera_ids.push(camera_id.clone());
                    }
                }
            }
        }

        // Create the alarm message with fields in the specific order
        let mut alarm_obj = serde_json::Map::new();
        alarm_obj.insert(
            "timestamp".to_string(),
            serde_json::json!(chrono::Utc::now().timestamp_millis()),
        );
        alarm_obj.insert("status".to_string(), serde_json::json!(status));
        alarm_obj.insert("reason".to_string(), serde_json::json!(reason));
        alarm_obj.insert("detections".to_string(), serde_json::json!(detections_json));
        alarm_obj.insert("camera_ids".to_string(), serde_json::json!(camera_ids));

        // Convert to JSON string with the ordered fields
        serde_json::Value::Object(alarm_obj).to_string()
    }
}

// Start MQTT subscription in a background thread, subscribe to PIR topics
fn start_mqtt_collection(
    security_state: Arc<Mutex<SecurityState>>,
    mqtt_client: Arc<Mutex<Option<Client>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mqtt_broker = "YOUR_IP_ADDRESS"; // Use the specified broker
        let mqtt_port = 1883;
        let client_id = "rust_security_client";

        println!(
            "Connecting to MQTT broker at {}:{}...",
            mqtt_broker, mqtt_port
        );

        let mut mqttoptions = MqttOptions::new(client_id, mqtt_broker, mqtt_port);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        mqttoptions.set_clean_session(true);

        let mut last_dummy_update = Instant::now();
        let dummy_update_interval = Duration::from_secs(10);

        loop {
            // Create a new client connection without using catch_unwind
            let client_connection_result = Client::new(mqttoptions.clone(), 10);

            // Normal Client::new returns a tuple, not a Result, so we're just proceeding with it
            let (mut client, mut connection) = client_connection_result;

            // Store client reference to allow publishing from main thread
            *mqtt_client.lock().unwrap() = Some(client.clone());

            // Subscribe to both camera detection topics, schedule topic, and PIR sensor topics
            let subscription_result_cam0 =
                client.subscribe("camera/detections/0", QoS::AtLeastOnce);
            let subscription_result_cam1 =
                client.subscribe("camera/detections/1", QoS::AtLeastOnce);
            let subscription_result_schedule = client.subscribe("schedule", QoS::AtLeastOnce);
            
            // Subscribe to PIR sensor topics
            let subscription_result_pir_nw = 
                client.subscribe("sensors/pir/sensor_nw_pir", QoS::AtLeastOnce);
            let subscription_result_pir_sw = 
                client.subscribe("sensors/pir/sensor_sw_pir", QoS::AtLeastOnce);
            let subscription_result_pir_ne = 
                client.subscribe("sensors/pir/sensor_ne_pir", QoS::AtLeastOnce);
            let subscription_result_pir_se = 
                client.subscribe("sensors/pir/sensor_se_pir", QoS::AtLeastOnce);

            let subscriptions_ok = subscription_result_cam0.is_ok()
                && subscription_result_cam1.is_ok()
                && subscription_result_schedule.is_ok()
                && subscription_result_pir_nw.is_ok()
                && subscription_result_pir_sw.is_ok()
                && subscription_result_pir_ne.is_ok()
                && subscription_result_pir_se.is_ok();

            if subscriptions_ok {
                println!("Connected to MQTT broker and subscribed to required topics");

                // Process incoming messages
                let mut connection_active = true;
                while connection_active {
                    match connection.recv_timeout(Duration::from_millis(100)) {
                        Ok(notification) => {
                            if let Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(msg))) =
                                notification
                            {
                                let topic = msg.topic.clone();
                                if let Ok(payload) = String::from_utf8(msg.payload.to_vec()) {
                                    println!("Received message on topic {}: {}", topic, payload);

                                    if topic == "camera/detections/0"
                                        || topic == "camera/detections/1"
                                    {
                                        if let Ok(camera_data) =
                                            serde_json::from_str::<CameraData>(&payload)
                                        {
                                            println!(
                                                "Parsed camera detection data from {}: {:?}",
                                                topic, camera_data
                                            );

                                            let mut state = security_state.lock().unwrap();

                                            // Use the camera ID from the data or topic if empty
                                            let camera_id = if camera_data.id.is_empty() {
                                                if topic == "camera/detections/0" {
                                                    "camera_0".to_string()
                                                } else {
                                                    "camera_1".to_string()
                                                }
                                            } else {
                                                camera_data.id.clone()
                                            };

                                            // Track which camera the data came from
                                            let mut camera_data = camera_data.clone();
                                            camera_data.id = camera_id.clone();

                                            state.cameras.insert(camera_id, camera_data);
                                            state.last_updated = SystemTime::now();
                                            state.new_data_available = true;
                                        } else {
                                            println!(
                                                "Failed to parse camera data JSON from {}: {}",
                                                topic, payload
                                            );
                                        }
                                    } else if topic == "schedule" {
                                        if let Ok(schedule_data) =
                                            serde_json::from_str::<ScheduleData>(&payload)
                                        {
                                            println!("Parsed schedule data: {:?}", schedule_data);

                                            let mut state = security_state.lock().unwrap();
                                            state.schedule = Some(schedule_data);
                                            state.last_updated = SystemTime::now();
                                            state.new_data_available = true;
                                        } else {
                                            println!(
                                                "Failed to parse schedule data JSON: {}",
                                                payload
                                            );
                                        }
                                    } else if topic.starts_with("sensors/pir/") {
                                        // Handle PIR sensor data
                                        if let Ok(pir_data) =
                                            serde_json::from_str::<PirSensorData>(&payload)
                                        {
                                            println!("Parsed PIR sensor data: {:?}", pir_data);

                                            let mut state = security_state.lock().unwrap();
                                            state.pir_sensors.insert(pir_data.id.clone(), pir_data);
                                            state.last_updated = SystemTime::now();
                                            state.new_data_available = true;
                                        } else {
                                            println!(
                                                "Failed to parse PIR sensor data JSON from {}: {}",
                                                topic, payload
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // Only show timeout errors once per minute to reduce log spam
                            if let RecvTimeoutError::Timeout = e {
                                // Timeout is normal, just continue
                                continue;
                            } else {
                                println!("MQTT connection error: {:?}", e);
                                connection_active = false;
                            }
                        }
                    }

                    // Small sleep to prevent CPU spinning
                    thread::sleep(Duration::from_millis(10));
                }
            } else {
                println!("Failed to subscribe to one or more topics:");
                if let Err(e) = &subscription_result_cam0 {
                    println!("  - camera/detections/0: {:?}", e);
                }
                if let Err(e) = &subscription_result_cam1 {
                    println!("  - camera/detections/1: {:?}", e);
                }
                if let Err(e) = &subscription_result_schedule {
                    println!("  - schedule: {:?}", e);
                }
                if let Err(e) = &subscription_result_pir_nw {
                    println!("  - sensors/pir/sensor_nw_pir: {:?}", e);
                }

                // During broker connectivity issues, insert dummy data for testing
                if last_dummy_update.elapsed() > dummy_update_interval {
                    println!("Adding test data for development (broker unavailable)");

                    // Use the correct format for the dummy data
                    let dummy_camera_data = CameraData {
                        id: "static_cam".to_string(),
                        timestamp: chrono::Utc::now().timestamp_millis() as u64,
                        data_type: "detections".to_string(),
                        status: "active".to_string(),
                        detections: vec![
                            Detection {
                                detection_type: "bus".to_string(),
                                confidence: 0.61,
                                bbox: vec![0.2765625, 0.4354166666666667, 0.5015625, 0.63125],
                            },
                            Detection {
                                detection_type: "truck".to_string(),
                                confidence: 0.42,
                                bbox: vec![0.275, 0.4375, 0.5015625, 0.6291666666666667],
                            },
                        ],
                        triggered_by: vec![],
                    };
                    
                    // Add dummy PIR sensor data
                    let dummy_pir_data = PirSensorData {
                        id: "sensor_nw_pir".to_string(),
                        t: chrono::Utc::now().timestamp_millis() as u64,
                        sensor_type: "PIR".to_string(),
                        intensity: 2, // Example intensity value
                    };

                    let dummy_schedule_data = ScheduleData {
                        hour: 19,
                        time: "evening".to_string(),
                    };

                    let mut state = security_state.lock().unwrap();
                    state
                        .cameras
                        .insert(dummy_camera_data.id.clone(), dummy_camera_data);
                    state.pir_sensors.insert(dummy_pir_data.id.clone(), dummy_pir_data);
                    state.schedule = Some(dummy_schedule_data);
                    state.last_updated = SystemTime::now();
                    state.new_data_available = true;

                    last_dummy_update = Instant::now();
                }
            }

            // Wait before reconnection attempt
            println!("MQTT connection lost or failed. Reconnecting in 5 seconds...");
            thread::sleep(Duration::from_secs(5));
        }
    })
}

// Send an MQTT message with the given topic and payload
fn send_mqtt_message(mqtt_client: &Arc<Mutex<Option<Client>>>, topic: &str, payload: &str) -> bool {
    if let Some(client) = &mut *mqtt_client.lock().unwrap() {
        match client.publish(topic, QoS::AtLeastOnce, false, payload.as_bytes()) {
            Ok(_) => {
                println!("Published message to {}: {}", topic, payload);
                true
            }
            Err(e) => {
                println!("Failed to publish message: {:?}", e);
                false
            }
        }
    } else {
        println!("Cannot publish message: MQTT client not connected");
        false
    }
}

// Convert detection data to RDF triples
fn detections_to_rdf(security_state: &SecurityState, test_time: Option<&str>) -> String {
    let mut rdf_parts = Vec::new();

    // Add XML header
    rdf_parts.push(
        r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org#">"#
            .to_string(),
    );

    // Get current time or use the test time if provided
    let (current_time_str, hour, _minute, date_str) = if let Some(test_time) = test_time {
        // Parse the test time in format HH:MM
        let parts: Vec<&str> = test_time.split(':').collect();
        if parts.len() == 2 {
            let hour: u32 = parts[0].parse().unwrap_or(0);
            let minute: u32 = parts[1].parse().unwrap_or(0);
            (
                test_time.to_string(),
                hour,
                minute,
                Local::now().format("%Y-%m-%d").to_string(),
            )
        } else {
            // Fallback to current time if test_time is invalid
            let now = Local::now();
            (
                format!("{:02}:{:02}", now.hour(), now.minute()),
                now.hour(),
                now.minute(),
                now.format("%Y-%m-%d").to_string(),
            )
        }
    } else if let Some(schedule) = &security_state.schedule {
        // Use schedule time if available
        let hour = schedule.hour;
        let minute = 0; // Schedule doesn't provide minutes, so default to 0
        (
            format!("{:02}:{:02}", hour, minute),
            hour,
            minute,
            Local::now().format("%Y-%m-%d").to_string(),
        )
    } else {
        // Use current local time
        let now = Local::now();
        (
            format!("{:02}:{:02}", now.hour(), now.minute()),
            now.hour(),
            now.minute(),
            now.format("%Y-%m-%d").to_string(),
        )
    };

    // Add environment definition with custom time
    rdf_parts.push(format!(
        r#"
          <!-- Environment definition -->
          <rdf:Description rdf:about="http://example.org#SecurityEnvironment">
            <ex:currentTime>{}</ex:currentTime>
            <ex:currentHour>{}</ex:currentHour>
            <ex:currentDate>{}</ex:currentDate>
            <ex:systemActive>true</ex:systemActive>
          </rdf:Description>"#,
        current_time_str, hour, date_str
    ));

    // Add schedule information if available
    if let Some(schedule) = &security_state.schedule {
        rdf_parts.push(format!(
            r#"
          <!-- Schedule information -->
          <rdf:Description rdf:about="http://example.org#CurrentSchedule">
            <ex:hour>{}</ex:hour>
            <ex:timeOfDay>{}</ex:timeOfDay>
          </rdf:Description>"#,
            schedule.hour, schedule.time
        ));
    }

    // Add security rule time windows
    rdf_parts.push(
        r#"
          <!-- Security Rules: Time Windows -->
          <rdf:Description rdf:about="http://example.org#NightTimeRule">
            <ex:startTime>22:00</ex:startTime>
            <ex:endTime>05:00</ex:endTime>
            <ex:description>Nobody allowed between 10pm and 5am</ex:description>
            <ex:requireSensor>motion</ex:requireSensor>
          </rdf:Description>
          
          <rdf:Description rdf:about="http://example.org#VehicleRestrictionRule">
            <ex:startTime>16:00</ex:startTime>
            <ex:endTime>10:00</ex:endTime>
            <ex:description>No vehicles allowed between 4pm and 10am</ex:description>
            <ex:restrictedObjects>car</ex:restrictedObjects>
            <ex:restrictedObjects>bus</ex:restrictedObjects>
            <ex:restrictedObjects>truck</ex:restrictedObjects>
          </rdf:Description>
          
          <rdf:Description rdf:about="http://example.org#DaytimeRule">
            <ex:startTime>05:00</ex:startTime>
            <ex:endTime>16:00</ex:endTime>
            <ex:description>Everyone allowed between 5am and 4pm</ex:description>
          </rdf:Description>"#
            .to_string(),
    );

    // Process all camera data and detections
    for (camera_id, camera_data) in &security_state.cameras {
        // Add camera definition
        rdf_parts.push(format!(
            r#"
          <!-- Camera definition -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:type>Camera</ex:type>
            <ex:status>{}</ex:status>
            <ex:lastUpdateTimestamp>{}</ex:lastUpdateTimestamp>
          </rdf:Description>"#,
            camera_id, camera_data.status, camera_data.timestamp
        ));

        // Add each detection as a separate event
        for (i, detection) in camera_data.detections.iter().enumerate() {
            let detection_id = format!("Detection_{}_{}", camera_id, i);

            // Convert timestamp to time format for rule comparison
            let detection_time = {
                let timestamp_seconds = camera_data.timestamp / 1000;
                let hour = (timestamp_seconds / 3600) % 24;
                let minute = (timestamp_seconds / 60) % 60;
                format!("{:02}:{:02}", hour, minute)
            };

            rdf_parts.push(format!(
                r#"
          <!-- Detection Event -->
          <rdf:Description rdf:about="http://example.org#{}">
            <ex:camera rdf:resource="http://example.org#{}"/>
            <ex:objectType>{}</ex:objectType>
            <ex:confidence>{}</ex:confidence>
            <ex:timeOfDay>{}</ex:timeOfDay>
            <ex:timestamp>{}</ex:timestamp>
          </rdf:Description>"#,
                detection_id,
                camera_id,
                detection.detection_type,
                detection.confidence,
                detection_time,
                camera_data.timestamp
            ));
        }
    }

    // Close the RDF
    rdf_parts.push("</rdf:RDF>".to_string());

    // Join all parts and return
    rdf_parts.join("\n")
}

// Check if a time is within a specified range (handles overnight ranges)
fn time_in_range(time: &str, start: &str, end: &str) -> bool {
    let time_parts: Vec<&str> = time.split(':').collect();
    let start_parts: Vec<&str> = start.split(':').collect();
    let end_parts: Vec<&str> = end.split(':').collect();

    if time_parts.len() != 2 || start_parts.len() != 2 || end_parts.len() != 2 {
        return false;
    }

    let time_hour: u32 = time_parts[0].parse().unwrap_or(0);
    let time_minute: u32 = time_parts[1].parse().unwrap_or(0);
    let time_minutes = time_hour * 60 + time_minute;

    let start_hour: u32 = start_parts[0].parse().unwrap_or(0);
    let start_minute: u32 = start_parts[1].parse().unwrap_or(0);
    let start_minutes = start_hour * 60 + start_minute;

    let end_hour: u32 = end_parts[0].parse().unwrap_or(0);
    let end_minute: u32 = end_parts[1].parse().unwrap_or(0);
    let end_minutes = end_hour * 60 + end_minute;

    // Handle overnight ranges (end time less than start time)
    if end_minutes < start_minutes {
        // Time is in range if it's after start OR before end
        time_minutes >= start_minutes || time_minutes <= end_minutes
    } else {
        // Normal range check
        time_minutes >= start_minutes && time_minutes <= end_minutes
    }
}

fn main() {
    println!("Starting security monitoring system with MQTT object detection...");

    // Create a shared state for our security data
    let security_state = Arc::new(Mutex::new(SecurityState::new()));

    // Create a shared MQTT client for sending messages
    let mqtt_client = Arc::new(Mutex::new(None::<Client>));

    // Set your test time here (in 24-hour format)
    let test_time: Option<String> = None; // Default to using schedule time instead of test time

    // Start MQTT collection in background
    let _mqtt_thread = start_mqtt_collection(security_state.clone(), mqtt_client.clone());

    println!("Waiting for detection data...");

    // Create database and knowledge graph
    let mut database;
    let mut kg;

    // Main processing loop
    let mut last_update_time = Instant::now();
    let update_interval = Duration::from_secs(2); // Process updates every 2 seconds

    loop {
        // Check if it's time to process updates
        if last_update_time.elapsed() >= update_interval {
            // Check if we have new data to process
            let should_process;

            {
                let mut state = security_state.lock().unwrap();
                should_process = state.new_data_available;
                if should_process {
                    state.new_data_available = false;
                    state.last_processed = SystemTime::now();
                }
            }

            if should_process {
                println!("\n=== Processing updated detection data ===");

                // Get current time from schedule or system
                let current_time_str;
                let is_morning;
                let is_night_time;
                let is_afternoon;
                let is_evening;
                let time_of_day;
                {
                    let state = security_state.lock().unwrap();
                    if let Some(schedule) = &state.schedule {
                        // Use time from schedule
                        current_time_str = format!("{:02}:00", schedule.hour);
                        time_of_day = schedule.time.clone();
                        println!(
                            "Using schedule time: {} ({})",
                            current_time_str, schedule.time
                        );
                    } else if let Some(ref time) = test_time {
                        // Use test time
                        current_time_str = time.to_string();
                        time_of_day = "unknown".to_string();
                        println!("Using test time: {}", current_time_str);
                    } else {
                        // Use current system time
                        let now = Local::now();
                        current_time_str = format!("{:02}:{:02}", now.hour(), now.minute());
                        time_of_day = if now.hour() >= 5 && now.hour() < 10 {
                            "morning".to_string()
                        } else if now.hour() >= 10 && now.hour() < 16 {
                            "afternoon".to_string()
                        } else if now.hour() >= 16 && now.hour() < 22 {
                            "evening".to_string()
                        } else {
                            "night".to_string()
                        };
                        println!("Using current system time: {}", current_time_str);
                    }
                    
                    // Determine the time periods
                    is_morning = time_in_range(&current_time_str, "05:00", "10:00");
                    is_afternoon = time_in_range(&current_time_str, "10:00", "16:00");
                    is_evening = time_in_range(&current_time_str, "16:00", "22:00");
                    is_night_time = time_in_range(&current_time_str, "22:00", "05:00");
                }

                // Convert the collected detection data to RDF/XML - pass the schedule time indirectly
                let rdf_xml_data = detections_to_rdf(&security_state.lock().unwrap(), None);

                // Create fresh database and knowledge graph for this update
                database = SparqlDatabase::new();
                kg = Reasoner::new();

                // Parse the RDF data into the database
                database.parse_rdf(&rdf_xml_data);
                println!("Database loaded with {} triples", database.triples.len());

                // FIXED: Load data into knowledge graph with proper lock handling
                // Collect triples first to avoid holding lock
                let triples_to_add: Vec<_> = database.triples.iter().cloned().collect();
                
                for triple in triples_to_add {
                    let dict = database.dictionary.read().unwrap();
                    let subject = dict.decode(triple.subject).map(|s| s.to_string());
                    let predicate = dict.decode(triple.predicate).map(|p| p.to_string());
                    let object = dict.decode(triple.object).map(|o| o.to_string());
                    drop(dict); // Release lock before calling add_abox_triple
                    
                    if let (Some(s), Some(p), Some(o)) = (subject, predicate, object) {
                        kg.add_abox_triple(&s, &p, &o);
                    }
                }

                println!("KnowledgeGraph loaded");

                // Define rule templates
                let rule_night_motion_template = r#"PREFIX ex: <http://example.org#>
RULE :UnauthorizedMotion(?detection) :- 
    WHERE { 
        ?env ex:currentTime ?time .
        ?detection ex:objectType ?type ;
                   ex:timeOfDay ?detTime .
        ?rule ex:startTime "22:00" ;
              ex:endTime "05:00" .
        FILTER(?type = "person" || ?type = "teddy bear")
    }
    => 
    { 
        ?detection ex:unauthorized "true" .
    }.
SELECT ?detection
WHERE { 
    :UnauthorizedMotion(?detection)
}"#;

                let rule_vehicle_restriction_template = r#"PREFIX ex: <http://example.org#>
RULE :UnauthorizedVehicle(?detection) :- 
    WHERE { 
        ?env ex:currentTime ?time .
        ?detection ex:objectType ?type ;
                   ex:timeOfDay ?detTime .
        ?rule ex:startTime "16:00" ;
              ex:endTime "10:00" .
        FILTER(?type = "truck" || ?type = "bus" || ?type = "car" || ?type = "train")
    }
    => 
    { 
        ?detection ex:unauthorized "true" .
    }.
SELECT ?detection
WHERE { 
    :UnauthorizedVehicle(?detection)
}"#;

                let rule_daytime_allowed_template = r#"PREFIX ex: <http://example.org#>
RULE :AuthorizedDaytime(?detection) :- 
    WHERE { 
        ?env ex:currentTime ?time .
        ?detection ex:objectType ?type ;
                   ex:timeOfDay ?detTime .
        ?rule ex:startTime "05:00" ;
              ex:endTime "16:00" .
    }
    => 
    { 
        ?detection ex:authorized "true" .
    }.
SELECT ?detection 
WHERE { 
    :AuthorizedDaytime(?detection)
}"#;

                // Only apply rules based on the current time to avoid conflicts
                let mut active_rules = Vec::new();

                // Check which time-based rules should be active
                if is_night_time {
                    // Night time - apply night motion restriction
                    println!("Time ({}): Night hours - applying night motion rules", current_time_str);
                    active_rules.push(rule_night_motion_template);
                } else if is_morning || is_afternoon {
                    // Daytime - apply daytime allowed rule
                    println!("Time ({}): Daytime hours - applying daytime allowed rules", current_time_str);
                    active_rules.push(rule_daytime_allowed_template);
                    
                    // Only apply vehicle restrictions from 16:00 to 10:00, but NOT during morning hours (after 5am)
                    if is_morning {
                        // Morning hours (5am-10am): Special handling
                        let state = security_state.lock().unwrap();
                        if let Some(schedule) = &state.schedule {
                            // If the schedule explicitly says "morning", do NOT apply vehicle restrictions
                            if schedule.time == "morning" {
                                println!("Morning schedule detected: Vehicle restrictions DISABLED");
                                // Do not add vehicle restriction rule
                            } else {
                                println!("Time ({}): Early morning hours - applying vehicle rules", current_time_str);
                                active_rules.push(rule_vehicle_restriction_template);
                            }
                        } else {
                            // No schedule info available, use standard logic
                            println!("Time ({}): Early morning hours - applying vehicle rules", current_time_str);
                            active_rules.push(rule_vehicle_restriction_template);
                        }
                    }
                } else if is_evening {
                    // Evening hours (16:00-22:00) - apply vehicle restrictions
                    println!("Time ({}): Evening hours - applying vehicle rules", current_time_str);
                    active_rules.push(rule_vehicle_restriction_template);
                }

                // Process only the active rules
                println!("Processing {} active security rules...", active_rules.len());
                for (idx, rule) in active_rules.iter().enumerate() {
                    match parse_combined_query(rule) {
                        Ok((_rest, combined_query)) => {
                            if let Some(rule) = combined_query.rule.clone() {
                                // FIXED: Acquire write lock for conversion
                                let mut dict = database.dictionary.write().unwrap();
                                let dynamic_rule = convert_combined_rule(
                                    rule.clone(),
                                    &mut dict,
                                    &combined_query.prefixes,
                                );
                                drop(dict); // Release lock
                                
                                println!("Dynamic rule #{}: {:#?}", idx + 1, dynamic_rule);
                                kg.add_rule(dynamic_rule.clone());
                                println!("Rule #{} added to KnowledgeGraph.", idx + 1);

                                // FIXED: Handle mapping the rule name with proper lock
                                if let Some(first_conclusion) = dynamic_rule.conclusion.first() {
                                    if let Term::Constant(code) = first_conclusion.1 {
                                        let dict = database.dictionary.read().unwrap();
                                        if let Some(expanded) = dict.decode(code) {
                                            let expanded = expanded.to_string();
                                            drop(dict); // Release lock
                                            
                                            if let Some(idx) = expanded.rfind('#') {
                                                let local = &expanded[idx + 1..];
                                                database.rule_map.insert(
                                                    local.to_lowercase(),
                                                    expanded.clone(),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            println!("Rule #{} - ERROR parsing rule: {:?}", idx + 1, err);
                        }
                    }
                }

                // Execute inference
                println!("Running inference engine...");
                let inferred_facts = kg.infer_new_facts_semi_naive();
                println!("Inferred {} new fact(s)", inferred_facts.len());

                // FIXED: Proper lock handling for decoding inferred facts
                let dict = database.dictionary.read().unwrap();
                for triple in inferred_facts.iter() {
                    let s = dict.decode(triple.subject).unwrap_or_default();
                    let p = dict.decode(triple.predicate).unwrap_or_default();
                    let o = dict.decode(triple.object).unwrap_or_default();
                    println!("  Inferred: {} {} {}", s, p, o);
                }
                drop(dict); // Release lock before modifying database

                // Add inferred facts to database
                for triple in inferred_facts.iter() {
                    database.triples.insert(triple.clone());
                }

                // Direct time-based checks for detections
                let mut alarm_reason = String::new();
                let mut alarm_needed = false;
                let mut status = "0"; // Default to authorized
                let mut detection_types: Vec<&str> = Vec::new();

                {
                    let mut state = security_state.lock().unwrap();
                    
                    // Use PIR for morning, afternoon, and night (but NOT evening)
                    let use_pir_sensors = is_morning || is_afternoon || is_night_time || 
                                         time_of_day == "morning" || time_of_day == "afternoon" || time_of_day == "night";
                    
                    println!("Current time period: {}", time_of_day);
                    println!("Using PIR sensors: {}", use_pir_sensors);
                    
                    if use_pir_sensors {
                        // Get PIR detection results
                        if state.has_pir_motion() {
                            // Create appropriate message based on time of day
                            if is_night_time || time_of_day == "night" {
                                alarm_reason = "PIR motion detected during night hours (10pm-5am)".to_string();
                                status = "1"; // Unauthorized during night
                            } else if is_morning || time_of_day == "morning" {
                                alarm_reason = "PIR motion detected during morning hours (5am-10am)".to_string();
                                status = "0"; // Authorized during morning
                            } else if is_afternoon || time_of_day == "afternoon" {
                                alarm_reason = "PIR motion detected during afternoon hours (10am-4pm)".to_string();
                                status = "0"; // Authorized during afternoon
                            } else {
                                alarm_reason = "PIR motion detected".to_string();
                                status = "0"; // Default to authorized
                            }
                            
                            alarm_needed = true;
                            detection_types.push("pir_motion");
                            
                            println!("PIR motion detected with reason: {}", alarm_reason);
                        }
                    } 
                    // Use object detection only for EVENING
                    else if is_evening || time_of_day == "evening" {
                        println!("Using camera object detection for evening hours");
                        
                        // Check for night time motion detection (10pm to 5am)
                        if is_night_time && state.has_motion_detection() {
                            alarm_reason = "Motion detected during restricted hours (10pm-5am)".to_string();
                            alarm_needed = true;
                            status = "1"; // Unauthorized
                            detection_types.push("person");
                            detection_types.push("teddy bear");
                        }
                        // Check for vehicle detection between 4pm and 5am
                        else if time_in_range(&current_time_str, "16:00", "05:00") && state.has_vehicle_detection() {
                            alarm_reason = "Vehicle detected during restricted hours (4pm-5am)".to_string();
                            alarm_needed = true;
                            status = "1"; // Unauthorized
                            detection_types.extend(&["car", "bus", "truck", "train"]);
                        }
                        // Special handling for morning hours (5am-10am)
                        else if is_morning {
                            if let Some(schedule) = &state.schedule {
                                // If it's explicitly "morning" according to schedule, allow vehicles
                                if schedule.time == "morning" {
                                    if state.has_motion_detection() || state.has_vehicle_detection() {
                                        alarm_reason = "Detection during morning hours (5am-10am)".to_string();
                                        alarm_needed = true;
                                        status = "0"; // Authorized
                                        detection_types = Vec::new(); // Include all detections
                                    }
                                } 
                                // Otherwise apply vehicle restrictions
                                else if state.has_vehicle_detection() {
                                    alarm_reason = "Vehicle detected during early morning restricted hours (5am-10am)".to_string();
                                    alarm_needed = true;
                                    status = "1"; // Unauthorized
                                    detection_types.extend(&["car", "bus", "truck", "train"]);
                                }
                                else if state.has_motion_detection() {
                                    alarm_reason = "Motion detected during allowed morning hours".to_string();
                                    alarm_needed = true;
                                    status = "0"; // Authorized
                                    detection_types.push("person");
                                    detection_types.push("teddy bear");
                                }
                            }
                            // No schedule available - use standard rules
                            else if state.has_vehicle_detection() {
                                alarm_reason = "Vehicle detected during early morning restricted hours (5am-10am)".to_string();
                                alarm_needed = true;
                                status = "1"; // Unauthorized
                                detection_types.extend(&["car", "bus", "truck", "train"]);
                            }
                            else if state.has_motion_detection() {
                                alarm_reason = "Motion detected during allowed morning hours".to_string();
                                alarm_needed = true;
                                status = "0"; // Authorized
                                detection_types.push("person");
                                detection_types.push("teddy bear");
                            }
                        }
                        // Daytime allowed detections (10am to 4pm)
                        else if is_afternoon && (state.has_motion_detection() || state.has_vehicle_detection()) {
                            alarm_reason = "Detection during allowed daytime hours (10am-4pm)".to_string();
                            alarm_needed = true;
                            status = "0"; // Authorized
                            detection_types = Vec::new(); // Include all detections
                        }
                        // Evening detection (16:00-22:00) - handle as original code
                        else if is_evening {
                            // Check for person detection
                            if state.has_motion_detection() {
                                alarm_reason = "Person detected during evening hours (4pm-10pm)".to_string();
                                alarm_needed = true;
                                status = "0"; // Authorized
                                detection_types.push("person");
                                detection_types.push("teddy bear");
                            }
                            // Check for vehicle detection (restricted)
                            else if state.has_vehicle_detection() {
                                alarm_reason = "Vehicle detected during restricted evening hours (4pm-10pm)".to_string();
                                alarm_needed = true;
                                status = "1"; // Unauthorized
                                detection_types.extend(&["car", "bus", "truck", "train"]);
                            }
                        }
                    } else {
                        println!("No active detection sources available - no PIR or camera detection active");
                    }
                    
                    // Update alarm state
                    state.current_alarm_state = alarm_needed && status == "1"; // Only set alarm state for unauthorized
                    
                    // Check if we can send an alarm message (throttling)
                    if alarm_needed && state.can_send_alarm() {
                        if status == "1" {
                            println!("\n🚨🚨 ALARM TRIGGERED 🚨🚨\n");
                        } else {
                            println!("\n✅ AUTHORIZED DETECTION ✅\n");
                        }
                        
                        // Generate the alarm message
                        let alarm_message = state.generate_alarm_message(&alarm_reason, status, &detection_types);
                        println!("Sending message to alarm topic: {}", alarm_message);
                        
                        // Send the message via MQTT to the alarm topic
                        if send_mqtt_message(&mqtt_client, "alarm", &alarm_message) {
                            // Update the last alarm sent time for throttling
                            state.last_alarm_sent = SystemTime::now();
                        }
                    } else if alarm_needed {
                        // Message is needed but we're in the cooldown period
                        println!("Detection condition detected, but in cooldown period. Not sending another message.");
                    }
                }

                // Check authorized detections from SPARQL (keep this part)
                let query_authorized = r#"PREFIX ex: <http://example.org#>
                SELECT ?detection ?type
                WHERE {
                    ?detection ex:authorized "true" ;
                            ex:objectType ?type .
                }"#;

                let authorized_results = execute_query(query_authorized, &mut database);
                if !authorized_results.is_empty() && security_state.lock().unwrap().can_send_alarm()
                {
                    println!("\n==> AUTHORIZED DETECTIONS FROM SPARQL:");

                    // Get the detection types from the results
                    let types: Vec<String> = authorized_results
                        .iter()
                        .filter_map(|row| {
                            if row.len() >= 2 {
                                Some(row[1].clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Just log
                    println!("Detection during allowed hours (from SPARQL): {:?}", types);

                    for row in authorized_results {
                        println!("{:?}", row);
                    }
                }

                println!("============================================\n");
            }

            last_update_time = Instant::now();
        }

        // Sleep a bit to prevent busy-waiting
        thread::sleep(Duration::from_millis(100));
    }
}

