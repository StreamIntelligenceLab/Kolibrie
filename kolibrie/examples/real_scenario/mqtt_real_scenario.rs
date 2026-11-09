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
use kolibrie::database::KolibrieDB;
use kolibrie::execute_query::execute;
use kolibrie::parser::convert_combined_rule;
use kolibrie::prelude::*;
use rumqttc::{Client, MqttOptions, QoS, RecvTimeoutError};
use serde::{Deserialize, Serialize};
use shared::terms::Term;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::time::{Duration, SystemTime};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct PirSensorData {
    id: String,
    t: u64,
    #[serde(rename = "type")]
    sensor_type: String,
    intensity: u32,
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
    timestamp: u64,
    #[serde(rename = "type")]
    data_type: String,
    status: String,
    detections: Vec<Detection>,
    triggered_by: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ScheduleData {
    hour: u32,
    time: String,
}

struct SecurityState {
    cameras: HashMap<String, CameraData>,
    schedule: Option<ScheduleData>,
    last_updated: SystemTime,
    last_processed: SystemTime,
    last_alarm_sent: SystemTime,
    new_data_available: bool,
    current_alarm_state: bool,
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
            pir_sensors: HashMap::new(),
            alarm_cooldown: Duration::from_secs(5),
            pir_intensity_threshold: 1,
        }
    }

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

    fn can_send_alarm(&self) -> bool {
        match self.last_alarm_sent.elapsed() {
            Ok(elapsed) => elapsed >= self.alarm_cooldown,
            Err(_) => true,
        }
    }

    fn generate_alarm_message(
        &self,
        reason: &str,
        status: &str,
        detection_types: &[&str],
    ) -> String {
        let mut detections_json = Vec::new();
        let mut camera_ids = Vec::new();

        for (camera_id, camera) in &self.cameras {
            for detection in &camera.detections {
                let obj_type = detection.detection_type.to_lowercase();
                let type_matches = detection_types.is_empty()
                    || detection_types
                        .iter()
                        .any(|t| obj_type.contains(&t.to_lowercase()));

                if type_matches {
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

        let mut alarm_obj = serde_json::Map::new();
        alarm_obj.insert(
            "timestamp".to_string(),
            serde_json::json!(chrono::Utc::now().timestamp_millis()),
        );
        alarm_obj.insert("status".to_string(), serde_json::json!(status));
        alarm_obj.insert("reason".to_string(), serde_json::json!(reason));
        alarm_obj.insert("detections".to_string(), serde_json::json!(detections_json));
        alarm_obj.insert("camera_ids".to_string(), serde_json::json!(camera_ids));

        serde_json::Value::Object(alarm_obj).to_string()
    }
}

fn start_mqtt_collection(
    security_state: Arc<Mutex<SecurityState>>,
    mqtt_client: Arc<Mutex<Option<Client>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mqtt_broker = "YOUR_IP_ADDRESS";
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
            let client_connection_result = Client::new(mqttoptions.clone(), 10);

            let (mut client, mut connection) = client_connection_result;

            *mqtt_client.lock().unwrap() = Some(client.clone());

            let subscription_result_cam0 =
                client.subscribe("camera/detections/0", QoS::AtLeastOnce);
            let subscription_result_cam1 =
                client.subscribe("camera/detections/1", QoS::AtLeastOnce);
            let subscription_result_schedule = client.subscribe("schedule", QoS::AtLeastOnce);

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

                                            let camera_id = if camera_data.id.is_empty() {
                                                if topic == "camera/detections/0" {
                                                    "camera_0".to_string()
                                                } else {
                                                    "camera_1".to_string()
                                                }
                                            } else {
                                                camera_data.id.clone()
                                            };

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
                            if let RecvTimeoutError::Timeout = e {
                                continue;
                            } else {
                                println!("MQTT connection error: {:?}", e);
                                connection_active = false;
                            }
                        }
                    }

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

                if last_dummy_update.elapsed() > dummy_update_interval {
                    println!("Adding test data for development (broker unavailable)");

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

                    let dummy_pir_data = PirSensorData {
                        id: "sensor_nw_pir".to_string(),
                        t: chrono::Utc::now().timestamp_millis() as u64,
                        sensor_type: "PIR".to_string(),
                        intensity: 2,
                    };

                    let dummy_schedule_data = ScheduleData {
                        hour: 19,
                        time: "evening".to_string(),
                    };

                    let mut state = security_state.lock().unwrap();
                    state
                        .cameras
                        .insert(dummy_camera_data.id.clone(), dummy_camera_data);
                    state
                        .pir_sensors
                        .insert(dummy_pir_data.id.clone(), dummy_pir_data);
                    state.schedule = Some(dummy_schedule_data);
                    state.last_updated = SystemTime::now();
                    state.new_data_available = true;

                    last_dummy_update = Instant::now();
                }
            }

            println!("MQTT connection lost or failed. Reconnecting in 5 seconds...");
            thread::sleep(Duration::from_secs(5));
        }
    })
}

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

fn detections_to_rdf(security_state: &SecurityState, test_time: Option<&str>) -> String {
    let mut rdf_parts = Vec::new();

    rdf_parts.push(
        r#"<?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                  xmlns:ex="http://example.org#">"#
            .to_string(),
    );

    let (current_time_str, hour, _minute, date_str) = if let Some(test_time) = test_time {
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
            let now = Local::now();
            (
                format!("{:02}:{:02}", now.hour(), now.minute()),
                now.hour(),
                now.minute(),
                now.format("%Y-%m-%d").to_string(),
            )
        }
    } else if let Some(schedule) = &security_state.schedule {
        let hour = schedule.hour;
        let minute = 0;
        (
            format!("{:02}:{:02}", hour, minute),
            hour,
            minute,
            Local::now().format("%Y-%m-%d").to_string(),
        )
    } else {
        let now = Local::now();
        (
            format!("{:02}:{:02}", now.hour(), now.minute()),
            now.hour(),
            now.minute(),
            now.format("%Y-%m-%d").to_string(),
        )
    };

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

    for (camera_id, camera_data) in &security_state.cameras {
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

        for (i, detection) in camera_data.detections.iter().enumerate() {
            let detection_id = format!("Detection_{}_{}", camera_id, i);

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

    rdf_parts.push("</rdf:RDF>".to_string());

    rdf_parts.join("\n")
}

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

    if end_minutes < start_minutes {
        time_minutes >= start_minutes || time_minutes <= end_minutes
    } else {
        time_minutes >= start_minutes && time_minutes <= end_minutes
    }
}

fn main() {
    println!("Starting security monitoring system with MQTT object detection...");

    let security_state = Arc::new(Mutex::new(SecurityState::new()));

    let mqtt_client = Arc::new(Mutex::new(None::<Client>));

    let test_time: Option<String> = None;

    let _mqtt_thread = start_mqtt_collection(security_state.clone(), mqtt_client.clone());

    println!("Waiting for detection data...");

    let mut database;
    let mut kg;

    let mut last_update_time = Instant::now();
    let update_interval = Duration::from_secs(2);

    loop {
        if last_update_time.elapsed() >= update_interval {
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

                let current_time_str;
                let is_morning;
                let is_night_time;
                let is_afternoon;
                let is_evening;
                let time_of_day;
                {
                    let state = security_state.lock().unwrap();
                    if let Some(schedule) = &state.schedule {
                        current_time_str = format!("{:02}:00", schedule.hour);
                        time_of_day = schedule.time.clone();
                        println!(
                            "Using schedule time: {} ({})",
                            current_time_str, schedule.time
                        );
                    } else if let Some(ref time) = test_time {
                        current_time_str = time.to_string();
                        time_of_day = "unknown".to_string();
                        println!("Using test time: {}", current_time_str);
                    } else {
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

                    is_morning = time_in_range(&current_time_str, "05:00", "10:00");
                    is_afternoon = time_in_range(&current_time_str, "10:00", "16:00");
                    is_evening = time_in_range(&current_time_str, "16:00", "22:00");
                    is_night_time = time_in_range(&current_time_str, "22:00", "05:00");
                }

                let rdf_xml_data = detections_to_rdf(&security_state.lock().unwrap(), None);

                database = KolibrieDB::new();
                kg = Reasoner::new();

                database.parse_rdf(&rdf_xml_data);
                println!(
                    "Database loaded with {} triples",
                    database.default_triple_count()
                );

                let triples_to_add = database.query_default_triples(None, None, None);

                for triple in triples_to_add {
                    let dict = database.dictionary();
                    let subject = dict.decode(triple.subject).map(|s| s.to_string());
                    let predicate = dict.decode(triple.predicate).map(|p| p.to_string());
                    let object = dict.decode(triple.object).map(|o| o.to_string());
                    drop(dict);

                    if let (Some(s), Some(p), Some(o)) = (subject, predicate, object) {
                        kg.add_abox_triple(&s, &p, &o);
                    }
                }

                println!("KnowledgeGraph loaded");

                let rule_night_motion_template = r#"PREFIX ex: <http://example.org#>
RULE :UnauthorizedMotion :-
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
RULE :UnauthorizedVehicle :-
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
RULE :AuthorizedDaytime :-
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

                let mut active_rules = Vec::new();

                if is_night_time {
                    println!(
                        "Time ({}): Night hours - applying night motion rules",
                        current_time_str
                    );
                    active_rules.push(rule_night_motion_template);
                } else if is_morning || is_afternoon {
                    println!(
                        "Time ({}): Daytime hours - applying daytime allowed rules",
                        current_time_str
                    );
                    active_rules.push(rule_daytime_allowed_template);

                    if is_morning {
                        let state = security_state.lock().unwrap();
                        if let Some(schedule) = &state.schedule {
                            if schedule.time == "morning" {
                                println!(
                                    "Morning schedule detected: Vehicle restrictions DISABLED"
                                );
                            } else {
                                println!(
                                    "Time ({}): Early morning hours - applying vehicle rules",
                                    current_time_str
                                );
                                active_rules.push(rule_vehicle_restriction_template);
                            }
                        } else {
                            println!(
                                "Time ({}): Early morning hours - applying vehicle rules",
                                current_time_str
                            );
                            active_rules.push(rule_vehicle_restriction_template);
                        }
                    }
                } else if is_evening {
                    println!(
                        "Time ({}): Evening hours - applying vehicle rules",
                        current_time_str
                    );
                    active_rules.push(rule_vehicle_restriction_template);
                }

                println!("Processing {} active security rules...", active_rules.len());
                for (idx, rule) in active_rules.iter().enumerate() {
                    match parse_combined_query(rule) {
                        Ok((_rest, combined_query)) => {
                            if let Some(rule) = combined_query
                                .single_rule()
                                .expect("expected at most one RULE block")
                                .cloned()
                            {
                                let mut dict = database.dictionary_mut();
                                let dynamic_rule = convert_combined_rule(
                                    rule.clone(),
                                    &mut dict,
                                    &combined_query.prefixes,
                                );
                                drop(dict);

                                println!("Dynamic rule #{}: {:#?}", idx + 1, dynamic_rule);
                                kg.add_rule(dynamic_rule.clone());
                                println!("Rule #{} added to KnowledgeGraph.", idx + 1);

                                if let Some(first_conclusion) = dynamic_rule.conclusion.first() {
                                    if let Term::Constant(code) = first_conclusion.1 {
                                        let dict = database.dictionary();
                                        if let Some(expanded) = dict.decode(code) {
                                            let expanded = expanded.to_string();
                                            drop(dict);

                                            if let Some(idx) = expanded.rfind('#') {
                                                let local = &expanded[idx + 1..];
                                                database.add_rule_mapping(
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

                println!("Running inference engine...");
                let inferred_facts = kg.infer_new_facts_semi_naive();
                println!("Inferred {} new fact(s)", inferred_facts.len());

                let dict = database.dictionary();
                for triple in inferred_facts.iter() {
                    let s = dict.decode(triple.subject).unwrap_or_default();
                    let p = dict.decode(triple.predicate).unwrap_or_default();
                    let o = dict.decode(triple.object).unwrap_or_default();
                    println!("  Inferred: {} {} {}", s, p, o);
                }
                drop(dict);

                for triple in inferred_facts.iter() {
                    database.add_triple(triple.clone());
                }

                let mut alarm_reason = String::new();
                let mut alarm_needed = false;
                let mut status = "0";
                let mut detection_types: Vec<&str> = Vec::new();

                {
                    let mut state = security_state.lock().unwrap();

                    let use_pir_sensors = is_morning
                        || is_afternoon
                        || is_night_time
                        || time_of_day == "morning"
                        || time_of_day == "afternoon"
                        || time_of_day == "night";

                    println!("Current time period: {}", time_of_day);
                    println!("Using PIR sensors: {}", use_pir_sensors);

                    if use_pir_sensors {
                        if state.has_pir_motion() {
                            if is_night_time || time_of_day == "night" {
                                alarm_reason =
                                    "PIR motion detected during night hours (10pm-5am)".to_string();
                                status = "1";
                            } else if is_morning || time_of_day == "morning" {
                                alarm_reason =
                                    "PIR motion detected during morning hours (5am-10am)"
                                        .to_string();
                                status = "0";
                            } else if is_afternoon || time_of_day == "afternoon" {
                                alarm_reason =
                                    "PIR motion detected during afternoon hours (10am-4pm)"
                                        .to_string();
                                status = "0";
                            } else {
                                alarm_reason = "PIR motion detected".to_string();
                                status = "0";
                            }

                            alarm_needed = true;
                            detection_types.push("pir_motion");

                            println!("PIR motion detected with reason: {}", alarm_reason);
                        }
                    } else if is_evening || time_of_day == "evening" {
                        println!("Using camera object detection for evening hours");

                        if is_night_time && state.has_motion_detection() {
                            alarm_reason =
                                "Motion detected during restricted hours (10pm-5am)".to_string();
                            alarm_needed = true;
                            status = "1";
                            detection_types.push("person");
                            detection_types.push("teddy bear");
                        } else if time_in_range(&current_time_str, "16:00", "05:00")
                            && state.has_vehicle_detection()
                        {
                            alarm_reason =
                                "Vehicle detected during restricted hours (4pm-5am)".to_string();
                            alarm_needed = true;
                            status = "1";
                            detection_types.extend(&["car", "bus", "truck", "train"]);
                        } else if is_morning {
                            if let Some(schedule) = &state.schedule {
                                if schedule.time == "morning" {
                                    if state.has_motion_detection() || state.has_vehicle_detection()
                                    {
                                        alarm_reason =
                                            "Detection during morning hours (5am-10am)".to_string();
                                        alarm_needed = true;
                                        status = "0";
                                        detection_types = Vec::new();
                                    }
                                } else if state.has_vehicle_detection() {
                                    alarm_reason = "Vehicle detected during early morning restricted hours (5am-10am)".to_string();
                                    alarm_needed = true;
                                    status = "1";
                                    detection_types.extend(&["car", "bus", "truck", "train"]);
                                } else if state.has_motion_detection() {
                                    alarm_reason =
                                        "Motion detected during allowed morning hours".to_string();
                                    alarm_needed = true;
                                    status = "0";
                                    detection_types.push("person");
                                    detection_types.push("teddy bear");
                                }
                            } else if state.has_vehicle_detection() {
                                alarm_reason = "Vehicle detected during early morning restricted hours (5am-10am)".to_string();
                                alarm_needed = true;
                                status = "1";
                                detection_types.extend(&["car", "bus", "truck", "train"]);
                            } else if state.has_motion_detection() {
                                alarm_reason =
                                    "Motion detected during allowed morning hours".to_string();
                                alarm_needed = true;
                                status = "0";
                                detection_types.push("person");
                                detection_types.push("teddy bear");
                            }
                        } else if is_afternoon
                            && (state.has_motion_detection() || state.has_vehicle_detection())
                        {
                            alarm_reason =
                                "Detection during allowed daytime hours (10am-4pm)".to_string();
                            alarm_needed = true;
                            status = "0";
                            detection_types = Vec::new();
                        } else if is_evening {
                            if state.has_motion_detection() {
                                alarm_reason =
                                    "Person detected during evening hours (4pm-10pm)".to_string();
                                alarm_needed = true;
                                status = "0";
                                detection_types.push("person");
                                detection_types.push("teddy bear");
                            } else if state.has_vehicle_detection() {
                                alarm_reason =
                                    "Vehicle detected during restricted evening hours (4pm-10pm)"
                                        .to_string();
                                alarm_needed = true;
                                status = "1";
                                detection_types.extend(&["car", "bus", "truck", "train"]);
                            }
                        }
                    } else {
                        println!("No active detection sources available - no PIR or camera detection active");
                    }

                    state.current_alarm_state = alarm_needed && status == "1";

                    if alarm_needed && state.can_send_alarm() {
                        if status == "1" {
                            println!("\n🚨🚨 ALARM TRIGGERED 🚨🚨\n");
                        } else {
                            println!("\n✅ AUTHORIZED DETECTION ✅\n");
                        }

                        let alarm_message =
                            state.generate_alarm_message(&alarm_reason, status, &detection_types);
                        println!("Sending message to alarm topic: {}", alarm_message);

                        if send_mqtt_message(&mqtt_client, "alarm", &alarm_message) {
                            state.last_alarm_sent = SystemTime::now();
                        }
                    } else if alarm_needed {
                        println!("Detection condition detected, but in cooldown period. Not sending another message.");
                    }
                }

                let query_authorized = r#"PREFIX ex: <http://example.org#>
                SELECT ?detection ?type
                WHERE {
                    ?detection ex:authorized "true" ;
                            ex:objectType ?type .
                }"#;

                let authorized_results = execute(query_authorized, &mut database)
                    .expect("query should run")
                    .into_rows();
                if !authorized_results.is_empty() && security_state.lock().unwrap().can_send_alarm()
                {
                    println!("\n==> AUTHORIZED DETECTIONS FROM SPARQL:");

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

                    println!("Detection during allowed hours (from SPARQL): {:?}", types);

                    for row in authorized_results {
                        println!("{:?}", row);
                    }
                }

                println!("============================================\n");
            }

            last_update_time = Instant::now();
        }

        thread::sleep(Duration::from_millis(100));
    }
}
