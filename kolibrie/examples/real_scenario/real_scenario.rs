/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::Utc;
use datalog::reasoning::Reasoner;
use kolibrie::database::KolibrieDB;
use kolibrie::execute_query::execute;
use kolibrie::parser::convert_combined_rule;
use kolibrie::prelude::*;
use rand::Rng;
use shared::terms::Term;

/// Generates RDF/XML data with random sensor values but fixed grid positions
fn generate_rdf_xml() -> String {
    let mut rng = rand::rng();

    let now = Utc::now();
    let current_datetime = now.format("%Y-%m-%d %H:%M:%S").to_string();

    let room_light_level = rng.random_range(60..95);
    let room_noise_level = rng.random_range(20..35);

    let camera1_motion = rng.random_bool(0.7);
    let camera2_motion = rng.random_bool(0.4);
    let motion1_detection = rng.random_bool(0.8);
    let motion2_detection = rng.random_bool(0.5);
    let motion3_detection = rng.random_bool(0.7);
    let noise1_detection = rng.random_bool(0.3);
    let noise2_detection = rng.random_bool(0.4);

    let noise1_level = rng.random_range(5..20);
    let noise2_level = rng.random_range(8..25);

    let camera1_coverage = match rng.random_range(0..3) {
        0 => "Narrow",
        1 => "Medium",
        2 => "Wide",
        _ => "Standard",
    };

    let camera2_angle = rng.random_range(0..360);

    let random_hour = rng.random_range(0..24);
    let random_minute = rng.random_range(0..60);
    let random_event_time = format!("{:02}:{:02}", random_hour, random_minute);

    let system_user = "system";

    let cat_a_slot1_start = format!("{:02}:00", rng.random_range(7..10));
    let cat_a_slot1_end = format!("{:02}:00", rng.random_range(11..14));
    let cat_a_slot2_start = format!("{:02}:00", rng.random_range(13..16));
    let cat_a_slot2_end = format!("{:02}:00", rng.random_range(17..20));

    let cat_b_slot1_start = format!("{:02}:00", rng.random_range(8..11));
    let cat_b_slot1_end = format!("{:02}:00", rng.random_range(12..15));
    let cat_b_slot2_start = format!("{:02}:00", rng.random_range(14..17));
    let cat_b_slot2_end = format!("{:02}:00", rng.random_range(18..21));

    let shared_slot_start = "10:00";
    let shared_slot_end = "10:30";

    format!(
        r#"
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
        room_light_level,
        room_noise_level,
        current_datetime,
        system_user,
        camera1_motion,
        camera1_coverage,
        current_datetime,
        camera2_motion,
        camera2_angle,
        current_datetime,
        motion1_detection,
        if rng.random_bool(0.5) {
            "High"
        } else {
            "Medium"
        },
        current_datetime,
        motion2_detection,
        if rng.random_bool(0.5) {
            "High"
        } else {
            "Medium"
        },
        current_datetime,
        motion3_detection,
        if rng.random_bool(0.5) {
            "High"
        } else {
            "Medium"
        },
        current_datetime,
        noise1_detection,
        noise1_level,
        current_datetime,
        noise2_detection,
        noise2_level,
        current_datetime,
        cat_a_slot1_start,
        cat_a_slot1_end,
        cat_a_slot2_start,
        cat_a_slot2_end,
        shared_slot_start,
        shared_slot_end,
        cat_b_slot1_start,
        cat_b_slot1_end,
        cat_b_slot2_start,
        cat_b_slot2_end,
        shared_slot_start,
        shared_slot_end,
        random_event_time,
        current_datetime
    )
}

fn main() {
    let rdf_xml_data = generate_rdf_xml();

    let mut database = KolibrieDB::new();
    database.parse_rdf(&rdf_xml_data);
    println!(
        "Database RDF triples: {:#?}",
        database.query_default_triples(None, None, None)
    );

    let mut kg = Reasoner::new();

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
    println!("KnowledgeGraph ABox loaded.");

    let rule1 = r#"PREFIX ex: <http://example.org#>
RULE :UseNoiseSensor :-
CONSTRUCT {
    ?room ex:detectionStrategy "NoiseBased" .
}
WHERE {
    ?room ex:noiseLevel ?level .
    FILTER (?level < 30)
}"#;

    let rule2 = r#"PREFIX ex: <http://example.org#>
RULE :DefaultMotionSensor :-
CONSTRUCT {
    ?room ex:fallbackDetectionStrategy "MotionBased" .
}
WHERE {
    ?room ex:noiseLevel ?level .
}"#;

    let rule3a = r#"PREFIX ex: <http://example.org#>
RULE :UseCameraDetection :-
CONSTRUCT {
    ?room ex:detectionStrategy "CameraBased" .
}
WHERE {
    ?room ex:lightLevel ?level .
    FILTER (?level > 50)
}"#;

    let rule3b = r#"PREFIX ex: <http://example.org#>
RULE :UseCameraIdentification :-
CONSTRUCT {
    ?room ex:identificationMethod "CameraIdentification" .
}
WHERE {
    ?room ex:lightLevel ?level .
    FILTER (?level > 50)
}"#;

    let rule_mark_all_unauthorized = r#"PREFIX ex: <http://example.org#>
RULE :MarkAllEventsUnauthorized :-
CONSTRUCT {
    ?event ex:unauthorized "true" .
}
WHERE {
    ?event ex:detectedCategory ?person .
}"#;

    let rule_mark_authorized_if_within_schedule = r#"PREFIX ex: <http://example.org#>
RULE :MarkAllEventsUnauthorized :-
CONSTRUCT {
    ?event ex:unauthorized "true" .
}
WHERE {
    ?event ex:detectedCategory ?person .
}"#;

    let rules = [
        rule1,
        rule2,
        rule3a,
        rule3b,
        rule_mark_all_unauthorized,
        rule_mark_authorized_if_within_schedule,
    ];

    for rule in rules.iter() {
        let (_rest, combined_query) =
            parse_combined_query(rule).expect("Failed to parse combined query");

        if let Some(rule_def) = combined_query
            .single_rule()
            .expect("expected at most one RULE block")
            .cloned()
        {
            let mut dict = database.dictionary_mut();
            let dynamic_rule = convert_combined_rule(rule_def, &mut dict, &combined_query.prefixes);
            drop(dict);

            println!("Dynamic rule: {:#?}", dynamic_rule);
            kg.add_rule(dynamic_rule.clone());

            if let Some(first_conclusion) = &dynamic_rule.conclusion.first() {
                if let Term::Constant(code) = first_conclusion.1 {
                    let dict = database.dictionary();
                    if let Some(expanded) = dict.decode(code) {
                        let expanded = expanded.to_string();
                        drop(dict);

                        if let Some(idx) = expanded.rfind('#') {
                            let local = &expanded[idx + 1..];
                            database.add_rule_mapping(local.to_lowercase(), expanded.clone());
                        }
                    }
                }
            }
        }
    }

    let inferred_facts = kg.infer_new_facts_semi_naive();
    println!("Inferred {} new fact(s):", inferred_facts.len());

    {
        let dict = database.dictionary();
        for triple in inferred_facts.iter() {
            let s = dict.decode(triple.subject).unwrap_or_default();
            let p = dict.decode(triple.predicate).unwrap_or_default();
            let o = dict.decode(triple.object).unwrap_or_default();
            println!("{} {} {}", s, p, o);
        }
    }

    for triple in inferred_facts.iter() {
        database.add_triple(triple.clone());
    }

    let query_grid_sensors = r#"PREFIX ex: <http://example.org#>
    SELECT ?sensor ?type ?x ?y
    WHERE {
        ?sensor ex:type ?type ;
                ex:gridX ?x ;
                ex:gridY ?y .
    }"#;

    let grid_sensor_results = execute(query_grid_sensors, &mut database)
        .expect("query should run")
        .into_rows();
    println!("\n==> Sensors in grid coordinates:");
    for row in grid_sensor_results {
        println!("{:?}", row);
    }

    let query_unauthorized = r#"PREFIX ex: <http://example.org#>
    SELECT ?event ?time
    WHERE {
        ?event ex:unauthorized "true" ;
                ex:timeOfDetection ?time .
    }"#;

    let unauthorized_results = execute(query_unauthorized, &mut database)
        .expect("query should run")
        .into_rows();
    println!("\n==> Unauthorized detection events:");
    for row in unauthorized_results {
        println!("{:?}", row);
    }
}
