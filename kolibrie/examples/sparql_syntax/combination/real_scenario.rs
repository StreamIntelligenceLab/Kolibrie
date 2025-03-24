use kolibrie::parser::*;
use kolibrie::sparql_database::SparqlDatabase;
use datalog::knowledge_graph::KnowledgeGraph;
use shared::terms::Term;

fn main() {
    // Define the RDF/XML data for the virtual room with sensors - without grid sections
    let rdf_xml_data = r#"
        <?xml version="1.0"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:ex="http://example.org#">
          
          <!-- Room definition -->
          <rdf:Description rdf:about="http://example.org#VirtualRoom">
            <ex:lightLevel>75</ex:lightLevel>
            <ex:noiseLevel>25</ex:noiseLevel>
          </rdf:Description>
          
          <!-- Camera in the left up corner -->
          <rdf:Description rdf:about="http://example.org#Camera1">
            <ex:type>Camera</ex:type>
            <ex:position>LeftUp</ex:position>
            <ex:detectedMotion>true</ex:detectedMotion>
            <ex:coverage>Wide</ex:coverage>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
          
          <!-- Rotating camera on right side -->
          <rdf:Description rdf:about="http://example.org#Camera2">
            <ex:type>RotatingCamera</ex:type>
            <ex:position>RightSide</ex:position>
            <ex:detectedMotion>false</ex:detectedMotion>
            <ex:coverage>Rotating</ex:coverage>
            <ex:currentAngle>45</ex:currentAngle>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
          
          <!-- Motion sensor in left down corner -->
          <rdf:Description rdf:about="http://example.org#MotionSensor1">
            <ex:type>MotionSensor</ex:type>
            <ex:position>LeftDown</ex:position>
            <ex:detectedMotion>true</ex:detectedMotion>
            <ex:sensitivity>High</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
          
          <!-- Motion sensor in right up corner -->
          <rdf:Description rdf:about="http://example.org#MotionSensor2">
            <ex:type>MotionSensor</ex:type>
            <ex:position>RightUp</ex:position>
            <ex:detectedMotion>false</ex:detectedMotion>
            <ex:sensitivity>Medium</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
          
          <!-- Motion sensor in right down corner -->
          <rdf:Description rdf:about="http://example.org#MotionSensor3">
            <ex:type>MotionSensor</ex:type>
            <ex:position>RightDown</ex:position>
            <ex:detectedMotion>true</ex:detectedMotion>
            <ex:sensitivity>High</ex:sensitivity>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
          
          <!-- Noise sensor in the middle down -->
          <rdf:Description rdf:about="http://example.org#NoiseSensor1">
            <ex:type>NoiseSensor</ex:type>
            <ex:position>MiddleDown</ex:position>
            <ex:detectedNoise>false</ex:detectedNoise>
            <ex:noiseLevel>10</ex:noiseLevel>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
          
          <!-- Light sensor data -->
          <rdf:Description rdf:about="http://example.org#LightSensor1">
            <ex:type>LightSensor</ex:type>
            <ex:position>MiddleUp</ex:position>
            <ex:lightLevel>85</ex:lightLevel>
            <ex:isDark>false</ex:isDark>
            <ex:isActive>true</ex:isActive>
          </rdf:Description>
        </rdf:RDF>
    "#;

    // Create and populate the database
    let mut database = SparqlDatabase::new();
    database.parse_rdf(rdf_xml_data);
    println!("Database RDF triples: {:#?}", database.triples);

    // Load data into knowledge graph
    let mut kg = KnowledgeGraph::new();
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

    // Execute rules
    let rules = [rule1, rule2, rule3a, rule3b];
    
    for rule in rules.iter() {
        let (_rest, combined_query) = parse_combined_query(rule)
            .expect("Failed to parse combined query");
        
        if let Some(rule_def) = combined_query.rule {
            let dynamic_rule = convert_combined_rule(rule_def, &mut database.dictionary, &combined_query.prefixes);
            println!("Dynamic rule: {:#?}", dynamic_rule);
            kg.add_rule(dynamic_rule.clone());
            
            let expanded = match dynamic_rule.conclusion.1 {
                Term::Constant(code) => {
                    database.dictionary.decode(code).unwrap_or_else(|| "")
                },
                _ => "",
            };
            let local = if let Some(idx) = expanded.rfind('#') {
                &expanded[idx + 1..]
            } else if let Some(idx) = expanded.rfind(':') {
                &expanded[idx + 1..]
            } else {
                &expanded
            };
            let rule_key = local.to_lowercase();
            database.rule_map.insert(rule_key, expanded.to_string());
        }
    }
    
    // Execute inference
    let inferred_facts = kg.infer_new_facts_semi_naive();
    println!("Inferred {} new fact(s):", inferred_facts.len());
    for triple in inferred_facts.iter() {
        println!("{}", database.triple_to_string(triple, &database.dictionary));
        database.triples.insert(triple.clone());
    }
}