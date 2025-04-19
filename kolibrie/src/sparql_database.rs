use shared::dictionary::Dictionary;
use crate::sliding_window::SlidingWindow;
use shared::triple::TimestampedTriple;
use shared::triple::Triple;
use shared::query::FilterExpression;
use crate::utils;
use crate::utils::current_timestamp;
use crate::utils::ClonableFn;
#[cfg(feature = "cuda")]
use crate::cuda::cuda_join::*;
use shared::index_manager::UnifiedIndex;
use crossbeam::channel::unbounded;
use crossbeam::scope;
use percent_encoding::percent_decode;
use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::Reader;
use rayon::prelude::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::x86_64::*;
#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use url::Url;

const MIN_CHUNK_SIZE: usize = 1024;
const HASHMAP_INITIAL_CAPACITY: usize = 4096;

const MIN_CHUNK_SIZE1: usize = 1024;
const HASHMAP_INITIAL_CAPACITY1: usize = 1024;

#[derive(Debug, Clone)]
pub struct SparqlDatabase {
    pub triples: BTreeSet<Triple>,
    pub streams: Vec<TimestampedTriple>,
    pub sliding_window: Option<SlidingWindow>,
    pub dictionary: Dictionary,
    pub prefixes: HashMap<String, String>,
    pub udfs: HashMap<String, ClonableFn>,
    pub index_manager: UnifiedIndex,
    pub rule_map: HashMap<String, String>,
}

#[allow(dead_code)]
impl SparqlDatabase {
    pub fn new() -> Self {
        Self {
            triples: BTreeSet::new(),
            streams: Vec::new(),
            sliding_window: None,
            dictionary: Dictionary::new(),
            prefixes: HashMap::new(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn add_triple(&mut self, triple: Triple) {
        self.triples.insert(triple.clone());
        self.index_manager.insert(&triple);
    }

    /// Helper function that accepts parts of a triple, constructs a Triple, and adds it.
    pub fn add_triple_parts(&mut self, subject: &str, predicate: &str, object: &str) {
        let subject_id = self.dictionary.encode(subject);
        let predicate_id = self.dictionary.encode(predicate);
        let object_id = self.dictionary.encode(object);

        let triple = Triple {
            subject: subject_id,
            predicate: predicate_id,
            object: object_id,
        };
        self.add_triple(triple);
    }

    pub fn generate_rdf_xml(&mut self) -> String {
        let mut xml = String::new();
        xml.push_str("<?xml version=\"1.0\"?>\n");
        xml.push_str("<rdf:RDF");
    
        // Write namespace declarations (from the stored prefixes)
        for (prefix, uri) in &self.prefixes {
            if prefix.is_empty() {
                xml.push_str(&format!(" xmlns=\"{}\"", uri));
            } else {
                xml.push_str(&format!(" xmlns:{}=\"{}\"", prefix, uri));
            }
        }
        // Always include the standard RDF namespace
        xml.push_str(" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"");
        xml.push_str(">\n");
    
        // Group triples by subject
        let mut subjects: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        for triple in &self.triples {
            let subject = self.dictionary.decode(triple.subject);
            let predicate = self.dictionary.decode(triple.predicate);
            let object = self.dictionary.decode(triple.object);
            subjects.entry(subject.unwrap().to_string()).or_default().push((predicate.unwrap().to_string(), object.unwrap().to_string()));
        }
    
        // For each subject, create an <rdf:Description> element.
        for (subject, po_pairs) in subjects {
            xml.push_str(&format!("  <rdf:Description rdf:about=\"{}\">\n", subject));
            for (predicate, object) in po_pairs {
                xml.push_str(&format!("    <{}>{}</{}>\n", predicate, object, predicate));
            }
            xml.push_str("  </rdf:Description>\n");
        }
    
        xml.push_str("</rdf:RDF>\n");
        xml
    }

    pub fn parse_rdf_from_file(&mut self, filename: &str) {
        let file = std::fs::File::open(filename).expect("Cannot open file");
        let reader = std::io::BufReader::new(file);
        let mut xml_reader = Reader::from_reader(reader);
        xml_reader.trim_text(true);
        xml_reader.check_comments(false);
        xml_reader.expand_empty_elements(false);

        let mut current_subject = Vec::with_capacity(128);
        let mut current_predicate = Vec::with_capacity(128);

        // First, read prefixes before spawning worker threads
        let mut buf = Vec::new();
        loop {
            match xml_reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if e.name() == QName(b"rdf:RDF") {
                        // Read prefixes
                        for attr in e.attributes().filter_map(Result::ok) {
                            let key = attr.key;
                            let value = attr.value;
                            if key.as_ref().starts_with(b"xmlns:") {
                                let prefix = std::str::from_utf8(&key.as_ref()[6..])
                                    .unwrap_or("")
                                    .to_string();
                                let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                self.prefixes.insert(prefix, uri);
                            } else if key.as_ref() == b"xmlns" {
                                // Default namespace
                                let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                self.prefixes.insert("".to_string(), uri);
                            }
                        }
                        break; // We have read the prefixes, proceed to the rest
                    }
                }
                Ok(Event::Eof) => {
                    eprintln!("Reached EOF before reading prefixes.");
                    break;
                }
                Err(e) => {
                    eprintln!("Error reading XML: {:?}", e);
                    break;
                }
                _ => {}
            }
            buf.clear();
        }

        // Continue reading and parsing the rest of the file
        let mut triples = Vec::with_capacity(8192);
        loop {
            match xml_reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name() {
                    QName(b"rdf:Description") => {
                        for attr in e.attributes().filter_map(Result::ok) {
                            if attr.key == QName(b"rdf:about") {
                                current_subject.clear();
                                current_subject.extend_from_slice(&attr.value);
                            }
                        }
                    }
                    QName(b"rdfs:Class") | QName(b"rdf:type") => {
                        current_predicate.clear();
                        current_predicate.extend_from_slice(b"rdf:type");
                    }
                    QName(b"rdfs:subClassOf") => {
                        current_predicate.clear();
                        current_predicate.extend_from_slice(b"rdfs:subClassOf");
                    }
                    QName(b"rdfs:label") => {
                        current_predicate.clear();
                        current_predicate.extend_from_slice(b"rdfs:label");
                    }
                    name => {
                        let name_str = std::str::from_utf8(name.as_ref()).unwrap_or("").to_string();
                        let resolved_predicate = self.resolve_term(&name_str);
                        current_predicate = resolved_predicate.clone().into_bytes();
                    }
                },
                Ok(Event::Empty(ref e)) => {
                    if let Ok(predicate) = std::str::from_utf8(e.name().as_ref()) {
                        let resolved_predicate = self.resolve_term(predicate);
                        let mut object = Vec::with_capacity(128);
                        for attr in e.attributes().filter_map(Result::ok) {
                            if attr.key == QName(b"rdf:resource") {
                                object.extend_from_slice(&attr.value);
                            }
                        }
                        if !object.is_empty() {
                            if let (Ok(subject_str), Ok(object_str)) = (
                                std::str::from_utf8(&current_subject),
                                std::str::from_utf8(&object),
                            ) {
                                let triple = Triple {
                                    subject: self.dictionary.encode(subject_str),
                                    predicate: self.dictionary.encode(&resolved_predicate),
                                    object: self.dictionary.encode(object_str),
                                };
                                triples.push(triple);
                            }
                        }
                    }
                }
                Ok(Event::Text(e)) => {
                    if let Ok(object) = e.unescape() {
                        if let Ok(subject_str) = std::str::from_utf8(&current_subject) {
                            if let Ok(predicate_str) = std::str::from_utf8(&current_predicate) {
                                let resolved_predicate = self.resolve_term(predicate_str);
                                let triple = Triple {
                                    subject: self.dictionary.encode(subject_str),
                                    predicate: self.dictionary.encode(&resolved_predicate),
                                    object: self.dictionary.encode(&object),
                                };
                                triples.push(triple);
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    if e.name() == QName(b"rdf:Description") {
                        current_subject.clear();
                        current_predicate.clear();
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    eprintln!("Error reading XML: {:?}", e);
                    break;
                }
                _ => {}
            }

            buf.clear();

            if triples.len() >= 8192 {
                // Process triples in parallel using Rayon
                let local_triples: BTreeSet<Triple> = triples.into_par_iter().collect();
                self.triples.extend(local_triples);
                triples = Vec::with_capacity(8192);
            }
        }

        if !triples.is_empty() {
            let local_triples: BTreeSet<Triple> = triples.into_par_iter().collect();
            self.triples.extend(local_triples);
        }
    }

    pub fn parse_rdf(&mut self, rdf_xml: &str) {
        let mut reader = Reader::from_str(rdf_xml);
        reader.trim_text(true);
        reader.check_comments(false);
        reader.expand_empty_elements(false);

        let mut current_subject = Vec::with_capacity(128);
        let mut current_predicate = Vec::with_capacity(128);

        let (sender, receiver) = unbounded::<Vec<Triple>>();
        let dictionary = Arc::new(RwLock::new(self.dictionary.clone()));
        let triples_set = Arc::new(Mutex::new(Vec::new()));
        let num_threads = utils::get_num_cpus();

        // Crossbeam scope to manage threads
        scope(|s| {
            // Spawn worker threads
            for _ in 0..num_threads {
                let receiver = receiver.clone();
                let triples_set = Arc::clone(&triples_set);
                s.spawn(move |_| {
                    while let Ok(chunk) = receiver.recv() {
                        if chunk.is_empty() {
                            // Termination signal
                            break;
                        }

                        // Process chunk using Rayon
                        let local_triples: BTreeSet<Triple> =
                            chunk.into_par_iter().map(|triple| triple).collect();

                        // Insert into shared triples set
                        let mut triples = triples_set.lock().unwrap();
                        triples.push(local_triples);
                    }
                });
            }

            // Parsing and sending chunks
            let mut triples = Vec::with_capacity(8192);
            loop {
                match reader.read_event() {
                    Ok(Event::Start(ref e)) => match e.name() {
                        QName(b"rdf:RDF") => {
                            for attr in e.attributes().filter_map(Result::ok) {
                                let key = attr.key;
                                let value = attr.value;
                                if key.as_ref().starts_with(b"xmlns:") {
                                    let prefix = std::str::from_utf8(&key.as_ref()[6..])
                                        .unwrap_or("")
                                        .to_string();
                                    let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                    self.prefixes.insert(prefix, uri);
                                } else if key.as_ref() == b"xmlns" {
                                    // Default namespace
                                    let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                    self.prefixes.insert("".to_string(), uri);
                                }
                            }
                        }
                        QName(b"rdf:Description") => {
                            for attr in e.attributes().filter_map(Result::ok) {
                                if attr.key == QName(b"rdf:about") {
                                    current_subject.truncate(0);
                                    current_subject.extend_from_slice(&attr.value);
                                }
                            }
                        }
                        QName(b"rdfs:Class") | QName(b"rdf:type") => {
                            current_predicate.truncate(0);
                            current_predicate.extend_from_slice(b"rdf:type");
                        }
                        QName(b"rdfs:subClassOf") => {
                            current_predicate.truncate(0);
                            current_predicate.extend_from_slice(b"rdfs:subClassOf");
                        }
                        QName(b"rdfs:label") => {
                            current_predicate.truncate(0);
                            current_predicate.extend_from_slice(b"rdfs:label");
                        }
                        name => {
                            let name_str =
                                std::str::from_utf8(name.as_ref()).unwrap_or("").to_string();
                            let resolved_predicate = self.resolve_term(&name_str);
                            current_predicate = resolved_predicate.clone().into_bytes();
                        }
                    },
                    Ok(Event::Empty(ref e)) => {
                        if let Ok(predicate) = std::str::from_utf8(e.name().as_ref()) {
                            let resolved_predicate = self.resolve_term(predicate);
                            let mut object = Vec::with_capacity(128);
                            for attr in e.attributes().filter_map(Result::ok) {
                                if attr.key == QName(b"rdf:resource") {
                                    object.extend_from_slice(&attr.value);
                                }
                            }
                            if !object.is_empty() {
                                if let (Ok(subject_str), Ok(object_str)) = (
                                    std::str::from_utf8(&current_subject),
                                    std::str::from_utf8(&object),
                                ) {
                                    // Lock the dictionary for encoding
                                    let mut dict = dictionary.write().unwrap();
                                    let triple = Triple {
                                        subject: dict.encode(subject_str),
                                        predicate: dict.encode(&resolved_predicate),
                                        object: dict.encode(object_str),
                                    };
                                    drop(dict); // Release the lock
                                    triples.push(triple);
                                }
                            }
                        }
                    }
                    Ok(Event::Text(e)) => {
                        if let Ok(object) = e.unescape() {
                            if let Ok(subject_str) = std::str::from_utf8(&current_subject) {
                                if let Ok(predicate_str) = std::str::from_utf8(&current_predicate) {
                                    let resolved_predicate = self.resolve_term(predicate_str);
                                    // Lock the dictionary for encoding
                                    let mut dict = dictionary.write().unwrap();
                                    let triple = Triple {
                                        subject: dict.encode(subject_str),
                                        predicate: dict.encode(&resolved_predicate),
                                        object: dict.encode(&object),
                                    };
                                    drop(dict); // Release the lock
                                    triples.push(triple);
                                }
                            }
                        }
                    }
                    Ok(Event::End(ref e)) => {
                        if e.name() == QName(b"rdf:Description") {
                            current_subject.truncate(0);
                            current_predicate.truncate(0);
                        }
                    }
                    Ok(Event::Eof) => break,
                    Err(e) => {
                        eprintln!("Error reading XML: {:?}", e);
                        break;
                    }
                    _ => {}
                }

                if triples.len() >= 8192 {
                    sender.send(triples).unwrap();
                    triples = Vec::with_capacity(8192);
                }
            }

            if !triples.is_empty() {
                sender.send(triples).unwrap();
            }

            // Send termination signals
            for _ in 0..num_threads {
                sender.send(Vec::new()).unwrap();
            }
        })
        .unwrap();

        // Merge all BTreeSets into the main triples set
        let triples_sets = Arc::try_unwrap(triples_set).unwrap().into_inner().unwrap();
        for local_triples in triples_sets {
            self.triples.extend(local_triples);
        }

        // Update the main dictionary
        self.dictionary = Arc::try_unwrap(dictionary).unwrap().into_inner().unwrap();
    }

    // New parse_turtle function
    pub fn parse_turtle(&mut self, turtle_data: &str) {
        let lines = turtle_data.lines();

        for line in lines {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("#") {
                continue;
            }

            // Parse triples
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                let subject = parts[0].trim_end_matches('.').to_string();
                let predicate = parts[1].trim_end_matches('.').to_string();
                let object = parts[2..].join(" ").trim_end_matches('.').to_string();

                // Clean up object by removing quotes and trimming whitespace
                let cleaned_object = object.trim().trim_matches('"').to_string();

                let triple = Triple {
                    subject: self.dictionary.encode(&subject),
                    predicate: self.dictionary.encode(&predicate),
                    object: self.dictionary.encode(&cleaned_object),
                };
                self.triples.insert(triple);
            } else {
                eprintln!("Skipping invalid line: {}", line);
            }
        }
    }

    // New parse_n3 function
    pub fn parse_n3(&mut self, n3_data: &str) {
        let lines: Vec<String> = n3_data.lines().map(|l| l.trim().to_string()).collect();
        let chunk_size = 1000;
        let chunks: Vec<Vec<String>> = lines
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();
    
        let partial_results: Vec<(BTreeSet<Triple>, Dictionary, HashMap<String, String>)> =
            chunks.par_iter().map(|chunk| {
                let mut local_db = SparqlDatabase::new();
                let mut statement = String::new();
    
                for raw_line in chunk {
                    let mut line = raw_line.as_str();
                    if let Some(comment_start) = line.find('#') {
                        line = &line[..comment_start];
                        line = line.trim();
                    }
                    if line.is_empty() {
                        continue;
                    }
                    if line.starts_with("@prefix") {
                        let line = line.trim_start_matches("@prefix").trim_end_matches('.');
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            let prefix = parts[0].trim_end_matches(':').to_string();
                            let uri = parts[1].trim_start_matches('<').trim_end_matches('>').to_string();
                            local_db.prefixes.insert(prefix, uri);
                        } else {
                            eprintln!("Invalid prefix declaration: {}", line);
                        }
                    } else {
                        statement.push_str(line);
                        statement.push(' ');
                        if line.ends_with('.') {
                            local_db.parse_statement(statement.trim());
                            statement.clear();
                        }
                    }
                }
    
                (local_db.triples, local_db.dictionary, local_db.prefixes)
            }).collect();
    
        for (triples, dict, pref) in partial_results {
            for t in triples {
                self.triples.insert(t);
            }
            self.dictionary.merge(&dict);
            for (k, v) in pref {
                self.prefixes.insert(k, v);
            }
        }
    }

    fn parse_statement(&mut self, statement: &str) {
        let mut tokens = statement.split_whitespace().peekable();
        let mut subject = String::new();
        let mut predicate = String::new();
        let mut current_state = "subject";

        while let Some(token) = tokens.next() {
            match token {
                ";" => {
                    predicate.clear();
                    current_state = "predicate";
                }
                "." => {
                    // End of statement
                    break;
                }
                _ => match current_state {
                    "subject" => {
                        subject = token.to_string();
                        current_state = "predicate";
                    }
                    "predicate" => {
                        predicate = token.to_string();
                        current_state = "object";
                    }
                    "object" => {
                        let mut object = token.to_string();

                        // Collect tokens until we reach ';', '.', or ','
                        while let Some(next_token) = tokens.peek() {
                            if *next_token == ";" || *next_token == "." || *next_token == "," {
                                break;
                            }
                            // Consume the token
                            let next_token = tokens.next().unwrap();
                            object.push(' ');
                            object.push_str(next_token);
                        }

                        // Resolve terms and store the triple
                        let resolved_subject = self.resolve_term(&subject);
                        let resolved_predicate = self.resolve_term(&predicate);
                        let resolved_object = self.resolve_term(&object);

                        let triple = Triple {
                            subject: self.dictionary.encode(&resolved_subject),
                            predicate: self.dictionary.encode(&resolved_predicate),
                            object: self.dictionary.encode(&resolved_object),
                        };
                        self.triples.insert(triple);

                        current_state = "predicate";
                    }
                    _ => {}
                },
            }
        }
    }

    fn resolve_term(&self, term: &str) -> String {
        if term.starts_with('<') && term.ends_with('>') {
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        } else if term.starts_with('"') {
            // It's a literal, possibly with a datatype or language tag
            if let Some(pos) = term.rfind('"') {
                let literal = &term[..=pos]; // Include the closing quote
                let rest = &term[pos + 1..]; // After the closing quote
                let mut result = literal.to_string();
                if rest.starts_with("^^") {
                    // It's a typed literal
                    let datatype = rest[2..].trim();
                    let resolved_datatype = self.resolve_term(datatype);
                    result.push_str("^^");
                    result.push_str(&resolved_datatype);
                } else if rest.starts_with('@') {
                    // It's a language-tagged literal
                    result.push_str(rest);
                }
                result
            } else {
                // Malformed literal
                term.to_string()
            }
        } else if term.contains(':')
            && !term.starts_with("http://")
            && !term.starts_with("https://")
        {
            let mut parts = term.splitn(2, ':');
            let prefix = parts.next().unwrap();
            let local_name = parts.next().unwrap_or("");
            if let Some(uri) = self.prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } else {
                eprintln!("Unknown prefix: {}", prefix);
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    pub fn resolve_query_term(&self, term: &str, prefixes: &HashMap<String, String>) -> String {
        if term.starts_with('<') && term.ends_with('>') {
            term.trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        } else if term.starts_with('"') && term.ends_with('"') {
            term.trim_matches('"').to_string()
        } else if term.contains(':')
            && !term.starts_with("http://")
            && !term.starts_with("https://")
        {
            let mut parts = term.splitn(2, ':');
            let prefix = parts.next().unwrap();
            let local_name = parts.next().unwrap_or("");
            
            // First check the passed prefixes map
            if let Some(uri) = prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } 
            // Then check the database's own prefixes map as a fallback
            else if let Some(uri) = self.prefixes.get(prefix) {
                format!("{}{}", uri, local_name)
            } else {
                eprintln!("Unknown prefix in query: {}", prefix);
                term.to_string()
            }
        } else {
            term.to_string()
        }
    }

    pub fn add_stream_data(&mut self, triple: Triple, timestamp: u64) {
        self.streams.push(TimestampedTriple { triple, timestamp });
    }

    pub fn time_based_window(&self, start: u64, end: u64) -> BTreeSet<Triple> {
        self.streams
            .iter()
            .filter(|ts_triple| ts_triple.timestamp >= start && ts_triple.timestamp <= end)
            .map(|ts_triple| ts_triple.triple.clone())
            .collect()
    }

    pub fn filter<F>(&self, predicate: F) -> Self
    where
        F: Fn(&&Triple) -> bool,
    {
        let filtered_triples = self.triples.iter().filter(predicate).cloned().collect();
        Self {
            triples: filtered_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn filter_by_value(&mut self, value: &str) -> Self {
        let value_id = self.dictionary.encode(value);
        let filtered_triples = self
            .triples
            .iter()
            .filter(|triple| triple.object == value_id)
            .cloned()
            .collect();
        Self {
            triples: filtered_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn apply_filters_simd<'a>(
        &self,
        results: Vec<BTreeMap<&'a str, String>>,
        filters: Vec<FilterExpression<'a>>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        results
            .into_iter()
            .filter(|result| {
                filters.iter().all(|filter_expr| {
                    match filter_expr {
                        FilterExpression::Comparison(var, operator, value) => {
                            if let Some(var_value_str) = result.get(var) {
                                // First, try parsing both values as numbers
                                let var_value_num = var_value_str.parse::<i32>();
                                let filter_value_num = value.parse::<i32>();

                                if var_value_num.is_ok() && filter_value_num.is_ok() {
                                    // Both values are numeric, perform SIMD numeric comparison
                                    let var_value = var_value_num.unwrap();
                                    let filter_value = filter_value_num.unwrap();

                                    // On x86 (SSE2) or x86_64 (SSE2) use SIMD intrinsics
                                    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
                                    {
                                        unsafe {
                                            // Load values into SIMD registers
                                            let var_simd = _mm_set1_epi32(var_value);
                                            let filter_simd = _mm_set1_epi32(filter_value);
                                            return match *operator {
                                                "=" => _mm_movemask_epi8(_mm_cmpeq_epi32(
                                                    var_simd,
                                                    filter_simd,
                                                )) == 0xFFFF,
                                                "!=" => _mm_movemask_epi8(_mm_cmpeq_epi32(
                                                    var_simd,
                                                    filter_simd,
                                                )) != 0xFFFF,
                                                ">" => _mm_movemask_epi8(_mm_cmpgt_epi32(
                                                    var_simd,
                                                    filter_simd,
                                                )) == 0xFFFF,
                                                ">=" => {
                                                    let eq = _mm_cmpeq_epi32(var_simd, filter_simd);
                                                    let gt = _mm_cmpgt_epi32(var_simd, filter_simd);
                                                    _mm_movemask_epi8(_mm_or_si128(eq, gt)) == 0xFFFF
                                                }
                                                "<" => _mm_movemask_epi8(_mm_cmpgt_epi32(
                                                    filter_simd,
                                                    var_simd,
                                                )) == 0xFFFF,
                                                "<=" => {
                                                    let eq = _mm_cmpeq_epi32(var_simd, filter_simd);
                                                    let lt = _mm_cmpgt_epi32(filter_simd, var_simd);
                                                    _mm_movemask_epi8(_mm_or_si128(eq, lt)) == 0xFFFF
                                                }
                                                _ => false,
                                            };
                                        }
                                    }

                                    // On ARM (aarch64) use NEON intrinsics
                                    #[cfg(target_arch = "aarch64")]
                                    {
                                        unsafe {
                                            let var_neon = vdupq_n_s32(var_value);
                                            let filter_neon = vdupq_n_s32(filter_value);
                                            return match *operator {
                                                "=" => {
                                                    let cmp = vceqq_s32(var_neon, filter_neon);
                                                    (vgetq_lane_u32(cmp, 0) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 1) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 2) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 3) == 0xFFFFFFFF)
                                                }
                                                "!=" => {
                                                    let cmp = vceqq_s32(var_neon, filter_neon);
                                                    !((vgetq_lane_u32(cmp, 0) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 1) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 2) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 3) == 0xFFFFFFFF))
                                                }
                                                ">" => {
                                                    let cmp = vcgtq_s32(var_neon, filter_neon);
                                                    (vgetq_lane_u32(cmp, 0) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 1) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 2) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 3) == 0xFFFFFFFF)
                                                }
                                                ">=" => {
                                                    let eq = vceqq_s32(var_neon, filter_neon);
                                                    let gt = vcgtq_s32(var_neon, filter_neon);
                                                    let cmp = vorrq_u32(eq, gt);
                                                    (vgetq_lane_u32(cmp, 0) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 1) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 2) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 3) == 0xFFFFFFFF)
                                                }
                                                "<" => {
                                                    let cmp = vcgtq_s32(filter_neon, var_neon);
                                                    (vgetq_lane_u32(cmp, 0) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 1) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 2) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 3) == 0xFFFFFFFF)
                                                }
                                                "<=" => {
                                                    let eq = vceqq_s32(var_neon, filter_neon);
                                                    let lt = vcgtq_s32(filter_neon, var_neon);
                                                    let cmp = vorrq_u32(eq, lt);
                                                    (vgetq_lane_u32(cmp, 0) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 1) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 2) == 0xFFFFFFFF)
                                                        && (vgetq_lane_u32(cmp, 3) == 0xFFFFFFFF)
                                                }
                                                _ => false,
                                            }
                                        }
                                    }

                                    // Fallback (or if compiled for a non‐SIMD platform)
                                    #[cfg(not(any(
                                        target_arch = "x86",
                                        target_arch = "x86_64",
                                        target_arch = "aarch64"
                                    )))]
                                    {
                                        return match *operator {
                                            "=" => var_value == filter_value,
                                            "!=" => var_value != filter_value,
                                            ">" => var_value > filter_value,
                                            ">=" => var_value >= filter_value,
                                            "<" => var_value < filter_value,
                                            "<=" => var_value <= filter_value,
                                            _ => false,
                                        };
                                    }
                                } else {
                                    // At least one value is a string, perform string comparison
                                    let var_bytes = var_value_str.as_bytes();
                                    let filter_bytes = value.as_bytes();

                                    let var_len = var_bytes.len();
                                    let filter_len = filter_bytes.len();

                                    // If lengths differ, they can't be equal
                                    if var_len != filter_len {
                                        return match *operator {
                                            "=" => false,
                                            "!=" => true,
                                            _ => false, // Other operators are not supported for strings
                                        };
                                    }

                                    let mut i = 0;
                                    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
                                    {
                                        unsafe {
                                            while i + 16 <= var_len {
                                                let var_chunk = _mm_loadu_si128(
                                                    var_bytes[i..].as_ptr() as *const __m128i,
                                                );
                                                let filter_chunk = _mm_loadu_si128(
                                                    filter_bytes[i..].as_ptr() as *const __m128i,
                                                );
                                                let cmp = _mm_cmpeq_epi8(var_chunk, filter_chunk);
                                                let mask = _mm_movemask_epi8(cmp);
                                                if mask != 0xFFFF {
                                                    return match *operator {
                                                        "=" => false,
                                                        "!=" => true,
                                                        _ => false,
                                                    };
                                                }
                                                i += 16;
                                            }
                                        }
                                    }

                                    #[cfg(target_arch = "aarch64")]
                                    {
                                        unsafe {
                                            while i + 16 <= var_len {
                                                let var_chunk = vld1q_u8(var_bytes[i..].as_ptr());
                                                let filter_chunk = vld1q_u8(filter_bytes[i..].as_ptr());
                                                let cmp = vceqq_u8(var_chunk, filter_chunk);
                                                let cmp_arr: [u8; 16] = std::mem::transmute(cmp);
                                                if cmp_arr.iter().any(|&lane| lane != 0xFF) {
                                                    return match *operator {
                                                        "=" => false,
                                                        "!=" => true,
                                                        _ => false,
                                                    };
                                                }
                                                i += 16;
                                            }
                                        }
                                    }

                                    // Handle remaining bytes
                                    if i < var_len {
                                        for j in i..var_len {
                                            if var_bytes[j] != filter_bytes[j] {
                                                return match *operator {
                                                    "=" => false,
                                                    "!=" => true,
                                                    _ => false,
                                                };
                                            }
                                        }
                                    }

                                    // Strings are equal
                                    match *operator {
                                        "=" => true,
                                        "!=" => false,
                                        _ => false, // Other operators not supported for strings
                                    }
                                }
                            } else {
                                false
                            }
                        },
                        FilterExpression::And(left, right) => {
                            self.evaluate_filter_expression(result, left) && 
                            self.evaluate_filter_expression(result, right)
                        },
                        FilterExpression::Or(left, right) => {
                            self.evaluate_filter_expression(result, left) || 
                            self.evaluate_filter_expression(result, right)
                        },
                        FilterExpression::Not(expr) => {
                            !self.evaluate_filter_expression(result, expr)
                        }
                    }
                })
            })
            .collect()
    }

    // Helper method to evaluate a filter expression against a result
    fn evaluate_filter_expression<'a>(
        &self,
        result: &BTreeMap<&'a str, String>,
        filter_expr: &FilterExpression<'a>
    ) -> bool {
        match filter_expr {
            FilterExpression::Comparison(var, operator, value) => {
                if let Some(var_value_str) = result.get(var) {
                    // Similar to your existing comparison logic, but without SIMD for simplicity
                    let var_value_num = var_value_str.parse::<i32>();
                    let filter_value_num = value.parse::<i32>();

                    if var_value_num.is_ok() && filter_value_num.is_ok() {
                        // Both values are numeric
                        let var_value = var_value_num.unwrap();
                        let filter_value = filter_value_num.unwrap();
                        
                        match *operator {
                            "=" => var_value == filter_value,
                            "!=" => var_value != filter_value,
                            ">" => var_value > filter_value,
                            ">=" => var_value >= filter_value,
                            "<" => var_value < filter_value,
                            "<=" => var_value <= filter_value,
                            _ => false,
                        }
                    } else {
                        // At least one value is a string
                        match *operator {
                            "=" => var_value_str == value,
                            "!=" => var_value_str != value,
                            _ => false, // Other operators not supported for strings
                        }
                    }
                } else {
                    false
                }
            },
            FilterExpression::And(left, right) => {
                self.evaluate_filter_expression(result, left) && 
                self.evaluate_filter_expression(result, right)
            },
            FilterExpression::Or(left, right) => {
                self.evaluate_filter_expression(result, left) || 
                self.evaluate_filter_expression(result, right)
            },
            FilterExpression::Not(expr) => {
                !self.evaluate_filter_expression(result, expr)
            }
        }
    }

    pub fn select_by_value(&mut self, value: &str) -> BTreeSet<Triple> {
        let mut results = BTreeSet::new();
        let mut subjects = BTreeSet::new();

        let value_id = self.dictionary.encode(value);

        // First, find all subjects where the object matches the specified value
        for triple in &self.triples {
            if triple.object == value_id {
                subjects.insert(triple.subject);
            }
        }

        // Then, find all triples related to the matched subjects
        for subject in subjects {
            for triple in &self.triples {
                if triple.subject == subject {
                    results.insert(triple.clone());
                }
            }
        }

        results
    }

    pub fn select_by_variable(
        &self,
        subject_var: Option<&str>,
        predicate_var: Option<&str>,
        object_var: Option<&str>,
    ) -> BTreeSet<Triple> {
        let mut results = BTreeSet::new();
        for triple in &self.triples {
            let subject_matches = match subject_var {
                Some(var) => self
                    .dictionary
                    .decode(triple.subject)
                    .map_or(false, |s| s.contains(var)),
                None => true,
            };
            let predicate_matches = match predicate_var {
                Some(var) => self
                    .dictionary
                    .decode(triple.predicate)
                    .map_or(false, |p| p.contains(var)),
                None => true,
            };
            let object_matches = match object_var {
                Some(var) => self
                    .dictionary
                    .decode(triple.object)
                    .map_or(false, |o| o.contains(var)),
                None => true,
            };

            if subject_matches && predicate_matches && object_matches {
                results.insert(triple.clone());
            }
        }
        results
    }

    pub fn distinct(&self) -> Self {
        // This will automatically ensure that only unique triples are retained
        let distinct_triples: BTreeSet<Triple> = self.triples.iter().cloned().collect();

        Self {
            triples: distinct_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn order_by<F>(&self, key: F) -> Vec<Triple>
    where
        F: Fn(&Triple) -> String,
    {
        let mut sorted_triples: Vec<Triple> = self.triples.iter().cloned().collect();
        sorted_triples.sort_by_key(key);
        sorted_triples
    }

    pub fn group_by<F>(&self, key: F) -> BTreeMap<String, Vec<Triple>>
    where
        F: Fn(&Triple) -> String,
    {
        let mut groups = BTreeMap::new();
        for triple in &self.triples {
            let group_key = key(triple);
            groups
                .entry(group_key)
                .or_insert_with(Vec::new)
                .push(triple.clone());
        }
        groups
    }

    pub fn union(&mut self, other: &SparqlDatabase) -> Self {
        // Create a new dictionary by merging the dictionaries of both databases
        let mut merged_dictionary = self.dictionary.clone();

        // Re-encode triples from the other database using the merged dictionary
        let mut re_encoded_triples = BTreeSet::new();
        for triple in &other.triples {
            let subject =
                merged_dictionary.encode(other.dictionary.decode(triple.subject).unwrap());
            let predicate =
                merged_dictionary.encode(other.dictionary.decode(triple.predicate).unwrap());
            let object = merged_dictionary.encode(other.dictionary.decode(triple.object).unwrap());
            re_encoded_triples.insert(Triple {
                subject,
                predicate,
                object,
            });
        }

        // Merge the triples and streams
        let union_triples: BTreeSet<Triple> =
            self.triples.union(&re_encoded_triples).cloned().collect();
        let mut union_streams = self.streams.clone();
        for ts_triple in &other.streams {
            let subject = merged_dictionary
                .encode(other.dictionary.decode(ts_triple.triple.subject).unwrap());
            let predicate = merged_dictionary
                .encode(other.dictionary.decode(ts_triple.triple.predicate).unwrap());
            let object =
                merged_dictionary.encode(other.dictionary.decode(ts_triple.triple.object).unwrap());
            let re_encoded_ts_triple = TimestampedTriple {
                triple: Triple {
                    subject,
                    predicate,
                    object,
                },
                timestamp: ts_triple.timestamp,
            };
            if !union_streams.contains(&re_encoded_ts_triple) {
                union_streams.push(re_encoded_ts_triple);
            }
        }

        Self {
            triples: union_triples,
            streams: union_streams,
            sliding_window: self.sliding_window.clone(),
            dictionary: merged_dictionary,
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn par_join(&mut self, other: &SparqlDatabase, predicate: &str) -> Self {
        let predicate_id = self.dictionary.encode(predicate);
        let other_map: BTreeMap<&u32, Vec<&Triple>> = other
            .triples
            .par_iter()
            .filter(|other_triple| other_triple.predicate == predicate_id)
            .flat_map(|other_triple| {
                vec![
                    (&other_triple.subject, other_triple),
                    (&other_triple.object, other_triple),
                ]
            })
            .fold(
                || BTreeMap::new(),
                |mut acc, (key, triple)| {
                    acc.entry(key).or_insert_with(Vec::new).push(triple);
                    acc
                },
            )
            .reduce(
                || BTreeMap::new(),
                |mut acc, map| {
                    for (key, triples) in map {
                        acc.entry(key).or_insert_with(Vec::new).extend(triples);
                    }
                    acc
                },
            );

        let joined_triples: BTreeSet<Triple> = self
            .triples
            .par_iter()
            .filter(|triple| triple.predicate == predicate_id)
            .fold(
                || BTreeSet::new(),
                |mut local_set, triple| {
                    if let Some(matching_triples) = other_map.get(&triple.object) {
                        for other_triple in matching_triples {
                            local_set.insert(Triple {
                                subject: triple.subject,
                                predicate: other_triple.predicate,
                                object: other_triple.object,
                            });
                        }
                    }
                    local_set
                },
            )
            .reduce(
                || BTreeSet::new(),
                |mut set1, set2| {
                    set1.extend(set2);
                    set1
                },
            );

        Self {
            triples: joined_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn join(&mut self, other: &SparqlDatabase, predicate: &str) -> Self {
        let predicate_id = self.dictionary.encode(predicate);
        let mut joined_triples = BTreeSet::new();

        let mut other_map = BTreeMap::new();
        for other_triple in &other.triples {
            if other_triple.predicate == predicate_id {
                other_map
                    .entry(&other_triple.subject)
                    .or_insert_with(Vec::new)
                    .push(other_triple);
                other_map
                    .entry(&other_triple.object)
                    .or_insert_with(Vec::new)
                    .push(other_triple);
            }
        }

        for triple in &self.triples {
            if triple.predicate == predicate_id {
                if let Some(matching_triples) = other_map.get(&triple.object) {
                    for other_triple in matching_triples {
                        joined_triples.insert(Triple {
                            subject: triple.subject,
                            predicate: other_triple.predicate,
                            object: other_triple.object,
                        });
                    }
                }
            }
        }

        Self {
            triples: joined_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn join_by_variable(
        &self,
        other: &SparqlDatabase,
        subject_var: Option<&str>,
        predicate_var: Option<&str>,
        object_var: Option<&str>,
    ) -> Self {
        let mut joined_triples = BTreeSet::new();

        let mut btree_map = BTreeMap::new();
        for other_triple in &other.triples {
            let key = (
                subject_var.map_or("", |_| {
                    &self.dictionary.decode(other_triple.subject).unwrap()
                }),
                predicate_var.map_or("", |_| {
                    &self.dictionary.decode(other_triple.predicate).unwrap()
                }),
                object_var.map_or("", |_| {
                    &self.dictionary.decode(other_triple.object).unwrap()
                }),
            );
            btree_map
                .entry(key)
                .or_insert_with(Vec::new)
                .push(other_triple);
        }

        self.triples.iter().for_each(|triple| {
            let key = (
                subject_var.map_or("", |_| &self.dictionary.decode(triple.subject).unwrap()),
                predicate_var.map_or("", |_| &self.dictionary.decode(triple.predicate).unwrap()),
                object_var.map_or("", |_| &self.dictionary.decode(triple.object).unwrap()),
            );
            if let Some(matching_triples) = btree_map.get(&key) {
                for other_triple in matching_triples {
                    joined_triples.insert(Triple {
                        subject: triple.subject,
                        predicate: other_triple.predicate,
                        object: other_triple.object,
                    });
                }
            }
        });

        Self {
            triples: joined_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn perform_join<'a>(
        &self,
        subject_var: &'a str,
        predicate: &'a str,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        let mut new_results = Vec::new();

        for triple in triples {
            let subject = dictionary.decode(triple.subject).unwrap();
            let pred = dictionary.decode(triple.predicate).unwrap();
            let object = dictionary.decode(triple.object).unwrap();

            if pred == predicate {
                for result in &final_results {
                    let mut extended_result = result.clone();
                    let mut valid_extension = true;

                    // Check and extend the result with the subject
                    if let Some(existing_subject) = extended_result.get(subject_var) {
                        if existing_subject != &subject {
                            valid_extension = false;
                        }
                    } else {
                        extended_result.insert(subject_var, subject.to_string());
                    }

                    // Check and extend the result with the object
                    if let Some(existing_object) = extended_result.get(object_var) {
                        if existing_object != &object {
                            valid_extension = false;
                        }
                    } else {
                        extended_result.insert(object_var, object.to_string());
                    }

                    if valid_extension {
                        new_results.push(extended_result);
                    }
                }
            }
        }

        new_results
    }

    pub fn perform_join_par_simd_with_strict_filter_1<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        if final_results.is_empty() {
            return Vec::new();
        }

        let predicate_bytes = predicate.as_bytes();
        let literal_filter_bytes = literal_filter.as_ref().map(|s| s.as_bytes());

        // Partition final_results into groups based on variable bindings
        let mut both_vars_bound: HashMap<(String, String), Vec<BTreeMap<&'a str, String>>> =
            HashMap::new();
        let mut subject_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut object_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut neither_var_bound: Vec<BTreeMap<&'a str, String>> = Vec::new();

        for result in final_results {
            let subject_binding = result.get(subject_var).cloned();
            let object_binding = result.get(object_var).cloned();

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_default()
                        .push(result);
                }
                (Some(subj_val), None) => {
                    subject_var_bound
                        .entry(subj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, Some(obj_val)) => {
                    object_var_bound
                        .entry(obj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, None) => {
                    neither_var_bound.push(result);
                }
            }
        }

        // Pre-allocate output vector
        let results = Mutex::new(Vec::new());

        // Using Rayon for parallel processing
        triples.par_chunks(256).for_each(|chunk| {
            let mut local_results = Vec::new();

            for triple in chunk {
                if let (Some(subject), Some(pred), Some(object)) = (
                    dictionary.decode(triple.subject),
                    dictionary.decode(triple.predicate),
                    dictionary.decode(triple.object),
                ) {
                    // SIMD predicate comparison
                    if pred.as_bytes() != predicate_bytes {
                        continue;
                    }

                    // SIMD literal filter comparison
                    if let Some(filter_bytes) = literal_filter_bytes {
                        if object.as_bytes() != filter_bytes {
                            continue;
                        }
                    }

                    // Process group both_vars_bound
                    {
                        let key = (subject.to_string(), object.to_string());
                        if let Some(results_vec) = both_vars_bound.get(&key) {
                            for result in results_vec {
                                let extended_result = result.clone();
                                local_results.push(extended_result);
                            }
                        }
                    }

                    // Process group subject_var_bound
                    {
                        if let Some(results_vec) = subject_var_bound.get(subject) {
                            for result in results_vec {
                                let mut extended_result = result.clone();
                                // Extend object_var
                                if let Some(existing_object) = extended_result.get(object_var) {
                                    if existing_object != &object {
                                        continue; // Inconsistent variable binding
                                    }
                                } else {
                                    extended_result.insert(object_var, object.to_string());
                                }
                                local_results.push(extended_result);
                            }
                        }
                    }

                    // Process group object_var_bound
                    {
                        if let Some(results_vec) = object_var_bound.get(object) {
                            for result in results_vec {
                                let mut extended_result = result.clone();
                                // Extend subject_var
                                if let Some(existing_subject) = extended_result.get(subject_var) {
                                    if existing_subject != &subject {
                                        continue; // Inconsistent variable binding
                                    }
                                } else {
                                    extended_result.insert(subject_var, subject.to_string());
                                }
                                local_results.push(extended_result);
                            }
                        }
                    }

                    // Process group neither_var_bound
                    for result in &neither_var_bound {
                        let mut extended_result = result.clone();
                        // Extend subject_var
                        if let Some(existing_subject) = extended_result.get(subject_var) {
                            if existing_subject != &subject {
                                continue; // Inconsistent variable binding
                            }
                        } else {
                            extended_result.insert(subject_var, subject.to_string());
                        }
                        // Extend object_var
                        if let Some(existing_object) = extended_result.get(object_var) {
                            if existing_object != &object {
                                continue; // Inconsistent variable binding
                            }
                        } else {
                            extended_result.insert(object_var, object.to_string());
                        }
                        local_results.push(extended_result);
                    }
                }
            }

            // Push local results to the shared results vector
            let mut global_results = results.lock().unwrap();
            global_results.extend(local_results);
        });

        results.into_inner().unwrap()
    }

    pub fn perform_join_par_simd_with_strict_filter_2<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        if final_results.is_empty() {
            return Vec::new();
        }

        let predicate_bytes = predicate.as_bytes();
        let literal_filter_bytes = literal_filter.as_ref().map(|s| s.as_bytes());

        // Partition final_results into groups based on variable bindings.
        let mut both_vars_bound: HashMap<(String, String), Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut subject_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut object_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut neither_var_bound: Vec<BTreeMap<&'a str, String>> = Vec::new();

        for result in final_results {
            let subject_binding = result.get(subject_var).cloned();
            let object_binding = result.get(object_var).cloned();

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_default()
                        .push(result);
                }
                (Some(subj_val), None) => {
                    subject_var_bound.entry(subj_val.clone()).or_default().push(result);
                }
                (None, Some(obj_val)) => {
                    object_var_bound.entry(obj_val.clone()).or_default().push(result);
                }
                (None, None) => {
                    neither_var_bound.push(result);
                }
            }
        }

        // Pre-allocate output vector.
        let results = Mutex::new(Vec::new());

        // Using Rayon for parallel processing.
        triples.par_chunks(256).for_each(|chunk| {
            let mut local_results = Vec::new();

            for triple in chunk {
                if let (Some(subject), Some(pred), Some(object)) = (
                    dictionary.decode(triple.subject),
                    dictionary.decode(triple.predicate),
                    dictionary.decode(triple.object),
                ) {
                    // SIMD predicate comparison using simd_eq.
                    if !unsafe { simd_eq(pred.as_bytes(), predicate_bytes) } {
                        continue;
                    }

                    // SIMD literal filter comparison.
                    if let Some(filter_bytes) = literal_filter_bytes {
                        if !unsafe { simd_eq(object.as_bytes(), filter_bytes) } {
                            continue;
                        }
                    }

                    // Process group both_vars_bound.
                    {
                        let key = (subject.to_string(), object.to_string());
                        if let Some(results_vec) = both_vars_bound.get(&key) {
                            for result in results_vec {
                                local_results.push(result.clone());
                            }
                        }
                    }

                    // Process group subject_var_bound.
                    {
                        if let Some(results_vec) = subject_var_bound.get(subject) {
                            for result in results_vec {
                                let mut extended_result = result.clone();
                                // Extend object_var.
                                if let Some(existing_object) = extended_result.get(object_var) {
                                    if existing_object != &object {
                                        continue; // Inconsistent variable binding.
                                    }
                                } else {
                                    extended_result.insert(object_var, object.to_string());
                                }
                                local_results.push(extended_result);
                            }
                        }
                    }

                    // Process group object_var_bound.
                    {
                        if let Some(results_vec) = object_var_bound.get(object) {
                            for result in results_vec {
                                let mut extended_result = result.clone();
                                // Extend subject_var.
                                if let Some(existing_subject) = extended_result.get(subject_var) {
                                    if existing_subject != &subject {
                                        continue; // Inconsistent variable binding.
                                    }
                                } else {
                                    extended_result.insert(subject_var, subject.to_string());
                                }
                                local_results.push(extended_result);
                            }
                        }
                    }

                    // Process group neither_var_bound.
                    for result in &neither_var_bound {
                        let mut extended_result = result.clone();
                        // Extend subject_var.
                        if let Some(existing_subject) = extended_result.get(subject_var) {
                            if existing_subject != &subject {
                                continue; // Inconsistent variable binding.
                            }
                        } else {
                            extended_result.insert(subject_var, subject.to_string());
                        }
                        // Extend object_var.
                        if let Some(existing_object) = extended_result.get(object_var) {
                            if existing_object != &object {
                                continue; // Inconsistent variable binding.
                            }
                        } else {
                            extended_result.insert(object_var, object.to_string());
                        }
                        local_results.push(extended_result);
                    }
                }
            }

            // Push local results to the shared results vector.
            let mut global_results = results.lock().unwrap();
            global_results.extend(local_results);
        });

        results.into_inner().unwrap()
    }

    pub fn perform_join_sequential<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        if final_results.is_empty() {
            return Vec::new();
        }

        let predicate_bytes = predicate.as_bytes();
        let literal_filter_bytes = literal_filter.as_ref().map(|s| s.as_bytes());

        // Partition final_results into groups based on variable bindings.
        let mut both_vars_bound: HashMap<(String, String), Vec<BTreeMap<&'a str, String>>> =
            HashMap::new();
        let mut subject_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut object_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut neither_var_bound: Vec<BTreeMap<&'a str, String>> = Vec::new();

        for result in final_results {
            let subject_binding = result.get(subject_var).cloned();
            let object_binding = result.get(object_var).cloned();

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_default()
                        .push(result);
                }
                (Some(subj_val), None) => {
                    subject_var_bound
                        .entry(subj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, Some(obj_val)) => {
                    object_var_bound
                        .entry(obj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, None) => {
                    neither_var_bound.push(result);
                }
            }
        }

        let mut results = Vec::new();

        // Process triples sequentially.
        for triple in triples {
            if let (Some(subject), Some(pred), Some(object)) = (
                dictionary.decode(triple.subject),
                dictionary.decode(triple.predicate),
                dictionary.decode(triple.object),
            ) {
                // Check if the predicate matches.
                if pred.as_bytes() != predicate_bytes {
                    continue;
                }

                // Check the literal filter if provided.
                if let Some(filter_bytes) = literal_filter_bytes {
                    if object.as_bytes() != filter_bytes {
                        continue;
                    }
                }

                // Process group where both variables are already bound.
                {
                    let key = (subject.to_string(), object.to_string());
                    if let Some(results_vec) = both_vars_bound.get(&key) {
                        for result in results_vec {
                            results.push(result.clone());
                        }
                    }
                }

                // Process group where only subject_var is bound.
                {
                    if let Some(results_vec) = subject_var_bound.get(subject) {
                        for result in results_vec {
                            let mut extended_result = result.clone();
                            // Extend the object_var binding.
                            if let Some(existing_object) = extended_result.get(object_var) {
                                if existing_object != &object {
                                    continue; // Inconsistent variable binding.
                                }
                            } else {
                                extended_result.insert(object_var, object.to_string());
                            }
                            results.push(extended_result);
                        }
                    }
                }

                // Process group where only object_var is bound.
                {
                    if let Some(results_vec) = object_var_bound.get(object) {
                        for result in results_vec {
                            let mut extended_result = result.clone();
                            // Extend the subject_var binding.
                            if let Some(existing_subject) = extended_result.get(subject_var) {
                                if existing_subject != &subject {
                                    continue; // Inconsistent variable binding.
                                }
                            } else {
                                extended_result.insert(subject_var, subject.to_string());
                            }
                            results.push(extended_result);
                        }
                    }
                }

                // Process group where neither variable is bound.
                for result in &neither_var_bound {
                    let mut extended_result = result.clone();
                    // Extend the subject_var binding.
                    if let Some(existing_subject) = extended_result.get(subject_var) {
                        if existing_subject != &subject {
                            continue; // Inconsistent variable binding.
                        }
                    } else {
                        extended_result.insert(subject_var, subject.to_string());
                    }
                    // Extend the object_var binding.
                    if let Some(existing_object) = extended_result.get(object_var) {
                        if existing_object != &object {
                            continue; // Inconsistent variable binding.
                        }
                    } else {
                        extended_result.insert(object_var, object.to_string());
                    }
                    results.push(extended_result);
                }
            }
        }

        results
    }

    pub fn perform_join_sequential_simd<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        if final_results.is_empty() {
            return Vec::new();
        }

        let predicate_bytes = predicate.as_bytes();
        let literal_filter_bytes = literal_filter.as_ref().map(|s| s.as_bytes());

        // Partition final_results into groups based on variable bindings.
        let mut both_vars_bound: HashMap<(String, String), Vec<BTreeMap<&'a str, String>>> =
            HashMap::new();
        let mut subject_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut object_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut neither_var_bound: Vec<BTreeMap<&'a str, String>> = Vec::new();

        for result in final_results {
            let subject_binding = result.get(subject_var).cloned();
            let object_binding = result.get(object_var).cloned();

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_default()
                        .push(result);
                }
                (Some(subj_val), None) => {
                    subject_var_bound
                        .entry(subj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, Some(obj_val)) => {
                    object_var_bound
                        .entry(obj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, None) => {
                    neither_var_bound.push(result);
                }
            }
        }

        let mut results = Vec::new();

        // Process triples sequentially.
        for triple in triples {
            if let (Some(subject), Some(pred), Some(object)) = (
                dictionary.decode(triple.subject),
                dictionary.decode(triple.predicate),
                dictionary.decode(triple.object),
            ) {
                // Use SIMD-based comparison for the predicate.
                if !simd_bytes_eq(pred.as_bytes(), predicate_bytes) {
                    continue;
                }

                // Use SIMD-based comparison for the literal filter if provided.
                if let Some(filter_bytes) = literal_filter_bytes {
                    if !simd_bytes_eq(object.as_bytes(), filter_bytes) {
                        continue;
                    }
                }

                // Process group where both variables are already bound.
                {
                    let key = (subject.to_string(), object.to_string());
                    if let Some(results_vec) = both_vars_bound.get(&key) {
                        for result in results_vec {
                            results.push(result.clone());
                        }
                    }
                }

                // Process group where only subject_var is bound.
                {
                    if let Some(results_vec) = subject_var_bound.get(subject) {
                        for result in results_vec {
                            let mut extended_result = result.clone();
                            // Extend the object_var binding.
                            if let Some(existing_object) = extended_result.get(object_var) {
                                if existing_object != &object {
                                    continue; // Inconsistent variable binding.
                                }
                            } else {
                                extended_result.insert(object_var, object.to_string());
                            }
                            results.push(extended_result);
                        }
                    }
                }

                // Process group where only object_var is bound.
                {
                    if let Some(results_vec) = object_var_bound.get(object) {
                        for result in results_vec {
                            let mut extended_result = result.clone();
                            // Extend the subject_var binding.
                            if let Some(existing_subject) = extended_result.get(subject_var) {
                                if existing_subject != &subject {
                                    continue; // Inconsistent variable binding.
                                }
                            } else {
                                extended_result.insert(subject_var, subject.to_string());
                            }
                            results.push(extended_result);
                        }
                    }
                }

                // Process group where neither variable is bound.
                for result in &neither_var_bound {
                    let mut extended_result = result.clone();
                    // Extend the subject_var binding.
                    if let Some(existing_subject) = extended_result.get(subject_var) {
                        if existing_subject != &subject {
                            continue; // Inconsistent variable binding.
                        }
                    } else {
                        extended_result.insert(subject_var, subject.to_string());
                    }
                    // Extend the object_var binding.
                    if let Some(existing_object) = extended_result.get(object_var) {
                        if existing_object != &object {
                            continue; // Inconsistent variable binding.
                        }
                    } else {
                        extended_result.insert(object_var, object.to_string());
                    }
                    results.push(extended_result);
                }
            }
        }

        results
    }

    pub fn perform_join_par_simd_with_strict_filter_3<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        // Early return for empty joins
        if final_results.is_empty() {
            return Vec::new();
        }

        // Pre-fetch predicate and filter bytes to avoid string comparisons
        let predicate_bytes = predicate.as_bytes();
        let literal_filter_bytes = literal_filter.as_ref().map(|s| s.as_bytes());

        // Preallocate with capacity estimation to avoid rehashing
        let estimated_capacity = (final_results.len() / 4).max(HASHMAP_INITIAL_CAPACITY);
        
        // Use with_capacity to preallocate hashmap space
        let mut both_vars_bound: HashMap<(String, String), Vec<usize>> = 
            HashMap::with_capacity(estimated_capacity);
        let mut subject_var_bound: HashMap<String, Vec<usize>> = 
            HashMap::with_capacity(estimated_capacity);
        let mut object_var_bound: HashMap<String, Vec<usize>> = 
            HashMap::with_capacity(estimated_capacity);
        let mut neither_var_bound: Vec<usize> = Vec::with_capacity(final_results.len() / 2);

        // Pre-compute and classify bindings - this is serial but much faster than doing it in parallel
        for (idx, result) in final_results.iter().enumerate() {
            let subject_binding = result.get(subject_var);
            let object_binding = result.get(object_var);

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_insert_with(|| Vec::with_capacity(4))
                        .push(idx);
                }
                (Some(subj_val), None) => {
                    subject_var_bound
                        .entry(subj_val.clone())
                        .or_insert_with(|| Vec::with_capacity(8))
                        .push(idx);
                }
                (None, Some(obj_val)) => {
                    object_var_bound
                        .entry(obj_val.clone())
                        .or_insert_with(|| Vec::with_capacity(8))
                        .push(idx);
                }
                (None, None) => {
                    neither_var_bound.push(idx);
                }
            }
        }

        // Immutable shared references for threading
        let final_results_arc = Arc::new(final_results);
        let both_vars_bound_arc = Arc::new(both_vars_bound);
        let subject_var_bound_arc = Arc::new(subject_var_bound);
        let object_var_bound_arc = Arc::new(object_var_bound);
        let neither_var_bound_arc = Arc::new(neither_var_bound);

        // Calculate optimal chunk size based on available processors and dataset size
        let chunk_size = (triples.len() / rayon::current_num_threads()).max(MIN_CHUNK_SIZE);
        
        // Process triples in chunks for better cache locality and load balancing
        let results = triples
            .par_chunks(chunk_size)
            .flat_map(|triple_chunk| {
                // Preallocate result vector for this chunk based on estimated hit rate
                let mut local_results = Vec::with_capacity(triple_chunk.len() / 4);
                
                // Process each triple in the chunk
                for triple in triple_chunk {
                    // Step 1: Quick predicate check first (early filter)
                    let pred_opt = dictionary.decode(triple.predicate);
                    if pred_opt.is_none() || pred_opt.as_ref().unwrap().as_bytes() != predicate_bytes {
                        continue;
                    }
                    
                    // Step 2: Filter check if needed
                    if let Some(filter_bytes) = &literal_filter_bytes {
                        let obj_opt = dictionary.decode(triple.object);
                        if obj_opt.is_none() || obj_opt.as_ref().unwrap().as_bytes() != *filter_bytes {
                            continue;
                        }
                        
                        // Decode subject only if predicate and object pass filters
                        if let Some(subj) = dictionary.decode(triple.subject) {
                            process_join(
                                &subj,
                                obj_opt.unwrap(),
                                subject_var,
                                object_var,
                                &both_vars_bound_arc,
                                &subject_var_bound_arc,
                                &object_var_bound_arc,
                                &neither_var_bound_arc,
                                &final_results_arc,
                                &mut local_results,
                            );
                        }
                    } else {
                        // No filter - decode both subject and object
                        let subj_opt = dictionary.decode(triple.subject);
                        let obj_opt = dictionary.decode(triple.object);
                        
                        if let (Some(subj), Some(obj)) = (subj_opt, obj_opt) {
                            process_join(
                                &subj,
                                &obj,
                                subject_var,
                                object_var,
                                &both_vars_bound_arc,
                                &subject_var_bound_arc,
                                &object_var_bound_arc,
                                &neither_var_bound_arc,
                                &final_results_arc,
                                &mut local_results,
                            );
                        }
                    }
                }
                
                local_results
            })
            .collect();

        results
    }

    pub fn perform_join_par_simd_with_strict_filter_4<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        // Early return for empty joins
        if final_results.is_empty() {
            return Vec::new();
        }

        // Pre-fetch predicate and filter bytes to avoid string comparisons
        let predicate_bytes = predicate.as_bytes();
        let literal_filter_bytes = literal_filter.as_ref().map(|s| s.as_bytes());

        let estimated_capacity = (final_results.len() / 3).max(HASHMAP_INITIAL_CAPACITY1);
        
        let mut both_vars_bound: HashMap<(String, String), Vec<usize>> = 
            HashMap::with_capacity(estimated_capacity / 2);  // This tends to be smaller
        let mut subject_var_bound: HashMap<String, Vec<usize>> = 
            HashMap::with_capacity(estimated_capacity);
        let mut object_var_bound: HashMap<String, Vec<usize>> = 
            HashMap::with_capacity(estimated_capacity);
        let mut neither_var_bound: Vec<usize> = Vec::with_capacity(final_results.len() / 2);

        // Pre-compute and classify bindings - this is serial but much faster than doing it in parallel
        for (idx, result) in final_results.iter().enumerate() {
            let subject_binding = result.get(subject_var);
            let object_binding = result.get(object_var);

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_insert_with(|| Vec::with_capacity(4))
                        .push(idx);
                }
                (Some(subj_val), None) => {
                    subject_var_bound
                        .entry(subj_val.clone())
                        .or_insert_with(|| Vec::with_capacity(8))
                        .push(idx);
                }
                (None, Some(obj_val)) => {
                    object_var_bound
                        .entry(obj_val.clone())
                        .or_insert_with(|| Vec::with_capacity(8))
                        .push(idx);
                }
                (None, None) => {
                    neither_var_bound.push(idx);
                }
            }
        }

        // Immutable shared references for threading
        let final_results_arc = Arc::new(final_results);
        let both_vars_bound_arc = Arc::new(both_vars_bound);
        let subject_var_bound_arc = Arc::new(subject_var_bound);
        let object_var_bound_arc = Arc::new(object_var_bound);
        let neither_var_bound_arc = Arc::new(neither_var_bound);

        let chunk_size = ((triples.len() / rayon::current_num_threads()) * 3 / 2).max(MIN_CHUNK_SIZE1);
        
        let results = triples
            .par_chunks(chunk_size)
            .fold(
                || Vec::with_capacity(chunk_size / 4),  // Local vector capacity based on chunk size
                |mut local_results, triple_chunk| {
                    // Create a local result buffer
                    process_triple_chunk(
                        triple_chunk,
                        predicate_bytes,
                        &literal_filter_bytes,
                        subject_var,
                        object_var,
                        &both_vars_bound_arc,
                        &subject_var_bound_arc,
                        &object_var_bound_arc,
                        &neither_var_bound_arc,
                        &final_results_arc,
                        &mut local_results,
                        dictionary,
                    );
                    
                    local_results
                },
            )
            .reduce(
                || Vec::new(),
                |mut acc, mut chunk| {
                    if acc.is_empty() {
                        return chunk;
                    }
                    if chunk.is_empty() {
                        return acc;
                    }
                    
                    // Pre-allocate to avoid reallocation during append
                    if acc.capacity() < acc.len() + chunk.len() {
                        acc.reserve(chunk.len());
                    }
                    acc.append(&mut chunk);
                    acc
                },
            );

        results
    }

    pub fn join_by_conditions_with_filters<F>(
        &self,
        other: &SparqlDatabase,
        conditions: Vec<(Option<&str>, Option<&str>, Option<&str>)>,
        filter: F,
    ) -> Self
    where
        F: Fn(&Triple, &Triple) -> bool,
    {
        let mut joined_triples = BTreeSet::new();

        for condition in &conditions {
            let mut btree_map = BTreeMap::new();
            for other_triple in &other.triples {
                let key = (
                    condition.0.map_or("", |_| {
                        &self.dictionary.decode(other_triple.subject).unwrap()
                    }),
                    condition.1.map_or("", |_| {
                        &self.dictionary.decode(other_triple.predicate).unwrap()
                    }),
                    condition.2.map_or("", |_| {
                        &self.dictionary.decode(other_triple.object).unwrap()
                    }),
                );
                btree_map
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(other_triple.clone());
            }

            self.triples.iter().for_each(|triple| {
                let key = (
                    condition
                        .0
                        .map_or("", |_| &self.dictionary.decode(triple.subject).unwrap()),
                    condition
                        .1
                        .map_or("", |_| &self.dictionary.decode(triple.predicate).unwrap()),
                    condition
                        .2
                        .map_or("", |_| &self.dictionary.decode(triple.object).unwrap()),
                );
                if let Some(matching_triples) = btree_map.get(&key) {
                    for other_triple in matching_triples {
                        if filter(triple, other_triple) {
                            joined_triples.insert(Triple {
                                subject: triple.subject,
                                predicate: other_triple.predicate,
                                object: other_triple.object,
                            });
                        }
                    }
                }
            });
        }

        Self {
            triples: joined_triples,
            streams: self.streams.clone(),
            sliding_window: self.sliding_window.clone(),
            dictionary: self.dictionary.clone(),
            prefixes: self.prefixes.clone(),
            udfs: HashMap::new(),
            index_manager: UnifiedIndex::new(),
            rule_map: HashMap::new(),
        }
    }

    pub fn istream(&self, last_timestamp: u64) -> Vec<Triple> {
        let mut new_triples = vec![];
        for ts_triple in &self.streams {
            if ts_triple.timestamp > last_timestamp {
                new_triples.push(ts_triple.triple.clone());
            }
        }
        new_triples
    }

    pub fn dstream(&self, last_timestamp: u64, current_timestamp: u64) -> Vec<Triple> {
        let mut old_triples = BTreeSet::new();
        let mut current_triples = BTreeSet::new();

        for ts_triple in &self.streams {
            if ts_triple.timestamp <= last_timestamp {
                old_triples.insert(ts_triple.triple.clone());
            }
            if ts_triple.timestamp <= current_timestamp {
                current_triples.insert(ts_triple.triple.clone());
            }
        }

        old_triples.difference(&current_triples).cloned().collect()
    }

    pub fn rstream(&self, start: u64, end: u64) -> Vec<Triple> {
        let mut current_triples = BTreeSet::new();

        for ts_triple in &self.streams {
            if ts_triple.timestamp >= start && ts_triple.timestamp <= end {
                current_triples.insert(ts_triple.triple.clone());
            }
        }

        current_triples.into_iter().collect()
    }

    pub fn print(&self, title: &str, grouped: bool) {
        println!("{}", title);
        if grouped {
            let groups = self.group_by(|triple| {
                self.dictionary
                    .decode(triple.predicate)
                    .unwrap()
                    .to_string()
            });
            for (key, triples) in groups {
                println!("Group: {}", key);
                for triple in triples {
                    println!(
                        "  Subject: {}, Predicate: {}, Object: {}",
                        self.dictionary.decode(triple.subject).unwrap(),
                        self.dictionary.decode(triple.predicate).unwrap(),
                        self.dictionary.decode(triple.object).unwrap()
                    );
                }
            }
        } else {
            for triple in &self.triples {
                println!(
                    "  Subject: {}, Predicate: {}, Object: {}",
                    self.dictionary.decode(triple.subject).unwrap(),
                    self.dictionary.decode(triple.predicate).unwrap(),
                    self.dictionary.decode(triple.object).unwrap()
                );
            }
        }
    }

    pub fn set_sliding_window(&mut self, width: u64, slide: u64) {
        self.sliding_window = Some(SlidingWindow::new(width, slide));
    }

    pub fn evaluate_sliding_window(&mut self) -> Vec<Triple> {
        if let Some(window) = &self.sliding_window {
            let current_time = current_timestamp();
            let start_time = if current_time > window.width {
                current_time - window.width
            } else {
                0
            };

            let result = self.rstream(start_time, current_time);

            // Update last evaluated time
            self.sliding_window.as_mut().unwrap().last_evaluated = current_time;

            result
        } else {
            Vec::new()
        }
    }

    pub fn window_close_policy(&mut self) -> Vec<Triple> {
        let mut result = vec![];
        if let Some(window) = &self.sliding_window {
            let current_time = current_timestamp();
            if current_time >= window.last_evaluated + window.slide {
                result = self.evaluate_sliding_window();
            }
        }
        result
    }

    pub fn content_change_policy(&mut self) -> Vec<Triple> {
        let mut _result = vec![];
        let initial_state: BTreeSet<_> = self.triples.clone();
        if let Some(_window) = &self.sliding_window {
            _result = self.evaluate_sliding_window();
            let current_state: BTreeSet<_> = self.triples.clone();
            if initial_state != current_state {
                return _result;
            }
        }
        vec![]
    }

    pub fn non_empty_content_policy(&mut self) -> Vec<Triple> {
        let result = self.evaluate_sliding_window();
        if !result.is_empty() {
            return result;
        }
        vec![]
    }

    pub fn periodic_policy(&mut self, interval: std::time::Duration) -> Vec<Triple> {
        let mut result = vec![];
        if let Some(window) = &self.sliding_window {
            let current_time = current_timestamp();
            if current_time >= window.last_evaluated + interval.as_secs() {
                result = self.evaluate_sliding_window();
            }
        }
        result
    }

    pub fn auto_policy_evaluation(&mut self) -> Vec<Triple> {
        let current_time = current_timestamp();
        let mut result = vec![];

        if let Some(window) = &self.sliding_window {
            if current_time >= window.last_evaluated + window.slide {
                println!("Window Close Policy");
                result.extend(self.evaluate_sliding_window());
            }
        }

        let initial_state: BTreeSet<_> = self.triples.clone();
        if let Some(_window) = &self.sliding_window {
            let current_state: BTreeSet<_> = self.triples.clone();
            if initial_state != current_state {
                println!("Content Change Policy");
                result.extend(self.evaluate_sliding_window());
            }
        }

        let non_empty_result = self.evaluate_sliding_window();
        if !non_empty_result.is_empty() {
            println!("Non-empty Content Policy");
            result.extend(non_empty_result);
        }

        let interval = std::time::Duration::new(5, 0);
        if let Some(window) = &self.sliding_window {
            if current_time >= window.last_evaluated + interval.as_secs() {
                println!("Periodic Policy");
                result.extend(self.evaluate_sliding_window());
            }
        }

        result
    }

    pub fn handle_query(&mut self, query: &str) -> String {
        // Assume the query string is in a basic format like "subject predicate object"
        let parts: Vec<&str> = query.split_whitespace().collect();

        if parts.len() != 3 {
            return "Invalid query format. Expected 'subject predicate object'.".to_string();
        }

        let subject = parts[0];
        let predicate = parts[1];
        let object = parts[2];

        let subject_id = self.dictionary.encode(subject);
        let predicate_id = self.dictionary.encode(predicate);
        let object_id = self.dictionary.encode(object);

        let mut result = String::new();
        for triple in &self.triples {
            if triple.subject == subject_id
                && triple.predicate == predicate_id
                && triple.object == object_id
            {
                result.push_str(&format!(
                    "Subject: {}, Predicate: {}, Object: {}\n",
                    self.dictionary.decode(triple.subject).unwrap(),
                    self.dictionary.decode(triple.predicate).unwrap(),
                    self.dictionary.decode(triple.object).unwrap()
                ));
            }
        }

        if result.is_empty() {
            result = "No matching triples found.".to_string();
        }

        result
    }

    pub fn handle_update(&mut self, update: &str) -> String {
        // Parse the SPARQL update and apply changes to the database
        if update.starts_with("INSERT") {
            // Extract the part between curly braces
            if let Some(start) = update.find('{') {
                if let Some(end) = update.find('}') {
                    let triple_str = &update[start + 1..end].trim();
                    let parts: Vec<&str> = triple_str.split_whitespace().collect();

                    if parts.len() == 3 {
                        let subject = parts[0].to_string();
                        let predicate = parts[1].to_string();
                        let object = parts[2].to_string();

                        let triple = Triple {
                            subject: self.dictionary.encode(&subject),
                            predicate: self.dictionary.encode(&predicate),
                            object: self.dictionary.encode(&object),
                        };
                        self.triples.insert(triple);
                        return "Update Successful".to_string();
                    }
                }
            }
        } else if update.starts_with("DELETE") {
            // Extract the part between curly braces
            if let Some(start) = update.find('{') {
                if let Some(end) = update.find('}') {
                    let triple_str = &update[start + 1..end].trim();
                    let parts: Vec<&str> = triple_str.split_whitespace().collect();

                    if parts.len() == 3 {
                        let subject = parts[0].to_string();
                        let predicate = parts[1].to_string();
                        let object = parts[2].to_string();

                        let triple = Triple {
                            subject: self.dictionary.encode(&subject),
                            predicate: self.dictionary.encode(&predicate),
                            object: self.dictionary.encode(&object),
                        };
                        self.triples.remove(&triple);
                        return "Update Successful".to_string();
                    }
                }
            }
        }
        "Update Failed".to_string()
    }

    pub fn handle_http_request(&mut self, request: &str) -> String {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut req = httparse::Request::new(&mut headers);
        req.parse(request.as_bytes()).unwrap();

        match req.method.unwrap() {
            "GET" => {
                let url = Url::parse(&("http://localhost".to_owned() + req.path.unwrap())).unwrap();
                let query_pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
                if let Some(query) = query_pairs.get("query") {
                    return self.handle_query(query);
                }
            }
            "POST" => {
                let content_type = req
                    .headers
                    .iter()
                    .find(|header| header.name.eq_ignore_ascii_case("Content-Type"))
                    .map(|header| header.value);

                if let Some(content_type) = content_type {
                    if content_type == b"application/sparql-query" {
                        // Direct POST query
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            return self.handle_query(body);
                        }
                    } else if content_type == b"application/x-www-form-urlencoded" {
                        // URL-encoded POST query or update
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            let body_decoded =
                                percent_decode(body.as_bytes()).decode_utf8().unwrap();
                            let params: HashMap<_, _> = body_decoded
                                .split('&')
                                .map(|pair| {
                                    let mut split = pair.split('=');
                                    (
                                        split.next().unwrap().to_string(),
                                        split.next().unwrap_or("").to_string(),
                                    )
                                })
                                .collect();

                            if let Some(query) = params.get("query") {
                                return self.handle_query(query);
                            } else if let Some(update) = params.get("update") {
                                return self.handle_update(update);
                            }
                        }
                    } else if content_type == b"application/sparql-update" {
                        // Direct POST update
                        if let Some(body) = request.split("\r\n\r\n").nth(1) {
                            return self.handle_update(body);
                        }
                    }
                }
            }
            _ => {}
        }

        "Bad Request".to_string()
    }

    pub fn debug_print_triples(&self) {
        for triple in &self.triples {
            println!(
                "Stored Triple -> Subject: {}, Predicate: {}, Object: {}",
                self.dictionary.decode(triple.subject).unwrap(),
                self.dictionary.decode(triple.predicate).unwrap(),
                self.dictionary.decode(triple.object).unwrap()
            );
        }
    }

    #[cfg(feature = "cuda")]
    pub fn perform_hash_join_cuda_wrapper<'a>(
        &self,
        subject_var: &'a str,
        predicate: String,
        object_var: &'a str,
        triples: Vec<Triple>,
        dictionary: &'a Dictionary,
        final_results: Vec<BTreeMap<&'a str, String>>,
        literal_filter: Option<String>,
    ) -> Vec<BTreeMap<&'a str, String>> {
        if final_results.is_empty() {
            return Vec::new();
        }

        // Prepare data for CUDA
        let subjects: Vec<u32> = triples.iter().map(|t| t.subject).collect();
        let predicates: Vec<u32> = triples.iter().map(|t| t.predicate).collect();
        let objects: Vec<u32> = triples.iter().map(|t| t.object).collect();

        let predicate_filter = dictionary.clone().encode(&predicate);

        let literal_filter_value = literal_filter
            .as_ref()
            .map(|lit| dictionary.clone().encode(lit))
            .unwrap_or(0);

        let literal_filter_option = if literal_filter.is_some() {
            Some(literal_filter_value)
        } else {
            None
        };

        // Call CUDA function
        let matching_indices = hash_join_cuda(
            &subjects,
            &predicates,
            &objects,
            predicate_filter,
            literal_filter_option,
        );

        // Prepare variable bindings
        let mut both_vars_bound: HashMap<(String, String), Vec<BTreeMap<&'a str, String>>> =
            HashMap::new();
        let mut subject_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut object_var_bound: HashMap<String, Vec<BTreeMap<&'a str, String>>> = HashMap::new();
        let mut neither_var_bound: Vec<BTreeMap<&'a str, String>> = Vec::new();

        for result in final_results {
            let subject_binding = result.get(subject_var).cloned();
            let object_binding = result.get(object_var).cloned();

            match (subject_binding, object_binding) {
                (Some(subj_val), Some(obj_val)) => {
                    both_vars_bound
                        .entry((subj_val.clone(), obj_val.clone()))
                        .or_default()
                        .push(result);
                }
                (Some(subj_val), None) => {
                    subject_var_bound
                        .entry(subj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, Some(obj_val)) => {
                    object_var_bound
                        .entry(obj_val.clone())
                        .or_default()
                        .push(result);
                }
                (None, None) => {
                    neither_var_bound.push(result);
                }
            }
        }

        // Reconstruct results
        let mut results = Vec::new();

        for idx in matching_indices {
            let triple = &triples[idx as usize];

            if let (Some(subject), Some(object)) = (
                dictionary.decode(triple.subject),
                dictionary.decode(triple.object),
            ) {
                // Process group both_vars_bound
                {
                    let key = (subject.to_string(), object.to_string());
                    if let Some(results_vec) = both_vars_bound.get(&key) {
                        for result in results_vec {
                            let extended_result = result.clone();
                            results.push(extended_result);
                        }
                    }
                }

                // Process group subject_var_bound
                {
                    if let Some(results_vec) = subject_var_bound.get(subject) {
                        for result in results_vec {
                            let mut extended_result = result.clone();
                            // Extend object_var
                            if let Some(existing_object) = extended_result.get(object_var) {
                                if existing_object != &object {
                                    continue; // Inconsistent variable binding
                                }
                            } else {
                                extended_result.insert(object_var, object.to_string());
                            }
                            results.push(extended_result);
                        }
                    }
                }

                // Process group object_var_bound
                {
                    if let Some(results_vec) = object_var_bound.get(object) {
                        for result in results_vec {
                            let mut extended_result = result.clone();
                            // Extend subject_var
                            if let Some(existing_subject) = extended_result.get(subject_var) {
                                if existing_subject != &subject {
                                    continue; // Inconsistent variable binding
                                }
                            } else {
                                extended_result.insert(subject_var, subject.to_string());
                            }
                            results.push(extended_result);
                        }
                    }
                }

                // Process group neither_var_bound
                for result in &neither_var_bound {
                    let mut extended_result = result.clone();
                    // Extend subject_var
                    if let Some(existing_subject) = extended_result.get(subject_var) {
                        if existing_subject != &subject {
                            continue; // Inconsistent variable binding
                        }
                    } else {
                        extended_result.insert(subject_var, subject.to_string());
                    }
                    // Extend object_var
                    if let Some(existing_object) = extended_result.get(object_var) {
                        if existing_object != &object {
                            continue; // Inconsistent variable binding
                        }
                    } else {
                        extended_result.insert(object_var, object.to_string());
                    }
                    results.push(extended_result);
                }
            }
        }

        results
    }

    // Create user defined function
    pub fn register_udf<F>(&mut self, name: &str, f: F)
    where
        F: Fn(Vec<&str>) -> String + Send + Sync + 'static,
    {
        self.udfs.insert(name.to_string(), ClonableFn::new(f));
    }

    /// Rebuild all indexes from the current state of `self.triples`.
    pub fn build_all_indexes(&mut self) {
        // Clear existing indexes
        self.index_manager.clear();

        // Create a snapshot of all the triples so that we're not borrowing `self` in the loop
        let triple_list: Vec<Triple> = self.triples.iter().cloned().collect();

        // Populate indexes for each triple
        for triple in &triple_list {
            self.index_manager.insert(triple);
        }

        // Sort + deduplicate all index values
        self.index_manager.optimize();
    }

    /// Triple to string
    pub fn triple_to_string(&self, triple: &Triple, dict: &Dictionary) -> String {
        let subject = dict.decode(triple.subject);
        let predicate = dict.decode(triple.predicate);
        let object = dict.decode(triple.object);
        format!("{} {} {}", subject.unwrap(), predicate.unwrap(), object.unwrap())
    }
}

#[cfg_attr(any(target_arch = "x86", target_arch = "x86_64"), target_feature(enable = "sse2"))]
#[cfg_attr(target_arch = "aarch64", target_feature(enable = "neon"))]
pub unsafe fn simd_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    // SSE2 implementation for x86/x86_64
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        let len = a.len();
        let chunks = len / 16;
        let mut i = 0;
        while i < chunks * 16 {
            let pa = a.as_ptr().add(i) as *const __m128i;
            let pb = b.as_ptr().add(i) as *const __m128i;
            let va = _mm_loadu_si128(pa);
            let vb = _mm_loadu_si128(pb);
            let cmp = _mm_cmpeq_epi8(va, vb);
            let mask = _mm_movemask_epi8(cmp);
            if mask != 0xFFFF {
                return false;
            }
            i += 16;
        }
        // Compare any remaining bytes
        for j in (chunks * 16)..len {
            if a[j] != b[j] {
                return false;
            }
        }
        return true;
    }

    // NEON implementation for aarch64
    #[cfg(target_arch = "aarch64")]
    {
        let len = a.len();
        let chunks = len / 16;
        let mut i = 0;
        while i < chunks * 16 {
            let pa = a.as_ptr().add(i);
            let pb = b.as_ptr().add(i);
            let va = vld1q_u8(pa);
            let vb = vld1q_u8(pb);
            let cmp = vceqq_u8(va, vb);
            let cmp_u64 = vreinterpretq_u64_u8(cmp);
            let low = vgetq_lane_u64(cmp_u64, 0);
            let high = vgetq_lane_u64(cmp_u64, 1);
            if low != u64::MAX || high != u64::MAX {
                return false;
            }
            i += 16;
        }
        // Compare any remaining bytes
        for j in (chunks * 16)..len {
            if a[j] != b[j] {
                return false;
            }
        }
        return true;
    }

    // Fallback for other architectures
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    {
        return a == b;
    }
}

#[inline]
fn simd_bytes_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    unsafe {
        use std::arch::x86_64::*;
        let mut i = 0;
        let len = a.len();
        while i + 16 <= len {
            let a_chunk = _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i);
            let b_chunk = _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i);
            let cmp = _mm_cmpeq_epi8(a_chunk, b_chunk);
            // If all 16 bytes match, _mm_movemask_epi8 returns 0xFFFF.
            if _mm_movemask_epi8(cmp) != 0xFFFF {
                return false;
            }
            i += 16;
        }
        // Compare any remaining bytes.
        for j in i..len {
            if a[j] != b[j] {
                return false;
            }
        }
        true
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        // Fallback on non-x86 architectures.
        a == b
    }
}

#[inline(always)]
fn process_join<'a>(
    subject: &str,
    object: &str,
    subject_var: &'a str,
    object_var: &'a str,
    both_vars_bound: &Arc<HashMap<(String, String), Vec<usize>>>,
    subject_var_bound: &Arc<HashMap<String, Vec<usize>>>,
    object_var_bound: &Arc<HashMap<String, Vec<usize>>>,
    neither_var_bound: &Arc<Vec<usize>>,
    final_results_arc: &Arc<Vec<BTreeMap<&'a str, String>>>,
    local_results: &mut Vec<BTreeMap<&'a str, String>>,
) {
    // Check both_vars_bound - most restrictive case first
    if let Some(result_indices) = both_vars_bound.get(&(subject.to_string(), object.to_string())) {
        for &idx in result_indices {
            local_results.push(final_results_arc[idx].clone());
        }
    }

    // Process subject_var_bound
    if let Some(result_indices) = subject_var_bound.get(subject) {
        for &idx in result_indices {
            let base_result = &final_results_arc[idx];
            // Check for object consistency if it exists
            if let Some(existing_object) = base_result.get(object_var) {
                if existing_object == object {
                    local_results.push(base_result.clone());
                }
            } else {
                // Bind the object variable
                let mut extended_result = base_result.clone();
                extended_result.insert(object_var, object.to_string());
                local_results.push(extended_result);
            }
        }
    }

    // Process object_var_bound
    if let Some(result_indices) = object_var_bound.get(object) {
        for &idx in result_indices {
            let base_result = &final_results_arc[idx];
            // Check for subject consistency if it exists
            if let Some(existing_subject) = base_result.get(subject_var) {
                if existing_subject == subject {
                    local_results.push(base_result.clone());
                }
            } else {
                // Bind the subject variable
                let mut extended_result = base_result.clone();
                extended_result.insert(subject_var, subject.to_string());
                local_results.push(extended_result);
            }
        }
    }

    // Process neither_var_bound - least restrictive case last
    for &idx in neither_var_bound.iter() {
        let base_result = &final_results_arc[idx];
        
        // Check both consistency constraints
        let subject_consistent = base_result
            .get(subject_var)
            .map_or(true, |existing| existing == subject);
        let object_consistent = base_result
            .get(object_var)
            .map_or(true, |existing| existing == object);

        if subject_consistent && object_consistent {
            let mut extended_result = base_result.clone();
            
            // Only insert if not already present
            if !base_result.contains_key(subject_var) {
                extended_result.insert(subject_var, subject.to_string());
            }
            if !base_result.contains_key(object_var) {
                extended_result.insert(object_var, object.to_string());
            }
            
            local_results.push(extended_result);
        }
    }
}

#[inline(always)]
fn process_triple_chunk<'a>(
    triple_chunk: &[Triple],
    predicate_bytes: &[u8],
    literal_filter_bytes: &Option<&[u8]>,
    subject_var: &'a str,
    object_var: &'a str,
    both_vars_bound: &Arc<HashMap<(String, String), Vec<usize>>>,
    subject_var_bound: &Arc<HashMap<String, Vec<usize>>>,
    object_var_bound: &Arc<HashMap<String, Vec<usize>>>,
    neither_var_bound: &Arc<Vec<usize>>,
    final_results_arc: &Arc<Vec<BTreeMap<&'a str, String>>>,
    local_results: &mut Vec<BTreeMap<&'a str, String>>,
    dictionary: &'a Dictionary,
) {
    // Pre-filter triples to avoid unnecessary decoding
    for triple in triple_chunk {
        let pred_opt = dictionary.decode(triple.predicate);
        if pred_opt.is_none() || pred_opt.as_ref().unwrap().as_bytes() != predicate_bytes {
            continue;
        }
        
        if let Some(filter_bytes) = literal_filter_bytes {
            let obj_opt = dictionary.decode(triple.object);
            if obj_opt.is_none() || obj_opt.as_ref().unwrap().as_bytes() != *filter_bytes {
                continue;
            }
            
            if let Some(subj) = dictionary.decode(triple.subject) {
                process_join_efficiently(
                    &subj,
                    obj_opt.unwrap(),
                    subject_var,
                    object_var,
                    both_vars_bound,
                    subject_var_bound,
                    object_var_bound,
                    neither_var_bound,
                    final_results_arc,
                    local_results,
                );
            }
        } else {
            let subj_opt = dictionary.decode(triple.subject);
            let obj_opt = dictionary.decode(triple.object);
            
            if let (Some(subj), Some(obj)) = (subj_opt, obj_opt) {
                process_join_efficiently(
                    &subj,
                    &obj,
                    subject_var,
                    object_var,
                    both_vars_bound,
                    subject_var_bound,
                    object_var_bound,
                    neither_var_bound,
                    final_results_arc,
                    local_results,
                );
            }
        }
    }
}


#[inline(always)]
fn process_join_efficiently<'a>(
    subject: &str,
    object: &str,
    subject_var: &'a str,
    object_var: &'a str,
    both_vars_bound: &Arc<HashMap<(String, String), Vec<usize>>>,
    subject_var_bound: &Arc<HashMap<String, Vec<usize>>>,
    object_var_bound: &Arc<HashMap<String, Vec<usize>>>,
    neither_var_bound: &Arc<Vec<usize>>,
    final_results_arc: &Arc<Vec<BTreeMap<&'a str, String>>>,
    local_results: &mut Vec<BTreeMap<&'a str, String>>,
) {
    if let Some(result_indices) = both_vars_bound.get(&(subject.to_string(), object.to_string())) {
        for &idx in result_indices {
            // Clone efficiently with pre-allocation
            let result = final_results_arc[idx].clone();
            local_results.push(result);
        }
        return; // Early return after handling the most restrictive case
    }

    // Check for subject var bound - second most restrictive
    if let Some(result_indices) = subject_var_bound.get(subject) {
        for &idx in result_indices {
            let base_result = &final_results_arc[idx];
            // Check for object consistency if it exists
            if let Some(existing_object) = base_result.get(object_var) {
                if existing_object == object {
                    local_results.push(base_result.clone());
                }
            } else {
                let mut extended_result = base_result.clone();
                extended_result.insert(object_var, object.to_string());
                local_results.push(extended_result);
            }
        }
    }

    // Check for object var bound
    if let Some(result_indices) = object_var_bound.get(object) {
        for &idx in result_indices {
            let base_result = &final_results_arc[idx];
            if let Some(existing_subject) = base_result.get(subject_var) {
                if existing_subject == subject {
                    local_results.push(base_result.clone());
                }
            } else {
                let mut extended_result = base_result.clone();
                extended_result.insert(subject_var, subject.to_string());
                local_results.push(extended_result);
            }
        }
    }

    // Process least restrictive case - neither var bound
    for &idx in neither_var_bound.iter() {
        let base_result = &final_results_arc[idx];
        
        // Check both consistency constraints
        let subject_consistent = base_result
            .get(subject_var)
            .map_or(true, |existing| existing == subject);
        let object_consistent = base_result
            .get(object_var)
            .map_or(true, |existing| existing == object);

        if subject_consistent && object_consistent {
            let mut extended_result = base_result.clone();
            
            // Only insert if not already present
            if !base_result.contains_key(subject_var) {
                extended_result.insert(subject_var, subject.to_string());
            }
            if !base_result.contains_key(object_var) {
                extended_result.insert(object_var, object.to_string());
            }
            
            local_results.push(extended_result);
        }
    }
}