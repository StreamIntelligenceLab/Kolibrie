/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reading RDF/XML, from a string or a file

use super::KolibrieDB;
use crate::utils;
use crossbeam::channel::unbounded;
use crossbeam::scope;
use quick_xml::events::Event;
use quick_xml::name::QName;
use quick_xml::Reader;
use rayon::prelude::*;
use shared::rdf_term;
use shared::triple::Triple;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

impl KolibrieDB {
    /// Parses RDF/XML and adds the triples
    pub fn parse_rdf(&mut self, rdf_xml: &str) {
        let mut reader = Reader::from_str(rdf_xml);

        let mut current_subject = Vec::with_capacity(128);
        let mut current_predicate = Vec::with_capacity(128);
        let mut current_datatype = Vec::with_capacity(64);
        let mut language_scope: Vec<Vec<u8>> = Vec::new();

        let (sender, receiver) = unbounded::<Vec<Triple>>();
        let dictionary = self.dictionary.clone();
        let triples_set = Arc::new(Mutex::new(Vec::new()));
        let num_threads = utils::get_num_cpus();

        scope(|s| {
            for _ in 0..num_threads {
                let receiver = receiver.clone();
                let triples_set = Arc::clone(&triples_set);
                s.spawn(move |_| {
                    while let Ok(chunk) = receiver.recv() {
                        if chunk.is_empty() {
                            break;
                        }

                        let local_triples: BTreeSet<Triple> =
                            chunk.into_par_iter().map(|triple| triple).collect();

                        let mut triples = triples_set.lock().unwrap();
                        triples.push(local_triples);
                    }
                });
            }

            let mut triples = Vec::with_capacity(8192);
            loop {
                match reader.read_event() {
                    Ok(Event::Start(ref e)) => {
                        current_datatype.truncate(0);
                        let mut language = language_scope.last().cloned().unwrap_or_default();
                        for attr in e.attributes().filter_map(Result::ok) {
                            match attr.key {
                                QName(b"rdf:datatype") => {
                                    current_datatype.extend_from_slice(&attr.value)
                                }
                                QName(b"xml:lang") => {
                                    language.clear();
                                    language.extend_from_slice(&attr.value);
                                }
                                _ => {}
                            }
                        }
                        language_scope.push(language);
                        match e.name() {
                            QName(b"rdf:RDF") => {
                                for attr in e.attributes().filter_map(Result::ok) {
                                    let key = attr.key;
                                    let value = attr.value;
                                    if key.as_ref().starts_with(b"xmlns:") {
                                        let prefix = std::str::from_utf8(&key.as_ref()[6..])
                                            .unwrap_or("")
                                            .to_string();
                                        let uri =
                                            std::str::from_utf8(&value).unwrap_or("").to_string();
                                        self.prefixes.insert(prefix, uri);
                                    } else if key.as_ref() == b"xmlns" {
                                        let uri =
                                            std::str::from_utf8(&value).unwrap_or("").to_string();
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
                        }
                    }
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
                                    let mut dict = dictionary.write();
                                    let triple = Triple {
                                        subject: dict.encode(subject_str),
                                        predicate: dict.encode(&resolved_predicate),
                                        object: dict.encode(object_str),
                                    };
                                    drop(dict);
                                    triples.push(triple);
                                }
                            }
                        }
                    }
                    Ok(Event::Text(e)) => {
                        if let Ok(object_str) = reader.decoder().decode(e.as_ref()) {
                            let trimmed_object = object_str.trim();
                            if !trimmed_object.is_empty() {
                                if let Ok(subject_str) = std::str::from_utf8(&current_subject) {
                                    if let Ok(predicate_str) =
                                        std::str::from_utf8(&current_predicate)
                                    {
                                        let resolved_predicate = self.resolve_term(predicate_str);
                                        let object = rdf_term::spell_literal(
                                            trimmed_object,
                                            std::str::from_utf8(&current_datatype)
                                                .ok()
                                                .filter(|iri| !iri.is_empty()),
                                            language_scope
                                                .last()
                                                .and_then(|tag| std::str::from_utf8(tag).ok())
                                                .filter(|tag| !tag.is_empty()),
                                        );
                                        let mut dict = dictionary.write();
                                        let triple = Triple {
                                            subject: dict.encode(subject_str),
                                            predicate: dict.encode(&resolved_predicate),
                                            object: dict.encode(&object),
                                        };
                                        drop(dict);
                                        triples.push(triple);
                                    }
                                }
                            }
                        }
                    }
                    Ok(Event::End(ref e)) => {
                        language_scope.pop();
                        if e.name() == QName(b"rdf:Description") {
                            current_subject.truncate(0);
                            current_predicate.truncate(0);
                        }
                    }
                    Ok(Event::Eof) => break,
                    Err(_e) => {
                        eprintln!("KOLIBRIE_OPERATION_FAILED");
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

            for _ in 0..num_threads {
                sender.send(Vec::new()).unwrap();
            }
        })
        .unwrap();

        let triples_sets = Arc::try_unwrap(triples_set).unwrap().into_inner().unwrap();
        for local_triples in triples_sets {
            for triple in local_triples {
                self.add_triple(triple);
            }
        }
    }

    /// Reads RDF/XML from a file and adds the triples
    pub fn parse_rdf_from_file(&mut self, filename: &str) {
        let file = std::fs::File::open(filename).expect("Cannot open file");
        let reader = std::io::BufReader::new(file);
        let mut xml_reader = Reader::from_reader(reader);

        let mut current_subject = Vec::with_capacity(128);
        let mut current_predicate = Vec::with_capacity(128);
        let mut current_datatype = Vec::with_capacity(64);
        let mut language_scope: Vec<Vec<u8>> = Vec::new();

        let mut buf = Vec::new();
        loop {
            match xml_reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    if e.name() == QName(b"rdf:RDF") {
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
                                let uri = std::str::from_utf8(&value).unwrap_or("").to_string();
                                self.prefixes.insert("".to_string(), uri);
                            }
                        }
                        break;
                    }
                }
                Ok(Event::Eof) => {
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                    break;
                }
                Err(_e) => {
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                    break;
                }
                _ => {}
            }
            buf.clear();
        }

        let mut triples = Vec::with_capacity(8192);
        loop {
            match xml_reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    current_datatype.truncate(0);
                    let mut language = language_scope.last().cloned().unwrap_or_default();
                    for attr in e.attributes().filter_map(Result::ok) {
                        match attr.key {
                            QName(b"rdf:datatype") => {
                                current_datatype.extend_from_slice(&attr.value)
                            }
                            QName(b"xml:lang") => {
                                language.clear();
                                language.extend_from_slice(&attr.value);
                            }
                            _ => {}
                        }
                    }
                    language_scope.push(language);
                    match e.name() {
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
                            let name_str =
                                std::str::from_utf8(name.as_ref()).unwrap_or("").to_string();
                            let resolved_predicate = self.resolve_term(&name_str);
                            current_predicate = resolved_predicate.clone().into_bytes();
                        }
                    }
                }
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
                                let mut dict = self.dictionary.write();
                                let triple = Triple {
                                    subject: dict.encode(subject_str),
                                    predicate: dict.encode(&resolved_predicate),
                                    object: dict.encode(object_str),
                                };
                                drop(dict);
                                triples.push(triple);
                            }
                        }
                    }
                }
                Ok(Event::Text(e)) => {
                    if let Ok(object_str) = xml_reader.decoder().decode(e.as_ref()) {
                        let trimmed_object = object_str.trim();
                        if !trimmed_object.is_empty() {
                            if let Ok(subject_str) = std::str::from_utf8(&current_subject) {
                                if let Ok(predicate_str) = std::str::from_utf8(&current_predicate) {
                                    let resolved_predicate = self.resolve_term(predicate_str);
                                    let object = rdf_term::spell_literal(
                                        trimmed_object,
                                        std::str::from_utf8(&current_datatype)
                                            .ok()
                                            .filter(|iri| !iri.is_empty()),
                                        language_scope
                                            .last()
                                            .and_then(|tag| std::str::from_utf8(tag).ok())
                                            .filter(|tag| !tag.is_empty()),
                                    );
                                    let mut dict = self.dictionary.write();
                                    let triple = Triple {
                                        subject: dict.encode(subject_str),
                                        predicate: dict.encode(&resolved_predicate),
                                        object: dict.encode(&object),
                                    };
                                    drop(dict);
                                    triples.push(triple);
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    language_scope.pop();
                    if e.name() == QName(b"rdf:Description") {
                        current_subject.clear();
                        current_predicate.clear();
                    }
                }
                Ok(Event::Eof) => break,
                Err(_e) => {
                    eprintln!("KOLIBRIE_OPERATION_FAILED");
                    break;
                }
                _ => {}
            }

            buf.clear();

            if triples.len() >= 8192 {
                let local_triples: BTreeSet<Triple> = triples.into_par_iter().collect();
                for triple in local_triples {
                    self.add_triple(triple);
                }
                triples = Vec::with_capacity(8192);
            }
        }

        if !triples.is_empty() {
            let local_triples: BTreeSet<Triple> = triples.into_par_iter().collect();
            for triple in local_triples {
                self.add_triple(triple);
            }
        }
    }
}
