/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

extern crate kolibrie;
use kolibrie::sparql_database::SparqlDatabase;
use shared::triple::Triple;

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_db() -> SparqlDatabase {
        let mut db = SparqlDatabase::new();
        
        // Create common IDs
        let person1 = db.dictionary.encode("http://example.org/person1");
        let person2 = db.dictionary.encode("http://example.org/person2");
        let company = db.dictionary.encode("http://example.org/company1");
        
        // Create predicates
        let predicates = [
            (db.dictionary.encode("ex:name"), "name"),
            (db.dictionary.encode("ex:age"), "age"),
            (db.dictionary.encode("ex:email"), "email"),
            (db.dictionary.encode("ex:worksFor"), "worksFor"),
            (db.dictionary.encode("ex:founded"), "founded"),
            (db.dictionary.encode("ex:industry"), "industry")
        ];
        
        // Create objects
        let objects = [
            (db.dictionary.encode("John Smith"), "john"),
            (db.dictionary.encode("Jane Doe"), "jane"),
            (db.dictionary.encode("ACME Corp"), "acme"),
            (db.dictionary.encode("30"), "age30"),
            (db.dictionary.encode("25"), "age25"),
            (db.dictionary.encode("john@example.com"), "email1"),
            (db.dictionary.encode("jane@example.com"), "email2"),
            (db.dictionary.encode("2000"), "year"),
            (db.dictionary.encode("Technology"), "tech")
        ];
        
        // Insert triples: Person 1 (John)
        let p = &predicates;
        let o = &objects;
        db.triples.insert(Triple { subject: person1, predicate: p[0].0, object: o[0].0 }); // name: John
        db.triples.insert(Triple { subject: person1, predicate: p[1].0, object: o[3].0 }); // age: 30
        db.triples.insert(Triple { subject: person1, predicate: p[2].0, object: o[5].0 }); // email: john@...
        db.triples.insert(Triple { subject: person1, predicate: p[3].0, object: company }); // worksFor: company1
        
        // Person 2 (Jane)
        db.triples.insert(Triple { subject: person2, predicate: p[0].0, object: o[1].0 }); // name: Jane
        db.triples.insert(Triple { subject: person2, predicate: p[1].0, object: o[4].0 }); // age: 25
        db.triples.insert(Triple { subject: person2, predicate: p[2].0, object: o[6].0 }); // email: jane@...
        db.triples.insert(Triple { subject: person2, predicate: p[3].0, object: company }); // worksFor: company1
        
        // Company
        db.triples.insert(Triple { subject: company, predicate: p[0].0, object: o[2].0 }); // name: ACME
        db.triples.insert(Triple { subject: company, predicate: p[4].0, object: o[7].0 }); // founded: 2000
        db.triples.insert(Triple { subject: company, predicate: p[5].0, object: o[8].0 }); // industry: Technology
        
        db
    }

    #[test]
    fn test_delete_triple() {
        let mut db = setup_test_db();
        
        // Initial count
        let initial_count = db.triples.len();
        assert_eq!(initial_count, 11);
        
        // Create a triple to delete
        let person1 = db.dictionary.encode("http://example.org/person1");
        let pred_name = db.dictionary.encode("ex:name");
        let obj_john = db.dictionary.encode("John Smith");
        
        let triple_to_delete = Triple {
            subject: person1,
            predicate: pred_name,
            object: obj_john
        };
        
        // Delete the triple
        let deleted = db.delete_triple(triple_to_delete.clone());
        assert!(deleted, "Triple should be deleted successfully");
        
        // Verify count decreased
        assert_eq!(db.triples.len(), initial_count - 1);
        
        // Verify triple is gone
        assert!(!db.triples.contains(&triple_to_delete));
    }

    #[test]
    fn test_delete_triple_parts() {
        let mut db = setup_test_db();
        
        // Initial count
        let initial_count = db.triples.len();
        assert_eq!(initial_count, 11);
        
        // Delete using string parts
        let deleted = db.delete_triple_parts(
            "http://example.org/person2",
            "ex:email",
            "jane@example.com"
        );
        assert!(deleted, "Triple should be deleted successfully");
        
        // Verify count decreased
        assert_eq!(db.triples.len(), initial_count - 1);
    }

    #[test]
    fn test_basic_filters() {
        let db = setup_test_db();
        
        // Test exact matches
        let subj_filter = db.query().with_subject("http://example.org/person1").get_triples();
        assert_eq!(subj_filter.len(), 4);
        
        let pred_filter = db.query().with_predicate("ex:name").get_triples();
        assert_eq!(pred_filter.len(), 3);
        
        let obj_filter = db.query().with_object("Jane Doe").get_triples();
        assert_eq!(obj_filter.len(), 1);
        
        // Test pattern matching
        let subj_like = db.query().with_subject_like("person").get_triples();
        assert_eq!(subj_like.len(), 8);
        
        let pred_like = db.query().with_predicate_like("name").get_triples();
        assert_eq!(pred_like.len(), 3);
        
        let obj_like = db.query().with_object_like("example.com").get_triples();
        assert_eq!(obj_like.len(), 2);
        
        // Test prefix/suffix matching
        let subj_start = db.query().with_subject_starting("http://example.org/person").get_triples();
        assert_eq!(subj_start.len(), 8);
        
        let pred_end = db.query().with_predicate_ending("For").get_triples();
        assert_eq!(pred_end.len(), 2);
        
        // Test custom filter
        let nums = db.query()
            .filter(|t| db.dictionary.decode(t.object).unwrap_or("").parse::<i32>().is_ok())
            .get_triples();
        assert_eq!(nums.len(), 3);
    }
    
    #[test]
    fn test_combination_and_results() {
        let db = setup_test_db();
        
        // Test compound query
        let person_names = db.query()
            .with_subject_like("person")
            .with_predicate("ex:name")
            .get_triples();
        assert_eq!(person_names.len(), 2);
        
        // Test count
        let name_count = db.query()
            .with_predicate("ex:name")
            .count();
        assert_eq!(name_count, 3);
        
        // Test get_subjects
        let name_subjects = db.query()
            .with_predicate("ex:name")
            .get_subjects();
        assert_eq!(name_subjects.len(), 3);
        assert!(name_subjects.contains(&"http://example.org/person1".to_string()));
        
        // Test get_predicates
        let person_predicates = db.query()
            .with_subject("http://example.org/person1")
            .get_predicates();
        assert_eq!(person_predicates.len(), 4);
        assert!(person_predicates.contains(&"ex:name".to_string()));
        
        // Test get_objects
        let age_values = db.query()
            .with_predicate("ex:age")
            .get_objects();
        assert_eq!(age_values.len(), 2);
        assert!(age_values.contains(&"30".to_string()));
        
        // Test get_decoded_triples
        let jane_triples = db.query()
            .with_subject("http://example.org/person2")
            .get_decoded_triples();
        assert_eq!(jane_triples.len(), 4);
        
        let expected = ("http://example.org/person2".to_string(), 
                        "ex:name".to_string(), 
                        "Jane Doe".to_string());
        assert!(jane_triples.contains(&expected));
    }
    
    #[test]
    fn test_result_manipulation() {
        let db = setup_test_db();
        
        // Test order_by
        let ages: Vec<_> = db.query()
            .with_predicate("ex:age")
            .order_by(|t| db.dictionary.decode(t.object).unwrap_or("").to_string())
            .get_triples()
            .into_iter()
            .collect();
        assert_eq!(ages.len(), 2);
        
        // Check that we got both ages (without assuming order)
        let age_values = vec![
            db.dictionary.decode(ages[0].object).unwrap_or(""),
            db.dictionary.decode(ages[1].object).unwrap_or(""),
        ];
        assert!(age_values.contains(&"25"));
        assert!(age_values.contains(&"30"));
        
        // Test limit and offset
        let all: Vec<_> = db.query().get_triples().into_iter().collect();
        
        let limited = db.query().limit(2).get_triples();
        assert_eq!(limited.len(), 2);
        
        let offset: Vec<_> = db.query().offset(2).limit(2).get_triples().into_iter().collect();
        assert_eq!(offset.len(), 2);
        assert_eq!(offset[0], all[2]);
        
        // Test grouping
        let groups = db.query().group_by(|t| 
            db.dictionary.decode(t.predicate).unwrap_or("").to_string()
        );
        
        assert_eq!(groups.get("ex:name").map_or(0, |g| g.len()), 3);
        assert_eq!(groups.get("ex:age").map_or(0, |g| g.len()), 2);
    }
    
    #[test]
    fn test_distinct_and_join() {
        let mut db = setup_test_db();
        
        // Test distinct
        let orig_count = db.triples.len();
        assert_eq!(orig_count, 11);
        
        // Add a second email for Person1
        let subject1 = db.dictionary.encode("http://example.org/person1");
        let pred_email = db.dictionary.encode("ex:email");
        let new_email = db.dictionary.encode("john.smith@example.com");
        db.triples.insert(Triple { subject: subject1, predicate: pred_email, object: new_email });
        
        // Test all emails for person1
        let emails = db.query()
            .with_subject("http://example.org/person1")
            .with_predicate("ex:email")
            .get_triples();
        assert_eq!(emails.len(), 2);
        
        // Test simple join
        let tech_companies = db.query()
            .with_predicate("ex:industry")
            .with_object("Technology")
            .get_subjects();
        
        let tech_employees = db.query()
            .with_predicate("ex:worksFor")
            .with_object(tech_companies[0].as_str())
            .get_subjects();
            
        assert_eq!(tech_employees.len(), 2);
        assert!(tech_employees.contains(&"http://example.org/person1".to_string()));
    }
    
    #[test]
    fn test_complex_query() {
        let db = setup_test_db();
        
        // Find people under 30 who work for ACME
        let acme = db.query()
            .with_predicate("ex:name")
            .with_object("ACME Corp")
            .get_subjects();
            
        let employees = db.query()
            .with_predicate("ex:worksFor")
            .with_object(acme[0].as_str())
            .get_triples();
            
        let young = employees.iter()
            .filter(|t| {
                let age_triples = db.query()
                    .with_subject(db.dictionary.decode(t.subject).unwrap_or(""))
                    .with_predicate("ex:age")
                    .get_triples();
                    
                if let Some(triple) = age_triples.iter().next() {
                    if let Some(age_str) = db.dictionary.decode(triple.object) {
                        if let Ok(age) = age_str.parse::<i32>() {
                            return age < 30;
                        }
                    }
                }
                false
            })
            .collect::<Vec<_>>();
            
        assert_eq!(young.len(), 1);
        assert_eq!(db.dictionary.decode(young[0].subject).unwrap_or(""), "http://example.org/person2");
    }
}
