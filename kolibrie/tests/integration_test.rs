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
use kolibrie::database::KolibrieDB;
use kolibrie::execute_query::execute;
use kolibrie::query_builder::Queryable;
use shared::triple::Triple;

#[cfg(test)]
mod tests {
    use super::*;

    fn default_triples(db: &KolibrieDB) -> Vec<Triple> {
        db.query_default_triples(None, None, None)
    }

    fn setup_test_db() -> KolibrieDB {
        let mut db = KolibrieDB::new();

        let mut dict = db.dictionary_mut();

        let person1 = dict.encode("http://example.org/person1");
        let person2 = dict.encode("http://example.org/person2");
        let company = dict.encode("http://example.org/company1");

        let predicates = [
            (dict.encode("ex:name"), "name"),
            (dict.encode("ex:age"), "age"),
            (dict.encode("ex:email"), "email"),
            (dict.encode("ex:worksFor"), "worksFor"),
            (dict.encode("ex:founded"), "founded"),
            (dict.encode("ex:industry"), "industry"),
        ];

        let objects = [
            (dict.encode("John Smith"), "john"),
            (dict.encode("Jane Doe"), "jane"),
            (dict.encode("ACME Corp"), "acme"),
            (dict.encode("30"), "age30"),
            (dict.encode("25"), "age25"),
            (dict.encode("john@example.com"), "email1"),
            (dict.encode("jane@example.com"), "email2"),
            (dict.encode("2000"), "year"),
            (dict.encode("Technology"), "tech"),
        ];

        drop(dict);

        let p = &predicates;
        let o = &objects;
        db.add_triple(Triple {
            subject: person1,
            predicate: p[0].0,
            object: o[0].0,
        });
        db.add_triple(Triple {
            subject: person1,
            predicate: p[1].0,
            object: o[3].0,
        });
        db.add_triple(Triple {
            subject: person1,
            predicate: p[2].0,
            object: o[5].0,
        });
        db.add_triple(Triple {
            subject: person1,
            predicate: p[3].0,
            object: company,
        });

        db.add_triple(Triple {
            subject: person2,
            predicate: p[0].0,
            object: o[1].0,
        });
        db.add_triple(Triple {
            subject: person2,
            predicate: p[1].0,
            object: o[4].0,
        });
        db.add_triple(Triple {
            subject: person2,
            predicate: p[2].0,
            object: o[6].0,
        });
        db.add_triple(Triple {
            subject: person2,
            predicate: p[3].0,
            object: company,
        });

        db.add_triple(Triple {
            subject: company,
            predicate: p[0].0,
            object: o[2].0,
        });
        db.add_triple(Triple {
            subject: company,
            predicate: p[4].0,
            object: o[7].0,
        });
        db.add_triple(Triple {
            subject: company,
            predicate: p[5].0,
            object: o[8].0,
        });

        db
    }

    #[test]
    fn test_delete_triple() {
        let mut db = setup_test_db();

        let initial_count = default_triples(&db).len();
        assert_eq!(initial_count, 11);

        let mut dict = db.dictionary_mut();
        let person1 = dict.encode("http://example.org/person1");
        let pred_name = dict.encode("ex:name");
        let obj_john = dict.encode("John Smith");
        drop(dict);

        let triple_to_delete = Triple {
            subject: person1,
            predicate: pred_name,
            object: obj_john,
        };

        let deleted = db.delete_triple(&triple_to_delete);
        assert!(deleted, "Triple should be deleted successfully");

        assert_eq!(default_triples(&db).len(), initial_count - 1);

        assert!(!default_triples(&db).contains(&triple_to_delete));
    }

    #[test]
    fn test_delete_triple_parts() {
        let mut db = setup_test_db();

        let initial_count = default_triples(&db).len();
        assert_eq!(initial_count, 11);

        let deleted =
            db.delete_triple_parts("http://example.org/person2", "ex:email", "jane@example.com");
        assert!(deleted, "Triple should be deleted successfully");

        assert_eq!(default_triples(&db).len(), initial_count - 1);
    }

    #[test]
    fn test_turtle_prefix_query_with_variable_predicate() {
        let mut db = KolibrieDB::new();
        db.parse_turtle(
            r#"
            @prefix ex: <http://example.org/> .
            ex:Alice ex:knows ex:Bob .
            ex:Bob ex:knows ex:Carol .
        "#,
        );

        let rows = execute(
            r#"
            SELECT ?person ?friend
            WHERE { ?person ?anything ?friend }
        "#,
            &mut db,
        )
        .expect("query should run")
        .into_rows();

        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![
            "http://example.org/Alice".to_string(),
            "http://example.org/Bob".to_string(),
        ]));
        assert!(rows.contains(&vec![
            "http://example.org/Bob".to_string(),
            "http://example.org/Carol".to_string(),
        ]));
    }

    #[test]
    fn test_turtle_semicolon_predicate_shorthand() {
        let mut db = KolibrieDB::new();
        db.parse_turtle(
            r#"
            @prefix ex: <http://example.org/> .
            ex:Alex ex:Age 10; ex:Friend ex:Bob .
        "#,
        );

        let rows = execute(
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?age ?friend
            WHERE {
                ex:Alex ex:Age ?age .
                ex:Alex ex:Friend ?friend .
            }
        "#,
            &mut db,
        )
        .expect("query should run")
        .into_rows();

        assert_eq!(
            rows,
            vec![vec!["10".to_string(), "http://example.org/Bob".to_string(),]]
        );
    }

    #[test]
    fn test_turtle_comma_object_shorthand() {
        let mut db = KolibrieDB::new();
        db.parse_turtle(
            r#"
            @prefix ex: <http://example.org/> .
            ex:Alex ex:Friend ex:Bob, ex:Charlie .
        "#,
        );

        let rows = execute(
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?friend
            WHERE {
                ex:Alex ex:Friend ?friend .
            }
        "#,
            &mut db,
        )
        .expect("query should run")
        .into_rows();

        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec!["http://example.org/Bob".to_string()]));
        assert!(rows.contains(&vec!["http://example.org/Charlie".to_string()]));
    }

    #[test]
    fn test_turtle_semicolon_and_comma_shorthand_together() {
        let mut db = KolibrieDB::new();
        db.parse_turtle(
            r#"
            @prefix ex: <http://example.org/> .
            ex:Alex ex:Age 10; ex:Friend ex:Bob, ex:Charlie .
        "#,
        );

        let rows = execute(
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?predicate ?object
            WHERE {
                ex:Alex ?predicate ?object .
            }
        "#,
            &mut db,
        )
        .expect("query should run")
        .into_rows();

        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&vec![
            "http://example.org/Age".to_string(),
            "10".to_string(),
        ]));
        assert!(rows.contains(&vec![
            "http://example.org/Friend".to_string(),
            "http://example.org/Bob".to_string(),
        ]));
        assert!(rows.contains(&vec![
            "http://example.org/Friend".to_string(),
            "http://example.org/Charlie".to_string(),
        ]));
    }

    #[test]
    fn test_basic_filters() {
        let db = setup_test_db();

        let subj_filter = db
            .query()
            .with_subject("http://example.org/person1")
            .get_triples();
        assert_eq!(subj_filter.len(), 4);

        let pred_filter = db.query().with_predicate("ex:name").get_triples();
        assert_eq!(pred_filter.len(), 3);

        let obj_filter = db.query().with_object("Jane Doe").get_triples();
        assert_eq!(obj_filter.len(), 1);

        let subj_like = db.query().with_subject_like("person").get_triples();
        assert_eq!(subj_like.len(), 8);

        let pred_like = db.query().with_predicate_like("name").get_triples();
        assert_eq!(pred_like.len(), 3);

        let obj_like = db.query().with_object_like("example.com").get_triples();
        assert_eq!(obj_like.len(), 2);

        let subj_start = db
            .query()
            .with_subject_starting("http://example.org/person")
            .get_triples();
        assert_eq!(subj_start.len(), 8);

        let pred_end = db.query().with_predicate_ending("For").get_triples();
        assert_eq!(pred_end.len(), 2);

        let dict = db.dictionary();
        let numeric_objects: Vec<_> = default_triples(&db)
            .iter()
            .filter(|t| dict.decode(t.object).unwrap_or("").parse::<i32>().is_ok())
            .cloned()
            .collect();
        drop(dict);
        assert_eq!(numeric_objects.len(), 3);
    }

    #[test]
    fn test_combination_and_results() {
        let db = setup_test_db();

        let person_names = db
            .query()
            .with_subject_like("person")
            .with_predicate("ex:name")
            .get_triples();
        assert_eq!(person_names.len(), 2);

        let name_count = db.query().with_predicate("ex:name").count();
        assert_eq!(name_count, 3);

        let name_subjects = db.query().with_predicate("ex:name").get_subjects();
        assert_eq!(name_subjects.len(), 3);
        assert!(name_subjects.contains(&"http://example.org/person1".to_string()));

        let person_predicates = db
            .query()
            .with_subject("http://example.org/person1")
            .get_predicates();
        assert_eq!(person_predicates.len(), 4);
        assert!(person_predicates.contains(&"ex:name".to_string()));

        let age_values = db.query().with_predicate("ex:age").get_objects();
        assert_eq!(age_values.len(), 2);
        assert!(age_values.contains(&"30".to_string()));

        let jane_triples = db
            .query()
            .with_subject("http://example.org/person2")
            .get_decoded_triples();
        assert_eq!(jane_triples.len(), 4);

        let expected = (
            "http://example.org/person2".to_string(),
            "ex:name".to_string(),
            "Jane Doe".to_string(),
        );
        assert!(jane_triples.contains(&expected));
    }

    #[test]
    fn test_result_manipulation() {
        let db = setup_test_db();

        let age_triples = db.query().with_predicate("ex:age").get_triples();
        assert_eq!(age_triples.len(), 2);

        let dict = db.dictionary();
        let mut age_values: Vec<_> = age_triples
            .iter()
            .map(|t| dict.decode(t.object).unwrap_or("").to_string())
            .collect();
        drop(dict);

        age_values.sort();
        assert_eq!(age_values, vec!["25", "30"]);

        let all: Vec<_> = db.query().get_triples().into_iter().collect();

        let limited = db.query().limit(2).get_triples();
        assert_eq!(limited.len(), 2);

        let offset: Vec<_> = db
            .query()
            .offset(2)
            .limit(2)
            .get_triples()
            .into_iter()
            .collect();
        assert_eq!(offset.len(), 2);
        assert_eq!(offset[0], all[2]);

        let dict = db.dictionary();
        let groups = db
            .query()
            .group_by(|t| dict.decode(t.predicate).unwrap_or("").to_string());
        drop(dict);

        assert_eq!(groups.get("ex:name").map_or(0, |g| g.len()), 3);
        assert_eq!(groups.get("ex:age").map_or(0, |g| g.len()), 2);
    }

    #[test]
    fn test_distinct_and_join() {
        let mut db = setup_test_db();

        let orig_count = default_triples(&db).len();
        assert_eq!(orig_count, 11);

        let mut dict = db.dictionary_mut();
        let subject1 = dict.encode("http://example.org/person1");
        let pred_email = dict.encode("ex:email");
        let new_email = dict.encode("john.smith@example.com");
        drop(dict);

        db.add_triple(Triple {
            subject: subject1,
            predicate: pred_email,
            object: new_email,
        });

        let emails = db
            .query()
            .with_subject("http://example.org/person1")
            .with_predicate("ex:email")
            .get_triples();
        assert_eq!(emails.len(), 2);

        let tech_companies = db
            .query()
            .with_predicate("ex:industry")
            .with_object("Technology")
            .get_subjects();

        let tech_employees = db
            .query()
            .with_predicate("ex:worksFor")
            .with_object(tech_companies[0].as_str())
            .get_subjects();

        assert_eq!(tech_employees.len(), 2);
        assert!(tech_employees.contains(&"http://example.org/person1".to_string()));
    }

    #[test]
    fn test_complex_query() {
        let db = setup_test_db();

        let acme = db
            .query()
            .with_predicate("ex:name")
            .with_object("ACME Corp")
            .get_subjects();

        let employees = db
            .query()
            .with_predicate("ex:worksFor")
            .with_object(acme[0].as_str())
            .get_triples();

        let dict = db.dictionary();
        let triples = default_triples(&db);
        let young_subject_ids: Vec<u32> = employees
            .iter()
            .filter_map(|t| {
                let age_pred = dict.string_to_id.get("ex:age")?;

                let age_triple = triples
                    .iter()
                    .find(|triple| triple.subject == t.subject && triple.predicate == *age_pred)?;

                let age_str = dict.decode(age_triple.object)?;
                let age: i32 = age_str.parse().ok()?;

                if age < 30 {
                    Some(t.subject)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(young_subject_ids.len(), 1);
        assert_eq!(
            dict.decode(young_subject_ids[0]).unwrap_or(""),
            "http://example.org/person2"
        );
        drop(dict);
    }
}
