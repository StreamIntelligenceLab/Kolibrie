use kolibrie::query_builder::QueryBuilder;
use kolibrie::sparql_database::SparqlDatabase;
use std::collections::BTreeSet;

fn fixture() -> SparqlDatabase {
    let mut db = SparqlDatabase::new();
    for (s, o) in [("s1", "b"), ("s2", "c"), ("s3", "a"), ("s1", "b")] {
        db.add_triple_parts(s, "p", o);
    }
    db
}

#[test]
fn natural_order_and_equal_custom_keys_preserve_unique_triples() {
    let db = fixture();
    let natural: Vec<_> = QueryBuilder::new(&db).get_triples().into_iter().collect();
    assert_eq!(natural.len(), 3);
    assert_eq!(QueryBuilder::new(&db).get_ordered_triples(), natural);
    assert_eq!(
        QueryBuilder::new(&db)
            .order_by(|_| "equal".to_string())
            .asc()
            .get_ordered_triples(),
        natural
    );
    assert_eq!(
        QueryBuilder::new(&db)
            .order_by(|_| "equal".to_string())
            .desc()
            .get_ordered_triples(),
        natural
    );
    assert!(QueryBuilder::new(&db)
        .offset(usize::MAX)
        .get_ordered_triples()
        .is_empty());
    assert!(QueryBuilder::new(&db)
        .limit(0)
        .get_ordered_triples()
        .is_empty());
}

#[test]
fn custom_order_precedes_slicing_and_decoding() {
    let db = fixture();
    let make = || QueryBuilder::new(&db).order_by(|triple| format!("{:010}", triple.object));
    let ascending = make().get_ordered_triples();
    let descending = make().desc().get_ordered_triples();
    assert_eq!(
        ascending.iter().rev().cloned().collect::<Vec<_>>(),
        descending
    );
    assert_eq!(
        make().offset(1).limit(1).get_ordered_triples(),
        ascending[1..2]
    );
    let decoded = make().get_ordered_decoded_triples();
    let dict = db.dictionary.read().unwrap();
    for (encoded, decoded) in ascending.iter().zip(decoded) {
        assert_eq!(dict.decode(encoded.object).unwrap(), decoded.2);
    }
    assert_eq!(
        make().offset(1).get_triples(),
        ascending[1..].iter().cloned().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        make().get_decoded_triples(),
        QueryBuilder::new(&db).get_decoded_triples()
    );
}

#[test]
fn join_retains_existing_unique_selection() {
    let db = fixture();
    let make = || QueryBuilder::new(&db).join(&db).join_on_predicate();
    let old = make().get_triples();
    let ordered = make()
        .order_by(|t| format!("{:010}", t.subject))
        .get_ordered_triples();
    assert_eq!(ordered.len(), old.len());
    assert_eq!(ordered.into_iter().collect::<BTreeSet<_>>(), old);
}
