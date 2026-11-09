/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::prelude::*;

fn insert() {
    let rdf_xml = r#"
<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:ex="http://example.org/">
  <rdf:Description rdf:about="http://example.org/JohnDoe">
    <ex:age>30</ex:age>
  </rdf:Description>
</rdf:RDF>
"#;

    let mut database = KolibrieDB::new();
    database.parse_rdf(rdf_xml);

    let sparql_query = r#"PREFIX ex: <http://example.org/> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> INSERT {<http://example.org/JohnDoe> ex:occupation "Software Developer"} WHERE {<http://example.org/JohnDoe> ex:age "30"}"#;

    let _results = execute(sparql_query, &mut database)
        .expect("query should run")
        .into_rows();

    database.debug_print_triples();
}

fn main() {
    insert();
}
