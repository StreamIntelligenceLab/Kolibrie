/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use kolibrie::parser::*;
use kolibrie::execute_query::*;
use kolibrie::sparql_database::SparqlDatabase;

fn main() {
    println!("=== Combined SPARQL / RDF-star / RULE / Provenance RULE Example ===\n");

    let mut database = SparqlDatabase::new();
    println!("[Stage 1] Loading base RDF facts (standard triples)...");

    database.add_triple_parts(
        "http://example.org/sensor/S1",
        "http://example.org/temperature",
        "92",
    );
    database.add_triple_parts(
        "http://example.org/sensor/S1",
        "http://example.org/pressure",
        "135",
    );
    database.add_triple_parts(
        "http://example.org/sensor/S2",
        "http://example.org/temperature",
        "71",
    );
    database.add_triple_parts(
        "http://example.org/sensor/S2",
        "http://example.org/pressure",
        "118",
    );
    database.add_triple_parts(
        "http://example.org/sensor/S3",
        "http://example.org/temperature",
        "88",
    );
    database.add_triple_parts(
        "http://example.org/sensor/S3",
        "http://example.org/pressure",
        "142",
    );

    let size_after_rdf = database.triples.len();
    println!("  Loaded {} RDF triples.", size_after_rdf);
    println!("  Sensors: S1 (temp=92, press=135), S2 (temp=71, press=118), S3 (temp=88, press=142)");

    println!("\n[Stage 2] Loading RDF-star annotations (quoted triples)...");
    let rdf_star_data = r#"<< <http://example.org/sensor/S1> <http://example.org/temperature> "92" >> <http://example.org/reliability> "0.95" .
<< <http://example.org/sensor/S2> <http://example.org/temperature> "71" >> <http://example.org/reliability> "0.80" .
<< <http://example.org/sensor/S3> <http://example.org/temperature> "88" >> <http://example.org/reliability> "0.85" ."#;

    database.parse_turtle(rdf_star_data);

    let size_after_rdf_star = database.triples.len();
    let rdf_star_count = size_after_rdf_star - size_after_rdf;
    println!("  Loaded {} RDF-star triples (reliability annotations).", rdf_star_count);
    println!("  Initial database size: {} triples.", size_after_rdf_star);

    println!("\n[Stage 3] SPARQL-star query — sensor calibration reliability...");
    println!("  Pattern:");
    println!("    << ?sensor <http://example.org/temperature> ?temp >>");
    println!("        <http://example.org/reliability> ?reliability .");

    database.get_or_build_stats();

    let sparql_star_query = r#"SELECT ?sensor ?temp ?reliability WHERE {
    << ?sensor <http://example.org/temperature> ?temp >> <http://example.org/reliability> ?reliability .
}"#;

    let star_results = execute_query_rayon_parallel2_volcano(sparql_star_query, &mut database);

    println!("  Results ({} rows):", star_results.len());
    for row in &star_results {
        if row.len() >= 3 {
            println!(
                "    sensor={} | temp={} | reliability={}",
                shorten(&row[0]),
                row[1],
                row[2]
            );
        }
    }

    println!("\n[Stage 4] Classical RULE :OverheatAlert — temperature > 80...");
    println!("  RULE :OverheatAlert :-");
    println!("    CONSTRUCT {{ ?sensor ex:overheatAlert true . }}");
    println!("    WHERE     {{ ?sensor ex:temperature ?t . FILTER(?t > 80) }}");

    let classical_rule = r#"PREFIX ex: <http://example.org/>

RULE :OverheatAlert :-
CONSTRUCT {
    ?sensor ex:overheatAlert true .
}
WHERE {
    ?sensor ex:temperature ?t .
    FILTER(?t > 80)
}"#;

    let size_before_classical = database.triples.len();

    match process_rule_definition(classical_rule, &mut database) {
        Ok((_, inferred)) => {
            let size_after_classical = database.triples.len();
            println!(
                "  Inferred {} new overheat-alert facts  (+{} triples).  Database: {} triples.",
                inferred.len(),
                size_after_classical - size_before_classical,
                size_after_classical
            );
        }
        Err(e) => eprintln!("  Classical rule error: {}", e),
    }

    println!("\n[Stage 5] SPARQL SELECT — sensors with overheat alert...");
    println!("  SELECT ?sensor WHERE {{ ?sensor ex:overheatAlert true . }}");

    let sparql_alert = r#"PREFIX ex: <http://example.org/>
SELECT ?sensor WHERE {
    ?sensor ex:overheatAlert true .
}"#;

    let alert_results = execute_query(sparql_alert, &mut database);
    println!("  Overheating sensors ({} found):", alert_results.len());
    for row in &alert_results {
        for val in row {
            println!("    {}", shorten(val));
        }
    }

    println!("\n[Stage 6] Provenance RULE :CriticalRisk PROB(combination=minmax)...");
    println!("  (Uses Stage 4 output — overheatAlert — as a premise!)");
    println!("  RULE :CriticalRisk PROB(combination=minmax) :-");
    println!("    CONSTRUCT {{ ?sensor ex:criticalRisk true . }}");
    println!("    WHERE     {{ ?sensor ex:overheatAlert true ; ex:pressure ?p . FILTER(?p > 130) }}");

    let prob_rule = r#"PREFIX ex: <http://example.org/>

RULE :CriticalRisk PROB(combination=minmax) :-
CONSTRUCT {
    ?sensor ex:criticalRisk true .
}
WHERE {
    ?sensor ex:overheatAlert true ;
            ex:pressure ?p .
    FILTER(?p > 130)
}"#;

    let size_before_prob = database.triples.len();

    match process_rule_definition(prob_rule, &mut database) {
        Ok((_, inferred)) => {
            let size_after_prob = database.triples.len();
            println!(
                "  Inferred {} new critical-risk facts  (+{} triples).  Database: {} triples.",
                inferred.len(),
                size_after_prob - size_before_prob,
                size_after_prob
            );
        }
        Err(e) => eprintln!("  Provenance rule error: {}", e),
    }

    println!("\n[Stage 7] SPARQL SELECT — sensors with critical risk...");
    println!("  SELECT ?sensor WHERE {{ ?sensor ex:criticalRisk true . }}");

    let sparql_critical = r#"PREFIX ex: <http://example.org/>
SELECT ?sensor WHERE {
    ?sensor ex:criticalRisk true .
}"#;

    let critical_results = execute_query(sparql_critical, &mut database);
    println!("  Critical-risk sensors ({} found):", critical_results.len());
    for row in &critical_results {
        for val in row {
            println!("    {}", shorten(val));
        }
    }

    println!("\n[Stage 8] SPARQL-star re-query — reliability annotations still accessible...");

    database.get_or_build_stats();
    let star_results2 = execute_query_rayon_parallel2_volcano(sparql_star_query, &mut database);
    println!("  Reliability annotations ({} rows):", star_results2.len());
    for row in &star_results2 {
        if row.len() >= 3 {
            println!(
                "    sensor={} | temp={} | reliability={}",
                shorten(&row[0]),
                row[1],
                row[2]
            );
        }
    }

    let final_size  = database.triples.len();
    let growth      = final_size as isize - size_after_rdf_star as isize;
    let growth_pct  = growth as f64 / size_after_rdf_star as f64 * 100.0;

    println!("\n=== Database Growth Summary ===");
    println!(
        "  Initial  (RDF + RDF-star):        {:>2} triples  ({} RDF + {} RDF-star)",
        size_after_rdf_star, size_after_rdf, rdf_star_count
    );
    println!(
        "  After classical RULE:              {:>2} triples  (+2 overheatAlert for S1, S3)",
        size_before_prob
    );
    println!(
        "  After provenance RULE:             {:>2} triples  (+2 criticalRisk for S1, S3)",
        final_size
    );
    println!(
        "  Total growth: +{} triples  ({:.0}%)",
        growth, growth_pct
    );
    println!();
    println!("  Syntax elements demonstrated:");
    println!("    * SPARQL SELECT            — Stages 5, 7");
    println!("    * RDF triples              — Stage 1");
    println!("    * RDF-star  << s p o >>    — Stage 2");
    println!("    * SPARQL-star              — Stages 3, 8");
    println!("    * RULE (classical)         — Stage 4");
    println!("    * RULE PROB(combination=minmax)");
    println!("                               — Stage 6  (provenance-based, uses Stage 4 output as premise)");
}

/// Shorten a full URI for display: keeps only the local name after the last '/' or '#'.
fn shorten(uri: &str) -> &str {
    if let Some(pos) = uri.rfind('/') {
        &uri[pos + 1..]
    } else if let Some(pos) = uri.rfind('#') {
        &uri[pos + 1..]
    } else {
        uri
    }
}
