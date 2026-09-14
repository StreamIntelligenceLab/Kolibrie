/*
 * Copyright © 2024 Volodymyr Kadzhaia
 * Copyright © 2024 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Tests for the RDF triple-pattern parser (`crate::rdf_parser`).

use shared::dictionary::Dictionary;
use shared::terms::Term;

use crate::rdf_parser::{parse_facts, parse_rules, parse_stream_shapes};
use crate::syntax::{Mode, TemporalAtom};

fn cst(t: &Term) -> u32 {
    match t {
        Term::Constant(id) => *id,
        other => panic!("expected constant, got {:?}", other),
    }
}

// ── The two IRI bugs the lift had to fix ──────────────────────────────────────

/// `#` inside `<…>` is part of the IRI, not the start of a line comment.
/// Without this, every `rdf:type` pattern lost its fragment.
#[test]
fn test_comment_strip_preserves_iri_fragment() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?s, <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>, ?o) :- (?s, <http://x/p>, ?o).",
        &mut dict,
        Mode::Streaming,
    )
    .expect("fragment IRI should parse");

    assert_eq!(rules.len(), 1);
    assert_eq!(
        dict.decode(cst(&rules[0].head.1)).unwrap(),
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    );
}

/// A real `#` comment after an IRI still terminates the line.
#[test]
fn test_comment_after_iri_still_strips() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?s, <http://x/p>, ?o) :- (?s, <http://x/q>, ?o). # trailing (?a, <http://x/r>, ?b).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();
    assert_eq!(rules.len(), 1, "commented-out rule must not be parsed");
}

/// Dots in a host name must not terminate a shape block.
#[test]
fn test_stream_iri_with_dots() {
    let mut dict = Dictionary::new();
    let shapes = parse_stream_shapes(
        "STREAM <http://utm.example.org/telemetry>\n\
           PATTERN (?s, <http://x/p>, ?o)\n\
           KEY ?s\n\
           STALENESS 10\n\
         .",
        &mut dict,
    )
    .expect("dotted STREAM IRI should parse");

    assert_eq!(shapes.len(), 1);
    assert_eq!(shapes[0].stream_iri, "http://utm.example.org/telemetry");
    assert_eq!(shapes[0].channel_key, vec!["s".to_string()]);
    assert_eq!(shapes[0].staleness.max_gap_ms, 10);
}

// ── Ground facts ──────────────────────────────────────────────────────────────

/// Plain N-Triples, as any RDF tool would emit it.
#[test]
fn test_parse_facts_ntriples() {
    let mut dict = Dictionary::new();
    let restricted = dict.encode("http://example.org/dront/Restricted");

    let facts = parse_facts(
        "# a comment\n\
         <http://utm.example.org/zone/hospital> <http://example.org/dront/status> <http://example.org/dront/Restricted> .\n\
         <http://utm.example.org/zone/event> <http://example.org/dront/status> <http://example.org/dront/Restricted> .",
        &mut dict,
    )
    .expect("N-Triples should parse");

    assert_eq!(facts.len(), 2);
    assert_eq!(facts[0].object, restricted);
    assert_eq!(
        dict.decode(facts[0].subject).unwrap(),
        "http://utm.example.org/zone/hospital"
    );
}

/// Dots inside IRIs must not terminate the statement.
#[test]
fn test_parse_facts_dotted_iris() {
    let mut dict = Dictionary::new();
    let facts = parse_facts(
        "<http://utm.example.org/a> <http://ex.co/p> <http://sub.domain.org/o> .",
        &mut dict,
    )
    .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(dict.decode(facts[0].object).unwrap(), "http://sub.domain.org/o");
}

/// A literal object may contain spaces and a trailing language tag.
#[test]
fn test_parse_facts_literal_with_spaces() {
    let mut dict = Dictionary::new();
    let facts = parse_facts(
        "<http://x/s> <http://x/label> \"UZ Gent Hospital\"@en .",
        &mut dict,
    )
    .unwrap();
    assert_eq!(facts.len(), 1);
    assert_eq!(dict.decode(facts[0].object).unwrap(), "\"UZ Gent Hospital\"@en");
}

/// PREFIX remains available as a convenience, though it is not strict N-Triples.
#[test]
fn test_parse_facts_prefixed_convenience() {
    let mut dict = Dictionary::new();
    let facts = parse_facts(
        "PREFIX zone: <http://utm.example.org/zone/>\n\
         PREFIX dront: <http://example.org/dront/>\n\
         zone:hospital dront:status dront:Restricted .",
        &mut dict,
    )
    .unwrap();
    assert_eq!(
        dict.decode(facts[0].subject).unwrap(),
        "http://utm.example.org/zone/hospital"
    );
}

#[test]
fn test_parse_facts_rejects_variables() {
    let mut dict = Dictionary::new();
    let err = parse_facts("?z <http://x/p> <http://x/o> .", &mut dict).unwrap_err();
    assert!(err.contains("?z"), "error should name the variable: {}", err);
    assert!(err.contains("ground"), "error should say facts must be ground: {}", err);
}

#[test]
fn test_parse_facts_wrong_arity() {
    let mut dict = Dictionary::new();
    let err = parse_facts("<http://x/s> <http://x/p> .", &mut dict).unwrap_err();
    assert!(err.contains("2 term"), "error should report the count: {}", err);
}

#[test]
fn test_parse_facts_empty_is_ok() {
    let mut dict = Dictionary::new();
    assert!(parse_facts("", &mut dict).unwrap().is_empty());
    assert!(parse_facts("# only a comment\n", &mut dict).unwrap().is_empty());
}

// ── EXPIRY ────────────────────────────────────────────────────────────────────

#[test]
fn test_expiry_patterns_parse() {
    let mut dict = Dictionary::new();
    let shapes = parse_stream_shapes(
        "PREFIX utm: <http://utm.example.org/>\n\
         STREAM <http://utm.example.org/telemetry>\n\
           PATTERN (?obs, <http://x/p>, ?drone)\n\
           KEY ?drone\n\
           STALENESS 10\n\
           EXPIRY (?drone, utm:channelStatus, utm:expired)\n\
         .",
        &mut dict,
    )
    .expect("EXPIRY should parse");

    assert_eq!(shapes[0].on_expiry.len(), 1);
    let p = &shapes[0].on_expiry[0];
    assert_eq!(p.0, Term::Variable("drone".to_string()));
    assert_eq!(
        dict.decode(cst(&p.1)).unwrap(),
        "http://utm.example.org/channelStatus"
    );
}

/// A shape with no EXPIRY section is still valid — it just transmits nothing
/// when a channel goes quiet, which is the old behaviour.
#[test]
fn test_expiry_is_optional() {
    let mut dict = Dictionary::new();
    let shapes = parse_stream_shapes(
        "STREAM <http://x/s>\n PATTERN (?a, <http://x/p>, ?b)\n KEY ?a\n STALENESS 5\n.",
        &mut dict,
    )
    .unwrap();
    assert!(shapes[0].on_expiry.is_empty());
}

/// Only the channel key survives expiry, so a non-key variable can never be
/// instantiated — reject it at parse time rather than emitting nothing.
#[test]
fn test_expiry_rejects_non_key_variable() {
    let mut dict = Dictionary::new();
    let err = parse_stream_shapes(
        "STREAM <http://x/s>\n\
           PATTERN (?obs, <http://x/p>, ?drone)\n\
           KEY ?drone\n\
           STALENESS 10\n\
           EXPIRY (?obs, <http://x/status>, <http://x/expired>)\n\
         .",
        &mut dict,
    )
    .unwrap_err();
    assert!(err.contains("?obs"), "error should name the variable: {}", err);
    assert!(err.contains("KEY"), "error should mention KEY: {}", err);
}

// ── PREFIX handling ───────────────────────────────────────────────────────────

#[test]
fn test_prefix_expansion_matches_full_iri() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "PREFIX dront: <http://example.org/dront/>\n\
         (?d, dront:inZone, ?z) :- (?d, <http://example.org/dront/inZone>, ?z).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();

    let head_pred = cst(&rules[0].head.1);
    let body_pred = match &rules[0].body[0] {
        TemporalAtom::Base(p) => cst(&p.1),
        other => panic!("expected base atom, got {:?}", other),
    };
    assert_eq!(
        head_pred, body_pred,
        "prefixed name must intern to the same id as the full IRI"
    );
    assert_eq!(
        dict.decode(head_pred).unwrap(),
        "http://example.org/dront/inZone"
    );
}

/// Back-compat: `:name` with no empty prefix declared is interned verbatim,
/// exactly as the playground and http-server have always relied on.
#[test]
fn test_bare_colon_name_interned_verbatim() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?x, :wasNear, ?y) :- (?x, :location, ?l).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();
    assert_eq!(dict.decode(cst(&rules[0].head.1)).unwrap(), ":wasNear");
}

#[test]
fn test_declared_empty_prefix_expands() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "PREFIX : <http://x/>\n(?x, :wasNear, ?y) :- (?x, :location, ?l).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();
    assert_eq!(
        dict.decode(cst(&rules[0].head.1)).unwrap(),
        "http://x/wasNear"
    );
}

#[test]
fn test_unknown_prefix_is_an_error() {
    let mut dict = Dictionary::new();
    let err = parse_rules(
        "(?x, nope:p, ?y) :- (?x, <http://x/q>, ?y).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap_err();
    assert!(err.contains("nope"), "error should name the prefix: {}", err);
}

// ── Rule labels ───────────────────────────────────────────────────────────────

#[test]
fn test_rule_label_becomes_id_else_index() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "[alpha] (?x, <http://x/a>, ?y) :- (?x, <http://x/b>, ?y).\n\
         (?x, <http://x/c>, ?y) :- (?x, <http://x/d>, ?y).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();

    assert_eq!(rules[0].id, "alpha");
    assert_eq!(rules[1].id, "1", "unlabelled rules keep the index id");
}

// ── Operators ─────────────────────────────────────────────────────────────────

#[test]
fn test_since_binary_atom() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?d, <http://x/s>, ?z) :- Since[0,600]((?d, <http://x/p>, <false>), (?d, <http://x/e>, ?z)).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();

    match &rules[0].body[0] {
        TemporalAtom::Since { interval, .. } => {
            assert_eq!(interval.start, 0);
            assert_eq!(interval.end, 600);
        }
        other => panic!("expected Since, got {:?}", other),
    }
}

#[test]
fn test_future_operator_rejected_in_streaming() {
    let mut dict = Dictionary::new();
    let err = parse_rules(
        "(?x, <http://x/a>, ?y) :- Boxplus[0,5](?x, <http://x/b>, ?y).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap_err();
    assert!(err.contains("requires static data"), "got: {}", err);

    parse_rules(
        "(?x, <http://x/a>, ?y) :- Boxplus[0,5](?x, <http://x/b>, ?y).",
        &mut Dictionary::new(),
        Mode::Static,
    )
    .expect("future operators are allowed in static mode");
}

#[test]
fn test_stacked_operators() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?x, <http://x/a>, ?y) :- Box[0,5]Diamond[0,2](?x, <http://x/b>, ?y).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();

    match &rules[0].body[0] {
        TemporalAtom::Box_ { inner, .. } => match inner.as_ref() {
            TemporalAtom::Diamond { .. } => {}
            other => panic!("expected nested Diamond, got {:?}", other),
        },
        other => panic!("expected Box, got {:?}", other),
    }
}

// ── The drone demo's shipped configuration ────────────────────────────────────
//
// Mirrors DEFAULT_RULES / DEFAULT_SHAPES in
// kolibrie/examples/real_scenario/drone_traffic_safety_demo.rs. The demo also
// parses its own text at startup and panics on failure, so drift shows up
// immediately when running it; this test guards the parser features it needs.

const DEMO_RULES: &str = r#"
PREFIX dront: <http://example.org/dront/>
PREFIX utm:   <http://utm.example.org/>

# Drone stayed inside a restricted zone for 30 consecutive ticks.
[sustainedGeofenceViolation]
(?d, utm:violatedZone, ?z) :-
    Box[0,30](?d, dront:inZone, ?z),
    (?z, dront:status, dront:Restricted).

# Control channel has been expired for 10 consecutive ticks.
[controlLinkLoss]
(?d, utm:status, utm:linkLost) :-
    Box[0,10](?d, utm:channelStatus, utm:expired).

# Off the filed flight plan ever since entering a restricted zone.
[offCourseSinceRestrictedEntry]
(?d, utm:status, utm:offCourse) :-
    (?d, dront:onFlightPlan, <false>),
    (?z, dront:status, dront:Restricted),
    Since[0,600]((?d, dront:onFlightPlan, <false>), (?d, dront:enteredZone, ?z)).

# Loitered 10 ticks in UZ Gent (Restricted1), then APPEARED in City Hall
# (Restricted2) within the next 10. The dwell is on the FIRST zone; a single
# tick in the second is enough, so a brief fly-through still trips it.
# Box[1,11] is offset by one tick so the two stays need not overlap, and the
# Diamond[0,10] is the travel gap. True only while the drone is in City Hall.
[zoneTransition]
(?d, utm:transitioned, ?z2) :-
    (?d, dront:inZone, ?z2),
    (?z2, dront:status, dront:Restricted2),
    Diamond[0,10](Box[1,11]((?d, dront:inZone, ?z),
                            (?z, dront:status, dront:Restricted1))).

# A deliberate tour: City Hall -> Citadelpark -> UZ Gent, dwelling 5 ticks in
# each. Written outside-in, so it reads BACKWARDS in time from "now": the last
# leg first, each Diamond stepping back to the leg before it. The Diamonds are
# the travel gaps, so the legs need not be contiguous (1-15 ticks between them).
# Drag drone B through the three zones to trigger it.
[zoneTour]
(?d, utm:tour, ?z3) :-
    Box[0,5]((?d, dront:inZone, ?z3), (?z3, dront:status, dront:Restricted1)),
    Diamond[6,20](
        Box[0,5]((?d, dront:inZone, ?z2), (?z2, dront:status, dront:Restricted3)),
        Diamond[6,20](
            Box[0,5]((?d, dront:inZone, ?z1), (?z1, dront:status, dront:Restricted2))
        )
    ).
"#;

const DEMO_SHAPES: &str = r#"
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX sosa:  <http://www.w3.org/ns/sosa/>
PREFIX dront: <http://example.org/dront/>
PREFIX utm:   <http://utm.example.org/>

STREAM <http://utm.example.org/telemetry>
    PATTERN (?obs, rdf:type, sosa:Observation)
            (?obs, sosa:madeBySensor, ?drone)
            (?obs, sosa:hasResult, ?tlm)
            (?drone, rdf:type, dront:Drone)
            (?tlm, rdf:type, dront:Telemetry)
            (?tlm, utm:position, ?pos)
            (?tlm, utm:altitude, ?alt)
            (?tlm, utm:aisStatus, ?status)
    KEY ?drone
    STALENESS 10
    EXPIRY (?drone, utm:channelStatus, utm:expired)
.
"#;

const DEMO_STATIC: &str = r#"
# Zone classification. Plain N-Triples — paste in anything an RDF tool emits.
#
# Each zone carries TWO classes. The generic `Restricted` is what the geofence
# rules join on; the numbered one identifies the individual zone for `zoneTour`.
# Drop the generic lines and the geofence rules go quiet with no error.
<http://utm.example.org/zone/hospital> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
<http://utm.example.org/zone/hospital> <http://example.org/dront/status> <http://example.org/dront/Restricted1> .
<http://utm.example.org/zone/government> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
<http://utm.example.org/zone/government> <http://example.org/dront/status> <http://example.org/dront/Restricted2> .
<http://utm.example.org/zone/event> <http://example.org/dront/status> <http://example.org/dront/Restricted> .
<http://utm.example.org/zone/event> <http://example.org/dront/status> <http://example.org/dront/Restricted3> .
"#;

#[test]
fn test_demo_defaults_parse() {
    let mut dict = Dictionary::new();

    let rules = parse_rules(DEMO_RULES, &mut dict, Mode::Streaming)
        .expect("demo default rules must parse");
    let ids: Vec<&str> = rules.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "sustainedGeofenceViolation",
            "controlLinkLoss",
            "offCourseSinceRestrictedEntry",
            "zoneTransition",
            "zoneTour"
        ]
    );

    // Background facts must be valid N-Triples and dual-classify every zone:
    // the generic class feeds the geofence rules, the numbered one zoneTour.
    let facts = crate::rdf_parser::parse_facts(DEMO_STATIC, &mut dict)
        .expect("demo background facts must parse");
    assert_eq!(facts.len(), 6, "three zones x (generic + numbered) class");

    let shapes = parse_stream_shapes(DEMO_SHAPES, &mut dict)
        .expect("demo default shape must parse");
    assert_eq!(shapes.len(), 1);
    assert_eq!(shapes[0].stream_iri, "http://utm.example.org/telemetry");
    assert_eq!(shapes[0].event_pattern.len(), 8);
    assert_eq!(shapes[0].channel_key, vec!["drone".to_string()]);
    assert_eq!(shapes[0].staleness.max_gap_ms, 10);
    // controlLinkLoss has nothing to observe without this.
    assert_eq!(shapes[0].on_expiry.len(), 1);
}

/// The demo's rules and shape must agree with `init_vocab`'s full IRIs — a
/// mismatch would parse and validate, then silently match nothing at runtime.
#[test]
fn test_demo_defaults_match_vocab_iris() {
    let mut dict = Dictionary::new();

    // Intern the vocab IRIs first, the way init_vocab does.
    let rdf_type = dict.encode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let in_zone = dict.encode("http://example.org/dront/inZone");
    let violated = dict.encode("http://utm.example.org/violatedZone");
    let xsd_false = dict.encode("false");

    let rules = parse_rules(DEMO_RULES, &mut dict, Mode::Streaming).unwrap();
    let shapes = parse_stream_shapes(DEMO_SHAPES, &mut dict).unwrap();

    assert_eq!(cst(&rules[0].head.1), violated);
    match &rules[0].body[0] {
        TemporalAtom::Box_ { inner, .. } => match inner.as_ref() {
            TemporalAtom::Base(p) => assert_eq!(cst(&p.1), in_zone),
            other => panic!("expected base atom, got {:?}", other),
        },
        other => panic!("expected Box, got {:?}", other),
    }
    // `<false>` is the demo's quoting device for the bare string "false".
    match &rules[2].body[0] {
        TemporalAtom::Base(p) => assert_eq!(cst(&p.2), xsd_false),
        other => panic!("expected base atom, got {:?}", other),
    }
    assert_eq!(cst(&shapes[0].event_pattern[0].1), rdf_type);
}

// ── Conjunction under an operator ─────────────────────────────────────────────

/// `Op[a,b]((p1), (p2))` scopes the operator over BOTH patterns, which is
/// strictly stronger than hoisting the second one out of the operator.
#[test]
fn test_operator_over_conjunction() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "PREFIX d: <http://x/>\n\
         (?x, d:out, ?z) :- Box[0,10]((?x, d:inZone, ?z), (?z, d:status, d:R2)).",
        &mut dict,
        Mode::Streaming,
    )
    .expect("operator over a conjunction should parse");

    match &rules[0].body[0] {
        TemporalAtom::Box_ { interval, inner } => {
            assert_eq!(interval.end, 10);
            match inner.as_ref() {
                TemporalAtom::Conj(atoms) => assert_eq!(atoms.len(), 2),
                other => panic!("expected Conj under Box, got {:?}", other),
            }
        }
        other => panic!("expected Box, got {:?}", other),
    }
}

/// A single parenthesised pattern must still parse as one Base atom, not a
/// one-element conjunction — the two forms must stay distinguishable.
#[test]
fn test_single_pattern_is_not_a_conjunction() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?x, <http://x/p>, ?z) :- Box[0,10](?x, <http://x/q>, ?z).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();
    match &rules[0].body[0] {
        TemporalAtom::Box_ { inner, .. } => match inner.as_ref() {
            TemporalAtom::Base(_) => {}
            other => panic!("expected Base under Box, got {:?}", other),
        },
        other => panic!("expected Box, got {:?}", other),
    }
}

/// Conjuncts may themselves be operator atoms, not just plain patterns.
#[test]
fn test_conjunction_of_nested_operators() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "PREFIX d: <http://x/>\n\
         (?x, d:out, ?z) :- Box[0,5]((?x, d:a, ?z), Diamond[0,2](?x, d:b, ?z)).",
        &mut dict,
        Mode::Streaming,
    )
    .unwrap();
    match &rules[0].body[0] {
        TemporalAtom::Box_ { inner, .. } => match inner.as_ref() {
            TemporalAtom::Conj(atoms) => {
                assert!(matches!(atoms[0], TemporalAtom::Base(_)));
                assert!(matches!(atoms[1], TemporalAtom::Diamond { .. }));
            }
            other => panic!("expected Conj, got {:?}", other),
        },
        other => panic!("expected Box, got {:?}", other),
    }
}

/// Redundant grouping parens around a single operator atom must be transparent:
/// `Diamond[0,10](Box[1,11](...))` is the same as writing the operators adjacent.
#[test]
fn test_redundant_grouping_parens() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "PREFIX d: <http://x/>\n\
         (?a, d:out, ?z) :- Diamond[0,10](Box[1,11]((?a, d:inZone, ?z), (?z, d:status, d:R1))).",
        &mut dict,
        Mode::Streaming,
    )
    .expect("grouping parens should be transparent");

    match &rules[0].body[0] {
        TemporalAtom::Diamond { inner, .. } => match inner.as_ref() {
            TemporalAtom::Box_ { inner, .. } => {
                assert!(matches!(inner.as_ref(), TemporalAtom::Conj(a) if a.len() == 2));
            }
            other => panic!("expected Box under Diamond, got {:?}", other),
        },
        other => panic!("expected Diamond, got {:?}", other),
    }
}

/// A bracketed literal must not be mistaken for an operator keyword.
#[test]
fn test_literal_with_brackets_is_not_an_operator() {
    let mut dict = Dictionary::new();
    let rules = parse_rules(
        "(?a, <http://x/p>, \"a[b]\") :- Box[0,2](?a, <http://x/q>, \"c[d]\").",
        &mut dict,
        Mode::Streaming,
    )
    .expect("literals containing brackets should parse");
    match &rules[0].body[0] {
        TemporalAtom::Box_ { inner, .. } => {
            assert!(matches!(inner.as_ref(), TemporalAtom::Base(_)));
        }
        other => panic!("expected Box over a Base atom, got {:?}", other),
    }
}
