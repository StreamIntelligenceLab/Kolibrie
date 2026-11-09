//! Rendering a reasoner as a DOT graph, for looking at rather than querying

use crate::reasoning::Reasoner;
use shared::term_encoder::TermView;
use shared::terms::{Term, TriplePattern};
use shared::triple::Triple;
use std::collections::HashSet;

/// Contains a method to convert a specific data structure into a representation in the DOT language
pub trait ToDot {
    /// Renders as a complete DOT `digraph`, ready to hand to Graphviz
    fn to_dot(&self) -> String;
}

fn resolve_term_to_string(term: &Term, dictionary: &TermView) -> String {
    match term {
        Term::Variable(s) => s.clone(),
        Term::Constant(c) => String::from(dictionary.decode(*c).unwrap()),
        Term::QuotedTriple(qt) => {
            let s = resolve_term_to_string(&qt.0, dictionary);
            let p = resolve_term_to_string(&qt.1, dictionary);
            let o = resolve_term_to_string(&qt.2, dictionary);
            format!("<< {} {} {} >>", s, p, o)
        }
    }
}

fn triple_patterns_to_dot(triple_patterns: &Vec<TriplePattern>, kg: &Reasoner) -> String {
    let dict = kg.term_view();

    triple_patterns
        .iter()
        .map(|pattern| {
            let subject = resolve_term_to_string(&pattern.0, &dict);
            let predicate_str = resolve_term_to_string(&pattern.1, &dict);
            let object_str = resolve_term_to_string(&pattern.2, &dict);
            format!("({}, {}, {})", subject, predicate_str, object_str)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn get_subject_object_iterator(triple: &Triple) -> impl Iterator<Item = u32> {
    std::iter::once(triple.subject).chain(std::iter::once(triple.object))
}

fn create_nodes(kg: &Reasoner, out: &mut String) {
    let dict = kg.term_view();

    let mut all_subject_object_ids: Vec<u32> = kg
        .dataset_index
        .query(None, None, None)
        .iter()
        .flat_map(|t| get_subject_object_iterator(&t))
        .collect::<HashSet<u32>>()
        .into_iter()
        .collect();
    all_subject_object_ids.sort();

    for id in all_subject_object_ids {
        let name = dict.decode(id).unwrap();
        out.push_str(&format!("{} [label=\"{}\"]\n", id, name));
    }

    for (i, rule) in kg.rules.iter().enumerate() {
        let premise_patterns_str = triple_patterns_to_dot(&rule.premise, kg);
        let conclusion_patterns_str = triple_patterns_to_dot(&rule.conclusion, kg);
        out.push_str(&format!(
            "Rule{}_premise [label=\"{}\", shape=box]\n",
            i, premise_patterns_str
        ));
        out.push_str(&format!(
            "Rule{}_conclusion [label=\"{}\", shape=box]\n",
            i, conclusion_patterns_str
        ));
    }
}

fn create_edges(kg: &Reasoner, out: &mut String) {
    let dict = kg.term_view();

    let all_facts = kg.dataset_index.query(None, None, None);
    for triple in all_facts {
        let label = dict.decode(triple.predicate).unwrap();
        out.push_str(&format!(
            "{} -> {} [label=\"{}\"]\n",
            triple.subject, triple.object, label
        ));
    }

    for (i, _) in kg.rules.iter().enumerate() {
        out.push_str(&format!("Rule{0}_premise -> Rule{0}_conclusion\n", i))
    }
}

/// Outputs a graph for the reasoner, making it easy to see what it consists of
impl ToDot for Reasoner {
    fn to_dot(&self) -> String {
        let mut out = String::new();
        out.push_str("digraph {\n");

        create_nodes(self, &mut out);

        out.push_str("\n");

        create_edges(self, &mut out);

        out.push_str("}");

        out
    }
}
