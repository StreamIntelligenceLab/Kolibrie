/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.
 */

use crate::reasoning::materialisation::provenance_semi_naive::semi_naive_with_initial_tags;
use crate::reasoning::Reasoner;
use shared::hybrid::{
    compile_lineage_to_sdd, evaluate_hybrid, CompiledSdd, HybridConfig, HybridError,
    HybridProbabilityResult, LineageId, LineageProvenance, SeedSnapshot,
};
use shared::rule::Rule;
use shared::tag_store::TagStore;
use shared::terms::Term;
use shared::triple::Triple;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Full lineage result. Evaluation is intentionally separate so callers can
/// compile only the hypotheses that matter to a decision.
pub struct LineageMaterialization {
    pub new_facts: Vec<Triple>,
    pub tags: TagStore<LineageProvenance>,
}

impl LineageMaterialization {
    pub fn lineage(&self, triple: &Triple) -> LineageId {
        self.tags.get_tag(triple)
    }

    pub fn evaluate(&self, triple: &Triple, config: &HybridConfig) -> HybridProbabilityResult {
        let provenance = self.tags.provenance();
        evaluate_hybrid(
            provenance.store(),
            provenance.seeds(),
            self.lineage(triple),
            config,
        )
    }

    pub fn compile_exact(
        &self,
        triple: &Triple,
        config: &HybridConfig,
    ) -> Result<CompiledSdd, shared::hybrid::HybridReason> {
        let provenance = self.tags.provenance();
        let store = provenance
            .store()
            .lock()
            .map_err(|_| shared::hybrid::HybridReason::DiagnosticOnly)?;
        compile_lineage_to_sdd(
            &store,
            provenance.seeds(),
            self.lineage(triple),
            config.sdd_budget,
            config.sdd_node_budget,
        )
    }
}

fn constant_predicate(term: &Term) -> Result<u32, HybridError> {
    match term {
        Term::Constant(id) => Ok(*id),
        Term::Variable(name) => Err(HybridError::UnsupportedRule(format!(
            "variable predicate ?{name} prevents acyclic dependency analysis"
        ))),
        Term::QuotedTriple(_) => Err(HybridError::UnsupportedRule(
            "quoted triples cannot be rule predicates in hybrid v1".into(),
        )),
    }
}

/// Reject predicate dependency cycles before lineage materialisation. This is
/// deliberately conservative: even a data-bounded recursive rule is rejected.
pub fn validate_hybrid_rules(rules: &[Rule]) -> Result<(), HybridError> {
    let mut graph: HashMap<u32, HashSet<u32>> = HashMap::new();
    for rule in rules {
        let heads: Vec<u32> = rule
            .conclusion
            .iter()
            .map(|pattern| constant_predicate(&pattern.1))
            .collect::<Result<_, _>>()?;
        let bodies: Vec<u32> = rule
            .premise
            .iter()
            .chain(rule.negative_premise.iter())
            .map(|pattern| constant_predicate(&pattern.1))
            .collect::<Result<_, _>>()?;
        for body in &bodies {
            for head in &heads {
                graph.entry(*body).or_default().insert(*head);
                graph.entry(*head).or_default();
            }
        }
    }

    fn visit(
        node: u32,
        graph: &HashMap<u32, HashSet<u32>>,
        visiting: &mut HashSet<u32>,
        visited: &mut HashSet<u32>,
    ) -> bool {
        if visited.contains(&node) {
            return false;
        }
        if !visiting.insert(node) {
            return true;
        }
        if graph.get(&node).is_some_and(|next| {
            next.iter()
                .any(|child| visit(*child, graph, visiting, visited))
        }) {
            return true;
        }
        visiting.remove(&node);
        visited.insert(node);
        false
    }

    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for node in graph.keys().copied() {
        if visit(node, &graph, &mut visiting, &mut visited) {
            return Err(HybridError::UnsupportedRecursion(format!(
                "predicate dependency cycle includes dictionary ID {node}"
            )));
        }
    }
    Ok(())
}

pub fn materialize_lineage(
    reasoner: &mut Reasoner,
    snapshot: SeedSnapshot,
) -> Result<LineageMaterialization, HybridError> {
    validate_hybrid_rules(&reasoner.rules)?;
    let snapshot = Arc::new(snapshot);
    let provenance = LineageProvenance::new(Arc::clone(&snapshot));
    let mut initial_tags = TagStore::new(provenance.clone());

    let max_seed = snapshot.records().map(|record| record.id.get()).max();
    if let Some(max_seed) = max_seed {
        initial_tags.seed_triples.resize(
            max_seed as usize + 1,
            Triple {
                subject: 0,
                predicate: 0,
                object: 0,
            },
        );
    }

    for (triple, ids) in snapshot.triples() {
        let mut literals = Vec::with_capacity(ids.len());
        for id in ids {
            literals.push(provenance.literal(*id));
            initial_tags.seed_triples[id.get() as usize] = triple.clone();
        }
        let tag = provenance
            .store()
            .lock()
            .map_err(|_| HybridError::PoisonedState)?
            .or(literals);
        initial_tags.set_tag(triple, tag);
        reasoner.insert_ground_triple(triple.clone());
    }

    let (new_facts, tags) = semi_naive_with_initial_tags(reasoner, provenance, initial_tags);
    Ok(LineageMaterialization { new_facts, tags })
}

impl Reasoner {
    pub fn infer_new_facts_with_hybrid(
        &mut self,
        snapshot: SeedSnapshot,
        config: &HybridConfig,
    ) -> Result<
        (
            Vec<Triple>,
            HashMap<Triple, HybridProbabilityResult>,
            LineageMaterialization,
        ),
        HybridError,
    > {
        config.validate()?;
        let materialization = materialize_lineage(self, snapshot)?;
        let mut results = HashMap::new();
        for triple in &materialization.new_facts {
            results.insert(triple.clone(), materialization.evaluate(triple, config));
        }
        Ok((materialization.new_facts.clone(), results, materialization))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::hybrid::{AlertDecision, HybridProbabilityResult};
    use shared::rule::Rule;
    use shared::terms::{Term, TriplePattern};

    fn pattern(s: Term, p: u32, o: Term) -> TriplePattern {
        (s, Term::Constant(p), o)
    }

    #[test]
    fn recursive_rules_are_rejected() {
        let rule = Rule {
            premise: vec![pattern(
                Term::Variable("x".into()),
                1,
                Term::Variable("y".into()),
            )],
            negative_premise: vec![],
            conclusion: vec![pattern(
                Term::Variable("x".into()),
                1,
                Term::Variable("y".into()),
            )],
            filters: vec![],
        };
        assert!(matches!(
            validate_hybrid_rules(&[rule]),
            Err(HybridError::UnsupportedRecursion(_))
        ));
    }

    #[test]
    fn duplicate_seed_triples_are_disjoined_before_reasoning() {
        let mut registry = shared::hybrid::SeedRegistry::new();
        let triple = Triple {
            subject: 1,
            predicate: 2,
            object: 3,
        };
        for probability in [0.5, 0.4] {
            let event = registry.next_event_key("stream", 1);
            registry
                .register_occurrence(event, triple.clone(), probability)
                .unwrap();
        }
        let mut reasoner = Reasoner::new();
        let materialized = materialize_lineage(&mut reasoner, registry.snapshot_all()).unwrap();
        let result = materialized.evaluate(&triple, &HybridConfig::default());
        match result {
            HybridProbabilityResult::Exact {
                probability,
                decision,
                ..
            } => {
                assert!((probability - 0.7).abs() < 1e-9);
                assert_eq!(decision, AlertDecision::Alert);
            }
            other => panic!("expected exact result, got {other:?}"),
        }
    }

    fn exact_probability(materialization: &LineageMaterialization, triple: &Triple) -> f64 {
        let compiled = materialization
            .compile_exact(triple, &HybridConfig::default())
            .unwrap();
        compiled.manager.wmc(compiled.root)
    }

    #[test]
    fn live_window_rebuild_omits_expired_support_without_reusing_ids() {
        let mut registry = shared::hybrid::SeedRegistry::new();
        let input = Triple {
            subject: 1,
            predicate: 2,
            object: 3,
        };
        let first_event = registry.next_event_key("stream", 1);
        let first = registry
            .register_occurrence(first_event, input.clone(), 0.5)
            .unwrap();
        let second_event = registry.next_event_key("stream", 2);
        let second = registry
            .register_occurrence(second_event, input.clone(), 0.4)
            .unwrap();

        let mut full_reasoner = Reasoner::new();
        let full = materialize_lineage(
            &mut full_reasoner,
            registry.snapshot_for_ids([first, second]).unwrap(),
        )
        .unwrap();
        assert!((exact_probability(&full, &input) - 0.7).abs() < 1e-9);

        let live_snapshot = registry.snapshot_for_ids([second]).unwrap();
        let mut live_reasoner = Reasoner::new();
        let live = materialize_lineage(&mut live_reasoner, live_snapshot.clone()).unwrap();
        assert!((exact_probability(&live, &input) - 0.4).abs() < 1e-9);

        let mut oracle_reasoner = Reasoner::new();
        let rebuilt = materialize_lineage(&mut oracle_reasoner, live_snapshot).unwrap();
        assert!(
            (exact_probability(&rebuilt, &input) - exact_probability(&live, &input)).abs() < 1e-9
        );

        let third_event = registry.next_event_key("stream", 3);
        let third = registry
            .register_occurrence(third_event, input, 0.6)
            .unwrap();
        assert!(third > second, "expired seed IDs must never be recycled");
    }
}
