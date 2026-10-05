#[allow(dead_code)]
mod mnist_oracle {
    use datalog::reasoning::{
        materialisation::sdd_seed_materialise::infer_new_facts_with_sdd_seed_specs, Reasoner,
    };
    use shared::{
        provenance::Provenance,
        rule::Rule,
        seed_spec::{ExclusiveChoice, SeedSpec},
        terms::{Term, TriplePattern},
        triple::Triple,
    };

    fn triple(s: u32, p: u32, o: u32) -> Triple {
        Triple {
            subject: s,
            predicate: p,
            object: o,
        }
    }

    fn pattern(t: &Triple) -> TriplePattern {
        (
            Term::Constant(t.subject),
            Term::Constant(t.predicate),
            Term::Constant(t.object),
        )
    }

    fn add_rule(reasoner: &mut Reasoner, body: &[Triple], head: Triple) {
        reasoner.add_rule(Rule {
            premise: body.iter().map(pattern).collect(),
            negative_premise: vec![],
            filters: vec![],
            conclusion: vec![pattern(&head)],
        });
    }

    pub fn answers(left: &[f64], right: &[f64], same_image: bool) -> Vec<f64> {
        let mut reasoner = Reasoner::new();
        let ids: std::collections::HashMap<u32, u32> = {
            let mut dict = reasoner.dictionary.write().unwrap();
            (0..19)
                .chain([100, 101, 999])
                .chain(200..208)
                .map(|id| (id, dict.encode(&format!("http://mnist/term/{id}"))))
                .collect()
        };
        let t = |s, p, o| triple(ids[&s], ids[&p], ids[&o]);
        let right_image = if same_image { 100 } else { 101 };
        let mut seeds = Vec::new();
        for (group, (subject, probabilities)) in [(100, left), (right_image, right)]
            .into_iter()
            .take(if same_image { 1 } else { 2 })
            .enumerate()
        {
            seeds.push(SeedSpec::ExclusiveGroup {
                group_id: group as u32,
                choices: (0..10)
                    .map(|digit| ExclusiveChoice {
                        triple: t(subject, 200, digit as u32),
                        prob: probabilities[digit],
                        choice_id: (group * 10 + digit) as u32,
                    })
                    .collect(),
            });
        }
        for x in 0..10 {
            for y in 0..10 {
                add_rule(
                    &mut reasoner,
                    &[t(100, 200, x), t(right_image, 200, y)],
                    t(999, 201, x + y),
                );
            }
            if x % 2 == 0 {
                add_rule(&mut reasoner, &[t(100, 200, x)], t(999, 202, 1));
            }
            if [2, 3, 5, 7].contains(&x) {
                add_rule(&mut reasoner, &[t(100, 200, x)], t(999, 203, 1));
            }
        }
        let even = t(999, 202, 1);
        let prime = t(999, 203, 1);
        add_rule(&mut reasoner, &[even.clone()], t(999, 204, 1));
        add_rule(&mut reasoner, &[prime.clone()], t(999, 204, 1));
        add_rule(&mut reasoner, &[even.clone(), prime], t(999, 205, 1));
        add_rule(&mut reasoner, &[even.clone(), even], t(999, 206, 1));
        add_rule(
            &mut reasoner,
            &[t(100, 200, 2), t(100, 200, 3)],
            t(999, 207, 1),
        );
        let (_, tags) = infer_new_facts_with_sdd_seed_specs(&mut reasoner, seeds);
        (0..19)
            .map(|s| t(999, 201, s))
            .chain((202..208).map(|p| t(999, p, 1)))
            .map(|target| {
                let tag = if reasoner
                    .dataset_index
                    .query(Some(target.subject), Some(target.predicate), Some(target.object))
                    .is_empty()
                {
                    tags.provenance().zero()
                } else {
                    tags.get_tag(&target)
                };
                tags.provenance().recover_probability(&tag)
            })
            .collect()
    }
}
