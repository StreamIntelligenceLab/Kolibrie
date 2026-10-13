/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! R2S: choosing which solutions reach the output stream.
//!
//! The relation-to-stream stage. Evaluating a window yields the full solution set
//! every time it fires, which is rarely what a consumer wants: most want to hear
//! about changes. The operator compares each firing against the previous one.

use std::collections::HashSet;
use std::hash::Hash;

#[derive(Clone, Debug)]
/// Which solutions of each firing reach the output stream.
pub enum StreamOperator{
    /// Every solution, every firing. The default.
    RSTREAM,
    /// Only solutions absent from the previous firing — insertions.
    ISTREAM,
    /// Only solutions present previously and now gone — deletions.
    DSTREAM
}

impl Default  for StreamOperator{
    fn default() -> Self {
        StreamOperator::RSTREAM
    }
}
/// Applies a [`StreamOperator`] by comparing each firing against the last.
///
/// Stateful: `ISTREAM` and `DSTREAM` are defined relative to the previous firing,
/// so one instance must see every firing of its query in order.
#[derive(Debug)]
pub struct Relation2StreamOperator<O> {
    stream_operator: StreamOperator,
    last_result: HashSet<O>,
}

impl <O> Relation2StreamOperator <O> where O: Clone + Hash + Eq {
    /// An operator with no previous firing recorded, so the first `ISTREAM`
    /// evaluation reports everything as new.
    pub fn new(stream_operator: StreamOperator, _start_time: usize) -> Relation2StreamOperator<O> {
        Relation2StreamOperator {
            stream_operator,
            last_result: HashSet::new(),
        }
    }

    /// The solutions to publish for this firing, updating the remembered state.
    pub fn eval(&mut self, new_response: Vec<O>, _ts: usize) -> Vec<O> {
        match self.stream_operator {
            StreamOperator::RSTREAM => new_response,
            StreamOperator::ISTREAM => {
                let new_set: HashSet<O> = new_response.iter().cloned().collect();
                let emitted: Vec<O> = new_response.into_iter()
                    .filter(|b| !self.last_result.contains(b))
                    .collect();
                self.last_result = new_set;
                emitted
            }
            StreamOperator::DSTREAM => {
                let new_set: HashSet<O> = new_response.into_iter().collect();
                let emitted: Vec<O> = self.last_result.iter()
                    .filter(|b| !new_set.contains(*b))
                    .cloned()
                    .collect();
                self.last_result = new_set;
                emitted
            }
        }
    }
}
#[cfg(test)]
mod tests{
    use crate::rsp::r2s::Relation2StreamOperator;
    use crate::rsp::r2s::StreamOperator::{DSTREAM, ISTREAM, RSTREAM};
    #[derive(Debug, Eq, PartialEq, Hash, Clone)]
    pub(crate) struct Binding{
        pub var: String,
        pub val: String
    }
    #[test]
    fn test_rstream(){
        let new_result  = vec![
            "this".to_string(),
            "is".to_string(),
            "a".to_string(),
            "test".to_string(),
        ];
        let mut s2r: Relation2StreamOperator<String> = Relation2StreamOperator::new(RSTREAM, 0);
        let expected_result = new_result.clone();

        assert_eq!(expected_result,s2r.eval(new_result,1));
    }
    #[test]
    fn test_dstream(){
        let old_result = vec!(
            vec!(Binding{var:"?1".to_string(),val:"1".to_string()},
                 Binding{var:"?2".to_string(),val:"2".to_string()}),
            vec!(Binding{var:"?1".to_string(),val:"1.2".to_string()},
                 Binding{var:"?2".to_string(),val:"2.2".to_string()})
        );
        let new_result = vec!(
            vec!(Binding{var:"?1".to_string(),val:"1".to_string()},
                 Binding{var:"?2".to_string(),val:"2".to_string()}),
            vec!(Binding{var:"?1".to_string(),val:"1.3".to_string()},
                 Binding{var:"?2".to_string(),val:"2.3".to_string()})
        );
        let expected_deletion = vec!(
            vec!(Binding{var:"?1".to_string(),val:"1.2".to_string()},
                 Binding{var:"?2".to_string(),val:"2.2".to_string()})
        );
        let mut s2r: Relation2StreamOperator<Vec<Binding>> = Relation2StreamOperator::new(DSTREAM, 0);
        s2r.eval(old_result,1);

        assert_eq!(expected_deletion,s2r.eval(new_result,2));
    }
    #[test]
    fn test_istream(){
        let old_result = vec!(
            vec!(Binding{var:"?1".to_string(),val:"1".to_string()},
                 Binding{var:"?2".to_string(),val:"2".to_string()}),
            vec!(Binding{var:"?1".to_string(),val:"1.2".to_string()},
                 Binding{var:"?2".to_string(),val:"2.2".to_string()})
        );
        let new_result = vec!(
            vec!(Binding{var:"?1".to_string(),val:"1".to_string()},
                 Binding{var:"?2".to_string(),val:"2".to_string()}),
            vec!(Binding{var:"?1".to_string(),val:"1.3".to_string()},
                 Binding{var:"?2".to_string(),val:"2.3".to_string()})
        );
        let expected_deletion = vec!(
            vec!(Binding{var:"?1".to_string(),val:"1.3".to_string()},
                 Binding{var:"?2".to_string(),val:"2.3".to_string()})
        );
        let mut s2r: Relation2StreamOperator<Vec<Binding>> = Relation2StreamOperator::new(ISTREAM, 0);
        s2r.eval(old_result,1);

        assert_eq!(expected_deletion,s2r.eval(new_result,2));
    }
}