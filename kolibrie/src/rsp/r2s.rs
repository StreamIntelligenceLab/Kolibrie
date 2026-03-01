/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum StreamOperator{
    RSTREAM, ISTREAM, DSTREAM
}

impl Default  for StreamOperator{
    fn default() -> Self {
        StreamOperator::RSTREAM
    }
}
pub struct Relation2StreamOperator<O> {
    stream_operator: StreamOperator,
    last_result: HashSet<O>,
}

impl <O> Relation2StreamOperator <O> where O: Clone + Hash + Eq {
    pub fn new(stream_operator: StreamOperator, _start_time: usize) -> Relation2StreamOperator<O> {
        Relation2StreamOperator {
            stream_operator,
            last_result: HashSet::new(),
        }
    }

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
    pub struct Binding{
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