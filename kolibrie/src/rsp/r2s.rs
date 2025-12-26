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
use std::mem;

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
    old_result: HashSet<O>,
    new_result: HashSet<O>,
    ts: usize
}

impl <O> Relation2StreamOperator <O> where O: Clone + Hash + Eq {
    pub fn new(stream_operator: StreamOperator, start_time: usize) -> Relation2StreamOperator<O> {
        match stream_operator {
            StreamOperator::RSTREAM => Relation2StreamOperator {stream_operator, old_result: HashSet::with_capacity(0), new_result: HashSet::with_capacity(0),ts: start_time},
            _ => Relation2StreamOperator {stream_operator, old_result: HashSet::new(), new_result: HashSet::new(),ts: start_time}
        }

    }
    pub fn eval(&mut self, new_response: Vec<O>, ts: usize) -> Vec<O>{
        match self.stream_operator {
            StreamOperator::RSTREAM => new_response,
            StreamOperator::ISTREAM => {
                let to_compare = new_response.clone();
                self.prepare_compare(new_response, ts);
                to_compare.into_iter().filter(|b| !self.old_result.contains(b)).collect()
            },
            StreamOperator::DSTREAM => {
                self.prepare_compare(new_response, ts);
                let to_compare = self.old_result.clone();
                to_compare.into_iter().filter(|b| !self.new_result.contains(b)).collect()
            }
        }
    }

    fn prepare_compare(&mut self, new_repsonse: Vec<O>, ts: usize) {
        if  self.ts < ts {
            mem::swap(&mut self.new_result, &mut self.old_result);
            self.new_result.clear();
            self.ts = ts;
        }
        new_repsonse.into_iter().for_each(|v| {
            self.new_result.insert(v);
            ()
        });
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