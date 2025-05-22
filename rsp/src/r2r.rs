/*
 * Copyright © 2025 Volodymyr Kadzhaia
 * Copyright © 2025 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */


pub trait R2ROperator<I,R,O>: Send{
    fn load_triples(&mut self, data: &str, syntax: String) -> Result<(),String>;
    fn load_rules(&mut self, data: &str) -> Result<(),&'static str>;
    fn add(&mut self, data: I);
    fn remove(&mut self, data: &I);
    fn materialize(&mut self) -> Vec<I>;
    fn execute_query(&self,query: &R) -> Vec<O>;
}

