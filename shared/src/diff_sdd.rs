/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use crate::sdd::{SddId, SddManager, VarKind};

pub fn wmc_gradient(manager: &mut SddManager, id: SddId) -> HashMap<u32, f64> {
    let vars: Vec<u32> = manager.variable_ids().collect();
    let mut grads = HashMap::new();

    for v in vars {
        let orig_pos = manager.pos_weight().get(v as usize).copied().unwrap_or(1.0);
        let orig_neg = manager.neg_weight().get(v as usize).copied().unwrap_or(0.0);

        manager.set_pos_weight(v, 1.0);
        manager.set_neg_weight(v, 0.0);
        let a_v = manager.wmc(id);

        let grad = match manager.var_kind(v) {
            VarKind::Independent => {
                manager.set_pos_weight(v, 0.0);
                manager.set_neg_weight(v, 1.0);
                let b_v = manager.wmc(id);
                a_v - b_v
            }
            VarKind::ExclusiveGroup(_) => a_v,
        };

        manager.set_pos_weight(v, orig_pos);
        manager.set_neg_weight(v, orig_neg);

        if grad.abs() > 1e-15 {
            grads.insert(v, grad);
        }
    }

    grads
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdd::{BoolOp, VarKind};

    const EPS: f64 = 1e-6;

    fn finite_difference(
        manager: &mut SddManager,
        target: SddId,
        var: u32,
        delta: f64,
    ) -> f64 {
        let orig_pos = manager.pos_weight()[var as usize];
        let orig_neg = manager.neg_weight()[var as usize];
        let kind = manager.var_kind(var);

        manager.set_pos_weight(var, (orig_pos + delta).clamp(0.0, 1.0));
        if matches!(kind, VarKind::Independent) {
            manager.set_neg_weight(var, (1.0 - orig_pos - delta).clamp(0.0, 1.0));
        }
        let plus = manager.wmc(target);

        manager.set_pos_weight(var, (orig_pos - delta).clamp(0.0, 1.0));
        if matches!(kind, VarKind::Independent) {
            manager.set_neg_weight(var, (1.0 - orig_pos + delta).clamp(0.0, 1.0));
        }
        let minus = manager.wmc(target);

        manager.set_pos_weight(var, orig_pos);
        manager.set_neg_weight(var, orig_neg);

        (plus - minus) / (2.0 * delta)
    }

    #[test]
    fn wmc_gradient_independent_vs_fd() {
        let mut mgr = SddManager::new();
        mgr.ensure_variable_weights(0, 0.7, 0.3, VarKind::Independent);
        mgr.ensure_variable_weights(1, 0.2, 0.8, VarKind::Independent);

        let x = mgr.literal(0, true);
        let y = mgr.literal(1, true);
        let formula = mgr.apply(x, y, BoolOp::Or);

        let grads = wmc_gradient(&mut mgr, formula);
        let fd = finite_difference(&mut mgr, formula, 0, 1e-6);
        assert!((grads.get(&0).copied().unwrap_or(0.0) - fd).abs() < EPS);
    }

    #[test]
    fn wmc_gradient_exclusive_vs_fd() {
        let mut mgr = SddManager::new();
        mgr.ensure_variable_weights(0, 0.7, 1.0, VarKind::ExclusiveGroup(0));
        mgr.ensure_variable_weights(1, 0.3, 1.0, VarKind::ExclusiveGroup(0));

        let eo = mgr.exactly_one(&[0, 1]);
        let zero_lit = mgr.literal(0, true);
        let target = mgr.apply(zero_lit, eo, BoolOp::And);

        let grads = wmc_gradient(&mut mgr, target);
        let fd = finite_difference(&mut mgr, target, 0, 1e-6);
        assert!((grads.get(&0).copied().unwrap_or(0.0) - fd).abs() < EPS);
    }
}
