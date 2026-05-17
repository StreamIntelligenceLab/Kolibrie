/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fs;

use candle_core::{Device, Tensor};
use candle_nn::VarMap;
use rand::Rng;
use serde::{Deserialize, Serialize};

use shared::query::OptimizerKind;

type MlResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputType {
    Binary,
    Categorical(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Activation {
    Relu,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DenseLayer {
    weights: Vec<Vec<f64>>,
    bias: Vec<f64>,
}

#[derive(Debug, Clone)]
struct ForwardCache {
    layer_inputs: Vec<Vec<Vec<f64>>>,
    pre_activations: Vec<Vec<Vec<f64>>>,
    outputs: Vec<Vec<f64>>,
}

#[derive(Debug, Clone)]
struct GradientState {
    weight_grads: Vec<Vec<Vec<f64>>>,
    bias_grads: Vec<Vec<f64>>,
    example_count: usize,
}

#[derive(Debug, Clone)]
struct AdamState {
    step: usize,
    weight_m: Vec<Vec<Vec<f64>>>,
    weight_v: Vec<Vec<Vec<f64>>>,
    bias_m: Vec<Vec<f64>>,
    bias_v: Vec<Vec<f64>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SavedModel {
    layers: Vec<DenseLayer>,
    hidden_act: Activation,
    output_type: OutputType,
}

pub struct MlpNeuralPredicate {
    pub varmap: VarMap,
    layers: RefCell<Vec<DenseLayer>>,
    hidden_act: Activation,
    output_type: OutputType,
    forward_queue: RefCell<VecDeque<ForwardCache>>,
    grad_state: RefCell<GradientState>,
    adam_state: RefCell<AdamState>,
}

impl MlpNeuralPredicate {
    pub fn new(input_dim: usize, hidden: &[usize], output_type: OutputType) -> MlResult<Self> {
        let mut rng = rand::rng();
        let mut dims = Vec::with_capacity(hidden.len() + 2);
        dims.push(input_dim);
        dims.extend_from_slice(hidden);
        dims.push(match output_type {
            OutputType::Binary => 1,
            OutputType::Categorical(size) => size,
        });

        let mut layers = Vec::new();
        for window in dims.windows(2) {
            let in_dim = window[0];
            let out_dim = window[1];
            let scale = (2.0 / (in_dim as f64).max(1.0)).sqrt();
            let mut weights = vec![vec![0.0; in_dim]; out_dim];
            for row in &mut weights {
                for weight in row {
                    *weight = rng.random_range(-scale..scale);
                }
            }
            layers.push(DenseLayer {
                weights,
                bias: vec![0.0; out_dim],
            });
        }

        let zero_grads = zero_grads_for_layers(&layers);
        let adam_state = zero_adam_for_layers(&layers);

        Ok(Self {
            varmap: VarMap::new(),
            layers: RefCell::new(layers),
            hidden_act: Activation::Relu,
            output_type,
            forward_queue: RefCell::new(VecDeque::new()),
            grad_state: RefCell::new(zero_grads),
            adam_state: RefCell::new(adam_state),
        })
    }

    pub fn forward_with_grads(&self, rows: &[Vec<f64>]) -> MlResult<(Tensor, Vec<Vec<f64>>)> {
        let layers = self.layers.borrow();
        if rows.is_empty() {
            let out_dim = output_dim(self.output_type);
            let tensor = Tensor::from_vec(Vec::<f32>::new(), (0, out_dim), &Device::Cpu)?;
            return Ok((tensor, Vec::new()));
        }

        let input_dim = layers.first().map(|layer| layer.weights[0].len()).unwrap_or(0);
        for row in rows {
            if row.len() != input_dim {
                return Err(format!(
                    "expected input_dim {}, got row with {} features",
                    input_dim,
                    row.len()
                )
                .into());
            }
        }

        let mut cache = ForwardCache {
            layer_inputs: Vec::with_capacity(layers.len()),
            pre_activations: Vec::with_capacity(layers.len()),
            outputs: Vec::new(),
        };

        let mut activations = rows.to_vec();
        for (layer_idx, layer) in layers.iter().enumerate() {
            cache.layer_inputs.push(activations.clone());
            let z = linear_batch(&activations, layer);
            cache.pre_activations.push(z.clone());
            activations = if layer_idx + 1 == layers.len() {
                output_activation(&z, self.output_type)
            } else {
                hidden_activation(&z, self.hidden_act)
            };
        }
        cache.outputs = activations.clone();

        let flat: Vec<f32> = activations.iter().flat_map(|row| row.iter().map(|v| *v as f32)).collect();
        let tensor = Tensor::from_vec(flat, (activations.len(), output_dim(self.output_type)), &Device::Cpu)?;
        drop(layers);
        self.forward_queue.borrow_mut().push_back(cache);
        Ok((tensor, activations))
    }

    pub fn surrogate_backward(
        &self,
        tracked: &Tensor,
        wmc_grads: &[HashMap<u32, f64>],
        var_to_col: &HashMap<u32, usize>,
    ) -> MlResult<()> {
        let _dims = tracked.dims();
        let cache = self.forward_queue.borrow_mut().pop_front().ok_or_else(|| {
            "surrogate_backward called without a matching forward pass".to_string()
        })?;

        if cache.outputs.len() != wmc_grads.len() {
            return Err(format!(
                "batch mismatch: forward cache has {}, gradients have {}",
                cache.outputs.len(),
                wmc_grads.len()
            )
            .into());
        }

        let layers = self.layers.borrow();
        let mut grad_state = self.grad_state.borrow_mut();

        for (sample_idx, sample_grads) in wmc_grads.iter().enumerate() {
            let probs = &cache.outputs[sample_idx];
            let mut d_output = vec![0.0; probs.len()];
            for (var, col) in var_to_col {
                if let Some(grad) = sample_grads.get(var) {
                    if *col < d_output.len() {
                        d_output[*col] += *grad;
                    }
                }
            }
            if d_output.iter().all(|value| value.abs() < 1e-15) {
                continue;
            }

            let mut delta = match self.output_type {
                OutputType::Binary => {
                    let y = probs[0];
                    vec![d_output[0] * y * (1.0 - y)]
                }
                OutputType::Categorical(_) => {
                    let dot: f64 = d_output.iter().zip(probs.iter()).map(|(g, y)| g * y).sum();
                    probs
                        .iter()
                        .enumerate()
                        .map(|(idx, y)| y * (d_output[idx] - dot))
                        .collect()
                }
            };

            for layer_idx in (0..layers.len()).rev() {
                let inputs = &cache.layer_inputs[layer_idx][sample_idx];
                for out_idx in 0..delta.len() {
                    grad_state.bias_grads[layer_idx][out_idx] += delta[out_idx];
                    for in_idx in 0..inputs.len() {
                        grad_state.weight_grads[layer_idx][out_idx][in_idx] += delta[out_idx] * inputs[in_idx];
                    }
                }

                if layer_idx > 0 {
                    let mut prev_delta = vec![0.0; layers[layer_idx].weights[0].len()];
                    for out_idx in 0..layers[layer_idx].weights.len() {
                        for in_idx in 0..layers[layer_idx].weights[out_idx].len() {
                            prev_delta[in_idx] += layers[layer_idx].weights[out_idx][in_idx] * delta[out_idx];
                        }
                    }
                    let prev_pre = &cache.pre_activations[layer_idx - 1][sample_idx];
                    for (idx, value) in prev_delta.iter_mut().enumerate() {
                        if prev_pre[idx] <= 0.0 {
                            *value = 0.0;
                        }
                    }
                    delta = prev_delta;
                }
            }

            grad_state.example_count += 1;
        }

        Ok(())
    }

    pub fn zero_grads(&self) {
        let layers = self.layers.borrow();
        *self.grad_state.borrow_mut() = zero_grads_for_layers(&layers);
        self.forward_queue.borrow_mut().clear();
    }

    pub fn optimizer_step(&self, optimizer: OptimizerKind, learning_rate: f64) {
        let mut layers = self.layers.borrow_mut();
        let grads = self.grad_state.borrow_mut();
        if grads.example_count == 0 {
            return;
        }
        let denom = grads.example_count as f64;

        match optimizer {
            OptimizerKind::Sgd => {
                for (layer_idx, layer) in layers.iter_mut().enumerate() {
                    for out_idx in 0..layer.weights.len() {
                        layer.bias[out_idx] -= learning_rate * grads.bias_grads[layer_idx][out_idx] / denom;
                        for in_idx in 0..layer.weights[out_idx].len() {
                            layer.weights[out_idx][in_idx] -=
                                learning_rate * grads.weight_grads[layer_idx][out_idx][in_idx] / denom;
                        }
                    }
                }
            }
            OptimizerKind::Adam => {
                let beta1 = 0.9;
                let beta2 = 0.999;
                let eps = 1e-8;
                let mut adam = self.adam_state.borrow_mut();
                adam.step += 1;
                let t = adam.step as f64;
                for (layer_idx, layer) in layers.iter_mut().enumerate() {
                    for out_idx in 0..layer.weights.len() {
                        let grad_b = grads.bias_grads[layer_idx][out_idx] / denom;
                        adam.bias_m[layer_idx][out_idx] =
                            beta1 * adam.bias_m[layer_idx][out_idx] + (1.0 - beta1) * grad_b;
                        adam.bias_v[layer_idx][out_idx] =
                            beta2 * adam.bias_v[layer_idx][out_idx] + (1.0 - beta2) * grad_b * grad_b;
                        let m_hat_b = adam.bias_m[layer_idx][out_idx] / (1.0 - beta1.powf(t));
                        let v_hat_b = adam.bias_v[layer_idx][out_idx] / (1.0 - beta2.powf(t));
                        layer.bias[out_idx] -= learning_rate * m_hat_b / (v_hat_b.sqrt() + eps);

                        for in_idx in 0..layer.weights[out_idx].len() {
                            let grad_w = grads.weight_grads[layer_idx][out_idx][in_idx] / denom;
                            adam.weight_m[layer_idx][out_idx][in_idx] =
                                beta1 * adam.weight_m[layer_idx][out_idx][in_idx] + (1.0 - beta1) * grad_w;
                            adam.weight_v[layer_idx][out_idx][in_idx] =
                                beta2 * adam.weight_v[layer_idx][out_idx][in_idx] + (1.0 - beta2) * grad_w * grad_w;
                            let m_hat = adam.weight_m[layer_idx][out_idx][in_idx] / (1.0 - beta1.powf(t));
                            let v_hat = adam.weight_v[layer_idx][out_idx][in_idx] / (1.0 - beta2.powf(t));
                            layer.weights[out_idx][in_idx] -= learning_rate * m_hat / (v_hat.sqrt() + eps);
                        }
                    }
                }
            }
        }
    }

    pub fn save(&self, path: &str) -> MlResult<()> {
        let payload = SavedModel {
            layers: self.layers.borrow().clone(),
            hidden_act: self.hidden_act,
            output_type: self.output_type,
        };
        fs::write(path, serde_json::to_vec_pretty(&payload)?)?;
        Ok(())
    }

    pub fn load(
        input_dim: usize,
        hidden: &[usize],
        output_type: OutputType,
        path: &str,
    ) -> MlResult<Self> {
        let payload: SavedModel = serde_json::from_slice(&fs::read(path)?)?;
        let mut model = Self::new(input_dim, hidden, output_type)?;
        if payload.output_type != output_type {
            return Err("saved model output type does not match requested output type".into());
        }
        *model.layers.borrow_mut() = payload.layers;
        model.hidden_act = payload.hidden_act;
        let layers = model.layers.borrow();
        *model.grad_state.borrow_mut() = zero_grads_for_layers(&layers);
        *model.adam_state.borrow_mut() = zero_adam_for_layers(&layers);
        drop(layers);
        Ok(model)
    }
}

fn output_dim(output_type: OutputType) -> usize {
    match output_type {
        OutputType::Binary => 1,
        OutputType::Categorical(size) => size,
    }
}

fn linear_batch(inputs: &[Vec<f64>], layer: &DenseLayer) -> Vec<Vec<f64>> {
    inputs
        .iter()
        .map(|row| {
            layer
                .weights
                .iter()
                .zip(layer.bias.iter())
                .map(|(weights, bias)| {
                    weights
                        .iter()
                        .zip(row.iter())
                        .map(|(weight, value)| weight * value)
                        .sum::<f64>()
                        + bias
                })
                .collect()
        })
        .collect()
}

fn hidden_activation(values: &[Vec<f64>], activation: Activation) -> Vec<Vec<f64>> {
    match activation {
        Activation::Relu => values
            .iter()
            .map(|row| row.iter().map(|value| value.max(0.0)).collect())
            .collect(),
    }
}

fn output_activation(values: &[Vec<f64>], output_type: OutputType) -> Vec<Vec<f64>> {
    match output_type {
        OutputType::Binary => values
            .iter()
            .map(|row| {
                let value = 1.0 / (1.0 + (-row[0]).exp());
                vec![value]
            })
            .collect(),
        OutputType::Categorical(_) => values.iter().map(|row| softmax(row)).collect(),
    }
}

fn softmax(values: &[f64]) -> Vec<f64> {
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let exp_values: Vec<f64> = values.iter().map(|value| (value - max).exp()).collect();
    let sum: f64 = exp_values.iter().sum();
    exp_values.into_iter().map(|value| value / sum.max(1e-15)).collect()
}

fn zero_grads_for_layers(layers: &[DenseLayer]) -> GradientState {
    GradientState {
        weight_grads: layers
            .iter()
            .map(|layer| layer.weights.iter().map(|row| vec![0.0; row.len()]).collect())
            .collect(),
        bias_grads: layers.iter().map(|layer| vec![0.0; layer.bias.len()]).collect(),
        example_count: 0,
    }
}

fn zero_adam_for_layers(layers: &[DenseLayer]) -> AdamState {
    AdamState {
        step: 0,
        weight_m: layers
            .iter()
            .map(|layer| layer.weights.iter().map(|row| vec![0.0; row.len()]).collect())
            .collect(),
        weight_v: layers
            .iter()
            .map(|layer| layer.weights.iter().map(|row| vec![0.0; row.len()]).collect())
            .collect(),
        bias_m: layers.iter().map(|layer| vec![0.0; layer.bias.len()]).collect(),
        bias_v: layers.iter().map(|layer| vec![0.0; layer.bias.len()]).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mlp_categorical_softmax_sums_to_one() {
        let model = MlpNeuralPredicate::new(2, &[4], OutputType::Categorical(3)).unwrap();
        let (_tracked, probs) = model.forward_with_grads(&[vec![1.0, 2.0], vec![0.5, -1.0]]).unwrap();
        for row in probs {
            let total: f64 = row.iter().sum();
            assert!((total - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn mlp_surrogate_grad_correct() {
        let model = MlpNeuralPredicate::new(1, &[], OutputType::Binary).unwrap();
        {
            let mut layers = model.layers.borrow_mut();
            layers[0].weights = vec![vec![1.0]];
            layers[0].bias = vec![0.0];
        }
        let (tracked, probs) = model.forward_with_grads(&[vec![2.0]]).unwrap();
        let prob = probs[0][0];
        let mut grads = HashMap::new();
        grads.insert(0u32, 1.5);
        model.surrogate_backward(&tracked, &[grads], &HashMap::from([(0u32, 0usize)])).unwrap();

        let grad_state = model.grad_state.borrow();
        let expected = 1.5 * prob * (1.0 - prob) * 2.0;
        assert!((grad_state.weight_grads[0][0][0] - expected).abs() < 1e-9);
    }
}
