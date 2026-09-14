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
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
#[cfg(feature = "ml")]
use sha2::{Digest, Sha256};
use shared::query::CombinedQuery;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MlError {
    Disabled,
    Forbidden,
    InvalidConfiguration,
    InvalidArtifact,
    ExecutionFailed,
}
impl fmt::Display for MlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Disabled => "ML_FEATURE_DISABLED",
            Self::Forbidden => "ML_FORBIDDEN",
            Self::InvalidConfiguration => "ML_INVALID_CONFIGURATION",
            Self::InvalidArtifact => "ML_INVALID_ARTIFACT",
            Self::ExecutionFailed => "ML_EXECUTION_FAILED",
        })
    }
}
impl std::error::Error for MlError {}

#[derive(Debug, Clone, Default)]
pub struct MlExecutionContext {
    mode: Mode,
}
#[derive(Debug, Clone, Default)]
enum Mode {
    #[default]
    Disabled,
    Approved(Arc<HashMap<String, ApprovedModel>>),
    TrustedLocal(PathBuf),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedModel {
    pub name: String,
    pub backend: Backend,
    pub artifact: PathBuf,
    pub sha256: String,
    #[serde(default)]
    pub allow_pickle: bool,
    pub module_name: Option<String>,
    pub module_path: Option<PathBuf>,
    pub module_sha256: Option<String>,
    #[serde(default)]
    pub input_dim: usize,
    #[serde(default)]
    pub hidden: Vec<usize>,
    /// Empty means binary probability output; otherwise categorical labels
    #[serde(default)]
    pub labels: Vec<String>,
}
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Native,
    Python,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    models: Vec<ApprovedModel>,
}

pub fn validate_model_name(name: &str) -> Result<(), MlError> {
    let mut bytes = name.bytes();
    if name.len() > 64
        || !bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        || !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(MlError::Forbidden);
    }
    Ok(())
}

pub fn validate_artifact_filename(name: &str) -> Result<(), MlError> {
    if !name.ends_with(".bin")
        || name.len() <= 4
        || name.chars().any(char::is_control)
        || name
            .bytes()
            .any(|b| b < 32 || b == 127 || b"/\\:\"<>|?*".contains(&b))
        || name.ends_with([' ', '.'])
    {
        return Err(MlError::Forbidden);
    }
    let stem = name
        .split('.')
        .next()
        .unwrap_or("")
        .trim_end_matches(' ')
        .to_ascii_uppercase();
    if matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
    ) || ["COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³"].contains(&stem.as_str())
        || (stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
    {
        return Err(MlError::Forbidden);
    }
    Ok(())
}

#[cfg(feature = "ml")]
pub fn verified_bytes(path: &Path, expected: &str) -> Result<Vec<u8>, MlError> {
    if expected.len() != 64 || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(MlError::InvalidConfiguration);
    }
    let bytes = fs::read(path).map_err(|_| MlError::InvalidArtifact)?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected.to_ascii_lowercase() {
        return Err(MlError::InvalidArtifact);
    }
    Ok(bytes)
}

#[cfg(not(feature = "ml"))]
pub fn verified_bytes(_path: &Path, _expected: &str) -> Result<Vec<u8>, MlError> {
    Err(MlError::Disabled)
}

impl MlExecutionContext {
    /// Prevent HTTP from inheriting local training authority
    pub(crate) fn for_http(&self) -> Self {
        match &self.mode {
            Mode::Approved(_) => self.clone(),
            _ => Self::disabled(),
        }
    }
    #[cfg(feature = "ml")]
    pub fn predict(&self, name: &str, input: &[Vec<f64>]) -> Result<Vec<String>, MlError> {
        let entry = self.approved_model(name)?;
        let artifact = verified_bytes(&entry.artifact, &entry.sha256)?;
        let result: Vec<String> = match entry.backend {
            Backend::Native => {
                if input
                    .iter()
                    .any(|r| r.len() != entry.input_dim || r.iter().any(|v| !v.is_finite()))
                {
                    return Err(MlError::ExecutionFailed);
                }
                let output = if entry.labels.is_empty() {
                    ml::OutputType::Binary
                } else {
                    ml::OutputType::Categorical(entry.labels.len())
                };
                let model = ml::MlpNeuralPredicate::from_bytes(
                    entry.input_dim,
                    &entry.hidden,
                    output,
                    &artifact,
                )
                .map_err(|_| MlError::InvalidArtifact)?;
                if input.is_empty() {
                    return Ok(Vec::new());
                }
                let (_, probabilities) = model
                    .forward_with_grads(input)
                    .map_err(|_| MlError::ExecutionFailed)?;
                probabilities
                    .iter()
                    .map(|row| {
                        if entry.labels.is_empty() {
                            row[0].to_string()
                        } else {
                            entry.labels[row
                                .iter()
                                .enumerate()
                                .max_by(|a, b| a.1.total_cmp(b.1))
                                .map(|(i, _)| i)
                                .unwrap_or(0)]
                            .clone()
                        }
                    })
                    .collect()
            }
            Backend::Python => {
                if !entry.allow_pickle {
                    return Err(MlError::Forbidden);
                }
                let path = entry
                    .module_path
                    .as_ref()
                    .ok_or(MlError::InvalidConfiguration)?;
                let source = verified_bytes(
                    path,
                    entry
                        .module_sha256
                        .as_deref()
                        .ok_or(MlError::InvalidConfiguration)?,
                )?;
                ml::MLHandler::predict_approved(
                    entry
                        .module_name
                        .as_deref()
                        .ok_or(MlError::InvalidConfiguration)?,
                    path.to_str().ok_or(MlError::InvalidConfiguration)?,
                    &source,
                    &artifact,
                    input,
                )
                .map_err(|error| {
                    pyo3::Python::with_gil(|py| {
                        if error.is_instance_of::<pyo3::exceptions::PyPermissionError>(py) {
                            MlError::Forbidden
                        } else {
                            MlError::ExecutionFailed
                        }
                    })
                })?
                .iter()
                .map(ToString::to_string)
                .collect()
            }
        };
        if result.len() != input.len() {
            return Err(MlError::ExecutionFailed);
        }
        Ok(result)
    }

    #[cfg(not(feature = "ml"))]
    pub fn predict(&self, _name: &str, _input: &[Vec<f64>]) -> Result<Vec<String>, MlError> {
        Err(MlError::Disabled)
    }

    pub fn disabled() -> Self {
        Self::default()
    }
    pub fn from_environment() -> Result<Self, MlError> {
        match std::env::var_os("KOLIBRIE_MODEL_ALLOWLIST") {
            Some(path) => Self::from_registry(Path::new(&path)),
            None => Ok(Self {
                mode: Mode::Approved(Arc::new(HashMap::new())),
            }),
        }
    }
    pub fn from_registry(path: &Path) -> Result<Self, MlError> {
        if !path.is_absolute() {
            return Err(MlError::InvalidConfiguration);
        }
        let registry: Registry =
            serde_json::from_slice(&fs::read(path).map_err(|_| MlError::InvalidConfiguration)?)
                .map_err(|_| MlError::InvalidConfiguration)?;
        let mut models = HashMap::new();
        for mut entry in registry.models {
            validate_model_name(&entry.name)?;
            if !entry.artifact.is_absolute() {
                return Err(MlError::InvalidConfiguration);
            }
            entry.artifact = entry
                .artifact
                .canonicalize()
                .map_err(|_| MlError::InvalidConfiguration)?;
            verified_bytes(&entry.artifact, &entry.sha256)?;
            if entry.backend == Backend::Python {
                if !entry.allow_pickle {
                    return Err(MlError::Forbidden);
                }
                validate_model_name(
                    entry
                        .module_name
                        .as_deref()
                        .ok_or(MlError::InvalidConfiguration)?,
                )?;
                let module = entry
                    .module_path
                    .as_mut()
                    .ok_or(MlError::InvalidConfiguration)?;
                if !module.is_absolute() {
                    return Err(MlError::InvalidConfiguration);
                }
                *module = module
                    .canonicalize()
                    .map_err(|_| MlError::InvalidConfiguration)?;
                verified_bytes(
                    module,
                    entry
                        .module_sha256
                        .as_deref()
                        .ok_or(MlError::InvalidConfiguration)?,
                )?;
            } else if entry.input_dim == 0 || entry.hidden.contains(&0) || entry.labels.len() == 1 {
                return Err(MlError::InvalidConfiguration);
            }
            if models.insert(entry.name.clone(), entry).is_some() {
                return Err(MlError::InvalidConfiguration);
            }
        }
        Ok(Self {
            mode: Mode::Approved(Arc::new(models)),
        })
    }
    /// Create a host-controlled local training context
    pub fn trusted_local(output: &Path) -> Result<Self, MlError> {
        if !output.is_absolute() {
            return Err(MlError::InvalidConfiguration);
        }
        fs::create_dir_all(output).map_err(|_| MlError::InvalidConfiguration)?;
        let output = output
            .canonicalize()
            .map_err(|_| MlError::InvalidConfiguration)?;
        if let Some(registry) = std::env::var_os("KOLIBRIE_MODEL_ALLOWLIST") {
            let registry_path = PathBuf::from(registry);
            let approved = Self::from_registry(&registry_path)?;
            if registry_path
                .canonicalize()
                .map_err(|_| MlError::InvalidConfiguration)?
                .starts_with(&output)
            {
                return Err(MlError::Forbidden);
            }
            if let Mode::Approved(models) = &approved.mode {
                if models.values().any(|m| {
                    m.artifact.starts_with(&output)
                        || m.module_path
                            .as_ref()
                            .is_some_and(|p| p.starts_with(&output))
                }) {
                    return Err(MlError::Forbidden);
                }
            }
        }
        Ok(Self {
            mode: Mode::TrustedLocal(output),
        })
    }
    pub fn require_local(&self) -> Result<(), MlError> {
        if !cfg!(feature = "ml") {
            return Err(MlError::Disabled);
        }
        if matches!(self.mode, Mode::TrustedLocal(_)) {
            Ok(())
        } else {
            Err(MlError::Forbidden)
        }
    }
    pub fn approved_model(&self, name: &str) -> Result<&ApprovedModel, MlError> {
        if !cfg!(feature = "ml") {
            return Err(MlError::Disabled);
        }
        validate_model_name(name)?;
        match &self.mode {
            Mode::Approved(models) => models.get(name).ok_or(MlError::Forbidden),
            Mode::Disabled | Mode::TrustedLocal(_) => Err(MlError::Forbidden),
        }
    }
    pub fn local_artifact(&self, filename: &str) -> Result<PathBuf, MlError> {
        self.require_local()?;
        validate_artifact_filename(filename)?;
        let Mode::TrustedLocal(base) = &self.mode else {
            return Err(MlError::Forbidden);
        };
        let path = base.join(filename);
        if fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(MlError::Forbidden);
        }
        Ok(path)
    }
    pub fn save_local_artifact(&self, filename: &str, bytes: &[u8]) -> Result<(), MlError> {
        let path = self.local_artifact(filename)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|_| MlError::Forbidden)?;
        file.write_all(bytes).map_err(|_| MlError::ExecutionFailed)
    }
    /// Validate the request before mutations
    pub fn validate_request(
        &self,
        query: &CombinedQuery<'_>,
        db: &crate::sparql_database::SparqlDatabase,
    ) -> Result<(), MlError> {
        let rule = query.rule.as_ref();
        let has_declarations = !query.model_decls.is_empty()
            || !query.neural_relation_decls.is_empty()
            || !query.train_neural_relation_decls.is_empty()
            || rule.is_some_and(|r| {
                !r.model_decls.is_empty()
                    || !r.neural_relation_decls.is_empty()
                    || !r.train_neural_relation_decls.is_empty()
            });
        if has_declarations {
            self.require_local()?;
        }
        for name in query
            .model_decls
            .iter()
            .chain(rule.into_iter().flat_map(|r| &r.model_decls))
            .map(|d| d.name.as_str())
            .chain(
                query
                    .neural_relation_decls
                    .iter()
                    .chain(rule.into_iter().flat_map(|r| &r.neural_relation_decls))
                    .map(|d| d.model_name.as_str()),
            )
        {
            validate_model_name(name)?;
        }
        for train in query.train_neural_relation_decls.iter().chain(
            rule.into_iter()
                .flat_map(|r| &r.train_neural_relation_decls),
        ) {
            if let Some(path) = &train.save_path {
                self.local_artifact(path)?;
            }
        }
        for predict in query
            .ml_predict
            .iter()
            .chain(rule.into_iter().filter_map(|r| r.ml_predict.as_ref()))
        {
            validate_model_name(predict.model)?;
            if self.require_local().is_err() {
                self.approved_model(predict.model)?;
            }
            let (remaining, nested) = crate::parser::parse_combined_query(predict.input_raw)
                .map_err(|_| MlError::Forbidden)?;
            if !remaining.trim().is_empty() || nested.sparql.is_none() {
                return Err(MlError::Forbidden);
            }
            self.validate_request(&nested, db)?;
        }
        // Registered relations require trusted-local execution
        if !db.neural_relation_decls.is_empty() {
            self.require_local()?;
        }
        Ok(())
    }
}
