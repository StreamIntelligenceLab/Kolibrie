/*
 * Copyright © 2026 Volodymyr Kadzhaia
 * Copyright © 2026 Pieter Bonte
 * KU Leuven — Stream Intelligence Lab, Belgium
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this file,
 * you can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Which models may run, and where their artifacts may live.
//!
//! This is the authority boundary for everything ML. A query may *name* a model, but
//! naming it is not permission to run it: the host decides, through an
//! [`MlExecutionContext`], and a context that has not been granted anything refuses
//! every request.
//!
//! # Three modes, deliberately separate
//!
//! | mode | granted by | may do |
//! |---|---|---|
//! | disabled | [`MlExecutionContext::disabled`] | nothing; the default |
//! | approved | [`MlExecutionContext::from_registry`] | run the listed models, each pinned to a SHA-256 |
//! | trusted-local | [`MlExecutionContext::trusted_local`] | train and write artifacts into one directory |
//!
//! Trusted-local authority is **not** reachable over HTTP. A request arriving that
//! way is served by a context that drops it, so an embedding application can allow
//! local training without exposing the ability to write artifacts to the network.
//!
//! # Artifacts are pinned, not trusted
//!
//! An approved model names a SHA-256, and [`verified_bytes`] refuses to load bytes
//! that do not match. Replacing a model file on disk therefore disables it rather
//! than silently changing what the engine computes.
//!
//! Artifact filenames are validated separately by [`validate_artifact_filename`],
//! which rejects path separators, control characters and the Windows reserved device
//! names — a filename is attacker-influenced input wherever a query can choose it.

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
/// Why an ML request was refused or failed.
///
/// Renders through [`fmt::Display`] as a stable `ML_*` string. Those strings reach
/// callers over the HTTP API, so they are part of the contract rather than
/// diagnostics.
pub enum MlError {
    /// The crate was built without the `ml` feature, so nothing can run.
    Disabled,
    /// The request was understood and refused: an unapproved model, or a filename
    /// the policy does not permit.
    Forbidden,
    /// The registry or context is malformed — a bad hash length, say.
    InvalidConfiguration,
    /// The artifact is missing, unreadable, or does not match its pinned hash.
    InvalidArtifact,
    /// The model ran and failed.
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
/// The authority to run or train models, held by the host rather than the query.
///
/// Default is [`Self::disabled`]: a context constructed without explicit grant
/// permits nothing. Cheap to clone — the approved-model registry is shared behind an
/// `Arc` — so an embedding application can hand one to each request.
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
/// One entry of the host's model registry.
///
/// Deserialised with `deny_unknown_fields`, so a typo in a registry file is an error
/// rather than a silently ignored setting that leaves a model less constrained than
/// intended.
pub struct ApprovedModel {
    /// Name queries refer to this model by.
    pub name: String,
    /// Which runtime executes it.
    pub backend: Backend,
    /// Path to the weights.
    pub artifact: PathBuf,
    /// SHA-256 the artifact must match. Replacing the file on disk disables the
    /// model rather than changing what it computes.
    pub sha256: String,
    /// Whether this model's artifact may be a pickle. Off unless stated, because
    /// unpickling executes arbitrary code.
    #[serde(default)]
    pub allow_pickle: bool,
    /// Python module providing the model, for the Python backend.
    pub module_name: Option<String>,
    /// Path to that module.
    pub module_path: Option<PathBuf>,
    /// SHA-256 the module must match, pinned like the artifact.
    pub module_sha256: Option<String>,
    /// Expected feature-vector length.
    #[serde(default)]
    pub input_dim: usize,
    /// Hidden layer widths, for a native model.
    #[serde(default)]
    pub hidden: Vec<usize>,
    /// Empty means binary probability output; otherwise categorical labels
    #[serde(default)]
    pub labels: Vec<String>,
}
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Which runtime executes an approved model.
pub enum Backend {
    /// The built-in Rust implementation.
    Native,
    /// A Python module, which requires the `ml` feature and a working interpreter.
    Python,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    models: Vec<ApprovedModel>,
}

/// Checks a model name is one the policy will consider.
///
/// Names come from query text, so this bounds the length and restricts the
/// character set before the name is used to look anything up.
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

/// Checks an artifact filename is safe to create.
///
/// A filename can be chosen by a query, so it is attacker-influenced. Rejects
/// anything not ending `.bin`, path separators and shell-significant characters,
/// control bytes, trailing spaces or dots, and the Windows reserved device names
/// such as `CON`, `NUL` and `COM1` — which remain reserved whatever extension
/// follows them.
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

/// Reads `path` only if its SHA-256 matches `expected`.
///
/// This is what makes an approved model's hash binding rather than advisory. A
/// mismatch is [`MlError::InvalidArtifact`]; a malformed `expected` is
/// [`MlError::InvalidConfiguration`], so a registry typo cannot weaken the check
/// into passing.
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

/// Always [`MlError::Disabled`] without the `ml` feature.
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
        /// Runs an approved model over `input`, one feature vector per row.
    ///
    /// Verifies the artifact against its pinned hash on every call, so a file
    /// swapped between calls is caught rather than used.
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

        /// Always [`MlError::Disabled`] without the `ml` feature.
    #[cfg(not(feature = "ml"))]
    pub fn predict(&self, _name: &str, _input: &[Vec<f64>]) -> Result<Vec<String>, MlError> {
        Err(MlError::Disabled)
    }

    /// A context permitting nothing. The default.
    pub fn disabled() -> Self {
        Self::default()
    }
    /// A context from `KOLIBRIE_MODEL_ALLOWLIST`, or disabled if it is unset.
    ///
    /// Absent configuration grants nothing, so forgetting to set it fails closed.
    pub fn from_environment() -> Result<Self, MlError> {
        match std::env::var_os("KOLIBRIE_MODEL_ALLOWLIST") {
            Some(path) => Self::from_registry(Path::new(&path)),
            None => Ok(Self {
                mode: Mode::Approved(Arc::new(HashMap::new())),
            }),
        }
    }
    /// A context permitting exactly the models listed in the registry at `path`.
    ///
    /// Each entry's name and artifact filename are validated as the registry is
    /// read, so a malformed entry is rejected rather than becoming a permitted
    /// model with a surprising path.
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
    /// A context permitting training, with artifacts written under `output`.
    ///
    /// `output` must be absolute and is canonicalised, so a relative or symlinked
    /// path cannot redirect writes elsewhere. If `KOLIBRIE_MODEL_ALLOWLIST` points
    /// inside `output` this is refused: training could otherwise rewrite the very
    /// registry that decides what is approved.
    ///
    /// This authority is not reachable over HTTP — see the module documentation.
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
    /// Succeeds only in trusted-local mode.
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
    /// The registry entry for `name`, or [`MlError::Forbidden`] if it is not
    /// approved.
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
    /// The path `filename` would occupy in the trusted-local directory.
    ///
    /// Validates the filename and refuses an existing symlink at that path, so a
    /// pre-planted link cannot divert a later write outside the directory.
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
    /// Writes `bytes` into the trusted-local directory under `filename`.
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
    /// Checks every model a parsed request names before any of it runs.
    ///
    /// Called up front so a request mentioning one unapproved model is refused
    /// whole, rather than partly executing and then failing.
    pub fn validate_request(
        &self,
        query: &CombinedQuery<'_>,
        db: &crate::sparql_database::SparqlDatabase,
    ) -> Result<(), MlError> {
        let has_declarations = !query.model_decls.is_empty()
            || !query.neural_relation_decls.is_empty()
            || !query.train_neural_relation_decls.is_empty()
            || query.rules.iter().any(|r| {
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
            .chain(query.rules.iter().flat_map(|r| &r.model_decls))
            .map(|d| d.name.as_str())
            .chain(
                query
                    .neural_relation_decls
                    .iter()
                    .chain(query.rules.iter().flat_map(|r| &r.neural_relation_decls))
                    .map(|d| d.model_name.as_str()),
            )
        {
            validate_model_name(name)?;
        }
        for train in query
            .train_neural_relation_decls
            .iter()
            .chain(query.rules.iter().flat_map(|r| &r.train_neural_relation_decls))
        {
            if let Some(path) = &train.save_path {
                self.local_artifact(path)?;
            }
        }
        for predict in query
            .ml_predict
            .iter()
            .chain(query.rules.iter().filter_map(|r| r.ml_predict.as_ref()))
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
