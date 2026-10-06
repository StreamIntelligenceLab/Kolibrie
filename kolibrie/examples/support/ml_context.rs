pub fn trusted_context() -> kolibrie::ml_policy::MlExecutionContext {
    let output = std::env::var_os("KOLIBRIE_TRAINING_OUTPUT")
        .expect("Set KOLIBRIE_TRAINING_OUTPUT to an absolute, private local-training directory");
    kolibrie::ml_policy::MlExecutionContext::trusted_local(std::path::Path::new(&output))
        .expect("Local-training directory must be separate from administrator-approved artifacts")
}
