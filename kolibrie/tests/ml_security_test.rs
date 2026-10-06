use kolibrie::execute_query::execute_sparql_query;
use kolibrie::ml_policy::{validate_artifact_filename, validate_model_name, MlExecutionContext};
use kolibrie::sparql_database::SparqlDatabase;

#[test]
fn artifact_names_are_portable_single_components() {
    for name in [
        "../x.bin",
        "a/b.bin",
        "a\\b.bin",
        "C:x.bin",
        "x:stream.bin",
        "CON.bin",
        "com1.bin",
        "NUL.x.bin",
        "x.bin ",
        "x\n.bin",
        "x\u{0085}.bin",
        "x.pkl",
        ".bin",
    ] {
        assert!(validate_artifact_filename(name).is_err(), "{name:?}");
    }
    assert!(validate_artifact_filename("new_model_1.bin").is_ok());
    assert!(validate_model_name("good_model").is_ok());
    assert!(validate_model_name("x');raise Exception()#").is_err());
}

#[test]
fn denied_declaration_does_not_register_prefixes_or_models() {
    let mut db = SparqlDatabase::new();
    let before = db.prefixes.clone();
    let query = r#"PREFIX secret: <urn:secret:> MODEL "m" { ARCH MLP { HIDDEN [4] } OUTPUT BINARY { true } }"#;
    let (remaining, _) = kolibrie::parser::parse_combined_query(query).unwrap();
    assert!(remaining.trim().is_empty());
    let error = execute_sparql_query(query, &mut db).unwrap_err();
    assert!(error.starts_with("ML_"), "{error}");
    assert_eq!(before, db.prefixes);
    assert!(db.model_decls.is_empty());
}

#[test]
fn restrictive_default_still_runs_ordinary_queries() {
    let mut db = SparqlDatabase::new();
    assert_eq!(
        execute_sparql_query("SELECT ?x WHERE { VALUES ?x { 7 } }", &mut db).unwrap(),
        vec![vec!["7"]]
    );
    assert!(MlExecutionContext::disabled()
        .approved_model("anything")
        .is_err());
}

#[test]
fn rule_ml_is_rejected_before_prefix_registration() {
    let mut db = SparqlDatabase::new();
    let request = r#"PREFIX ex: <urn:canary:>
RULE :Predict :- CONSTRUCT { ?s ex:p ?prediction } WHERE { ?s ex:x ?x }
ML.PREDICT(MODEL "unapproved", INPUT { SELECT ?x WHERE { ?s ex:x ?x } }, OUTPUT ?prediction)"#;
    let error = kolibrie::parser::process_rule_definition(request, &mut db).unwrap_err();
    assert!(error.starts_with("ML_"), "{error}");
    assert!(db.prefixes.is_empty());
    assert!(db.model_decls.is_empty());
}

#[test]
fn default_policy_rejects_prediction_relation_and_training_routes() {
    for request in [
        r#"ML.PREDICT(MODEL "unapproved", INPUT { SELECT ?x WHERE { VALUES ?x { 1 } } }, OUTPUT ?prediction)"#,
        r#"NEURAL RELATION <urn:prediction> USING MODEL "unapproved" { INPUT { ?s <urn:x> ?x } FEATURES { ?x } }"#,
        r#"TRAIN NEURAL RELATION <urn:prediction> {
            DATA { ?s <urn:label> ?label . }
            LABEL ?label
            TARGET { ?s <urn:prediction> true }
            LOSS binary_cross_entropy
            OPTIMIZER adam
            LEARNING_RATE 0.1
            EPOCHS 1
            BATCH_SIZE 1
            SAVE_TO "../canary.bin"
        }"#,
    ] {
        let (remaining, _) = kolibrie::parser::parse_combined_query(request).unwrap();
        assert!(remaining.trim().is_empty());
        let mut db = SparqlDatabase::new();
        let error = execute_sparql_query(request, &mut db).unwrap_err();
        assert_eq!(
            error,
            if cfg!(feature = "ml") {
                "ML_FORBIDDEN"
            } else {
                "ML_FEATURE_DISABLED"
            }
        );
        assert!(db.prefixes.is_empty());
        assert!(db.model_decls.is_empty());
        assert!(db.neural_relation_decls.is_empty());
    }
}

#[cfg(feature = "ml")]
mod enabled {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "kolibrie-security-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn registry(&self, model: serde_json::Value) -> PathBuf {
            let path = self.0.join("registry.json");
            fs::write(
                &path,
                serde_json::to_vec(&serde_json::json!({"models":[model]})).unwrap(),
            )
            .unwrap();
            path
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    #[test]
    fn legacy_http_cannot_inherit_trusted_local_authority() {
        let fixture = Fixture::new();
        let local = MlExecutionContext::trusted_local(&fixture.0.join("training")).unwrap();
        let mut db = SparqlDatabase::with_ml_context(local);
        let query = r#"PREFIX secret: <urn:secret:> MODEL "m" { ARCH MLP { HIDDEN [4] } OUTPUT BINARY { true } }"#;
        let request = format!(
            "POST /sparql HTTP/1.1\r\nContent-Type: application/sparql-query\r\n\r\n{query}"
        );
        let response = db.handle_http_request(&request);
        assert!(response.contains("403 Forbidden"), "{response}");
        assert!(!response.contains("urn:secret:"));
        assert!(db.prefixes.is_empty());
        assert!(db.model_decls.is_empty());
        assert!(db.ml_context.require_local().is_ok());
        assert_eq!(
            execute_sparql_query("SELECT ?x WHERE { VALUES ?x { 7 } }", &mut db).unwrap(),
            vec![vec!["7"]]
        );
    }

    #[test]
    fn local_outputs_require_separate_administrator_approval() {
        let fixture = Fixture::new();
        let local = MlExecutionContext::trusted_local(&fixture.0.join("training")).unwrap();
        let bytes = ml::MlpNeuralPredicate::new(1, &[], ml::OutputType::Binary)
            .unwrap()
            .to_bytes()
            .unwrap();
        local.save_local_artifact("new.bin", &bytes).unwrap();
        let empty = fixture.0.join("empty.json");
        fs::write(&empty, r#"{"models":[]}"#).unwrap();
        let unapproved = MlExecutionContext::from_registry(&empty).unwrap();
        assert!(unapproved.predict("new", &[vec![0.0]]).is_err());
        let approved_artifact = fixture.0.join("approved.bin");
        fs::copy(local.local_artifact("new.bin").unwrap(), &approved_artifact).unwrap();
        let registry = fixture.registry(serde_json::json!({
            "name":"new", "backend":"native", "artifact":approved_artifact,
            "sha256":digest(&bytes), "input_dim":1, "hidden":[]
        }));
        let approved = MlExecutionContext::from_registry(&registry).unwrap();
        assert_eq!(approved.predict("new", &[vec![0.0]]).unwrap(), vec!["0.5"]);
        assert!(approved.predict("unlisted", &[vec![0.0]]).is_err());
        // Registry changes do not apply during runtime
        fs::write(&registry, r#"{"models":[]}"#).unwrap();
        assert!(approved.predict("new", &[vec![0.0]]).is_ok());
        fs::write(&approved_artifact, b"tampered").unwrap();
        assert!(approved.predict("new", &[vec![0.0]]).is_err());
    }

    #[test]
    fn exclusive_local_writes_preserve_canaries() {
        let fixture = Fixture::new();
        let local = MlExecutionContext::trusted_local(&fixture.0.join("training")).unwrap();
        let canary = fixture.0.join("canary.bin");
        fs::write(&canary, b"SENSITIVE_CANARY").unwrap();
        local
            .save_local_artifact("existing.bin", b"ORIGINAL")
            .unwrap();
        for name in [
            "../canary.bin",
            "..\\canary.bin",
            "C:canary.bin",
            "x:stream.bin",
            "existing.bin",
        ] {
            assert!(local.save_local_artifact(name, b"CHANGED").is_err());
        }
        assert_eq!(fs::read(&canary).unwrap(), b"SENSITIVE_CANARY");
        assert_eq!(
            fs::read(local.local_artifact("existing.bin").unwrap()).unwrap(),
            b"ORIGINAL"
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&canary, fixture.0.join("training/link.bin")).unwrap();
            assert!(local.save_local_artifact("link.bin", b"CHANGED").is_err());
            assert_eq!(fs::read(canary).unwrap(), b"SENSITIVE_CANARY");
        }
    }

    #[test]
    fn approved_python_uses_verified_module_and_pickle_bytes() {
        let fixture = Fixture::new();
        let source = b"class Model:\n    def predict(self, rows):\n        return [row[0] + 1 for row in rows]\n";
        let artifact = b"ckolibrie_approved_test\nModel\n)R.";
        let module_path = fixture.0.join("kolibrie_approved_test.py");
        let model_path = fixture.0.join("model.pkl");
        fs::write(&module_path, source).unwrap();
        fs::write(&model_path, artifact).unwrap();
        let mut entry = serde_json::json!({
            "name":"approved", "backend":"python", "artifact":model_path,
            "sha256":digest(artifact), "allow_pickle":false,
            "module_name":"kolibrie_approved_test", "module_path":module_path, "module_sha256":digest(source)
        });
        assert!(MlExecutionContext::from_registry(&fixture.registry(entry.clone())).is_err());
        entry["allow_pickle"] = true.into();
        let context = MlExecutionContext::from_registry(&fixture.registry(entry)).unwrap();
        assert_eq!(
            context.predict("approved", &[vec![2.0]]).unwrap(),
            vec!["3"]
        );
        assert!(ml::MLHandler::predict_approved(
            "kolibrie_approved_test",
            "unexpected-origin",
            source,
            artifact,
            &[vec![2.0]]
        )
        .is_err());
        fs::write(&module_path, b"raise RuntimeError('SENSITIVE_CANARY')").unwrap();
        assert!(context.predict("approved", &[vec![2.0]]).is_err());
    }

    #[test]
    fn native_loading_rejects_dimension_mismatch() {
        let bytes = ml::MlpNeuralPredicate::new(2, &[3], ml::OutputType::Binary)
            .unwrap()
            .to_bytes()
            .unwrap();
        assert!(
            ml::MlpNeuralPredicate::from_bytes(1, &[3], ml::OutputType::Binary, &bytes).is_err()
        );
        assert!(
            ml::MlpNeuralPredicate::from_bytes(2, &[4], ml::OutputType::Binary, &bytes).is_err()
        );
        assert!(
            ml::MlpNeuralPredicate::from_bytes(2, &[3], ml::OutputType::Binary, &bytes).is_ok()
        );
    }
}
