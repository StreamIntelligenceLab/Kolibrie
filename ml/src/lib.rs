pub fn execute_ml_prediction(model: &str, features: &[Vec<f64>]) -> Vec<bool> {
    let features_json = serde_json::to_string(&features).unwrap();
    
    let output = std::process::Command::new("python")
        .arg("ml_predict.py")
        .arg(model)
        .arg(features_json)
        .output()
        .expect("Failed to execute python script");

    let predictions: Vec<bool> = serde_json::from_str(
        &String::from_utf8_lossy(&output.stdout)
    ).unwrap_or_else(|_| vec![]);

    predictions
}