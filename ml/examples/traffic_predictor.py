#
# Copyright © 2024 Volodymyr Kadzhaia
# Copyright © 2024 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
import numpy as np
import pickle
import os
import time
import psutil
from mlschema import MLSchema

class BasePredictor:
    def __init__(self, feature_names=None):
        self.scaler = StandardScaler()
        self.feature_names = feature_names or ['avgSpeed', 'vehicleCount']
        
    def train(self, X, y):
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        start_time = time.time()
        self.model.fit(X_scaled, y)
        self.training_time = time.time() - start_time
        
    def predict(self, X):
        X_scaled = self.scaler.transform(X)
        process = psutil.Process(os.getpid())
        self.memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        
        start_time = time.time()
        predictions = self.model.predict(X_scaled)
        self.prediction_time = time.time() - start_time
        
        # Capture CPU usage
        self.cpu_usage = psutil.cpu_percent(interval=0.1)
        
        # Ensure predictions are in valid congestion range [0, 1]
        predictions = np.clip(predictions, 0.0, 1.0)
        
        return predictions
    
    def predict_proba(self, X):
        # Default implementation - override in subclasses if needed
        return None
    
    def get_performance_metrics(self):
        return {
            'training_time': getattr(self, 'training_time', 0),
            'prediction_time': getattr(self, 'prediction_time', 0),
            'memory_usage_mb': getattr(self, 'memory_usage', 0),
            'cpu_usage_percent': getattr(self, 'cpu_usage', 0)
        }
    
    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
    
    def save_with_schema(self, filename, X_train, y_train, X_test, y_test):
        # Save model to pkl
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
        
        # Generate schema
        schema = MLSchema()
        
        # Define an evaluation function that captures performance metrics
        def eval_func(model, X_test, y_test):
            y_pred = model.predict(X_test)
            
            from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
            metrics = {
                'mse': mean_squared_error(y_test, y_pred),
                'mae': mean_absolute_error(y_test, y_pred),
                'r2': r2_score(y_test, y_pred),
                'training_time': model.get_performance_metrics().get('training_time', 0),
                'prediction_time': model.get_performance_metrics().get('prediction_time', 0),
                'memory_usage_mb': model.get_performance_metrics().get('memory_usage_mb', 0),
                'cpu_usage_percent': model.get_performance_metrics().get('cpu_usage_percent', 0)
            }
            return metrics
        
        # Generate schema
        schema.convert_model(
            self,
            X_train, y_train,
            X_test, y_test,
            feature_names=self.feature_names,
            cpu_time_used=self.get_performance_metrics().get('training_time', 0),
            model_uri=f"http://example.org/traffic/models/{os.path.basename(filename)}",
            evaluation_function=eval_func
        )
        
        # Save schema to file
        schema_filename = filename.replace('.pkl', '.ttl')
        with open(schema_filename, 'w') as f:
            f.write(schema.serialize(format='turtle'))
        
        return schema_filename
    
    def evaluate(self, X_test, y_test):
        """Calculate evaluation metrics and store them"""
        from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
        
        y_pred = self.predict(X_test)
        
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        self.evaluation_metrics = {
            'mse': mse,
            'mae': mae,
            'r2': r2
        }
        
        return self.evaluation_metrics
    
    def get(self, attribute_name):
        """Helper method to get attributes safely"""
        return getattr(self, attribute_name, None)
    
    @classmethod
    def load(cls, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)

class LinearRegressionPredictor(BasePredictor):
    def __init__(self, fit_intercept=True, normalize=None, feature_names=None):
        super().__init__(feature_names)
        
        # In scikit-learn 1.0+, normalize parameter was removed
        # Check scikit-learn version
        import sklearn
        from packaging import version

        try:
            if version.parse(sklearn.__version__) >= version.parse('1.0.0'):
                # For scikit-learn 1.0+
                self.model = LinearRegression(fit_intercept=fit_intercept)
                if normalize:
                    print("Warning: 'normalize' parameter is deprecated in scikit-learn 1.0+. Using StandardScaler instead.")
            else:
                # For scikit-learn < 1.0
                self.model = LinearRegression(fit_intercept=fit_intercept, normalize=normalize)
        except Exception as e:
            print(f"Error initializing LinearRegression: {e}")
            # Fallback to simplest constructor
            self.model = LinearRegression()
    
    def predict_proba(self, X):
        # Linear regression doesn't have built-in uncertainty estimation
        # Return a simple constant uncertainty value
        X_scaled = self.scaler.transform(X)
        return np.ones(X_scaled.shape[0]) * 0.15  # Lower uncertainty for linear model

class RandomForestPredictor(BasePredictor):
    def __init__(self, n_estimators=100, max_depth=10, random_state=42, feature_names=None):
        super().__init__(feature_names)
        self.model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=random_state
        )
        
    def predict_proba(self, X):
        X_scaled = self.scaler.transform(X)
        predictions = []
        for tree in self.model.estimators_:
            predictions.append(tree.predict(X_scaled))
        return np.std(predictions, axis=0)

class GradientBoostingPredictor(BasePredictor):
    def __init__(self, n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42, feature_names=None):
        super().__init__(feature_names)
        self.model = GradientBoostingRegressor(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            max_depth=max_depth,
            random_state=random_state
        )
    
    def predict_proba(self, X):
        X_scaled = self.scaler.transform(X)
        # Calculate prediction standard deviation
        return np.std([tree[0].predict(X_scaled) for tree in self.model.estimators_], axis=0)

# Generate traffic congestion training data
np.random.seed(42)
n_samples = 1000

# Traffic features based on the Rust example
# Average speed (km/h) - highway: 80-120, city: 30-60, downtown: 10-40
avg_speed = np.concatenate([
    np.random.normal(100, 15, n_samples//3),  # Highway speeds
    np.random.normal(45, 10, n_samples//3),   # City speeds  
    np.random.normal(25, 8, n_samples//3),    # Downtown speeds
    np.random.normal(35, 20, n_samples - 3*(n_samples//3))  # Mixed
])

# Vehicle count - varies by road type and time
vehicle_count = np.concatenate([
    np.random.normal(150, 30, n_samples//3),  # Highway traffic
    np.random.normal(80, 20, n_samples//3),   # City traffic
    np.random.normal(120, 40, n_samples//3),  # Downtown traffic
    np.random.normal(100, 35, n_samples - 3*(n_samples//3))  # Mixed
])

# Ensure realistic ranges
avg_speed = np.clip(avg_speed, 5, 150)  # 5-150 km/h
vehicle_count = np.clip(vehicle_count, 0, 300)  # 0-300 vehicles

# Create congestion level target (0 = no congestion, 1 = severe congestion)
# Based on traffic flow theory: congestion increases with vehicle count and decreases with speed
congestion_level = np.zeros(n_samples)

for i in range(n_samples):
    speed = avg_speed[i]
    count = vehicle_count[i]
    
    # Congestion formula based on traffic flow principles
    # Lower speed + higher count = higher congestion
    speed_factor = max(0, (60 - speed) / 60)  # Normalize around 60 km/h free flow
    count_factor = min(1, count / 200)        # Normalize vehicle count
    
    # Combine factors with some randomness
    base_congestion = (speed_factor * 0.7 + count_factor * 0.3)
    noise = np.random.normal(0, 0.1)
    
    congestion_level[i] = np.clip(base_congestion + noise, 0, 1)

# Split data into train and test sets
from sklearn.model_selection import train_test_split
X = np.column_stack([avg_speed, vehicle_count])
y = congestion_level
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train and save traffic congestion models
models_dir = os.path.join(os.path.dirname(__file__), "models")
os.makedirs(models_dir, exist_ok=True)

print("Training traffic congestion prediction models...")
print(f"Training data shape: {X_train.shape}")
print(f"Feature ranges - Speed: {avg_speed.min():.1f}-{avg_speed.max():.1f} km/h, Count: {vehicle_count.min():.0f}-{vehicle_count.max():.0f}")
print(f"Congestion level range: {congestion_level.min():.3f}-{congestion_level.max():.3f}")

# RandomForest model for traffic congestion
rf_model = RandomForestPredictor(feature_names=['avgSpeed', 'vehicleCount'])
rf_model.train(X_train, y_train)
rf_model.predict(X_test)  # Run once to get performance metrics
rf_metrics = rf_model.evaluate(X_test, y_test)
rf_schema_file = rf_model.save_with_schema(os.path.join(models_dir, "rf_congestion_predictor.pkl"), 
                                         X_train, y_train, X_test, y_test)

# GradientBoosting model for traffic congestion  
gb_model = GradientBoostingPredictor(feature_names=['avgSpeed', 'vehicleCount'])
gb_model.train(X_train, y_train)
gb_model.predict(X_test)  # Run once to get performance metrics
gb_metrics = gb_model.evaluate(X_test, y_test)
gb_schema_file = gb_model.save_with_schema(os.path.join(models_dir, "gb_congestion_predictor.pkl"), 
                                         X_train, y_train, X_test, y_test)

# Linear Regression model for traffic congestion
lr_model = LinearRegressionPredictor(feature_names=['avgSpeed', 'vehicleCount'])
lr_model.train(X_train, y_train)
lr_model.predict(X_test)  # Run once to get performance metrics
lr_metrics = lr_model.evaluate(X_test, y_test)
lr_schema_file = lr_model.save_with_schema(os.path.join(models_dir, "lr_congestion_predictor.pkl"),
                                         X_train, y_train, X_test, y_test)

print(f"\nModels saved successfully:")
print(f"RandomForest model: {os.path.join(models_dir, 'rf_congestion_predictor.pkl')}")
print(f"RandomForest schema: {rf_schema_file}")
print(f"GradientBoosting model: {os.path.join(models_dir, 'gb_congestion_predictor.pkl')}")
print(f"GradientBoosting schema: {gb_schema_file}")
print(f"LinearRegression model: {os.path.join(models_dir, 'lr_congestion_predictor.pkl')}")
print(f"LinearRegression schema: {lr_schema_file}")

print("\nTraffic Congestion Model Performance Comparison:")

print("\nRandomForest Model:")
print(f"  Training Time: {rf_model.get_performance_metrics()['training_time']:.4f}s")
print(f"  Prediction Time: {rf_model.get_performance_metrics()['prediction_time']:.4f}s")
print(f"  Memory Usage: {rf_model.get_performance_metrics()['memory_usage_mb']:.2f} MB")
print(f"  CPU Usage: {rf_model.get_performance_metrics()['cpu_usage_percent']:.2f}%")
print(f"  MSE: {rf_metrics['mse']:.6f}")
print(f"  MAE: {rf_metrics['mae']:.6f}")
print(f"  R² Score: {rf_metrics['r2']:.6f}")

print("\nGradientBoosting Model:")
print(f"  Training Time: {gb_model.get_performance_metrics()['training_time']:.4f}s")
print(f"  Prediction Time: {gb_model.get_performance_metrics()['prediction_time']:.4f}s")
print(f"  Memory Usage: {gb_model.get_performance_metrics()['memory_usage_mb']:.2f} MB")
print(f"  CPU Usage: {gb_model.get_performance_metrics()['cpu_usage_percent']:.2f}%")
print(f"  MSE: {gb_metrics['mse']:.6f}")
print(f"  MAE: {gb_metrics['mae']:.6f}")
print(f"  R² Score: {gb_metrics['r2']:.6f}")

print("\nLinearRegression Model:")
print(f"  Training Time: {lr_model.get_performance_metrics()['training_time']:.4f}s")
print(f"  Prediction Time: {lr_model.get_performance_metrics()['prediction_time']:.4f}s")
print(f"  Memory Usage: {lr_model.get_performance_metrics()['memory_usage_mb']:.2f} MB")
print(f"  CPU Usage: {lr_model.get_performance_metrics()['cpu_usage_percent']:.2f}%")
print(f"  MSE: {lr_metrics['mse']:.6f}")
print(f"  MAE: {lr_metrics['mae']:.6f}")
print(f"  R² Score: {lr_metrics['r2']:.6f}")

# Determine best model based on R² score
best_model = "RandomForest"
best_r2 = rf_metrics['r2']

if gb_metrics['r2'] > best_r2:
    best_model = "GradientBoosting"
    best_r2 = gb_metrics['r2']

if lr_metrics['r2'] > best_r2:
    best_model = "LinearRegression"
    best_r2 = lr_metrics['r2']

print(f"\nBest Model: {best_model} (R² = {best_r2:.6f})")

# Test models with sample traffic data matching the Rust example
print("\nTesting with sample traffic scenarios:")

test_scenarios = [
    ([45.0, 120], "HighwayA1 - Moderate speed, high traffic"),
    ([25.0, 85], "CityRoadB2 - Low speed, medium traffic"), 
    ([15.0, 200], "DowntownC3 - Very low speed, high traffic"),
    ([80.0, 50], "Highway - High speed, low traffic"),
    ([10.0, 250], "Traffic jam scenario")
]

for features, description in test_scenarios:
    X_sample = np.array([features])
    
    rf_pred = rf_model.predict(X_sample)[0]
    gb_pred = gb_model.predict(X_sample)[0]
    lr_pred = lr_model.predict(X_sample)[0]
    
    print(f"\n{description}:")
    print(f"  Speed: {features[0]} km/h, Vehicles: {features[1]}")
    print(f"  RandomForest: {rf_pred:.3f}")
    print(f"  GradientBoosting: {gb_pred:.3f}")
    print(f"  LinearRegression: {lr_pred:.3f}")
    
    # Severity classification (matching Rust logic)
    avg_pred = (rf_pred + gb_pred + lr_pred) / 3
    if avg_pred >= 0.8:
        severity = "SEVERE"
    elif avg_pred >= 0.6:
        severity = "HIGH"
    elif avg_pred >= 0.4:
        severity = "MODERATE"
    elif avg_pred >= 0.2:
        severity = "LOW"
    else:
        severity = "MINIMAL"
    
    print(f"  Average Prediction: {avg_pred:.3f} ({severity})")

print(f"\nTraffic congestion prediction models ready for dynamic rule processing!")