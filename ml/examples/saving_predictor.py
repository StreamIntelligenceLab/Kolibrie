# 
# Copyright © 2024 ladroid
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
        self.feature_names = feature_names or ['income', 'spending', 'savings_rate']
        
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
            
            from sklearn.metrics import mean_squared_error, r2_score
            metrics = {
                'mse': mean_squared_error(y_test, y_pred),
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
            model_uri=f"http://example.org/models/{os.path.basename(filename)}",
            evaluation_function=eval_func
        )
        
        # Save schema to file
        schema_filename = filename.replace('.pkl', '.ttl')
        with open(schema_filename, 'w') as f:
            f.write(schema.serialize(format='turtle'))
        
        return schema_filename
    
    def evaluate(self, X_test, y_test):
        """Calculate evaluation metrics and store them"""
        from sklearn.metrics import mean_squared_error, r2_score
        
        y_pred = self.predict(X_test)
        
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        self.evaluation_metrics = {
            'mse': mse,
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
        return np.ones(X_scaled.shape[0]) * 0.5  # Constant uncertainty

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

# Generate training data
np.random.seed(42)
n_samples = 1000

# Financial features: income, spending, savings_rate
income = np.random.normal(5000, 2000, n_samples)  # Monthly income in $
spending = np.random.normal(3500, 1500, n_samples)  # Monthly spending in $
savings_rate = np.clip(np.random.normal(0.15, 0.1, n_samples), 0.01, 0.5)  # Savings rate as percentage

# Create target variable: future_savings with some noise and factors
# Future money saved is influenced by income, spending habits, and savings rate
future_savings = (
    (income * 0.6) +  # Higher income increases savings
    (spending * -0.4) +  # Higher spending decreases savings
    (savings_rate * income * 5) +  # Savings rate directly affects how much is saved
    (income - spending) * 0.3 +  # Disposable income has a positive effect
    np.random.normal(0, 500, n_samples)  # Random market fluctuations and unpredictable events
)

# Split data into train and test sets
from sklearn.model_selection import train_test_split
X = np.column_stack([income, spending, savings_rate])
y = future_savings
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train and save models
models_dir = os.path.join(os.path.dirname(__file__), "models")
os.makedirs(models_dir, exist_ok=True)

# Define feature names for better model interpretability
feature_names = ['income', 'spending', 'savings_rate']

# RandomForest model
rf_model = RandomForestPredictor(feature_names=feature_names)
rf_model.train(X_train, y_train)
rf_model.predict(X_test)  # Run once to get performance metrics
rf_model.evaluate(X_test, y_test)  # Get and store evaluation metrics
rf_schema_file = rf_model.save_with_schema(os.path.join(models_dir, "rf_money_predictor.pkl"), 
                                         X_train, y_train, X_test, y_test)

# GradientBoosting model
gb_model = GradientBoostingPredictor(feature_names=feature_names)
gb_model.train(X_train, y_train)
gb_model.predict(X_test)  # Run once to get performance metrics
gb_model.evaluate(X_test, y_test)  # Get and store evaluation metrics
gb_schema_file = gb_model.save_with_schema(os.path.join(models_dir, "gb_money_predictor.pkl"), 
                                         X_train, y_train, X_test, y_test)

# Linear Regression model
lr_model = LinearRegressionPredictor(feature_names=feature_names)
lr_model.train(X_train, y_train)
lr_model.predict(X_test)  # Run once to get performance metrics
lr_model.evaluate(X_test, y_test)  # Get and store evaluation metrics
lr_schema_file = lr_model.save_with_schema(os.path.join(models_dir, "lr_money_predictor.pkl"),
                                         X_train, y_train, X_test, y_test)

print(f"RandomForest model saved to {os.path.join(models_dir, 'rf_money_predictor.pkl')}")
print(f"RandomForest schema saved to {rf_schema_file}")
print(f"GradientBoosting model saved to {os.path.join(models_dir, 'gb_money_predictor.pkl')}")
print(f"GradientBoosting schema saved to {gb_schema_file}")
print(f"LinearRegression model saved to {os.path.join(models_dir, 'lr_money_predictor.pkl')}")
print(f"LinearRegression schema saved to {lr_schema_file}")

print("\nPerformance Comparison:")
rf_metrics = rf_model.get_performance_metrics()
gb_metrics = gb_model.get_performance_metrics()
lr_metrics = lr_model.get_performance_metrics()
rf_eval = rf_model.evaluation_metrics if hasattr(rf_model, 'evaluation_metrics') else {}
gb_eval = gb_model.evaluation_metrics if hasattr(gb_model, 'evaluation_metrics') else {}
lr_eval = lr_model.evaluation_metrics if hasattr(lr_model, 'evaluation_metrics') else {}

print("\nRandomForest Model:")
for key, value in rf_metrics.items():
    print(f"  {key}: {value}")
for key, value in rf_eval.items():
    print(f"  {key}: {value}")

print("\nGradientBoosting Model:")
for key, value in gb_metrics.items():
    print(f"  {key}: {value}")
for key, value in gb_eval.items():
    print(f"  {key}: {value}")

print("\nLinearRegression Model:")
for key, value in lr_metrics.items():
    print(f"  {key}: {value}")
for key, value in lr_eval.items():
    print(f"  {key}: {value}")

# Print feature importance if available
print("\nFeature Importance (RandomForest):")
if hasattr(rf_model.model, 'feature_importances_'):
    for feature, importance in zip(feature_names, rf_model.model.feature_importances_):
        print(f"  {feature}: {importance:.4f}")

print("\nFeature Importance (GradientBoosting):")
if hasattr(gb_model.model, 'feature_importances_'):
    for feature, importance in zip(feature_names, gb_model.model.feature_importances_):
        print(f"  {feature}: {importance:.4f}")

print("\nModel Coefficients (LinearRegression):")
if hasattr(lr_model.model, 'coef_'):
    for feature, coef in zip(feature_names, lr_model.model.coef_):
        print(f"  {feature}: {coef:.4f}")
    print(f"  intercept: {lr_model.model.intercept_:.4f}")