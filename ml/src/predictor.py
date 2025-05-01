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
        self.feature_names = feature_names or ['temperature', 'humidity', 'occupancy']
        
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

temperature = np.random.normal(22, 5, n_samples)
humidity = np.random.normal(50, 15, n_samples)
occupancy = np.random.randint(0, 20, n_samples)

# Create target variable with some noise
future_temp = (
    temperature * 0.7 +
    (humidity - 50) * 0.02 +
    occupancy * 0.1 +
    np.random.normal(0, 1, n_samples)
)

# Split data into train and test sets
from sklearn.model_selection import train_test_split
X = np.column_stack([temperature, humidity, occupancy])
y = future_temp
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train and save models
models_dir = os.path.join(os.path.dirname(__file__), "models")
os.makedirs(models_dir, exist_ok=True)

# RandomForest model
rf_model = RandomForestPredictor()
rf_model.train(X_train, y_train)
rf_model.predict(X_test)  # Run once to get performance metrics
rf_schema_file = rf_model.save_with_schema(os.path.join(models_dir, "rf_temperature_predictor.pkl"), 
                                         X_train, y_train, X_test, y_test)

# GradientBoosting model
gb_model = GradientBoostingPredictor()
gb_model.train(X_train, y_train)
gb_model.predict(X_test)  # Run once to get performance metrics
gb_schema_file = gb_model.save_with_schema(os.path.join(models_dir, "gb_temperature_predictor.pkl"), 
                                         X_train, y_train, X_test, y_test)

# Linear Regression model
lr_model = LinearRegressionPredictor()
lr_model.train(X_train, y_train)
lr_model.predict(X_test)  # Run once to get performance metrics
lr_schema_file = lr_model.save_with_schema(os.path.join(models_dir, "lr_temperature_predictor.pkl"),
                                         X_train, y_train, X_test, y_test)

print(f"RandomForest model saved to {os.path.join(models_dir, 'rf_temperature_predictor.pkl')}")
print(f"RandomForest schema saved to {rf_schema_file}")
print(f"GradientBoosting model saved to {os.path.join(models_dir, 'gb_temperature_predictor.pkl')}")
print(f"GradientBoosting schema saved to {gb_schema_file}")
print(f"LinearRegression model saved to {os.path.join(models_dir, 'lr_temperature_predictor.pkl')}")
print(f"LinearRegression schema saved to {lr_schema_file}")

print("\nPerformance Comparison:")
rf_metrics = rf_model.get_performance_metrics()
gb_metrics = gb_model.get_performance_metrics()
lr_metrics = lr_model.get_performance_metrics()

print("\nRandomForest Model:")
for key, value in rf_metrics.items():
    print(f"  {key}: {value}")

print("\nGradientBoosting Model:")
for key, value in gb_metrics.items():
    print(f"  {key}: {value}")

print("\nLinearRegression Model:")
for key, value in lr_metrics.items():
    print(f"  {key}: {value}")