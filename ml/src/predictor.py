# predictor.py
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import numpy as np
import pickle
import os

class TemperaturePredictor:
    def __init__(self):
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.feature_names = ['temperature', 'humidity', 'occupancy']
        
    def train(self, X, y):
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled, y)
        
    def predict(self, X):
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)
    
    def predict_proba(self, X):
        X_scaled = self.scaler.transform(X)
        predictions = []
        for tree in self.model.estimators_:
            predictions.append(tree.predict(X_scaled))
        return np.std(predictions, axis=0)
    
    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)

np.random.seed(42)
n_samples = 1000

temperature = np.random.normal(22, 5, n_samples)
humidity = np.random.normal(50, 15, n_samples)
occupancy = np.random.randint(0, 20, n_samples)

future_temp = (
    temperature * 0.7 +
    (humidity - 50) * 0.02 +
    occupancy * 0.1 +
    np.random.normal(0, 1, n_samples)
)

X = np.column_stack([temperature, humidity, occupancy])
y = future_temp

model = TemperaturePredictor()
model.train(X, y)

# Update the save path to use the models directory
import os
models_dir = os.path.join(os.path.dirname(__file__), "models")
os.makedirs(models_dir, exist_ok=True)
model.save(os.path.join(models_dir, "temperature_predictor.pkl"))
