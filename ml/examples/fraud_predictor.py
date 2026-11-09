#
# Copyright © 2026 Volodymyr Kadzhaia
# Copyright © 2026 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
#

import os
import pickle
import time

import numpy as np
import psutil
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from mlschema import MLSchema


class FraudDetectionPredictor:
    """Single GradientBoosting fraud classifier"""

    FEATURE_NAMES = [
        "amount",
        "hour_of_day",
        "day_of_week",
        "merchant_risk",
        "velocity_1h",
        "distance_km",
        "is_foreign",
        "card_present",
        "flag_highVelocity",
        "flag_largeAmount",
        "flag_highMerchantRisk",
        "flag_foreignHighRisk",
        "flag_riskHigh",
        "recent_fraud_count",
    ]
    MODEL_VERSION = "uncertain-calibrated-v3"

    def __init__(
        self,
        n_estimators=90,
        learning_rate=0.04,
        max_depth=2,
        subsample=0.75,
        min_samples_leaf=80,
        random_state=42,
    ):
        self.scaler = StandardScaler()
        self.feature_names = self.FEATURE_NAMES
        self.model_version = self.MODEL_VERSION
        base_model = GradientBoostingClassifier(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            max_depth=max_depth,
            subsample=subsample,
            min_samples_leaf=min_samples_leaf,
            random_state=random_state,
        )
        try:
            self.model = CalibratedClassifierCV(
                estimator=base_model,
                method="sigmoid",
                cv=3,
            )
        except TypeError:
            self.model = CalibratedClassifierCV(
                base_estimator=base_model,
                method="sigmoid",
                cv=3,
            )
        self.training_time = 0.0
        self.prediction_time = 0.0
        self.memory_usage = 0.0
        self.cpu_usage = 0.0
        self.evaluation_metrics = {}

    def train(self, X, y):
        X_scaled = self.scaler.fit_transform(X)
        start_time = time.time()
        self.model.fit(X_scaled, y)
        self.training_time = time.time() - start_time

    def predict(self, X):
        """Return P(fraud) as a float array — same shape as regression output"""
        X_scaled = self.scaler.transform(X)
        process = psutil.Process(os.getpid())
        self.memory_usage = process.memory_info().rss / 1024 / 1024
        start_time = time.time()
        probs = self.model.predict_proba(X_scaled)[:, 1]
        self.prediction_time = time.time() - start_time
        self.cpu_usage = psutil.cpu_percent(interval=0.1)
        return probs

    def predict_proba(self, X):
        """Uncertainty estimate: high near 0.5, low near confident 0/1"""
        X_scaled = self.scaler.transform(X)
        probs = self.model.predict_proba(X_scaled)[:, 1]
        return 1.0 - np.abs((probs * 2.0) - 1.0)

    def evaluate(self, X_test, y_test):
        probs = self.predict(X_test)
        preds = (probs >= 0.5).astype(int)
        self.evaluation_metrics = {
            "roc_auc": roc_auc_score(y_test, probs),
            "avg_precision": average_precision_score(y_test, probs),
            "precision": precision_score(y_test, preds, zero_division=0),
            "recall": recall_score(y_test, preds, zero_division=0),
            "f1": f1_score(y_test, preds, zero_division=0),
        }
        return self.evaluation_metrics

    def get_performance_metrics(self):
        return {
            "training_time": getattr(self, "training_time", 0),
            "prediction_time": getattr(self, "prediction_time", 0),
            "memory_usage_mb": getattr(self, "memory_usage", 0),
            "cpu_usage_percent": getattr(self, "cpu_usage", 0),
        }

    def get(self, attribute_name):
        """Attribute accessor used by the Rust MLS schema path"""
        return getattr(self, attribute_name, None)

    def save(self, filename):
        with open(filename, "wb") as f:
            pickle.dump(self, f)

    def save_with_schema(self, filename, X_train, y_train, X_test, y_test):
        """Pickle the model and write a companion MLS Turtle (.ttl) schema"""
        with open(filename, "wb") as f:
            pickle.dump(self, f)

        schema = MLSchema()

        def eval_func(model, X_test, y_test):
            probs = model.predict(X_test)
            preds = (probs >= 0.5).astype(int)
            return {
                "mse": mean_squared_error(y_test, probs),
                "r2": r2_score(y_test, probs),
                "roc_auc": roc_auc_score(y_test, probs),
                "avg_precision": average_precision_score(y_test, probs),
                "precision": precision_score(y_test, preds, zero_division=0),
                "recall": recall_score(y_test, preds, zero_division=0),
                "f1": f1_score(y_test, preds, zero_division=0),
                "training_time": model.get_performance_metrics().get("training_time", 0),
                "prediction_time": model.get_performance_metrics().get("prediction_time", 0),
                "memory_usage_mb": model.get_performance_metrics().get("memory_usage_mb", 0),
                "cpu_usage_percent": model.get_performance_metrics().get("cpu_usage_percent", 0),
            }

        schema.convert_model(
            self,
            X_train,
            y_train,
            X_test,
            y_test,
            feature_names=self.feature_names,
            cpu_time_used=self.get_performance_metrics().get("training_time", 0),
            model_uri=f"http://example.org/models/{os.path.basename(filename)}",
            evaluation_function=eval_func,
        )

        schema_filename = filename.replace(".pkl", ".ttl")
        with open(schema_filename, "w") as f:
            f.write(schema.serialize(format="turtle"))
        return schema_filename

    @classmethod
    def load(cls, filename):
        with open(filename, "rb") as f:
            return pickle.load(f)


def _generate_training_data(n_samples=20000, fraud_rate=0.12, random_state=42):
    """Build a synthetic fraud dataset"""
    rng = np.random.default_rng(random_state)
    n_fraud = int(n_samples * fraud_rate)
    n_legit = n_samples - n_fraud

    night_hours = np.concatenate([np.arange(0, 5), np.arange(22, 24)])

    legit_amount = np.where(
        rng.random(n_legit) < 0.14,
        rng.lognormal(mean=6.2, sigma=0.8, size=n_legit),
        rng.lognormal(mean=4.3, sigma=1.1, size=n_legit),
    ).clip(1, 10000)
    legit_hour = np.where(
        rng.random(n_legit) < 0.10,
        rng.choice(night_hours, size=n_legit),
        rng.integers(6, 22, size=n_legit),
    )
    legit_dow = rng.integers(0, 7, size=n_legit)
    legit_merch_risk = np.where(
        rng.random(n_legit) < 0.12,
        rng.beta(4, 3, size=n_legit) * 100,
        rng.beta(1.8, 7, size=n_legit) * 100,
    ).astype(int)
    legit_velocity = np.where(
        rng.random(n_legit) < 0.16,
        rng.poisson(5.0, size=n_legit),
        rng.poisson(1.3, size=n_legit),
    ).clip(0, 12)
    legit_distance = np.where(
        rng.random(n_legit) < 0.10,
        rng.exponential(220, size=n_legit),
        rng.exponential(18, size=n_legit),
    ).clip(0, 5000)
    legit_is_foreign = rng.binomial(1, 0.09, size=n_legit)
    legit_card_present = rng.binomial(1, 0.80, size=n_legit)
    legit_y = np.zeros(n_legit, dtype=int)

    legit_borderline = rng.random(n_legit) < 0.045
    legit_amount[legit_borderline] = rng.uniform(1100, 3200, size=legit_borderline.sum())
    legit_hour[legit_borderline] = rng.integers(9, 18, size=legit_borderline.sum())
    legit_merch_risk[legit_borderline] = rng.integers(15, 61, size=legit_borderline.sum())
    legit_velocity[legit_borderline] = rng.integers(6, 11, size=legit_borderline.sum())
    legit_distance[legit_borderline] = rng.uniform(2, 60, size=legit_borderline.sum())
    legit_is_foreign[legit_borderline] = 0
    legit_card_present[legit_borderline] = 1

    fraud_amount = np.where(
        rng.random(n_fraud) < 0.32,
        rng.lognormal(mean=4.8, sigma=1.0, size=n_fraud),
        rng.lognormal(mean=6.4, sigma=1.15, size=n_fraud),
    ).clip(10, 20000)
    fraud_hour = np.where(
        rng.random(n_fraud) < 0.48,
        rng.choice(night_hours, size=n_fraud),
        rng.integers(6, 22, size=n_fraud),
    )
    fraud_dow = rng.integers(0, 7, size=n_fraud)
    fraud_merch_risk = np.where(
        rng.random(n_fraud) < 0.30,
        rng.beta(2.2, 5, size=n_fraud) * 100,
        rng.beta(4.5, 2.5, size=n_fraud) * 100,
    ).astype(int)
    fraud_velocity = np.where(
        rng.random(n_fraud) < 0.28,
        rng.poisson(2.2, size=n_fraud),
        rng.poisson(6.2, size=n_fraud),
    ).clip(0, 15)
    fraud_distance = np.where(
        rng.random(n_fraud) < 0.45,
        rng.exponential(35, size=n_fraud),
        rng.exponential(450, size=n_fraud),
    ).clip(0, 15000)
    fraud_is_foreign = rng.binomial(1, 0.42, size=n_fraud)
    fraud_card_present = rng.binomial(1, 0.38, size=n_fraud)
    fraud_y = np.ones(n_fraud, dtype=int)

    fraud_borderline = rng.random(n_fraud) < 0.35
    fraud_amount[fraud_borderline] = rng.uniform(1100, 3200, size=fraud_borderline.sum())
    fraud_hour[fraud_borderline] = rng.integers(9, 18, size=fraud_borderline.sum())
    fraud_merch_risk[fraud_borderline] = rng.integers(15, 61, size=fraud_borderline.sum())
    fraud_velocity[fraud_borderline] = rng.integers(6, 11, size=fraud_borderline.sum())
    fraud_distance[fraud_borderline] = rng.uniform(2, 60, size=fraud_borderline.sum())
    fraud_is_foreign[fraud_borderline] = 0
    fraud_card_present[fraud_borderline] = 1

    def flags(amt, vel, mr, isf):
        hv = (vel > 5).astype(float)
        la = (amt > 1000).astype(float)
        hmr = (mr > 70).astype(float)
        fhr = ((isf > 0) & (mr > 70)).astype(float)
        rh = ((amt > 1000) & (vel > 5)).astype(float)
        return hv, la, hmr, fhr, rh

    legit_fhv, legit_fla, legit_fhmr, legit_ffhr, legit_frh = flags(
        legit_amount, legit_velocity, legit_merch_risk, legit_is_foreign
    )
    fraud_fhv, fraud_fla, fraud_fhmr, fraud_ffhr, fraud_frh = flags(
        fraud_amount, fraud_velocity, fraud_merch_risk, fraud_is_foreign
    )

    legit_rfc = rng.choice([0, 1, 2], size=n_legit, p=[0.86, 0.12, 0.02]).astype(float)
    fraud_rfc = rng.choice([0, 1, 2, 3], size=n_fraud, p=[0.25, 0.35, 0.25, 0.15]).astype(float)

    X = np.column_stack(
        [
            np.concatenate([legit_amount, fraud_amount]),
            np.concatenate([legit_hour, fraud_hour]),
            np.concatenate([legit_dow, fraud_dow]),
            np.concatenate([legit_merch_risk, fraud_merch_risk]),
            np.concatenate([legit_velocity, fraud_velocity]),
            np.concatenate([legit_distance, fraud_distance]),
            np.concatenate([legit_is_foreign, fraud_is_foreign]),
            np.concatenate([legit_card_present, fraud_card_present]),
            np.concatenate([legit_fhv, fraud_fhv]),
            np.concatenate([legit_fla, fraud_fla]),
            np.concatenate([legit_fhmr, fraud_fhmr]),
            np.concatenate([legit_ffhr, fraud_ffhr]),
            np.concatenate([legit_frh, fraud_frh]),
            np.concatenate([legit_rfc, fraud_rfc]),
        ]
    )
    y = np.concatenate([legit_y, fraud_y])
    label_noise = np.concatenate(
        [
            rng.random(n_legit) < 0.012,
            rng.random(n_fraud) < 0.080,
        ]
    )
    y[label_noise] = 1 - y[label_noise]
    idx = rng.permutation(n_samples)
    return X[idx], y[idx]


def _do_train(model_path):
    """Train a fresh FraudDetectionPredictor and save pkl + ttl"""
    print("[fraud_predictor] Training GradientBoosting classifier ...")
    X, y = _generate_training_data(n_samples=20000, fraud_rate=0.12)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )
    m = FraudDetectionPredictor()
    m.train(X_train, y_train)
    m.predict(X_test)
    schema_path = m.save_with_schema(model_path, X_train, y_train, X_test, y_test)
    print(f"[fraud_predictor] Model  saved → {model_path}")
    print(f"[fraud_predictor] Schema saved → {schema_path}")
    metrics = m.evaluate(X_test, y_test)
    print("[fraud_predictor] Evaluation:")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}")
    probs = m.predict(X_test)
    q05, q25, q50, q75, q95 = np.quantile(probs, [0.05, 0.25, 0.50, 0.75, 0.95])
    uncertain = np.mean((probs >= 0.20) & (probs <= 0.80))
    print("[fraud_predictor] Score distribution:")
    print(f"  q05/q25/q50/q75/q95: {q05:.3f} / {q25:.3f} / {q50:.3f} / {q75:.3f} / {q95:.3f}")
    print(f"  uncertain_share_20_80: {uncertain:.3f}")
    print("[fraud_predictor] Performance:")
    for k, v in m.get_performance_metrics().items():
        print(f"  {k}: {v}")


def generate_models():
    """Explicit local generation"""
    models_dir = os.environ["KOLIBRIE_TRAINING_OUTPUT"]
    os.makedirs(models_dir, exist_ok=True)

    _model_path = os.path.join(models_dir, "fraud_predictor.pkl")

    _needs_training = True

    if os.path.exists(_model_path):
        try:
            with open(_model_path, "rb") as _f:
                _probe = pickle.load(_f)
            if hasattr(_probe, "feature_names") and len(_probe.feature_names) != len(
                FraudDetectionPredictor.FEATURE_NAMES
            ):
                raise ValueError(
                    f"Feature count mismatch: pkl has {len(_probe.feature_names)} "
                    f"features, current class has "
                    f"{len(FraudDetectionPredictor.FEATURE_NAMES)}. Retraining."
                )
            if getattr(_probe, "model_version", None) != FraudDetectionPredictor.MODEL_VERSION:
                raise ValueError(
                    f"Model version mismatch: pkl has "
                    f"{getattr(_probe, 'model_version', 'unknown')}, current class has "
                    f"{FraudDetectionPredictor.MODEL_VERSION}. Retraining."
                )
            _needs_training = False
            print("[fraud_predictor] Existing model verified OK → skipping training.")
        except Exception as _load_err:
            print(f"[fraud_predictor] Existing pkl cannot be loaded ({_load_err}).")
            print(
                "[fraud_predictor] Likely saved from __main__ context — deleting and retraining ..."
            )
            os.remove(_model_path)
            _ttl = _model_path.replace(".pkl", ".ttl")
            if os.path.exists(_ttl):
                os.remove(_ttl)

    if _needs_training:
        _do_train(_model_path)


if __name__ == "__main__":
    import importlib

    importlib.import_module("fraud_predictor").generate_models()
