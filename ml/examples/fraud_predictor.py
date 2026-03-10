#
# Copyright © 2026 Volodymyr Kadzhaia
# Copyright © 2026 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_score, recall_score, f1_score,
    mean_squared_error, r2_score,
)
import numpy as np
import pickle
import os
import time
import psutil
from mlschema import MLSchema


# ─────────────────────────────────────────────────────────────────────────────
# Predictor class
# ─────────────────────────────────────────────────────────────────────────────

class FraudDetectionPredictor:
    """
    Single GradientBoosting fraud classifier.

    Same interface as BasePredictor / GradientBoostingPredictor in predictor.py
    so the Rust MLHandler path needs no changes.
    """

    # ─── Feature order (must match Rust) ──────────────────────────────────────
    # Columns 0-7:  raw transaction features (same as before)
    # Columns 8-12: Datalog Pass-1 symbolic flags (0/1 booleans)
    #               The ML model sees what the symbolic layer already concluded,
    #               so it can learn "flag X was present → weight this signal more".
    # Columns 13-14: per-account history
    #               These give the model temporal context across transactions.
    # ─── Feature order (must match Rust) ──────────────────────────────────────
    # Columns 0-7:  raw transaction features
    # Columns 8-12: Datalog Pass-1 symbolic flags (0/1 booleans)
    #               The ML model sees what the symbolic layer concluded.
    # Column  13:   per-account verdict history count
    #
    # NOTE: avg_ml_score_recent was deliberately removed.
    # It was derived from the ML output itself, creating a circular dependency:
    #   GBM training: fraud → avg 35-89, legit → avg 0-24 (non-overlapping)
    #   Runtime:      avg always 0 (ML score is 0 because avg is 0)
    # GBM latched onto it as the sole discriminator → every runtime tx scored 0.
    FEATURE_NAMES = [
        # raw features (0-7)
        'amount',
        'hour_of_day',
        'day_of_week',
        'merchant_risk',
        'velocity_1h',
        'distance_km',
        'is_foreign',
        'card_present',
        # Pass-1 Datalog flags (8-12)  — Symbolic → ML direction
        'flag_highVelocity',       # R1: velocity_1h > 5
        'flag_largeAmount',        # R2: amount > 1000
        'flag_highMerchantRisk',   # R3: merchant_risk > 70
        'flag_foreignHighRisk',    # R4: is_foreign > 0 AND merchant_risk > 70
        'flag_riskHigh',           # R5: amount > 1000 AND velocity_1h > 5
        # account verdict-history (13)
        'recent_fraud_count',      # FRAUD/SUSPICIOUS verdicts in last 5 tx
    ]

    def __init__(
        self,
        n_estimators=200,
        learning_rate=0.08,
        max_depth=4,
        subsample=0.8,
        min_samples_leaf=20,
        random_state=42,
    ):
        self.scaler = StandardScaler()
        self.feature_names = self.FEATURE_NAMES
        self.model = GradientBoostingClassifier(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            max_depth=max_depth,
            subsample=subsample,
            min_samples_leaf=min_samples_leaf,
            random_state=random_state,
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
        """Return P(fraud) as a float array — same shape as regression output."""
        X_scaled = self.scaler.transform(X)
        process = psutil.Process(os.getpid())
        self.memory_usage = process.memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()
        probs = self.model.predict_proba(X_scaled)[:, 1]   # P(class=1)
        self.prediction_time = time.time() - start_time
        self.cpu_usage = psutil.cpu_percent(interval=0.1)
        return probs

    def predict_proba(self, X):
        """
        Uncertainty estimate: std of per-estimator raw predictions.
        Mirrors GradientBoostingPredictor.predict_proba in predictor.py.
        """
        X_scaled = self.scaler.transform(X)
        return np.std(
            [tree[0].predict(X_scaled) for tree in self.model.estimators_],
            axis=0,
        )

    def evaluate(self, X_test, y_test):
        probs = self.predict(X_test)
        preds = (probs >= 0.5).astype(int)
        self.evaluation_metrics = {
            'roc_auc':       roc_auc_score(y_test, probs),
            'avg_precision': average_precision_score(y_test, probs),
            'precision':     precision_score(y_test, preds, zero_division=0),
            'recall':        recall_score(y_test, preds, zero_division=0),
            'f1':            f1_score(y_test, preds, zero_division=0),
        }
        return self.evaluation_metrics

    def get_performance_metrics(self):
        return {
            'training_time':     getattr(self, 'training_time', 0),
            'prediction_time':   getattr(self, 'prediction_time', 0),
            'memory_usage_mb':   getattr(self, 'memory_usage', 0),
            'cpu_usage_percent': getattr(self, 'cpu_usage', 0),
        }

    def get(self, attribute_name):
        """Attribute accessor used by the Rust MLS schema path."""
        return getattr(self, attribute_name, None)

    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    def save_with_schema(self, filename, X_train, y_train, X_test, y_test):
        """Pickle the model and write a companion MLS Turtle (.ttl) schema."""
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

        schema = MLSchema()

        def eval_func(model, X_test, y_test):
            probs = model.predict(X_test)
            preds = (probs >= 0.5).astype(int)
            return {
                # mse / r2 required by the generic Rust TTL parser in lib.rs
                'mse':               mean_squared_error(y_test, probs),
                'r2':                r2_score(y_test, probs),
                'roc_auc':           roc_auc_score(y_test, probs),
                'avg_precision':     average_precision_score(y_test, probs),
                'precision':         precision_score(y_test, preds, zero_division=0),
                'recall':            recall_score(y_test, preds, zero_division=0),
                'f1':                f1_score(y_test, preds, zero_division=0),
                'training_time':     model.get_performance_metrics().get('training_time', 0),
                'prediction_time':   model.get_performance_metrics().get('prediction_time', 0),
                'memory_usage_mb':   model.get_performance_metrics().get('memory_usage_mb', 0),
                'cpu_usage_percent': model.get_performance_metrics().get('cpu_usage_percent', 0),
            }

        schema.convert_model(
            self,
            X_train, y_train,
            X_test, y_test,
            feature_names=self.feature_names,
            cpu_time_used=self.get_performance_metrics().get('training_time', 0),
            model_uri=f"http://example.org/models/{os.path.basename(filename)}",
            evaluation_function=eval_func,
        )

        schema_filename = filename.replace('.pkl', '.ttl')
        with open(schema_filename, 'w') as f:
            f.write(schema.serialize(format='turtle'))
        return schema_filename

    @classmethod
    def load(cls, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic training data
# ─────────────────────────────────────────────────────────────────────────────

def _generate_training_data(n_samples=20000, fraud_rate=0.05, random_state=42):
    """
    Build a synthetic fraud dataset.

    merchant_risk is integer 0-100 to match the integer-only FILTER constraint
    in parser.rs (parse_comparison uses take_while1(|c| c.is_digit(10)) which
    stops at decimal points, so 0.7 would break parsing).
    """
    rng = np.random.default_rng(random_state)
    n_fraud = int(n_samples * fraud_rate)
    n_legit = n_samples - n_fraud

    # Legitimate transactions
    legit_amount       = rng.lognormal(mean=4.0, sigma=1.2, size=n_legit).clip(1, 5000)
    legit_hour         = rng.integers(6, 22, size=n_legit)
    legit_dow          = rng.integers(0, 7,  size=n_legit)
    legit_merch_risk   = (rng.beta(1.5, 8, size=n_legit) * 100).astype(int)
    legit_velocity     = rng.poisson(1.2, size=n_legit)
    legit_distance     = rng.exponential(15, size=n_legit).clip(0, 500)
    legit_is_foreign   = rng.binomial(1, 0.05, size=n_legit)
    legit_card_present = rng.binomial(1, 0.85, size=n_legit)
    legit_y            = np.zeros(n_legit, dtype=int)

    # Fraudulent transactions
    fraud_amount       = rng.lognormal(mean=6.5, sigma=1.5, size=n_fraud).clip(50, 20000)
    fraud_hour         = rng.choice(
                             np.concatenate([np.arange(0, 5), np.arange(22, 24)]),
                             size=n_fraud)
    fraud_dow          = rng.integers(0, 7,  size=n_fraud)
    fraud_merch_risk   = (rng.beta(5, 2, size=n_fraud) * 100).astype(int)
    fraud_velocity     = rng.poisson(7, size=n_fraud)
    fraud_distance     = rng.exponential(300, size=n_fraud).clip(0, 15000)
    fraud_is_foreign   = rng.binomial(1, 0.60, size=n_fraud)
    fraud_card_present = rng.binomial(1, 0.15, size=n_fraud)
    fraud_y            = np.ones(n_fraud, dtype=int)

    # ── Datalog Pass-1 flag features  (Symbolic → ML direction) ────────────────
    # Computed deterministically from the raw features, mirroring R1-R5 exactly.
    # Including these in training teaches the GBM how to weight a flag *in
    # combination* with the raw features rather than treating each in isolation.
    def flags(amt, vel, mr, isf):
        hv  = (vel  > 5).astype(float)
        la  = (amt  > 1000).astype(float)
        hmr = (mr   > 70).astype(float)
        fhr = ((isf > 0) & (mr > 70)).astype(float)
        rh  = ((amt > 1000) & (vel > 5)).astype(float)
        return hv, la, hmr, fhr, rh

    legit_fhv,  legit_fla,  legit_fhmr,  legit_ffhr,  legit_frh  = flags(
        legit_amount, legit_velocity, legit_merch_risk, legit_is_foreign)
    fraud_fhv,  fraud_fla,  fraud_fhmr,  fraud_ffhr,  fraud_frh  = flags(
        fraud_amount, fraud_velocity, fraud_merch_risk, fraud_is_foreign)

    # ── Account-history features ─────────────────────────────────────────────
    # In production these come from the rolling AccountHistory struct in Rust.
    # For training we simulate plausible values correlated with the label:
    #   fraud row → higher recent_fraud_count, higher avg historical score
    #   legit row → lower counts and scores
    # recent_fraud_count: count of suspicious/fraud verdicts in last 5 tx.
    # Legit accounts rarely have recent fraud verdicts; fraud accounts often do.
    # This feature IS safe to use: it's based on binary verdicts, not the
    # continuous ML score, so there is no circular dependency.
    legit_rfc = rng.integers(0, 2, size=n_legit).astype(float)   # 0 or 1
    fraud_rfc = rng.integers(1, 4, size=n_fraud).astype(float)   # 1-3

    X = np.column_stack([
        # raw features (0-7)
        np.concatenate([legit_amount,       fraud_amount]),
        np.concatenate([legit_hour,         fraud_hour]),
        np.concatenate([legit_dow,          fraud_dow]),
        np.concatenate([legit_merch_risk,   fraud_merch_risk]),
        np.concatenate([legit_velocity,     fraud_velocity]),
        np.concatenate([legit_distance,     fraud_distance]),
        np.concatenate([legit_is_foreign,   fraud_is_foreign]),
        np.concatenate([legit_card_present, fraud_card_present]),
        # Pass-1 symbolic flag features (8-12)
        np.concatenate([legit_fhv,  fraud_fhv]),
        np.concatenate([legit_fla,  fraud_fla]),
        np.concatenate([legit_fhmr, fraud_fhmr]),
        np.concatenate([legit_ffhr, fraud_ffhr]),
        np.concatenate([legit_frh,  fraud_frh]),
        # account verdict-history (13) — NOT avg_ml_score (circular dependency)
        np.concatenate([legit_rfc,  fraud_rfc]),
    ])
    y = np.concatenate([legit_y, fraud_y])
    idx = rng.permutation(n_samples)
    return X[idx], y[idx]


def _do_train(model_path):
    """Train a fresh FraudDetectionPredictor and save pkl + ttl."""
    print('[fraud_predictor] Training GradientBoosting classifier ...')
    X, y = _generate_training_data(n_samples=20000, fraud_rate=0.05)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y,
    )
    m = FraudDetectionPredictor()
    m.train(X_train, y_train)
    m.predict(X_test)   # populate performance metrics before saving
    schema_path = m.save_with_schema(model_path, X_train, y_train, X_test, y_test)
    print(f'[fraud_predictor] Model  saved → {model_path}')
    print(f'[fraud_predictor] Schema saved → {schema_path}')
    metrics = m.evaluate(X_test, y_test)
    print('[fraud_predictor] Evaluation:')
    for k, v in metrics.items():
        print(f'  {k}: {v:.4f}')
    print('[fraud_predictor] Performance:')
    for k, v in m.get_performance_metrics().items():
        print(f'  {k}: {v}')


# ─────────────────────────────────────────────────────────────────────────────
# Module-level training block
# ─────────────────────────────────────────────────────────────────────────────
#
# CRITICAL — must be at MODULE LEVEL, not inside if __name__ == '__main__'.
#
# When lib.rs calls generate_ml_models() it does:
#   importlib.import_module('fraud_predictor')
# This executes the block below.  Because the module is imported as
# 'fraud_predictor', pickle will store the class path as
# 'fraud_predictor.FraudDetectionPredictor'.  lib.rs can then resolve it on
# the subsequent pickle.load() call.
#
# Self-healing load-test
# ─────────────────────
# An existing pkl saved from a __main__ run stores the class as
# '__main__.FraudDetectionPredictor'.  We catch the resulting AttributeError
# (or any other pickle failure), delete the stale files, and retrain
# automatically — no manual deletion of the pkl required.

models_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(models_dir, exist_ok=True)

_model_path = os.path.join(models_dir, 'fraud_predictor.pkl')

_needs_training = True

if os.path.exists(_model_path):
    try:
        with open(_model_path, 'rb') as _f:
            _probe = pickle.load(_f)
        if (hasattr(_probe, 'feature_names')
                and len(_probe.feature_names) != len(FraudDetectionPredictor.FEATURE_NAMES)):
            raise ValueError(
                f"Feature count mismatch: pkl has {len(_probe.feature_names)} "
                f"features, current class has "
                f"{len(FraudDetectionPredictor.FEATURE_NAMES)}. Retraining."
            )
        _needs_training = False
        print(f'[fraud_predictor] Existing model verified OK → skipping training.')
    except Exception as _load_err:
        print(f'[fraud_predictor] Existing pkl cannot be loaded ({_load_err}).')
        print('[fraud_predictor] Likely saved from __main__ context — deleting and retraining ...')
        os.remove(_model_path)
        _ttl = _model_path.replace('.pkl', '.ttl')
        if os.path.exists(_ttl):
            os.remove(_ttl)

if _needs_training:
    _do_train(_model_path)