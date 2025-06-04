#
# Copyright © 2024 Volodymyr Kadzhaia
# Copyright © 2024 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

from rdflib import Graph, Literal, RDF, URIRef, Namespace, BNode
from rdflib.namespace import RDF, XSD, RDFS, OWL
from rdflib import XSD, Literal as RDFLiteral

# Optional imports for framework detection
try:
    import torch
    from torch.nn import Module as TorchModule
except ImportError:
    torch = None
    TorchModule = None

try:
    import tensorflow as tf
    from tensorflow import keras
except ImportError:
    keras = None

class MLSchema:
    def __init__(self):
        self.g = Graph()
        # Namespaces
        self.EX = Namespace("http://example.org/")
        self.MLS = Namespace("http://www.w3.org/ns/mls#")
        self.DCTERMS = Namespace("http://purl.org/dc/terms/")
        self.g.bind("ex", self.EX)
        self.g.bind("mls", self.MLS)
        self.g.bind("dcterms", self.DCTERMS)
        self.model_eval_counter = 1  # Counter for unique model evaluations

    def convert_model(
        self,
        model,
        X_train,
        y_train,
        X_test,
        y_test,
        feature_names=None,
        class_names=None,
        cpu_time_used=None,
        model_uri=None,
        evaluation_function=None,
        evaluation_metrics=None
    ):
        """
        Convert a machine learning or deep learning model and related data into RDF format.

        Parameters:
        - model: Trained ML/DL model.
        - X_train, y_train: Training data and labels.
        - X_test, y_test: Test data and labels.
        - feature_names: List of feature names (optional).
        - class_names: List of class names (optional).
        - cpu_time_used: CPU time used during training.
        - model_uri: URI for the model in RDF.
        - evaluation_function: A callable that takes (model, X_test, y_test) and returns a dict of metrics.
                               If None, tries default approaches (like classification_report for sklearn).
        - evaluation_metrics: List of evaluation metric names if relying on built-in logic for sklearn-like models.
        """
        if not model_uri:
            model_uri = self.EX['model1']
        else:
            model_uri = URIRef(model_uri)
        
        # Create a Run instance
        run_uri = self.EX['run1']
        self.g.add((run_uri, RDF.type, self.MLS.Run))
        
        # Link the model to the Run
        self.g.add((run_uri, self.MLS.hasOutput, model_uri))
        
        # Model as an MLS Model
        self.g.add((model_uri, RDF.type, self.MLS.Model))
        
        # Implementation of the algorithm
        implementation_uri = self.EX['implementation1']
        self.g.add((implementation_uri, RDF.type, self.MLS.Implementation))
        self.g.add((run_uri, self.MLS.executes, implementation_uri))
        
        # Algorithm
        algorithm_name = type(model).__name__
        algorithm_uri = self.EX[f'algorithm/{algorithm_name}']
        self.g.add((algorithm_uri, RDF.type, self.MLS.Algorithm))
        self.g.add((implementation_uri, self.MLS.implements, algorithm_uri))
        
        # Add 'realizes' relation between Run and Algorithm
        self.g.add((run_uri, self.MLS.realizes, algorithm_uri))
        
        # Software - try to detect framework
        software_name = model.__module__.split('.')[0] if hasattr(model, '__module__') else "unknown"
        software_uri = self.EX[f'software/{software_name}']
        self.g.add((software_uri, RDF.type, self.MLS.Software))
        self.g.add((software_uri, self.MLS.hasPart, implementation_uri))
        
        # Hyperparameters (if available)
        self._add_hyperparameters(model, implementation_uri, run_uri)
        
        # Data (Training and Testing datasets)
        train_data_uri = self.EX['data/training']
        test_data_uri = self.EX['data/testing']
        self.g.add((train_data_uri, RDF.type, self.MLS.Dataset))
        self.g.add((test_data_uri, RDF.type, self.MLS.Dataset))
        self.g.add((run_uri, self.MLS.hasInput, train_data_uri))
        self.g.add((run_uri, self.MLS.hasInput, test_data_uri))
        
        # Input Data Characteristics
        self._add_dataset_characteristics(train_data_uri, X_train, 'Training')
        self._add_dataset_characteristics(test_data_uri, X_test, 'Testing')
        
        # Task
        task_uri = self.EX['task1']
        self.g.add((task_uri, RDF.type, self.MLS.Task))
        self.g.add((run_uri, self.MLS.achieves, task_uri))
        self.g.add((task_uri, self.MLS.definedOn, train_data_uri))
        
        # Evaluation Specification
        eval_spec_uri = self.EX['evalspec1']
        self.g.add((eval_spec_uri, RDF.type, self.MLS.EvaluationSpecification))
        self.g.add((task_uri, self.MLS.definedOn, eval_spec_uri))
        self.g.add((eval_spec_uri, self.MLS.defines, task_uri))
        
        # Evaluation Measures
        self._add_evaluation_measures(model, X_test, y_test, eval_spec_uri, run_uri, evaluation_function, evaluation_metrics, class_names)
        
        # Extract model characteristics
        self._add_model_characteristics(model, model_uri, feature_names, class_names)
        
        # Include CPU time used in the Run
        if cpu_time_used is not None:
            self._add_cpu_time(run_uri, cpu_time_used)
    
    def _add_hyperparameters(self, model, implementation_uri, run_uri):
        # Try to get hyperparameters from a get_params() method if available
        if hasattr(model, 'get_params'):
            params = model.get_params()
            for param_name, param_value in params.items():
                hyperparam_uri = self.EX[f'hyperparameter/{param_name}']
                self.g.add((hyperparam_uri, RDF.type, self.MLS.HyperParameter))
                self.g.add((hyperparam_uri, RDFS.label, Literal(param_name)))
                self.g.add((implementation_uri, self.MLS.hasHyperParameter, hyperparam_uri))
                
                hyperparam_setting_uri = BNode()
                self.g.add((hyperparam_setting_uri, RDF.type, self.MLS.HyperParameterSetting))
                self.g.add((hyperparam_setting_uri, self.MLS.specifiedBy, hyperparam_uri))
                self.g.add((hyperparam_setting_uri, self.MLS.hasValue, Literal(str(param_value))))
                self.g.add((run_uri, self.MLS.hasInput, hyperparam_setting_uri))
        else:
            # For deep learning models (like Keras or PyTorch), hyperparams might not be as straightforward.
            pass
    
    def _add_dataset_characteristics(self, data_uri, X_data, dataset_type):
        num_instances = X_data.shape[0]
        num_features = X_data.shape[1] if len(X_data.shape) > 1 else 1
        data_size_bytes = X_data.nbytes if hasattr(X_data, 'nbytes') else 0
        data_format = str(type(X_data))
        
        # Number of instances
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.DatasetCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Number of Instances')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(num_instances, datatype=XSD.integer)))
        self.g.add((data_uri, self.MLS.hasQuality, char_uri))

        # Number of features
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.DatasetCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Number of Features')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(num_features, datatype=XSD.integer)))
        self.g.add((data_uri, self.MLS.hasQuality, char_uri))

        # Data size in bytes
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.DatasetCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Data Size in Bytes')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(data_size_bytes, datatype=XSD.integer)))
        self.g.add((data_uri, self.MLS.hasQuality, char_uri))

        # Data format
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.DatasetCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Data Format')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(data_format)))
        self.g.add((data_uri, self.MLS.hasQuality, char_uri))
        
    def _add_evaluation_measures(self, model, X_test, y_test, eval_spec_uri, run_uri, evaluation_function, evaluation_metrics, class_names):
        # If an evaluation_function is provided by user, use it:
        if evaluation_function is not None:
            # Must return a dict {metric_name: value, ...}
            metrics = evaluation_function(model, X_test, y_test)
            for metric_name, metric_value in metrics.items():
                self._add_single_evaluation(metric_name, metric_value, eval_spec_uri, run_uri)
            return
        
        # Otherwise, try a default approach if scikit-learn style
        if evaluation_metrics is None:
            evaluation_metrics = ['accuracy']
        
        # Try classification_report if it's a classification problem
        from sklearn.metrics import classification_report
        # Check if y_test is categorical
        report = classification_report(y_test, model.predict(X_test), output_dict=True)
        
        # Add all metrics from the classification report
        for label, metrics in report.items():
            if label.isdigit() or label in ('accuracy', 'macro avg', 'weighted avg'):
                # Add these as evaluation measures
                if label == 'accuracy':
                    # Overall accuracy
                    acc = metrics
                    self._add_single_evaluation('Predictive Accuracy', acc, eval_spec_uri, run_uri, measure_uri_name='predictiveAccuracy')
                else:
                    # It's a class-specific or average metric
                    # label_name is used as the measure URI suffix
                    label_name = label.replace(' ', '_')
                    for metric_name, metric_value in metrics.items():
                        if isinstance(metric_value, (float, int)):
                            self._add_single_evaluation(f'{metric_name} {label}', metric_value, eval_spec_uri, run_uri, measure_uri_name=f'{metric_name}_{label_name}')

    # Then update the _add_single_evaluation method
    def _add_single_evaluation(self, metric_name, metric_value, eval_spec_uri, run_uri, measure_uri_name=None):
        if measure_uri_name is None:
            measure_uri_name = metric_name.replace(' ', '_')
        eval_measure_uri = self.EX[measure_uri_name]
        self.g.add((eval_measure_uri, RDF.type, OWL.NamedIndividual))
        self.g.add((eval_measure_uri, RDF.type, self.MLS.EvaluationMeasure))
        self.g.add((eval_measure_uri, RDFS.label, RDFLiteral(metric_name)))
        self.g.add((eval_spec_uri, self.MLS.hasPart, eval_measure_uri))
        
        model_eval_uri = self.EX[f'modelEvaluation{self.model_eval_counter}']
        self.model_eval_counter += 1
        self.g.add((model_eval_uri, RDF.type, OWL.NamedIndividual))
        self.g.add((model_eval_uri, RDF.type, self.MLS.ModelEvaluation))
        self.g.add((model_eval_uri, self.MLS.specifiedBy, eval_measure_uri))
        
        # Ensure metric_value is a float and use XSD.double datatype
        metric_value_float = float(metric_value)
        self.g.add((model_eval_uri, self.MLS.hasValue, RDFLiteral(metric_value_float, datatype=XSD.double)))
        self.g.add((run_uri, self.MLS.hasOutput, model_eval_uri))

    def _add_model_characteristics(self, model, model_uri, feature_names, class_names):
        # Check if it's a sklearn-like model
        if hasattr(model, 'coef_'):
            self._add_linear_model_characteristics(model, model_uri, feature_names, class_names)
        elif hasattr(model, 'feature_importances_'):
            self._add_tree_model_characteristics(model, model_uri, feature_names)
        elif keras is not None and isinstance(model, keras.Model):
            self._add_keras_model_characteristics(model, model_uri)
        elif torch is not None and TorchModule is not None and isinstance(model, TorchModule):
            self._add_torch_model_characteristics(model, model_uri)
        else:
            # Minimal info if unknown type
            characteristic_uri = BNode()
            self.g.add((characteristic_uri, RDF.type, self.MLS.ModelCharacteristic))
            self.g.add((characteristic_uri, RDFS.label, Literal('Generic Model')))
            self.g.add((characteristic_uri, self.MLS.hasValue, Literal('No specific characteristics extracted')))
            self.g.add((model_uri, self.MLS.hasQuality, characteristic_uri))

    def _add_linear_model_characteristics(self, model, model_uri, feature_names, class_names):
        coefficients = model.coef_
        if coefficients.ndim == 1:
            coefficients = [coefficients]
        for class_idx, coef_vector in enumerate(coefficients):
            class_name = class_names[class_idx] if (class_names is not None and class_idx < len(class_names)) else f'Class {class_idx}'
            for feature_idx, coef_value in enumerate(coef_vector):
                feature_name = feature_names[feature_idx] if (feature_names is not None and feature_idx < len(feature_names)) else f'Feature {feature_idx}'
                characteristic_uri = BNode()
                self.g.add((characteristic_uri, RDF.type, self.MLS.ModelCharacteristic))
                self.g.add((characteristic_uri, RDFS.label, Literal(f'Coefficient for class {class_name}, feature {feature_name}')))
                self.g.add((characteristic_uri, self.MLS.hasValue, Literal(coef_value, datatype=XSD.decimal)))
                self.g.add((model_uri, self.MLS.hasQuality, characteristic_uri))
        
        # Intercepts
        if hasattr(model, 'intercept_'):
            intercepts = model.intercept_
            if isinstance(intercepts, float):
                intercepts = [intercepts]
            for class_idx, intercept_value in enumerate(intercepts):
                class_name = class_names[class_idx] if (class_names is not None and class_idx < len(class_names)) else f'Class {class_idx}'
                characteristic_uri = BNode()
                self.g.add((characteristic_uri, RDF.type, self.MLS.ModelCharacteristic))
                self.g.add((characteristic_uri, RDFS.label, Literal(f'Intercept for class {class_name}')))
                self.g.add((characteristic_uri, self.MLS.hasValue, Literal(intercept_value, datatype=XSD.decimal)))
                self.g.add((model_uri, self.MLS.hasQuality, characteristic_uri))
        
        # Number of parameters
        if hasattr(model, 'coef_'):
            n_parameters = model.coef_.size + (model.intercept_.size if hasattr(model.intercept_, 'size') else 1)
            char_uri = BNode()
            self.g.add((char_uri, RDF.type, self.MLS.ModelCharacteristic))
            self.g.add((char_uri, RDFS.label, Literal('Number of Parameters')))
            self.g.add((char_uri, self.MLS.hasValue, Literal(n_parameters, datatype=XSD.integer)))
            self.g.add((model_uri, self.MLS.hasQuality, char_uri))

    def _add_tree_model_characteristics(self, model, model_uri, feature_names):
        importances = model.feature_importances_
        for feature_idx, importance in enumerate(importances):
            feature_name = feature_names[feature_idx] if (feature_names is not None and feature_idx < len(feature_names)) else f'Feature {feature_idx}'
            characteristic_uri = BNode()
            self.g.add((characteristic_uri, RDF.type, self.MLS.ModelCharacteristic))
            self.g.add((characteristic_uri, RDFS.label, Literal(f'Feature importance for {feature_name}')))
            self.g.add((characteristic_uri, self.MLS.hasValue, Literal(importance, datatype=XSD.decimal)))
            self.g.add((model_uri, self.MLS.hasQuality, characteristic_uri))
        
        # Number of parameters (for tree-based models, we might just say #features)
        n_parameters = len(importances)
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.ModelCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Number of Parameters')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(n_parameters, datatype=XSD.integer)))
        self.g.add((model_uri, self.MLS.hasQuality, char_uri))

    def _add_keras_model_characteristics(self, model, model_uri):
        # Add layer information
        for i, layer in enumerate(model.layers):
            characteristic_uri = BNode()
            layer_name = layer.name
            layer_class = layer.__class__.__name__
            self.g.add((characteristic_uri, RDF.type, self.MLS.ModelCharacteristic))
            self.g.add((characteristic_uri, RDFS.label, Literal(f'Layer {i}: {layer_name} ({layer_class})')))
            # Possibly the number of parameters in the layer
            layer_params = layer.count_params()
            self.g.add((characteristic_uri, self.MLS.hasValue, Literal(layer_params, datatype=XSD.integer)))
            self.g.add((model_uri, self.MLS.hasQuality, characteristic_uri))
        
        # Total number of parameters
        total_params = model.count_params()
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.ModelCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Number of Parameters')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(total_params, datatype=XSD.integer)))
        self.g.add((model_uri, self.MLS.hasQuality, char_uri))

    def _add_torch_model_characteristics(self, model, model_uri):
        # Count parameters
        total_params = sum(p.numel() for p in model.parameters())
        char_uri = BNode()
        self.g.add((char_uri, RDF.type, self.MLS.ModelCharacteristic))
        self.g.add((char_uri, RDFS.label, Literal('Number of Parameters')))
        self.g.add((char_uri, self.MLS.hasValue, Literal(total_params, datatype=XSD.integer)))
        self.g.add((model_uri, self.MLS.hasQuality, char_uri))
        
        arch_char = BNode()
        arch_str = str(model)
        self.g.add((arch_char, RDF.type, self.MLS.ModelCharacteristic))
        self.g.add((arch_char, RDFS.label, Literal('Model Architecture')))
        self.g.add((arch_char, self.MLS.hasValue, Literal(arch_str)))
        self.g.add((model_uri, self.MLS.hasQuality, arch_char))

    def _add_cpu_time(self, run_uri, cpu_time_used):
        cpu_time_char_uri = BNode()
        self.g.add((cpu_time_char_uri, RDF.type, self.MLS.Quality))
        self.g.add((cpu_time_char_uri, RDFS.label, Literal('CPU Time Used')))
        self.g.add((cpu_time_char_uri, self.MLS.hasValue, Literal(cpu_time_used, datatype=XSD.decimal)))
        self.g.add((run_uri, self.MLS.hasQuality, cpu_time_char_uri))
        
    def serialize(self, format='turtle'):
        rdf_bytes = self.g.serialize(format=format, encoding='utf-8')
        return rdf_bytes.decode('utf-8')
    
    def query(self, query_string):
        return self.g.query(query_string)