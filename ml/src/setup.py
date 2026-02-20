#
# Copyright © 2024 Volodymyr Kadzhaia
# Copyright © 2024 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

from setuptools import setup, find_packages

setup(
    name="mlschema",
    version="0.1.1",
    py_modules=["mlschema"],
    packages=find_packages(),
    install_requires=[
        "rdflib>=6.0.0",
        "numpy>=1.20.0",
        "scikit-learn>=1.0.0",
        "packaging>=20.0"
    ],
    python_requires=">=3.7",
    author="Volodymyr Kadzhaia, Pieter Bonte",
    description="ML Schema converter for RDF",
    long_description="Convert machine learning models to RDF using ML Schema ontology",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)