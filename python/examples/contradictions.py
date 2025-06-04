#
# Copyright © 2024 Volodymyr Kadzhaia
# Copyright © 2024 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

from py_kolibrie import (
    PyKnowledgeGraph,
    PyTriplePattern,
    PyTerm,
    PyRule,
)

def example_with_contradictions():
    """Builds a knowledge graph that has contradictory facts and constraints"""

    kg = PyKnowledgeGraph()

    # Add some basic facts that will create a contradiction
    kg.add_abox_triple("john", "isA", "professor")
    kg.add_abox_triple("john", "isA", "student")
    kg.add_abox_triple("john", "teaches", "math101")
    kg.add_abox_triple("john", "enrolledIn", "physics101")

    # Add a constraint: "No one can be both a professor and a student."
    # The premise is:
    #       X "isA" "professor"
    #       X "isA" "student"
    #
    # The conclusion is a "dummy" triple (0, 0, 0), just to trigger a violation.
    constraint_premise = [
        PyTriplePattern(
            subject=PyTerm.Variable("X"),
            predicate=PyTerm.Constant(kg.encode_term("isA")),
            object=PyTerm.Constant(kg.encode_term("professor")),
        ),
        PyTriplePattern(
            subject=PyTerm.Variable("X"),
            predicate=PyTerm.Constant(kg.encode_term("isA")),
            object=PyTerm.Constant(kg.encode_term("student")),
        ),
    ]
    constraint_conclusion = [  # Changed to a list containing one triple pattern
        PyTriplePattern(
            subject=PyTerm.Constant(0),
            predicate=PyTerm.Constant(0),
            object=PyTerm.Constant(0),
        )
    ]
    constraint_rule = PyRule(
        premise=constraint_premise,
        filters=[],
        conclusion=constraint_conclusion,
    )
    kg.add_constraint(constraint_rule)

    # Inference rule #1: If X teaches Y, then X is a professor
    professor_rule_premise = [
        PyTriplePattern(
            subject=PyTerm.Variable("X"),
            predicate=PyTerm.Constant(kg.encode_term("teaches")),
            object=PyTerm.Variable("Y"),
        )
    ]
    professor_rule_conclusion = [  # Changed to a list containing one triple pattern
        PyTriplePattern(
            subject=PyTerm.Variable("X"),
            predicate=PyTerm.Constant(kg.encode_term("isA")),
            object=PyTerm.Constant(kg.encode_term("professor")),
        )
    ]
    professor_rule = PyRule(
        premise=professor_rule_premise,
        filters=[],
        conclusion=professor_rule_conclusion,
    )
    kg.add_rule(professor_rule)

    # Inference rule #2: If X is enrolled in Y, then X is a student
    student_rule_premise = [
        PyTriplePattern(
            subject=PyTerm.Variable("X"),
            predicate=PyTerm.Constant(kg.encode_term("enrolledIn")),
            object=PyTerm.Variable("Y"),
        )
    ]
    student_rule_conclusion = [  # Changed to a list containing one triple pattern
        PyTriplePattern(
            subject=PyTerm.Variable("X"),
            predicate=PyTerm.Constant(kg.encode_term("isA")),
            object=PyTerm.Constant(kg.encode_term("student")),
        )
    ]
    student_rule = PyRule(
        premise=student_rule_premise,
        filters=[],
        conclusion=student_rule_conclusion,
    )
    kg.add_rule(student_rule)

    return kg

def print_all_facts(kg: PyKnowledgeGraph):
    """Queries and prints all ABox facts in the knowledge graph"""
    all_facts = kg.query_abox()
    for (subj, pred, obj) in all_facts:
        print(f"{subj} {pred} {obj}")

def main():
    # Build the initial knowledge graph
    kg = example_with_contradictions()

    print("Initial facts:")
    print_all_facts(kg)

    # Run semi-naive inference *with* repairs
    inferred = kg.infer_new_facts_semi_naive_with_repairs()

    print("\nAfter inference with repairs:")
    print_all_facts(kg)

    # Print newly inferred facts
    print("\nNewly inferred facts:")
    for (subj, pred, obj) in inferred:
        print(f"{subj} {pred} {obj}")

    # Query for John's roles under repair semantics
    query_for_john = PyTriplePattern(
        subject=PyTerm.Constant(kg.encode_term("john")),
        predicate=PyTerm.Constant(kg.encode_term("isA")),
        object=PyTerm.Variable("Role"),
    )
    results = kg.query_with_repairs(query_for_john)

    print("\nQuery results for John's roles:")
    for bindings in results:
        # Each 'bindings' is a dict: { "Role": PyTerm.Constant(u32) }
        role_term = bindings.get("Role")
        if role_term is not None:
            # If you had a decode method, e.g. kg.decode_term(...),
            # you could convert the integer back to string. For now,
            # we'll just print the PyTerm as-is:
            print(f"Role: {role_term}")

if __name__ == "__main__":
    main()
