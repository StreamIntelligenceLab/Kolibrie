#
# Copyright © 2024 Volodymyr Kadzhaia
# Copyright © 2024 Pieter Bonte
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

'''
Before running the test, run such commands in the terminal:
1. cd python
2. python3 -m venv .venv
3. maturin develop
4. source .venv/bin/activate
5. run python script
'''

import kolibrie

def knowledge_graph():
    # Create the knowledge graph instance
    graph = kolibrie.KnowledgeGraph()

    # Add ABox triples (instance-level)
    graph.add_abox_triple("Alice", "hasParent", "Bob")
    graph.add_abox_triple("Bob", "hasParent", "Charlie")

    # Use the helper to encode the predicates
    has_parent_const = graph.encode_term("hasParent")
    has_grandparent_const = graph.encode_term("hasGrandparent")

    # DEBUG: Query before rule application
    print("\nInitial Facts in ABox:")
    for subject, predicate, obj in graph.query_abox():
        print(f"{subject} -- {predicate} -- {obj}")

    # Define a dynamic rule:
    # If X hasParent Y and Y hasParent Z, then X hasGrandparent Z
    grandparent_rule = kolibrie.Rule(
        premise=[
            kolibrie.TriplePattern(
                kolibrie.Term.Variable("X"),
                kolibrie.Term.Constant(has_parent_const),
                kolibrie.Term.Variable("Y"),
            ),
            kolibrie.TriplePattern(
                kolibrie.Term.Variable("Y"),
                kolibrie.Term.Constant(has_parent_const),
                kolibrie.Term.Variable("Z"),
            )
        ],
        filters=[],  # No filters
        conclusion=[  # Changed from single pattern to list of patterns
            kolibrie.TriplePattern(
                kolibrie.Term.Variable("X"),
                kolibrie.Term.Constant(has_grandparent_const),
                kolibrie.Term.Variable("Z"),
            )
        ]
    )

    # Add the rule to the knowledge graph
    graph.add_rule(grandparent_rule)

    # Infer new facts
    inferred_facts = graph.infer_new_facts()

    # DEBUG: Print all inferred facts
    print("\nInferred Facts:")
    for subject, predicate, obj in inferred_facts:
        print(f"{subject} -- {predicate} -- {obj}")

# Run the function
if __name__ == "__main__":
    knowledge_graph()
