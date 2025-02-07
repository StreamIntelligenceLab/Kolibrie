'''
Before running the test, run such commands in the terminal:
1. cd python
2. python3 -m venv .venv
3. maturin develop
4. source .venv/bin/activate
5. run python script
'''

import py_knowledge_graph

def knowledge_graph():
    # Create the knowledge graph instance
    graph = py_knowledge_graph.PyKnowledgeGraph()

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
    grandparent_rule = py_knowledge_graph.PyRule(
        premise=[
            py_knowledge_graph.PyTriplePattern(
                py_knowledge_graph.PyTerm.Variable("X"),
                py_knowledge_graph.PyTerm.Constant(has_parent_const),
                py_knowledge_graph.PyTerm.Variable("Y"),
            ),
            py_knowledge_graph.PyTriplePattern(
                py_knowledge_graph.PyTerm.Variable("Y"),
                py_knowledge_graph.PyTerm.Constant(has_parent_const),
                py_knowledge_graph.PyTerm.Variable("Z"),
            )
        ],
        filters=[],  # No filters
        conclusion=py_knowledge_graph.PyTriplePattern(
            py_knowledge_graph.PyTerm.Variable("X"),
            py_knowledge_graph.PyTerm.Constant(has_grandparent_const),
            py_knowledge_graph.PyTerm.Variable("Z"),
        )
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
