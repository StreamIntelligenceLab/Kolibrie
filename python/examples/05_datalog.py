"""Datalog reasoning: rules that derive facts from facts.

A `KnowledgeGraph` holds facts and rules, and derives what follows. A rule is a
premise -- one or more triple patterns sharing variables -- and a conclusion built
from the same variables.

Terms in a rule are not strings. A variable is `Term.Variable("X")` and a constant is
`Term.Constant(id)`, where the id comes from `encode_term`: the graph stores terms as
integers, so a rule has to name them the way the graph does. That is the one piece of
bookkeeping this API asks for.

Run with `scripts/smoke_python.py`. Every claim below is asserted.
"""

import kolibrie


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def pattern(subject, predicate, obj):
    """A triple pattern, from terms."""
    return kolibrie.TriplePattern(subject, predicate, obj)


def transitive_rule(graph):
    heading("One rule, applied once")
    graph.add_abox_triple("alice", "hasParent", "bob")
    graph.add_abox_triple("bob", "hasParent", "carol")

    print("  facts:")
    for subject, predicate, obj in sorted(graph.query_abox()):
        print(f"    {subject:8} {predicate:14} {obj}")

    # The graph stores terms as integers, so the rule names them that way.
    has_parent = graph.encode_term("hasParent")
    has_grandparent = graph.encode_term("hasGrandparent")

    # X hasParent Y, Y hasParent Z  =>  X hasGrandparent Z
    graph.add_rule(
        kolibrie.Rule(
            premise=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_parent),
                    kolibrie.Term.Variable("Y"),
                ),
                pattern(
                    kolibrie.Term.Variable("Y"),
                    kolibrie.Term.Constant(has_parent),
                    kolibrie.Term.Variable("Z"),
                ),
            ],
            filters=[],
            conclusion=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_grandparent),
                    kolibrie.Term.Variable("Z"),
                )
            ],
        )
    )

    derived = graph.infer_new_facts()
    print("  derived:")
    for subject, predicate, obj in sorted(derived):
        print(f"    {subject:8} {predicate:14} {obj}")

    assert ("alice", "hasGrandparent", "carol") in [tuple(f) for f in derived], derived
    # Only the grandparent fact is new; the two parent facts were already there.
    assert len(derived) == 1, derived


def chaining_to_a_fixpoint():
    heading("Rules that feed each other")
    graph = kolibrie.KnowledgeGraph()
    # A chain long enough that one pass is not enough: the ancestor of an ancestor
    # has to be found from a fact the rule itself derived.
    for parent, child in [("bob", "alice"), ("carol", "bob"), ("dave", "carol")]:
        graph.add_abox_triple(child, "hasParent", parent)

    has_parent = graph.encode_term("hasParent")
    has_ancestor = graph.encode_term("hasAncestor")

    # X hasParent Y => X hasAncestor Y
    graph.add_rule(
        kolibrie.Rule(
            premise=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_parent),
                    kolibrie.Term.Variable("Y"),
                )
            ],
            filters=[],
            conclusion=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_ancestor),
                    kolibrie.Term.Variable("Y"),
                )
            ],
        )
    )
    # X hasAncestor Y, Y hasAncestor Z => X hasAncestor Z
    graph.add_rule(
        kolibrie.Rule(
            premise=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_ancestor),
                    kolibrie.Term.Variable("Y"),
                ),
                pattern(
                    kolibrie.Term.Variable("Y"),
                    kolibrie.Term.Constant(has_ancestor),
                    kolibrie.Term.Variable("Z"),
                ),
            ],
            filters=[],
            conclusion=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_ancestor),
                    kolibrie.Term.Variable("Z"),
                )
            ],
        )
    )

    # Semi-naive evaluation reaches a fixpoint, so the transitive closure is
    # complete rather than one pass deep.
    derived = [tuple(fact) for fact in graph.infer_new_facts_semi_naive()]
    ancestors = sorted((s, o) for s, p, o in derived if p == "hasAncestor")
    print(f"  {len(ancestors)} ancestor facts:")
    for descendant, ancestor in ancestors:
        print(f"    {descendant:8} hasAncestor  {ancestor}")

    # alice's ancestors are bob, carol and dave -- the last of which needs two
    # applications of the transitive rule to reach.
    alice = {ancestor for descendant, ancestor in ancestors if descendant == "alice"}
    assert alice == {"bob", "carol", "dave"}, alice


def asking_backwards():
    heading("Backward chaining: one goal, not every fact")
    graph = kolibrie.KnowledgeGraph()
    graph.add_abox_triple("alice", "hasParent", "bob")
    graph.add_abox_triple("bob", "hasParent", "carol")

    has_parent = graph.encode_term("hasParent")
    has_grandparent = graph.encode_term("hasGrandparent")
    graph.add_rule(
        kolibrie.Rule(
            premise=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_parent),
                    kolibrie.Term.Variable("Y"),
                ),
                pattern(
                    kolibrie.Term.Variable("Y"),
                    kolibrie.Term.Constant(has_parent),
                    kolibrie.Term.Variable("Z"),
                ),
            ],
            filters=[],
            conclusion=[
                pattern(
                    kolibrie.Term.Variable("X"),
                    kolibrie.Term.Constant(has_grandparent),
                    kolibrie.Term.Variable("Z"),
                )
            ],
        )
    )

    # Forward chaining derives everything that follows; backward chaining answers
    # one question, working from the goal to the facts.
    goal = pattern(
        kolibrie.Term.Variable("who"),
        kolibrie.Term.Constant(has_grandparent),
        kolibrie.Term.Variable("whose"),
    )
    answers = graph.backward_chaining(goal)
    print(f"  asked for hasGrandparent, got {len(answers)} answer(s)")

    # The bindings come back in the graph's own terms -- ids, not strings, and
    # including the internal variables the solver introduced while working the goal
    # backwards. `decode_term` turns the ids into something readable, and the two
    # variables the goal asked about are the ones worth reading.
    for answer in answers:
        readable = {
            name: graph.decode_term(term.id)
            for name, term in answer.items()
            if name in ("who", "whose") and hasattr(term, "id")
        }
        print(f"    {readable}")

    assert answers, "alice hasGrandparent carol follows from the two facts"
    decoded = [
        {
            name: graph.decode_term(term.id)
            for name, term in answer.items()
            if hasattr(term, "id")
        }
        for answer in answers
    ]
    assert any(
        binding.get("who") == "alice" and binding.get("whose") == "carol"
        for binding in decoded
    ), decoded


if __name__ == "__main__":
    transitive_rule(kolibrie.KnowledgeGraph())
    chaining_to_a_fixpoint()
    asking_backwards()
    print("\nall assertions held")
