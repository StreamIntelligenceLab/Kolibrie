"""Datalog reasoning: rules that derive facts from facts"""

import kolibrie

EX = "http://example.org/"


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def n3_rules():
    heading("N3 rules, as text")
    graph = kolibrie.KnowledgeGraph()
    graph.add_abox_triple("alice", f"{EX}hasParent", "bob")
    graph.add_abox_triple("bob", f"{EX}hasParent", "carol")

    added = graph.add_n3_rules(
        f"""@prefix ex: <{EX}> .
        {{ ?x ex:hasParent ?y . ?y ex:hasParent ?z . }} => {{ ?x ex:hasGrandparent ?z . }}.
        """
    )
    print(f"  rules parsed: {added}")
    assert added == 1

    derived = [tuple(fact) for fact in graph.infer_new_facts()]
    for subject, predicate, obj in sorted(derived):
        print(f"    {subject:8} {predicate.rsplit('/', 1)[-1]:16} {obj}")
    assert ("alice", f"{EX}hasGrandparent", "carol") in derived, derived


def several_rules_in_one_document():
    heading("Several rules, one prefix block")
    graph = kolibrie.KnowledgeGraph()
    for parent, child in [("bob", "alice"), ("carol", "bob"), ("dave", "carol")]:
        graph.add_abox_triple(child, f"{EX}hasParent", parent)

    added = graph.add_n3_rules(
        f"""@prefix ex: <{EX}> .
        {{ ?x ex:hasParent ?y . }} => {{ ?x ex:hasAncestor ?y . }}.
        {{ ?x ex:hasAncestor ?y . ?y ex:hasAncestor ?z . }} => {{ ?x ex:hasAncestor ?z . }}.
        """
    )
    print(f"  rules parsed: {added}")
    assert added == 2

    derived = [tuple(fact) for fact in graph.infer_new_facts_semi_naive()]
    ancestors = sorted(
        (subject, obj) for subject, predicate, obj in derived if predicate.endswith("hasAncestor")
    )
    for descendant, ancestor in ancestors:
        print(f"    {descendant:8} hasAncestor  {ancestor}")

    alice = {ancestor for descendant, ancestor in ancestors if descendant == "alice"}
    assert alice == {"bob", "carol", "dave"}, alice


def construct_rules():
    heading("Kolibrie CONSTRUCT rules, as text")
    database = kolibrie.KolibrieDB()
    database.load_ntriples(
        "<urn:alice> <urn:hasParent> <urn:bob> .\n<urn:bob> <urn:hasParent> <urn:carol> .\n"
    )

    derived = database.add_rule(
        """
        RULE :Grandparent :-
        CONSTRUCT { ?x <urn:hasGrandparent> ?z . }
        WHERE { ?x <urn:hasParent> ?y . ?y <urn:hasParent> ?z . } .
        """
    )
    print(f"  derived {len(derived)}:")
    for subject, predicate, obj in derived:
        print(f"    {subject:12} {predicate:22} {obj}")
    assert derived == [("urn:alice", "urn:hasGrandparent", "urn:carol")], derived

    rows = database.exec_query("SELECT ?a ?b WHERE { ?a <urn:hasGrandparent> ?b }")
    print(f"  queryable: {rows}")
    assert rows == [["urn:alice", "urn:carol"]], rows

    try:
        database.add_rule("RULE :Broken :- CONSTRUCT {")
        raise AssertionError("a malformed rule should raise")
    except ValueError as error:
        print(f"  a malformed rule raises: {str(error)[:50]}...")


def asking_backwards():
    heading("Backward chaining: one goal, not every fact")
    graph = kolibrie.KnowledgeGraph()
    graph.add_abox_triple("alice", f"{EX}hasParent", "bob")
    graph.add_abox_triple("bob", f"{EX}hasParent", "carol")
    graph.add_n3_rules(
        f"""@prefix ex: <{EX}> .
        {{ ?x ex:hasParent ?y . ?y ex:hasParent ?z . }} => {{ ?x ex:hasGrandparent ?z . }}.
        """
    )

    goal = kolibrie.TriplePattern(
        kolibrie.Term.Variable("who"),
        kolibrie.Term.Constant(graph.encode_term(f"{EX}hasGrandparent")),
        kolibrie.Term.Variable("whose"),
    )
    answers = graph.backward_chaining(goal)
    print(f"  asked for hasGrandparent, got {len(answers)} answer(s)")

    for answer in answers:
        readable = {
            name: graph.decode_term(term.id)
            for name, term in answer.items()
            if name in ("who", "whose") and hasattr(term, "id")
        }
        print(f"    {readable}")

    assert answers, "alice hasGrandparent carol follows from the two facts"
    assert any(
        {
            name: graph.decode_term(term.id) for name, term in answer.items() if hasattr(term, "id")
        }.get("who")
        == "alice"
        for answer in answers
    ), answers


def building_a_rule_from_code():
    heading("Building a rule, for when it comes from code")
    graph = kolibrie.KnowledgeGraph()
    graph.add_abox_triple("alice", f"{EX}hasParent", "bob")
    graph.add_abox_triple("bob", f"{EX}hasParent", "carol")

    has_parent = kolibrie.Term.Constant(graph.encode_term(f"{EX}hasParent"))
    has_grandparent = kolibrie.Term.Constant(graph.encode_term(f"{EX}hasGrandparent"))
    x, y, z = (kolibrie.Term.Variable(name) for name in ("X", "Y", "Z"))

    graph.add_rule(
        kolibrie.Rule(
            premise=[
                kolibrie.TriplePattern(x, has_parent, y),
                kolibrie.TriplePattern(y, has_parent, z),
            ],
            filters=[],
            conclusion=[kolibrie.TriplePattern(x, has_grandparent, z)],
        )
    )
    derived = [tuple(fact) for fact in graph.infer_new_facts()]
    print(f"  derived: {sorted(derived)}")
    assert ("alice", f"{EX}hasGrandparent", "carol") in derived, derived


if __name__ == "__main__":
    n3_rules()
    several_rules_in_one_document()
    construct_rules()
    asking_backwards()
    building_a_rule_from_code()
    print("\nall assertions held")
