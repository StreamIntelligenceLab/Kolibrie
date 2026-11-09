"""Reading and building RDF terms from Python"""

import kolibrie

XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def short(iri):
    """The last segment of an IRI, after either separator"""
    return iri.rsplit("/", 1)[-1].rsplit(":", 1)[-1]


def loaded_database():
    database = kolibrie.KolibrieDB()
    database.parse_turtle(
        """@prefix ex: <http://example.org/> .
        <urn:alice> <urn:age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <urn:bob>   <urn:age> "7"^^<http://www.w3.org/2001/XMLSchema#integer> .
        <urn:alice> <urn:name> "Alice"@en .
        <urn:alice> <urn:nick> "Ali" .
        <urn:alice> ex:knows <urn:bob> .
        """
    )
    return database


def what_a_query_returns():
    heading("What a query returns")
    database = loaded_database()
    rows = sorted(database.exec_query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"))
    for subject, predicate, obj in rows:
        kind = kolibrie.term_kind(obj)
        print(f"  {subject:12} {short(predicate):8} {obj!r:52} [{kind}]")

    objects = {short(predicate): obj for _, predicate, obj in rows}

    assert objects["age"].startswith('"'), objects["age"]
    assert kolibrie.literal_datatype(objects["age"]) == XSD_INTEGER
    assert objects["nick"] == "Ali"
    assert objects["name"] == '"Alice"@en'
    assert kolibrie.literal_language(objects["name"]) == "en"
    assert kolibrie.term_kind(objects["knows"]) == "iri"
    assert objects["knows"] == "urn:bob"


def reading_a_value():
    heading("Reading a value out of a term")
    database = loaded_database()
    ages = [o for _, o in database.exec_query("SELECT ?s ?o WHERE { ?s <urn:age> ?o }")]
    for term in sorted(ages):
        print(
            f"  {term!r:52} lexical={kolibrie.lexical_value(term)!r:6} "
            f"numeric={kolibrie.numeric_value(term)!r}"
        )

    assert sorted(kolibrie.numeric_value(term) for term in ages) == [7.0, 30.0]

    through_engine = database.exec_query("SELECT SUM(?o) WHERE { ?s <urn:age> ?o }")
    print(
        f"  engine SUM={through_engine}  python sum={sum(kolibrie.numeric_value(t) for t in ages)}"
    )
    assert kolibrie.numeric_value(through_engine[0][0]) == 37.0

    assert kolibrie.numeric_value("urn:alice") is None
    assert kolibrie.numeric_value('"Alice"@en') is None


def building_terms():
    heading("Building terms")
    plain = kolibrie.make_literal("25")
    typed = kolibrie.make_literal("25", XSD_INTEGER)
    as_string = kolibrie.make_literal("25", kolibrie.XSD_STRING)
    tagged = kolibrie.make_literal("hello", None, "en")
    for label, term in [
        ("plain", plain),
        ("xsd:integer", typed),
        ("xsd:string", as_string),
        ("@en", tagged),
    ]:
        print(f"  {label:12} {term!r}")

    assert plain == as_string
    assert plain != typed
    assert kolibrie.term_kind("http://example.org/x") == "iri"


def blank_nodes():
    heading("Blank nodes and their scope")
    first = kolibrie.new_document_scope()
    second = kolibrie.new_document_scope()
    in_first = kolibrie.make_blank_node("b", first)
    in_second = kolibrie.make_blank_node("b", second)
    print(f"  `_:b` in one document   {in_first}")
    print(f"  `_:b` in another        {in_second}")

    assert in_first != in_second, "two documents, two nodes"
    assert kolibrie.make_blank_node("b", first) == in_first, "one document, one node"
    assert kolibrie.term_kind(in_first) == "blank_node"


def exchanging_data():
    heading("Exchanging data with another system")
    scope = kolibrie.new_document_scope()
    node = kolibrie.make_blank_node("b", scope)

    mine = kolibrie.skolemise(node, "http://alice.example")
    theirs = kolibrie.skolemise(node, "http://bob.example")
    print(f"  as alice.example  {mine}")
    print(f"  as bob.example    {theirs}")

    assert kolibrie.term_kind(mine) == "iri", "an IRI on the wire, so a merge is safe"
    assert mine != theirs, "two authorities cannot collide"
    assert kolibrie.deskolemise(mine) == node, "and the node is recoverable"
    assert kolibrie.deskolemise("http://example.org/ordinary") is None

    print(f"  to_ntriples_term  {kolibrie.to_ntriples_term('urn:alice')}")
    assert kolibrie.to_ntriples_term("urn:alice") == "<urn:alice>"
    assert kolibrie.to_ntriples_term("Ali") == '"Ali"'
    assert kolibrie.to_ntriples_term(node) == node


def publishing_and_merging():
    heading("Publishing a graph, and merging someone else's")
    document = '_:b <urn:reading> "1" .\n'

    alice = kolibrie.KolibrieDB()
    alice.load_ntriples(document)
    bob = kolibrie.KolibrieDB()
    bob.load_ntriples(document)

    print(f"  alice, plain       {alice.to_ntriples().strip()}")
    print(f"  bob,   plain       {bob.to_ntriples().strip()}")

    from_alice = alice.to_ntriples_skolemised("http://alice.example")
    from_bob = bob.to_ntriples_skolemised("http://bob.example")
    print(f"  alice, skolemised  {from_alice.strip()}")
    print(f"  bob,   skolemised  {from_bob.strip()}")
    assert from_alice != from_bob

    merged = kolibrie.KolibrieDB()
    merged.load_ntriples(from_alice)
    merged.load_ntriples(from_bob)
    subjects = {s for (s,) in merged.exec_query("SELECT ?s WHERE { ?s <urn:reading> ?o }")}
    print(f"  merged subjects    {len(subjects)}")
    assert len(subjects) == 2, f"two nodes, got {subjects}"
    assert all(kolibrie.term_kind(s) == "iri" for s in subjects), subjects

    shared_label = kolibrie.make_blank_node("b", kolibrie.new_document_scope())
    as_alice = kolibrie.skolemise(shared_label, "http://alice.example")
    as_bob = kolibrie.skolemise(shared_label, "http://bob.example")
    print(f"  one label, as alice {as_alice}")
    print(f"  one label, as bob   {as_bob}")
    assert as_alice != as_bob, "the authority is what separates them"
    assert kolibrie.deskolemise(as_alice) == kolibrie.deskolemise(as_bob) == shared_label

    source = (
        '<urn:a> <urn:age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
        '<urn:a> <urn:name> "Alice"@en .\n'
        '<urn:a> <urn:nick> "Ali" .\n'
    )
    there = kolibrie.KolibrieDB()
    there.load_ntriples(source)
    back = kolibrie.KolibrieDB()
    back.load_ntriples(there.to_ntriples())
    objects = sorted(o for _, o in back.exec_query("SELECT ?s ?o WHERE { ?s ?p ?o }"))
    print(f"  round-tripped      {objects}")
    assert objects == [
        '"30"^^<http://www.w3.org/2001/XMLSchema#integer>',
        '"Alice"@en',
        "Ali",
    ], objects


if __name__ == "__main__":
    what_a_query_returns()
    reading_a_value()
    building_terms()
    blank_nodes()
    exchanging_data()
    publishing_and_merging()
    print("\nall assertions held")
