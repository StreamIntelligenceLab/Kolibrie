"""Reading and building RDF terms from Python.

A query hands back terms as strings, and a term's string is its canonical spelling:
an IRI bare, a plain literal bare, a typed literal as `"25"^^<...#integer>`, a
language-tagged one as `"hello"@en`, a blank node as `_:label`. That spelling is the
term's identity, which is why it carries the datatype instead of discarding it -- and
why reading a *value* out of one means asking rather than using the string directly.

Run it from a directory holding the built extension module as `kolibrie.so`:
`scripts/smoke_python.py` does that for you.

Every claim below is asserted, so this file fails loudly rather than printing
something wrong.
"""

import kolibrie

XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def short(iri):
    """The last segment of an IRI, after either separator.

    `urn:age` and `http://example.org/knows` both have a last segment; only the
    second has a slash, which is why splitting on one alone is not enough.
    """
    return iri.rsplit("/", 1)[-1].rsplit(":", 1)[-1]


def loaded_database():
    database = kolibrie.SparqlDatabase()
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

    # A typed literal keeps its datatype, so the string is not the value.
    assert objects["age"].startswith('"'), objects["age"]
    assert kolibrie.literal_datatype(objects["age"]) == XSD_INTEGER
    # A plain literal is spelled bare, exactly the text it holds.
    assert objects["nick"] == "Ali"
    # A language tag survives, and is not glued onto the text.
    assert objects["name"] == '"Alice"@en'
    assert kolibrie.literal_language(objects["name"]) == "en"
    # An IRI is an IRI whatever its scheme, and a prefixed name is expanded.
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

    # The number lives in the lexical form, not the spelling. Without this, Python
    # arithmetic on a typed literal would have to strip the datatype by hand.
    assert sorted(kolibrie.numeric_value(term) for term in ages) == [7.0, 30.0]

    # And the engine agrees with Python about the sum, which is the property that
    # breaks silently if either side reads the spelling instead of the value.
    through_engine = database.exec_query("SELECT SUM(?o) WHERE { ?s <urn:age> ?o }")
    print(f"  engine SUM={through_engine}  python sum={sum(kolibrie.numeric_value(t) for t in ages)}")
    assert kolibrie.numeric_value(through_engine[0][0]) == 37.0

    # A term that denotes no number says so, rather than guessing.
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

    # RDF 1.1 section 3.3: a plain literal *is* an xsd:string literal, so one term.
    assert plain == as_string
    # Differing only in datatype is still two terms.
    assert plain != typed
    # There is no make_iri, and that is the point: an IRI is spelled as itself.
    assert kolibrie.term_kind("http://example.org/x") == "iri"


def blank_nodes():
    heading("Blank nodes and their scope")
    # A label identifies a node only within the document it came from, so building
    # one needs a scope. One scope per document.
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

    # A blank node label means nothing outside its document, so publishing one and
    # merging it elsewhere fuses unrelated nodes. Skolemisation replaces it with an
    # IRI under an authority you control, which is global.
    mine = kolibrie.skolemise(node, "http://alice.example")
    theirs = kolibrie.skolemise(node, "http://bob.example")
    print(f"  as alice.example  {mine}")
    print(f"  as bob.example    {theirs}")

    assert kolibrie.term_kind(mine) == "iri", "an IRI on the wire, so a merge is safe"
    assert mine != theirs, "two authorities cannot collide"
    assert kolibrie.deskolemise(mine) == node, "and the node is recoverable"
    assert kolibrie.deskolemise("http://example.org/ordinary") is None

    # Writing a term out is not the same as its spelling: N-Triples needs the
    # brackets a canonical IRI does not carry.
    print(f"  to_ntriples_term  {kolibrie.to_ntriples_term('urn:alice')}")
    assert kolibrie.to_ntriples_term("urn:alice") == "<urn:alice>"
    assert kolibrie.to_ntriples_term("Ali") == '"Ali"'
    assert kolibrie.to_ntriples_term(node) == node


if __name__ == "__main__":
    what_a_query_returns()
    reading_a_value()
    building_terms()
    blank_nodes()
    exchanging_data()
    print("\nall assertions held")
