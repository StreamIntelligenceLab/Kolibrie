"""Reading and writing RDF: N-Triples, Turtle, RDF/XML"""

import pathlib
import tempfile

import kolibrie

XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def from_ntriples():
    heading("N-Triples, from a string")
    database = kolibrie.KolibrieDB()
    database.load_ntriples(
        f'<urn:alice> <urn:age> "30"^^<{XSD_INTEGER}> .\n'
        '<urn:alice> <urn:name> "Alice"@en .\n'
        "<urn:alice> <urn:knows> <urn:bob> .\n"
    )
    rows = sorted(database.exec_query("SELECT ?p ?o WHERE { ?s ?p ?o }"))
    for predicate, obj in rows:
        print(f"  {predicate:14} {obj!r}")
    assert len(rows) == 3
    return database


def from_turtle():
    heading("Turtle, with prefixes")
    database = kolibrie.KolibrieDB()
    database.parse_turtle(
        """@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://example.org/> .
        <urn:alice> <urn:age> "30"^^xsd:integer .
        <urn:alice> ex:worksAt <urn:kuleuven> .
        """
    )
    rows = sorted(database.exec_query("SELECT ?p ?o WHERE { ?s ?p ?o }"))
    for predicate, obj in rows:
        print(f"  {predicate:24} {obj!r}")

    objects = dict(rows)
    assert "http://example.org/worksAt" in objects, objects
    assert kolibrie.literal_datatype(objects["urn:age"]) == XSD_INTEGER
    return database


def every_reader_agrees():
    heading("Every reader spells a term the same way")
    as_ntriples = kolibrie.KolibrieDB()
    as_ntriples.load_ntriples(f'<urn:a> <urn:age> "30"^^<{XSD_INTEGER}> .\n')

    as_turtle = kolibrie.KolibrieDB()
    as_turtle.parse_turtle(
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        '<urn:a> <urn:age> "30"^^xsd:integer .\n'
    )

    with tempfile.TemporaryDirectory() as directory:
        path = pathlib.Path(directory) / "data.rdf"
        path.write_text(
            '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"\n'
            '         xmlns:ex="http://example.org/">\n'
            '  <rdf:Description rdf:about="urn:a">\n'
            f'    <ex:age rdf:datatype="{XSD_INTEGER}">30</ex:age>\n'
            "  </rdf:Description>\n"
            "</rdf:RDF>\n"
        )
        as_rdfxml = kolibrie.KolibrieDB()
        as_rdfxml.load_file(str(path), "rdfxml")

    terms = {}
    for label, database in [
        ("n-triples", as_ntriples),
        ("turtle", as_turtle),
        ("rdf/xml", as_rdfxml),
    ]:
        objects = [o for (o,) in database.exec_query("SELECT ?o WHERE { ?s ?p ?o }")]
        terms[label] = objects[0]
        print(f"  {label:10} {objects[0]!r}")

    assert len(set(terms.values())) == 1, f"the readers disagree: {terms}"


def writing_it_back():
    heading("Writing it back out")
    database = from_ntriples()
    wire = database.to_ntriples()
    print("  to_ntriples():")
    for line in wire.strip().splitlines():
        print(f"    {line}")

    assert "<urn:alice>" in wire
    assert f'"30"^^<{XSD_INTEGER}>' in wire
    assert '"Alice"@en' in wire

    back = kolibrie.KolibrieDB()
    back.load_ntriples(wire)
    assert sorted(back.exec_query("SELECT ?p ?o WHERE { ?s ?p ?o }")) == sorted(
        database.exec_query("SELECT ?p ?o WHERE { ?s ?p ?o }")
    )

    print(f"  to_nquads() lines: {len(database.to_nquads().strip().splitlines())}")


def building_a_graph_by_hand():
    heading("Building a graph without parsing anything")
    database = kolibrie.KolibrieDB()
    database.add_triple("urn:alice", "urn:age", kolibrie.make_literal("30", XSD_INTEGER))
    database.add_triple("urn:alice", "urn:nick", kolibrie.make_literal("Ali"))
    database.add_triple("urn:alice", "urn:knows", "urn:bob")

    for predicate, obj in sorted(database.exec_query("SELECT ?p ?o WHERE { ?s ?p ?o }")):
        print(f"  {predicate:12} {obj!r:52} [{kolibrie.term_kind(obj)}]")

    assert database.exec_query("SELECT SUM(?o) WHERE { ?s <urn:age> ?o }") == [["30"]]


if __name__ == "__main__":
    from_ntriples()
    from_turtle()
    every_reader_agrees()
    writing_it_back()
    building_a_graph_by_hand()
    print("\nall assertions held")
