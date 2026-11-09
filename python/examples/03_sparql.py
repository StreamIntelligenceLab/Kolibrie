"""Querying with SPARQL, two ways"""

import kolibrie

XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def people():
    database = kolibrie.KolibrieDB()
    database.load_ntriples(
        f"""<urn:alice> <urn:age> "30"^^<{XSD_INTEGER}> .
<urn:bob> <urn:age> "7"^^<{XSD_INTEGER}> .
<urn:carol> <urn:age> "21"^^<{XSD_INTEGER}> .
<urn:alice> <urn:name> "Alice"@en .
<urn:bob> <urn:name> "Bob"@en .
<urn:carol> <urn:name> "Carol"@en .
<urn:alice> <urn:knows> <urn:bob> .
<urn:bob> <urn:knows> <urn:carol> .
"""
    )
    return database


def selecting():
    heading("SELECT")
    database = people()
    rows = sorted(database.exec_query("SELECT ?s ?o WHERE { ?s <urn:age> ?o }"))
    for subject, age in rows:
        print(f"  {subject:12} {age!r}")
    assert len(rows) == 3

    assert database.exec_query("SELECT ?s WHERE { ?s <urn:nothing> ?o }") == []
    try:
        database.exec_query("SELECT ?s WHERE {")
        raise AssertionError("a malformed query should raise")
    except RuntimeError as error:
        print(f"  a malformed query raises: {str(error)[:52]}...")


def joining_and_filtering():
    heading("Joins and FILTER")
    database = people()

    joined = sorted(
        database.exec_query("SELECT ?a ?b WHERE { ?a <urn:knows> ?b . ?b <urn:age> ?age }")
    )
    print(f"  who knows whom:  {joined}")
    assert joined == [["urn:alice", "urn:bob"], ["urn:bob", "urn:carol"]]

    adults = sorted(
        s
        for (s,) in database.exec_query("SELECT ?s WHERE { ?s <urn:age> ?age . FILTER(?age > 10) }")
    )
    print(f"  age > 10:        {adults}")
    assert adults == ["urn:alice", "urn:carol"], adults

    next_year = sorted(
        s
        for (s,) in database.exec_query(
            "SELECT ?s WHERE { ?s <urn:age> ?age . FILTER(?age + 1 > 22) }"
        )
    )
    print(f"  age + 1 > 22:    {next_year}")
    assert next_year == ["urn:alice"], next_year


def aggregating_and_ordering():
    heading("Aggregates and ORDER BY")
    database = people()

    for expression, expected in [("COUNT(?s)", "3"), ("SUM(?age)", "58"), ("MIN(?age)", None)]:
        rows = database.exec_query(f"SELECT {expression} WHERE {{ ?s <urn:age> ?age }}")
        print(f"  {expression:12} {rows}")
        if expected is not None:
            assert rows == [[expected]], rows

    ordered = [
        kolibrie.numeric_value(age)
        for (age,) in database.exec_query("SELECT ?age WHERE { ?s <urn:age> ?age } ORDER BY ?age")
    ]
    print(f"  ORDER BY ?age    {ordered}")
    assert ordered == [7.0, 21.0, 30.0], ordered

    limited = [
        kolibrie.numeric_value(age)
        for (age,) in database.exec_query(
            "SELECT ?age WHERE { ?s <urn:age> ?age } ORDER BY ?age LIMIT 2"
        )
    ]
    print(f"  ... LIMIT 2      {limited}")
    assert limited == [7.0, 21.0], limited


def building_a_query_instead_of_writing_one():
    heading("The query builder")
    database = people()

    builder = database.query().with_predicate("urn:knows")
    print(f"  with_predicate('urn:knows') count: {builder.count()}")
    assert builder.count() == 2

    subjects = sorted(database.query().with_predicate("urn:age").get_subjects())
    print(f"  subjects with an age:  {subjects}")
    assert subjects == ["urn:alice", "urn:bob", "urn:carol"]

    grouped = database.query().with_predicate("urn:knows").group_by_subject()
    print(f"  grouped by subject:    {sorted(grouped)}")
    assert sorted(grouped) == ["urn:alice", "urn:bob"]

    assert len(database.query().with_predicate("urn:age").limit(2).get_subjects()) == 2


def updating():
    heading("INSERT and DELETE")
    database = people()

    database.update(f'INSERT DATA {{ <urn:dave> <urn:age> "44"^^<{XSD_INTEGER}> }}')
    assert database.exec_query("SELECT SUM(?age) WHERE { ?s <urn:age> ?age }") == [["102"]]
    print("  after INSERT, SUM(?age) = 102")

    database.update("DELETE DATA { <urn:alice> <urn:knows> <urn:bob> }")
    remaining = database.exec_query("SELECT ?a ?b WHERE { ?a <urn:knows> ?b }")
    print(f"  after DELETE, knows:   {remaining}")
    assert remaining == [["urn:bob", "urn:carol"]], remaining

    try:
        database.update("INSERT DATA {")
        raise AssertionError("a malformed update should raise")
    except ValueError as error:
        print(f"  a malformed update raises: {str(error)[:48]}...")


if __name__ == "__main__":
    selecting()
    joining_and_filtering()
    aggregating_and_ordering()
    building_a_query_instead_of_writing_one()
    updating()
    print("\nall assertions held")
