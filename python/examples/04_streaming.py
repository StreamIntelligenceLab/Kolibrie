"""RDF Stream Processing: windows over streams, and joins across them"""

import kolibrie

READING = "http://example/Reading"
HOT_ROOM = "http://example/HotRoom"
HIGH_CO2 = "http://example/HighCO2"


def heading(text):
    print(f"\n-- {text} " + "-" * max(0, 68 - len(text)))


def one_window():
    heading("One window over one stream")
    engine = kolibrie.RspEngine(
        f"""
        REGISTER RSTREAM <http://out/readings> AS
        SELECT *
        FROM NAMED WINDOW :recent ON :sensors [RANGE 4 STEP 2]
        WHERE {{ WINDOW :recent {{ ?s a <{READING}> . }} }}
        """
    )
    assert engine.is_running()

    for tick in range(8):
        added = engine.add("sensors", f"<http://example/r{tick}> a <{READING}> .", tick)
        assert added == 1, f"one triple per call, got {added}"

    engine.stop()
    assert not engine.is_running()

    solutions = engine.results()
    print(f"  streamed 8 readings, got {len(solutions)} solutions")
    assert solutions, "a window that fires should produce solutions"

    streamed = {f"http://example/r{tick}" for tick in range(8)}
    for solution in solutions:
        bound = dict(solution)
        assert set(bound) == {"s"}, bound
        assert bound["s"] in streamed, bound
    print(f"  distinct subjects seen: {len({dict(s)['s'] for s in solutions})}")


def two_windows_joined():
    heading("Two windows, joined across streams")
    engine = kolibrie.RspEngine(
        f"""
        REGISTER RSTREAM <http://out/alerts> AS
        SELECT *
        FROM NAMED WINDOW :warm ON :temperature [RANGE 4 STEP 2]
        FROM NAMED WINDOW :stale ON :co2 [RANGE 4 STEP 2]
        WHERE {{
            WINDOW :warm {{ ?room a <{HOT_ROOM}> . }}
            WINDOW :stale {{ ?zone a <{HIGH_CO2}> . }}
        }}
        """
    )

    for tick in range(6):
        engine.add("temperature", f"<http://example/room{tick}> a <{HOT_ROOM}> .", tick)
        engine.add("co2", f"<http://example/zone{tick}> a <{HIGH_CO2}> .", tick)

    engine.stop()
    solutions = engine.results()
    print(f"  streamed 6 + 6, got {len(solutions)} joined solutions")
    for solution in solutions[:3]:
        bound = dict(solution)
        print(
            f"    room={bound['room'].rsplit('/', 1)[-1]:8} zone={bound['zone'].rsplit('/', 1)[-1]}"
        )

    assert solutions, "both windows fired, so the join should produce solutions"
    for solution in solutions:
        bound = dict(solution)
        assert set(bound) == {"room", "zone"}, bound
        assert kolibrie.term_kind(bound["room"]) == "iri"
        assert kolibrie.term_kind(bound["zone"]) == "iri"


def results_are_complete_when_stop_returns():
    heading("stop() waits, so you do not have to")
    engine = kolibrie.RspEngine(
        f"""
        REGISTER RSTREAM <http://out/readings> AS
        SELECT *
        FROM NAMED WINDOW :recent ON :sensors [RANGE 2 STEP 1]
        WHERE {{ WINDOW :recent {{ ?s a <{READING}> . }} }}
        """
    )
    for tick in range(5):
        engine.add("sensors", f"<http://example/r{tick}> a <{READING}> .", tick)

    engine.stop()
    settled = engine.result_count()
    assert engine.result_count() == settled, "a result arrived after stop() returned"
    print(f"  {settled} solutions, and the count does not move after stop()")

    engine.stop()
    try:
        engine.add("sensors", f"<http://example/late> a <{READING}> .", 99)
        raise AssertionError("adding to a stopped engine should raise")
    except RuntimeError as error:
        print(f"  adding after stop raises: {error}")


def reading_values_from_a_stream():
    heading("Terms in a solution are terms")
    xsd_integer = "http://www.w3.org/2001/XMLSchema#integer"
    engine = kolibrie.RspEngine(
        """
        REGISTER RSTREAM <http://out/temps> AS
        SELECT *
        FROM NAMED WINDOW :recent ON :sensors [RANGE 2 STEP 1]
        WHERE { WINDOW :recent { ?s <http://example/celsius> ?c . } }
        """
    )
    for tick, celsius in enumerate([19, 23, 27]):
        engine.add(
            "sensors",
            f'<http://example/s{tick}> <http://example/celsius> "{celsius}"^^<{xsd_integer}> .',
            tick,
        )
    engine.stop()

    readings = {dict(s)["c"] for s in engine.results()}
    for term in sorted(readings):
        print(f"  {term!r:52} -> {kolibrie.numeric_value(term)}")
    assert readings, "the window should have fired"
    for term in readings:
        assert kolibrie.literal_datatype(term) == xsd_integer, term
        assert kolibrie.numeric_value(term) in (19.0, 23.0, 27.0), term


if __name__ == "__main__":
    one_window()
    two_windows_joined()
    results_are_complete_when_stop_returns()
    reading_values_from_a_stream()
    print("\nall assertions held")
