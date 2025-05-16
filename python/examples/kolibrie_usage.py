# 
# Copyright © 2024 ladroid
# KU Leuven — Stream Intelligence Lab, Belgium
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# you can obtain one at https://mozilla.org/MPL/2.0/.
# 

#!/usr/bin/env python3
from py_kolibrie import PySparqlDatabase

def main():
    db = PySparqlDatabase()

    # load some data
    db.add_triple("http://example.org/Alice",   "http://example.org/knows", "http://example.org/Bob")
    db.add_triple("http://example.org/Bob",     "http://example.org/knows", "http://example.org/Carol")
    db.add_triple("http://example.org/Alice",   "http://example.org/likes", "http://example.org/IceCream")

    # now build & run a query
    qb = (
        db.query()
          .with_subject("http://example.org/Alice")
          .distinct()
          .limit(20)
    )

    triples = qb.get_decoded_triples()
    print("Decoded triples:")
    for s, p, o in triples:
        print(f"  {s} -- {p} --> {o}")

    subjects = qb.get_subjects()
    print("\nDistinct subjects:")
    for s in subjects:
        print(" ", s)

    count = qb.count()
    print(f"\nTotal matching triples: {count}")

if __name__ == "__main__":
    main()
