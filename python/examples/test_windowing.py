import py_kolibrie as kolibrie

def test_streaming_query_integration():
    # Create a new database
    db = kolibrie.PySparqlDatabase()

    stream_query = (db.query()
                      .window(10, 2)
                      .with_predicate("knows")
                      .with_stream_operator(kolibrie.PyStreamOperator.RSTREAM)
                      .as_stream())
    
    result1 = stream_query.add_stream_triple("Alice", "knows", "Bob", 1)
    result2 = stream_query.add_stream_triple("Bob", "knows", "Charlie", 2)
    result3 = stream_query.add_stream_triple("Alice", "likes", "Pizza", 3)

    results = stream_query.get_stream_results()
    print(f"Retrieved {len(results)} result batches")

    # Decode and display results
    print("\nStreaming results (decoded):")
    if results:
        total_triples = 0
        for i, batch in enumerate(results, 1):
            print(f"  Batch {i}: {len(batch)} triples")
            for j, (subject, predicate, obj) in enumerate(batch, 1):
                print(f"    {j}. {subject} -> {predicate} -> {obj}")
                total_triples += 1
        
        print(f"\nTotal triples in results: {total_triples}")
        
        # Verify filtering worked (should only have "knows" predicates)
        knows_count = 0
        other_count = 0
        for batch in results:
            for _, predicate, _ in batch:
                if predicate == "knows":
                    knows_count += 1
                else:
                    other_count += 1
        
        print(f"'knows' predicates: {knows_count}")
        print(f"Other predicates: {other_count}")

    stream_query.stop_stream()

def test_istream_operator():
    """
    Python equivalent of test_istream_operator()
    Tests ISTREAM operator with incremental results.
    """
    
    # Create a new database
    db = kolibrie.PySparqlDatabase()
    
    stream_query = (db.query()
                      .window(10, 2)
                      .with_subject_like("Alice")
                      .with_stream_operator(kolibrie.PyStreamOperator.ISTREAM)
                      .as_stream())
    
    print("\nAdding triples over multiple time points...")
    
    stream_query.add_stream_triple("Alice", "knows", "Bob", 1)
    print("Added: Alice -> knows -> Bob (timestamp: 1)")
        
    results1 = stream_query.get_stream_results()
    print(f"Retrieved results1: {len(results1)} batches")
        
    # Second triple
    stream_query.add_stream_triple("Alice", "knows", "Charlie", 5)
    print("Added: Alice -> knows -> Charlie (timestamp: 5)")
        
    results2 = stream_query.get_stream_results()
    print(f"Retrieved results2: {len(results2)} batches")
    
    print("\nDecoding and displaying ISTREAM results...")
    
    # Display results1 (decoded)
    print("\nISTREAM results 1 (decoded):")
    if results1:
        total_triples_1 = 0
        for i, batch in enumerate(results1, 1):
            print(f"  Batch {i}: {len(batch)} triples")
            for j, (subject, predicate, obj) in enumerate(batch, 1):
                print(f"    {j}. {subject} -> {predicate} -> {obj}")
                total_triples_1 += 1
        print(f"  Total in results1: {total_triples_1}")
    else:
        print("  No results in batch 1")
    
    # Display results2 (decoded)
    print("\nISTREAM results 2 (decoded):")
    if results2:
        total_triples_2 = 0
        for i, batch in enumerate(results2, 1):
            print(f"  Batch {i}: {len(batch)} triples")
            for j, (subject, predicate, obj) in enumerate(batch, 1):
                print(f"    {j}. {subject} -> {predicate} -> {obj}")
                total_triples_2 += 1
        print(f"  Total in results2: {total_triples_2}")
    else:
        print("  No results in batch 2")
    
    # Analyze ISTREAM behavior
    print("\nISTREAM Analysis:")
    print(f"  - Results1 batches: {len(results1)}")
    print(f"  - Results2 batches: {len(results2)}")
    
    all_results = stream_query.get_all_stream_results()
    print(f"  - All accumulated batches: {len(all_results)}")
        
    if all_results:
        print("\nAll accumulated results (decoded):")
        for i, batch in enumerate(all_results, 1):
            print(f"  Batch {i}: {len(batch)} triples")
            for j, (subject, predicate, obj) in enumerate(batch, 1):
                print(f"    {j}. {subject} -> {predicate} -> {obj}")
                    
    # Verify subject filtering (should only have subjects containing "Alice")
    alice_count = 0
    other_count = 0
    for batch in all_results:
        for subject, _, _ in batch:
            if "Alice" in subject:
                alice_count += 1
            else:
                other_count += 1
    
    print(f"\n Subjects containing 'Alice': {alice_count}")
    print(f" Other subjects: {other_count}")
    
    if other_count == 0:
        print(" Subject filtering working correctly!")
    else:
        print(" Warning: Some non-Alice subjects found")
    
    # Clean up
    stream_query.stop_stream()

def main():
    print("Running streaming query integration test...")
    test_streaming_query_integration()
    
    print("\nRunning ISTREAM operator test...")
    test_istream_operator()

if __name__ == "__main__":
    main()