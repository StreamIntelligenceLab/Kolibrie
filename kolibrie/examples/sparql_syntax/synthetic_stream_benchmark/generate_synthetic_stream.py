import random
import json
import argparse

def generate_synthetic_data(num_triples, num_subjects, num_predicates, num_objects, window_size, slide_size, output_nt, output_queries):
    subjects = [f"<http://example.org/s{i}>" for i in range(num_subjects)]
    predicates = [f"<http://example.org/p{i}>" for i in range(num_predicates)]
    objects = [f"<http://example.org/o{i}>" for i in range(num_objects)]
    
    # 1. Generate the triple stream
    with open(output_nt, 'w') as f:
        for _ in range(num_triples):
            s = random.choice(subjects)
            p = random.choice(predicates)
            o = random.choice(objects)
            f.write(f"{s} {p} {o} .\n")
            
    def get_distinct(pool, k):
        return random.sample(pool, k)

    # 2. Pick specific constants for queries to ensure they match generated data
    p_vars = get_distinct(predicates, 9)
    o_vars = get_distinct(objects, 5)
    s_vars = get_distinct(subjects, 4)
    p_q6 = get_distinct(predicates, 20)
    
    # 3. Formulate standard SPARQL SELECT queries
    # NOTE: we are not using RSPQL window functions as for some reason these are insanely slow, obscuring any performance difference due to indexing strategy
    # Q6 helper: matches a subject with 20 distinct properties
    q6_where = " ".join([f"?s {p_q6[i]} ?o{i} ." for i in range(20)])
    q6_select = " ".join([f"?o{i}" for i in range(20)])

    # Generate 500 distinct predicates to force BucketIndex to create 500 buckets
    p_q9 = get_distinct(predicates, 500)
    
    q9_where = " ".join([f"?s {p_q9[i]} ?o{i} ." for i in range(500)])
    q9_select = " ".join([f"?o{i}" for i in range(500)])

    p_q10 = get_distinct(predicates, 500) 
    s_q10 = get_distinct(subjects, 5)
    o_q10 = get_distinct(objects, 5)

    # 2. Build the Q10 WHERE clause explicitly
    q10_where_clauses = [
        # --- SAVE TABLE ---
        # 1. S _ _ : Highly selective subject bound (~20 matches). Table does 1 scan.
        f"{s_q10[0]} ?p_start ?v_core .", 
        
        # 2. _ P O : The 20 matches join against this. Probability of a match is near zero.
        # The join collapses immediately.
        f"?v_core {p_q10[0]} {o_q10[0]} .",
        
        # --- KILL PARTIAL HEXASTORE ---
        # Force the dynamic indexer to build all remaining permutations statically
        f"{s_q10[1]} {p_q10[1]} ?v_core .",    # S P _
        f"{s_q10[2]} ?p_mid {o_q10[1]} .",     # S _ O
        f"?v_core ?p_end {o_q10[2]} .",        # _ _ O
        f"{s_q10[3]} {p_q10[2]} {o_q10[3]} .", # S P O
    ]

    # --- KILL BUCKETS ---
    # Add 490+ distinct _ P _ patterns to force massive bucket allocation
    for i in range(3, 500):
        q10_where_clauses.append(f"?v_core {p_q10[i]} ?v_ext_{i} .")

    q10_where = " ".join(q10_where_clauses)
    
    queries = {
        "Q1": f"SELECT ?s WHERE {{ ?s {p_vars[0]} {o_vars[0]} . }}",
        "Q2": f"SELECT ?s ?o2 ?o3 WHERE {{ ?s {p_vars[1]} ?o2 . ?s {p_vars[2]} ?o3 . }}",
        "Q3": f"SELECT * WHERE {{ {s_vars[0]} {p_vars[3]} {o_vars[1]} . }}",
        "Q4": f"SELECT ?v1 ?v5 WHERE {{ ?v1 {p_vars[4]} ?v2 . ?v2 {p_vars[5]} ?v3 . ?v4 {p_vars[6]} ?v3 . ?v4 {p_vars[7]} ?v5 . }}",
        "Q5": f"SELECT ?p WHERE {{ {s_vars[1]} ?p {o_vars[2]} . {s_vars[2]} ?p {o_vars[3]} . }}",
        "Q6": f"SELECT ?s {q6_select} WHERE {{ {q6_where} }}",
        "Q7": f"SELECT ?s4 ?p5 ?o6 WHERE {{ {s_vars[3]} ?p1 ?o1 . ?s2 {p_vars[8]} ?o2 . ?s3 ?p3 {o_vars[4]} . ?s4 ?p1 ?o4 . ?s5 ?p5 ?o2 . ?s3 ?p6 ?o6 . ?s4 ?p5 ?o6 }}",
        "Q8": f"SELECT * WHERE {{ ?n1 ?e12 ?n2 .  ?n2 ?e23 ?n3 .  ?n3 ?e34 ?n4 .  ?n4 ?e41 ?n1 .  ?n1 ?e13 ?n3 .  ?n3 ?e31 ?n1 .  ?n2 ?e24 ?n4 .  ?n4 ?e42 ?n2 .  ?n2 ?e21 ?n1 .  ?n4 ?e43 ?n3 . }}",
        "Q9": f"SELECT ?s {q9_select} WHERE {{ {q9_where} }}",
        "Q10": f"SELECT * WHERE {{ {q10_where} }}"
    }
    
    with open(output_queries, 'w') as f:
        json.dump(queries, f, indent=4)
        
    print(f"Generated {num_triples} triples in {output_nt}")
    print(f"Generated {len(queries)} SPARQL queries in {output_queries}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--triples", type=int, default=100_000)
    parser.add_argument("--subjects", type=int, default=5_000)
    parser.add_argument("--predicates", type=int, default=50)
    parser.add_argument("--objects", type=int, default=30_000)
    parser.add_argument("--window_size", type=int, default=5000)
    parser.add_argument("--slide_size", type=int, default=1000)
    parser.add_argument("--output_nt", type=str, default="benchmark_dataset/synthetic_1M.nt")
    parser.add_argument("--output_queries", type=str, default="benchmark_dataset/synthetic_queries.json")
    args = parser.parse_args()
    
    generate_synthetic_data(
        args.triples, args.subjects, args.predicates, args.objects,
        args.window_size, args.slide_size,
        args.output_nt, args.output_queries
    )
