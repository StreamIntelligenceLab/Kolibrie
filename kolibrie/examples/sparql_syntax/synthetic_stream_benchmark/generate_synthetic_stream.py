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
    p_vars = get_distinct(predicates, 8)
    o_vars = get_distinct(objects, 4)
    s_vars = get_distinct(subjects, 3)
    p_q6 = get_distinct(predicates, 20)
    
    # 3. Formulate standard SPARQL SELECT queries
    # NOTE: we are not using RSPQL window functions as for some reason these are insanely slow, obscuring any performance difference due to indexing strategy
    # Q6 helper: matches a subject with 20 distinct properties
    q6_where = " ".join([f"?s {p_q6[i]} ?o{i} ." for i in range(20)])
    q6_select = " ".join([f"?o{i}" for i in range(20)])

    queries = {
        "Q1": f"SELECT ?s WHERE {{ ?s {p_vars[0]} {o_vars[0]} . }}",
        "Q2": f"SELECT ?s ?o2 ?o3 WHERE {{ ?s {p_vars[1]} ?o2 . ?s {p_vars[2]} ?o3 . }}",
        "Q3": f"SELECT * WHERE {{ {s_vars[0]} {p_vars[3]} {o_vars[1]} . }}",
        "Q4": f"SELECT ?v1 ?v5 WHERE {{ ?v1 {p_vars[4]} ?v2 . ?v2 {p_vars[5]} ?v3 . ?v4 {p_vars[6]} ?v3 . ?v4 {p_vars[7]} ?v5 . }}",
        "Q5": f"SELECT ?p WHERE {{ {s_vars[1]} ?p {o_vars[2]} . {s_vars[2]} ?p {o_vars[3]} . }}",
        "Q6": f"SELECT ?s {q6_select} WHERE {{ {q6_where} }}"
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