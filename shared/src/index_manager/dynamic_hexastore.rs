//hexastore, but only builds indexes if they would be used
//uses heuristic to determine when index is valuable
//Step 1: dynamic hexastore is initialized with an array of access patterns represented as Triples
//(with bound and or unbound variables, eg: (s, p, ?o) could be determined with sp scan or ps scan)
//Step 2: dynamic hexastore chooses initial necessary indexes, eg if all triples can be solved with
//ps scan, then only having pso index is sufficient.
//Step 3: dynamic hexastore creates index pools per necessary index. And assigns an index pool to
//every access pattern (eg p and s bound, only p bound, ...)
//(An index pool is a pool of indexes with the only rule being that if you join all indexes in a
//pool, every triple in the current window must be present. We use this to switch between used
//indexes dynamically later on.
//Step 4: now we ingest data, when building the index from data or adding a data entry, we want to
//add this entry to every pool exactly once. This means every pool has at most 1 active index to
//which we insert. At first this will just be the only index present in the pool (like pso in our
//example). 
//Step 5: every set time interval we also check a heuristic to determine if we should switch the
//index used in a pool. To do this, we use the number of unique subjects, predicates and objects
//from the existing stats implementation which has the cardinalities of each of these.
//The heuristic works as follows: We determine a cost as an overhead value (which is static and set
//at build time) + the amount of hashset/map lookups. You can guess the amount of hashset/map
//lookups as follows: if you scan for example an spo index for a certain predicate, you will do #s
//lookups to find the s number of po maps. If you scan for a bound subject (predicate and object
//not bound) on this same index, you will only have to do 1 lookup to find the po map that has all
//entries. Having only a bound object, will necessitate a full table scan: first find every po map
//(#s lookups), then find every o set, (#p lookups or #s*#p lookups in total), then for every set
//check if the bound object is present (#s*#p*1 total lookups) We then also guess the cost of
//maintaining an index as follows: MAP_OVERHEAD * (1 + #s) + SET_OVERHEAD * (#s * #p) + SPACE_OVERHEAD * (#s * #p * #o)
//Then we find the set of indexes to maintain that minimizes cost = sum of cost heurists for each
//access pattern passed during initialisation + sum of cost heuristics for every index in that set
//There will already be existing indexes, since we at first initialize these based on which access
//patterns we have exclusively (not taking into account data cardinalities, since we dont know that
//yet at initialisation). To successfully transition from the old indexes without having to do an
//expensive complete copy operation between the indexes, we do the following:
//1) Create a new pool for every index in the new index set
//2) For every access pattern, use the heuristics to find the best pool to assign it to.
//3) For every new pool, find the old pool that creates the lowest total cost across all access
//   patterns assigned to the new pool.
//4) Any unassigned old pools are deleted.
//5) Of the old pool(s) that were assigned to a new pool, check if the desired index of the new
//   pool is present in the old pool (doesnt matter if active or not).
//   -  If so: we dont have to create any new indexes for that pool, and the old pool just becomes
//   the new pool, only (possibly) changing the active index.
//   -  If not: create a new index of the type that we want for the new pool, set that to the
//   active index, and maintain the indexes of the old pool in the new pool to maintain the data
//6) In the data deletion function, that deletes a triple from the triplestore, (attempt to) delete
//   the triple from every index in every pool. Also check for every index if the index is empty
//   after this. If so, and it is not the active index, delete that index from the pool.
// By adding to only one index per pool and deleting unilaterally, eventually, unless we keep
// switching active indexes, each pool will converge to have one index.
