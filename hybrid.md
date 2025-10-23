
<!-- toc -->

- [Queries](#queries)
- [Reranking](#reranking)
  * [Rrf](#reciprocal-rank-fusion)
  * [Linear](#linear-reranking)
- [Merging queries results](#merging-queries-results)

<!-- tocstop -->

# Queries

Reindexer supports hybrid search by full text and knn in one query. Fulltext and KNN conditions may be combined with `AND` or `OR` logical operators:
```SQL
-- Requires both fulltext and KNN matches in the result documents
SELECT * FROM ns WHERE ft_idx = 'search_str' AND KNN(vec_idx, [2.4, 3.5, ...], k=100)
-- Requires fulltext, KNN or both matches in the result documents
SELECT * FROM ns WHERE ft_idx = 'search_str' OR KNN(vec_idx, [2.4, 3.5, ...], k=100)
```
In hybrid search, there must be exactly one full text condition and exactly one knn condition.\
In this case, full text and knn conditions must be inside the same bracket or outside the brackets:
```SQL
SELECT * FROM ns WHERE (ft_idx = 'search_str' AND id > 50 AND KNN(vec_idx, [2.4, 3.5, ...], k=100)) AND id < 10000
SELECT * FROM ns WHERE (ft_idx = 'search_str' OR KNN(vec_idx, [2.4, 3.5, ...], k=100) AND id > 50 ) AND id < 10000
```
Placing full text and knn conditions in different brackets is prohibited:
```SQL
SELECT * FROM ns WHERE (ft_idx = 'search_str' AND id > 100) OR (KNN(vec_idx, [2.4, 3.5, ...], k=100) AND id < 100)
```
Possible use auto-embedding query with hybrid search (Need to configure [Query embedder](float_vector.md#embedding-configuration) for `vec_idx`):
```SQL
SELECT * FROM ns WHERE (ft_idx = 'search_str' AND id > 100) OR (KNN(vec_idx, '*SOME TEXT FOR SEARCH VECTOR GENERATION*', k=100) AND id < 100)
```
In this case, if the Embedding service does not respond, the KNN request will be ignored (fallback logic - skip unapproachable, the resulting error is logged with a warning level)

# Reranking

To recalculate the total rank based on the ranks of full text and knn conditions, the recalculation expression must be specified in the statement `ORDER BY`.
By default `RRF()` is used.

## Reciprocal rank fusion

Reciprocal rank fusion `RRF` reranking expression may be specified as follows:
```SQL
SELECT * FROM ns WHERE ft_idx = 'search_str' AND KNN(vec_idx, [2.4, 3.5, ...], k=100)
   ORDER BY 'RRF(rank_const=120)'
```
`rank_const` is optional, default value is 60, and minimum value is 1.\
In this case the rank is calculated using the following formula:
```
rank = 1.0 / (rank_const + pos_ft) + 1.0 / (rank_const + pos_knn)
```
where `pos_ft` and `pos_knn` are documents' positions in the results of the queries
```SQL
SELECT * FROM ns WHERE ft_idx = 'search_str'
```
and
```SQL
SELECT * FROM ns WHERE KNN(vec_idx, [2.4, 3.5, ...], k=100)
```
respectively.

## Linear reranking

Another supported reranking expression is linear function based on the full text and knn ranks.
```SQL
SELECT * FROM ns WHERE ft_idx = 'search_str' OR KNN(vec_idx, [2.4, 3.5, ...], k=100)
   ORDER BY '30 * rank(ft_idx) + 50 * rank(vec_idx, 100.0) + 100'
```
where `rank(index_name, default_rank)` is rank of full text or knn condition, `default_rank` is optional default rank in case of absence of result for a certain condition, default value is 0.0.
General form of the linear reranking expression is
```
A * rank(ft_idx, r1) + B * rank(vec_idx, r2) + C
```
where `A`, `B`, `C`, `r1` and `r2` are numeric values.

Expected values range for the `rank(ft_idx)` expression is [1, 255], where 1 is the least possible fulltext rank and 255 is the highest one.
The values range for the `rank(vec_idx)` expressions depends on the specified similarity metric. For example, in case of the `cosine`-metric it would be [-1.0, 1.0], where -1.0 corresponds to the least relevant vectors and 1.0 corresponds to the most relevant vectors.

# Merging queries results
It is possible to merge multiple queries results and sort final result by ranks calculated by their own rerankers.
```SQL
SELECT * FROM ns WHERE ft_idx = 'search_str' OR KNN(vec_idx, [2.4, 3.5, ...], k=100)
   ORDER BY '30 * rank(ft_idx) + 50 * rank(vec_idx, 100.0) + 100'
	 MERGE(
		 SELECT * FROM ns2 WHERE ft_idx = 'search_str' AND KNN(vec_idx, [2.4, 3.5, ...], k=100)
		    ORDER BY 'RRF(rank_const=120)'
	 )
	 MERGE(
		 SELECT * FROM ns3 WHERE ft_idx = 'search_str' OR KNN(vec_idx, [2.4, 3.5, ...], k=100)
	 )
```
