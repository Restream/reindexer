#include "query_joins_table.h"
#include "core/query/query.h"

namespace reindexer::joins {

QueryJoinsTable::QueryJoinsTable(const Query& q) { buildJoinsOffsetTable(q); }

/**
 * @brief Builds a lookup table mapping (parentNsId, joinIndex) → child join's namespace ID.
 *
 * Assigns unique namespace IDs to every JOIN in the query tree, following a
 * three‑zone partitioning scheme:
 *
 *   NSID 0          → the main query
 *   NSIDs 1..M      → the M merge queries (peers of the main query)
 *   NSIDs M+1..     → all JOIN queries, assigned via DFS traversal
 *
 * This design enables uniform lookup: GetJoinedNsId(parentNsId, joinIndex)
 * works identically regardless of whether the parent is the main query,
 * a merge query, or another JOIN.
 *
 * ── Example (M = 2 merge queries) ──────────────────────────────────
 *
 *   NSID=0: Main Query
 *     ├── Merged Query A ──────────────────► NSID=1
 *     ├── Merged Query B ──────────────────► NSID=2
 *     │
 *     ├── JOIN #0 (main) ──────────────────► NSID=3
 *     │     └── JOIN #0 (nested) ──────────► NSID=4
 *     │
 *     └── JOIN #1 (main) ──────────────────► NSID=5
 *
 *   NSID=1: Merged Query A
 *     └── JOIN #0 ─────────────────────────► NSID=6
 *
 *   NSID=2: Merged Query B
 *     └── JOIN #0 ─────────────────────────► NSID=7
 *
 *   ── joinQueriesNsids_ contents ──
 *
 *     {0, 0} → 3    main's 1st join
 *     {0, 1} → 5    main's 2nd join
 *     {3, 0} → 4    nested join of main's 1st join
 *     {1, 0} → 6    merged A's join
 *     {2, 0} → 7    merged B's join
 *
 *   ── GetJoinedNsId queries ──
 *
 *     GetJoinedNsId(0, 0)  → 3
 *     GetJoinedNsId(0, 1)  → 5
 *     GetJoinedNsId(3, 0)  → 4
 *     GetJoinedNsId(1, 0)  → 6
 *     GetJoinedNsId(2, 0)  → 7
 *
 * ── Algorithm ─────────────────────────────────────────────────────
 *
 * 1.  Reserve NSIDs 1..M for the merge queries themselves.
 *     Their JOINs will be assigned IDs beyond M during step 2b.
 *
 * 2a. Process the main query: DFS over its JOIN tree, assigning a new
 *     NSID to every JOIN encountered and recording the mapping.
 *
 * 2b. Process each merge query (in order): same DFS, but starting from
 *     the merge query's own pre‑allocated NSID as the parent.
 *
 * Because NSIDs are assigned sequentially during a DFS traversal,
 * the children of any JOIN immediately follow their parent in the ID
 * space (as shown above: main's JOIN #0 is NSID 3, its nested child
 * is NSID 4).
 */
void QueryJoinsTable::buildJoinsOffsetTable(const Query& q) {
	joinQueriesNsids_.clear();
	joinedQueriesTotal_ = 0;

	uint16_t nextNsId = 1 + static_cast<int>(q.GetMergeQueries().size());
	processQuery(q, 0, nextNsId);

	uint16_t mergedNsId = 1;
	for (const auto& mergedQuery : q.GetMergeQueries()) {
		processQuery(mergedQuery, mergedNsId++, nextNsId);
	}
}

void QueryJoinsTable::setJoinNsId(uint16_t parentNsId, size_t joinIndex, uint16_t nsId) {
	const size_t parentNsIdx{static_cast<size_t>(parentNsId)};
	if (parentNsIdx >= joinQueriesNsids_.size()) {
		joinQueriesNsids_.resize(parentNsIdx + 1);
	}
	auto& parentJoinNsids{joinQueriesNsids_[parentNsIdx]};
	if (joinIndex == parentJoinNsids.size()) {
		parentJoinNsids.emplace_back(nsId);
	} else {
		assertrx(joinIndex < parentJoinNsids.size());
		parentJoinNsids[joinIndex] = nsId;
	}
}

void QueryJoinsTable::processQuery(const Query& query, uint16_t parentNsId, uint16_t& nextNsId) {
	for (size_t i = 0; i < query.GetJoinQueries().size(); ++i) {
		++joinedQueriesTotal_;

		const Query& joinQuery{query.GetJoinQueries()[i]};
		const uint16_t childNsId{nextNsId++};
		setJoinNsId(parentNsId, i, childNsId);

		if (!joinQuery.GetJoinQueries().empty()) {
			processQuery(joinQuery, childNsId, nextNsId);
		}
	}
}

}  // namespace reindexer::joins
