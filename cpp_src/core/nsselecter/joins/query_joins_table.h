#pragma once

#include <cstddef>
#include <cstdint>
#include "estl/defines.h"
#include "estl/h_vector.h"
#include "tools/assertrx.h"

namespace reindexer {

class ConstQueryImpl;

namespace joins {

/**
 * @brief Resolves namespace IDs for JOIN queries in a query tree.
 */
class [[nodiscard]] QueryJoinsTable {
public:
	using JoinSize = uint16_t;

	/**
	 * Constructs a JOIN lookup table for the given query tree.
	 * @param q - Query to analyze
	 */
	explicit QueryJoinsTable(ConstQueryImpl q);

	/**
	 * Returns the namespace ID of a JOIN query.
	 * @param parentNsId - NsId of the parent query (0 = main query, 1..N = merged queries, >N = nested JOIN).
	 * @param joinIndex - Index of the JOIN in parent's join list (0-based).
	 * @return NsId of the target JOIN query.
	 */
	RX_ALWAYS_INLINE int GetJoinedNsId(int parentNsId, size_t joinIndex) const {
		assertrx(parentNsId >= 0);
		const size_t parentNsIdIdx{static_cast<size_t>(parentNsId)};
		assertrx(parentNsIdIdx < joinQueriesNsids_.size());
		const auto& parentJoinNsids{joinQueriesNsids_[parentNsIdIdx]};
		assertrx(joinIndex < parentJoinNsids.size());
		return static_cast<int>(parentJoinNsids[joinIndex]);
	}

	/**
	 * Tries to obtain the namespace ID of a JOIN query.
	 * @param parentNsId - NsId of the parent query (0 = main query, 1..N = merged queries, >N = nested JOIN).
	 * @param joinIndex - Index of the JOIN in parent's join list (0-based).
	 * @param joinedNsId - NsId of the target JOIN query.
	 * @return true, if NsId was successfully obtained (only when joined data for [parentNsId, joinIndex] exists).
	 */
	RX_ALWAYS_INLINE bool TryGetJoinedNsId(int parentNsId, size_t joinIndex, int& joinedNsId) const noexcept {
		if (parentNsId < 0) {
			return false;
		}
		const size_t parentNsIdIdx{static_cast<size_t>(parentNsId)};
		if (parentNsIdIdx >= joinQueriesNsids_.size()) {
			return false;
		}
		const auto& parentJoinNsids{joinQueriesNsids_[parentNsIdIdx]};
		if (joinIndex < parentJoinNsids.size()) {
			joinedNsId = static_cast<int>(parentJoinNsids[joinIndex]);
			return true;
		}
		return false;
	}

	/**
	 * Returns total number of JOIN queries in the query tree.
	 * @return Total JOIN count.
	 */
	JoinSize GetJoinQueriesCount() const noexcept { return joinedQueriesTotal_; }

private:
	/**
	 * Builds the lookup table from the query tree.
	 * @param q - Query to process.
	 */
	void buildJoinsOffsetTable(ConstQueryImpl q);

	/**
	 * Process query and all it's nested queries with DFS algorithm.
	 * @param query - Query.
	 * @param parentNsId - parent's NsId.
	 * @param nextNsId - value of the next consequent NsId.
	 */
	void processQuery(ConstQueryImpl query, uint16_t parentNsId, uint16_t& nextNsId);

	/**
	 * Sets NsId for a parent's JOIN, growing the table lazily while building the query tree.
	 * @param parentNsId - parent's NsId.
	 * @param joinIndex - Index of the JOIN in the parent's join list.
	 * @param nsId - NsId of the child JOIN query.
	 */
	void setJoinNsId(uint16_t parentNsId, size_t joinIndex, uint16_t nsId);

private:
	/// Total number of JOIN queries in the entire query tree
	JoinSize joinedQueriesTotal_{0};
	/// parentNsId → joinIndex → child JOIN NsId
	h_vector<h_vector<uint16_t, 4>, 4> joinQueriesNsids_;
};

}  // namespace joins
}  // namespace reindexer
