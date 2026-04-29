#pragma once

#include <memory>
#include <string>
#include <vector>
#include "tools/errors.h"

namespace reindexer {

constexpr size_t kHashMapStatsVersion = 1;

class AsyncStorage;
class Index;

struct [[nodiscard]] IndexHashMapStats {
	IndexHashMapStats() = default;
	IndexHashMapStats(const IndexHashMapStats&) = default;
	IndexHashMapStats(IndexHashMapStats&&) noexcept = default;
	IndexHashMapStats(std::string_view in, std::string_view st) : indexName(in) {
		stats.reserve(st.size());
		stats.insert(stats.end(), st.begin(), st.end());
	}

	std::string indexName;
	std::vector<char> stats;
};

Error LoadIndexesHashMapStats(AsyncStorage& storage_, const std::string& storageKey, std::vector<IndexHashMapStats>& indexesStats);

void UpdateNamespaceHashMapsStats(size_t numThreads, std::string_view namespaceName, const std::vector<std::unique_ptr<Index>>& indexes,
								  AsyncStorage& storage, const std::string& storageKey) noexcept;

}  // namespace reindexer