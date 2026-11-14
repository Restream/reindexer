#pragma once

#include "estl/h_vector.h"
#include "sort/pdqsort.hpp"
#include "tools/assertrx.h"

namespace reindexer {

class [[nodiscard]] AreasSorter {
protected:
	h_vector<h_vector<std::pair<unsigned, unsigned>, 10>, 5> areasByArrayIdxs_;

	template <class AreaType>
	void initAreas(size_t arraySize, const h_vector<AreaType, 2>& areas) {
		areasByArrayIdxs_.resize(0);
		areasByArrayIdxs_.resize(arraySize);

		for (size_t idx = 0; idx < areas.size(); idx++) {
			assertrx_dbg(areas[idx].arrayIdx < arraySize);
			areasByArrayIdxs_[areas[idx].arrayIdx].emplace_back(areas[idx].start, areas[idx].end);
		}
	}

	void sortAndJoinIntersectingAreas() {
		for (auto& areasArr : areasByArrayIdxs_) {
			boost::sort::pdqsort_branchless(areasArr.begin(), areasArr.end(), [](auto& a1, auto& a2) { return a1.first < a2.first; });

			size_t lastAreaIdx = 0;
			for (size_t i = 1; i < areasArr.size(); i++) {
				if (areasArr[i].first <= areasArr[lastAreaIdx].second) {
					areasArr[lastAreaIdx].second = std::max(areasArr[lastAreaIdx].second, areasArr[i].second);
				} else {
					lastAreaIdx++;
					areasArr[lastAreaIdx] = areasArr[i];
				}
			}

			if (!areasArr.empty()) {
				areasArr.resize(lastAreaIdx + 1);
			}
		}
	}
};

}  // namespace reindexer