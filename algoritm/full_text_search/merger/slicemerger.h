#pragma once
#include <algorithm>
#include <set>
namespace search_engine {

using std::set;

class SliceMerger {
public:
	SliceMerger() : max_procent_(0), grown_position_cof_(0), step_(0), size_(0) {}

	void CheckPosition(size_t pos);

	struct PositionLine {
		size_t min;
		size_t max;
	};

	struct PositionLineComparator {
		using is_transparent = void;

		bool operator()(const PositionLine &a, const PositionLine &b) const { return a.max < b.min; }
		bool operator()(const PositionLine &a, const size_t b) const { return a.max + 1 < b; }
		bool operator()(const size_t a, const PositionLine &b) const { return a + 1 < b.min; }
		double result_procent_;
	};
	double max_procent_;
	double grown_position_cof_;
	double step_;

	size_t size_;
	typedef set<PositionLine, PositionLineComparator> LinesType;
	LinesType::iterator CheckNext(LinesType::iterator it, PositionLine &line);

	LinesType lines;
};
}  // namespace search_engine
