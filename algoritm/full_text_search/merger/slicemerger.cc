#include "slicemerger.h"
#include <cassert>
namespace search_engine {

// SliceMerger::SliceMerger() {
//	/* : max_procent_(max_procent), size_(size), grown_position_cof_(pos_grown_base / 100) {
//	 step_ = (100 - pos_grown_base) / (size_ - 1) / 100;
//}

SliceMerger::LinesType::iterator SliceMerger::CheckNext(LinesType::iterator it, PositionLine& line) {
	LinesType::iterator cur = it++;

	if (it == lines.end()) {
		return cur;
	}

	if (it->min == line.max || it->min == line.max + 1) {
		line.max = it->max;
		cur = lines.erase(it);
		return --cur;
	}

	return cur;
}

void SliceMerger::CheckPosition(size_t pos) {
	PositionLine line{pos - 1, pos + 1};
	auto it = lines.find(line);

	if (it != lines.end()) {
		line = *it;

		if (it->max < pos) {
			line.max = pos;

		} else if (it->min > pos) {
			line.min = pos;

		} else {
			// assert(false);
		}

		it = CheckNext(it, line);
		lines.erase(it);
		lines.insert(line);
		return;
	}

	lines.insert({pos, pos});
	return;
}

}  // namespace search_engine
