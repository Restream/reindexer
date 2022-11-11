#include "areaholder.h"
#include "sort/pdqsort.hpp"

namespace reindexer {
using std::vector;

void AreaHolder::Reserve(int size) { areas.reserve(size); }

void AreaHolder::AddTreeGramm(int pos, int filed, int maxAreasInDoc) {
	Area thr_area(0, 0);
	if (pos < space_size_) {
		thr_area.start_ = 0;
		thr_area.end_ = pos + (buffer_size_ - 1) - space_size_;
	} else if (pos > (total_size_ - space_size_)) {
		thr_area.start_ = pos - space_size_;
		thr_area.end_ = buffer_size_ - 1 + total_size_ - (2 * space_size_);
	} else {
		thr_area.start_ = pos - space_size_;
		thr_area.end_ = pos - space_size_ + (buffer_size_ - 1);
	}

	insertArea(std::move(thr_area), filed, maxAreasInDoc); // NOLINT(performance-move-const-arg)
}

void AreaHolder::Commit() {
	commited_ = true;
	for (auto &area : areas) {
		boost::sort::pdqsort(area.begin(), area.end(), [](const Area &rhs, const Area &lhs) { return rhs.start_ < lhs.start_; });
		if (!area.empty()) {
			for (auto vit = area.begin() + 1; vit != area.end(); ++vit) {
				auto prev = vit - 1;
				if (vit->Concat(*prev)) {
					vit = area.erase(prev);
				}
			}
		}
	}
}

bool AreaHolder::AddWord(int start_pos, int /*size*/, int filed, int maxAreasInDoc) {
	Area thr_area{start_pos, start_pos + 1};
	return insertArea(std::move(thr_area), filed, maxAreasInDoc); // NOLINT(performance-move-const-arg)
}

bool AreaHolder::insertArea(Area &&area, int field, int maxAreasInDoc) {
	commited_ = false;
	if (areas.size() <= size_t(field)) areas.resize(field + 1);
	if (areas[field].empty() || !areas[field].back().Concat(area)) {
		if (maxAreasInDoc >= 0 && areas[field].size() >= unsigned(maxAreasInDoc)) return false;
		areas[field].emplace_back(std::move(area)); // NOLINT(performance-move-const-arg)
	}
	return true;
}

void AreaHolder::ReserveField(int size) { areas.resize(size); }

AreaVec *AreaHolder::GetAreas(int field) {
	if (!commited_) Commit();
	if (areas.size() <= size_t(field)) return nullptr;
	return &areas[field];
}
}  // namespace reindexer
