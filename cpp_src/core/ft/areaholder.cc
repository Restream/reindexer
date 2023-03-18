#include "areaholder.h"
#include "sort/pdqsort.hpp"

namespace reindexer {
using std::vector;

void AreaHolder::Reserve(int size) { areas.reserve(size); }

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

bool AreaHolder::AddWord(int pos, int filed, int maxAreasInDoc) {
	Area thr_area{pos, pos + 1};
	return InsertArea(std::move(thr_area), filed, maxAreasInDoc);
}

bool AreaHolder::InsertArea(Area &&area, int field, int maxAreasInDoc) {
	commited_ = false;
	if (areas.size() <= size_t(field)) areas.resize(field + 1);
	auto &fieldAreas = areas[field];
	if (fieldAreas.empty() || !fieldAreas.back().Concat(area)) {
		if (maxAreasInDoc >= 0 && fieldAreas.size() >= unsigned(maxAreasInDoc)) return false;
		fieldAreas.emplace_back(std::move(area));
	}
	return true;
}

void AreaHolder::ReserveField(int size) { areas.resize(size); }

AreaVec *AreaHolder::GetAreas(int field) {
	if (!commited_) Commit();
	if (areas.size() <= size_t(field)) return nullptr;
	return &areas[field];
}
AreaVec *AreaHolder::GetAreasRaw(int field) {
	if (areas.size() <= size_t(field)) return nullptr;
	return &areas[field];
}

}  // namespace reindexer
