#pragma once
#include <list>
#include <vector>
#include "algoritm/full_text_search/dataholder/basebuildedholder.h"
#include "positionholder.h"
namespace search_engine {
using std::vector;

class PositionMerger {
public:
	explicit PositionMerger(size_t find_word_size) : find_word_size_(find_word_size) {}

	void AddPosition(Info* info, uint16_t curent_pos);
	BaseHolder::SearchTypePtr CalcResult();

private:
	map<IdType, PositionHolder> context_;
	size_t find_word_size_;
	const uint32_t word_size = 3;
};
}  // namespace search_engine
