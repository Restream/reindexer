#pragma once
#include <map>
#include <set>
#include "algoritm/full_text_search/dataholder/basebuildedholder.h"
#include "slicemerger.h"
namespace search_engine {

class PositionHolder {
public:
	explicit PositionHolder(size_t find_word_size) : total_size_(0), find_word_size_(find_word_size) {}

	bool AddPosition(void *info_ptr, IdContext &index_pos, PosType position);

	ProcType CalcResult();
	uint32_t total_size_;

	const ProcType CPositionProc = 30;
	const ProcType CInterProc = 20;
	const ProcType CMainProc = 50;
	const ProcType ReqestProc = 30;

private:
	ProcType CalcIntersection();

	struct Context {
		PosType positions;
		ProcType procet_;
		bool pare_;
	};

	size_t find_word_size_;

	map<void *, proc_map::iterator> next_iter;
	map<PosType, Context> positions;
	SliceMerger index_merger_;
	SliceMerger target_merger_;
};
}  // namespace search_engine
