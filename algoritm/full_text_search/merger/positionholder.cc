#include "positionholder.h"
#include <tools/strings.h>

using namespace std;
namespace search_engine {

// Main merge Magic - don't change anything if you dont understand how it's worked
bool PositionHolder::AddPosition(void* info_ptr, IdContext& index_pos, PosType position) {
	auto npit = next_iter.find(info_ptr);
	proc_map::iterator nit;
	total_size_ = index_pos.tota_size_;

	if (npit == next_iter.end()) {
		next_iter[info_ptr] = index_pos.proc_.begin();
		nit = index_pos.proc_.begin();
	} else if (npit->second == index_pos.proc_.end()) {
		// search text bigger then index in tree all data founded
		return false;
	} else {
		nit = npit->second;
	}

	auto it = index_pos.proc_.find(position);
	// if same positon and not used
	if (it != index_pos.proc_.end() && positions.find(position) == positions.end()) {
		positions[position] = Context{position, it->second, true};
		index_merger_.CheckPosition(position);
		target_merger_.CheckPosition(position);
		while (nit != index_pos.proc_.end() && positions.find(nit->first) != positions.end()) {
			nit++;
		}
		next_iter[info_ptr] = nit;
		return true;
	}
	// find on another postion
	positions[nit->first] = Context{position, next_iter[info_ptr]->second, false};
	index_merger_.CheckPosition(nit->first);
	target_merger_.CheckPosition(position);
	nit++;
	while (nit != index_pos.proc_.end() && positions.find(nit->first) != positions.end()) {
		nit++;
	}
	next_iter[info_ptr] = nit;

	return true;
}

ProcType PositionHolder::CalcResult() {
	ProcType intersection = CalcIntersection();
	if (positions.size() != 0) {
		intersection = intersection / static_cast<float>(positions.size());
		intersection = intersection * CInterProc / static_cast<float>(100);
	} else {
		intersection = 0;
	}
	ProcType result_proc = 0;
	for (auto& position : positions) {
		result_proc += intersection * position.second.procet_;
		if (position.second.pare_) {
			result_proc += CPositionProc / static_cast<float>(100) * position.second.procet_;
		}
		result_proc += CMainProc / static_cast<float>(100) * position.second.procet_;
	}

	//  calc  reqset positions
	float reqest_proc = ReqestProc * (positions.size() / static_cast<float>(find_word_size_));

	result_proc = result_proc * static_cast<float>(100 - ReqestProc) / static_cast<float>(100);

	result_proc += reqest_proc;

	// for bigger reqest
	if (find_word_size_ > total_size_) {
		result_proc = static_cast<float>(result_proc) * (total_size_ / static_cast<float>(find_word_size_));
	}
	return result_proc;
}
ProcType PositionHolder::CalcIntersection() {
	auto sit = index_merger_.lines.begin();
	auto tit = target_merger_.lines.begin();
	ProcType result = 0;
	PosType slen;
	PosType tlen;
	for (; sit != index_merger_.lines.end() && tit != target_merger_.lines.end();) {
		slen = (sit->max - sit->min);
		tlen = (tit->max - tit->min);
		if (tlen > slen) {
			result += slen;
			++sit;
		} else {
			result += tlen;
			++tit;
		}
	}
	return result;
}
}  // namespace search_engine
