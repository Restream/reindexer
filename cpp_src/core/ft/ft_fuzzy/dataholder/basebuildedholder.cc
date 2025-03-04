#include "basebuildedholder.h"

using namespace reindexer;

namespace search_engine {

DIt BaseHolder::GetData(const wchar_t* key) {
#ifndef DEBUG_FT
	return data_.find(std::wstring(key, cfg_.bufferSize));
#else
	return data_.find(reindexer::HashTreGram(key));
#endif
}
void BaseHolder::SetSize(uint32_t size, VDocIdType id, int field) { words_[id][field] += size; }
void BaseHolder::AddDada(const wchar_t* key, VDocIdType id, int pos, int field) {
#ifndef DEBUG_FT
	std::wstring wkey(key, cfg_.bufferSize);
	auto it = tmp_data_.find(wkey);
	if (it == tmp_data_.end()) {
		auto res = tmp_data_.try_emplace(wkey);
		it = res.first;
	}

	it->second.Add(id, pos, field);

#else
	uint32_t current_hash = reindexer::HashTreGram(key);
	auto it = tmp_data_.find(current_hash);
	if (it == tmp_data_.end()) {
		auto res = tmp_data_.emplace(current_hash, IdRelSet());
		it = res.first;
	}
	it->second.Add(id, pos, field);
#endif
}

void BaseHolder::Commit() {
	data_.reserve(tmp_data_.size());
	data_.clear();
	for (auto& val : tmp_data_) {
		data_.insert(std::make_pair(val.first, AdvacedPackedVec(std::move(val.second))));
	}

	ClearTemp();
}

}  // namespace search_engine
