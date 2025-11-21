#pragma once
#include <stdint.h>
#include <memory>
#include "core/ft/config/ftfuzzyconfig.h"
#include "core/ft/ft_fuzzy/advacedpackedvec.h"
#include "core/ft/idrelset.h"
#include "datastruct.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "tools/customhash.h"
namespace search_engine {

#ifndef DEBUG_FT
struct [[nodiscard]] DataStructHash {
	inline size_t operator()(const std::wstring& ent) const noexcept { return reindexer::Hash(ent); }
};
struct [[nodiscard]] DataStructEQ {
	inline bool operator()(const std::wstring& ent, const std::wstring& ent1) const noexcept { return ent == ent1; }
};
struct [[nodiscard]] DataStructLess {
	inline bool operator()(const std::wstring& ent, const std::wstring& ent1) const noexcept { return ent < ent1; }
};
template <typename T1>
using data_map = tsl::hopscotch_map<std::wstring, T1, DataStructHash, DataStructEQ>;
typedef reindexer::fast_hash_set<std::wstring, DataStructHash, DataStructEQ> data_set;

#else
struct [[nodiscard]] DataStructHash {
	inline size_t operator()(const uint32_t ent) const { return ent; }
};

template <typename T1>
using data_map = fast_hash_map<uint32_t, T1, DataStructHash>;
typedef reindexer::fast_hash_set<uint32_t, DataStructHash> data_set;
#endif
typedef data_map<reindexer::AdvacedPackedVec>::iterator DIt;
typedef reindexer::fast_hash_map<int, reindexer::fast_hash_map<int, uint32_t>> word_size_map;

class [[nodiscard]] BaseHolder {
public:
	typedef shared_ptr<BaseHolder> Ptr;
	BaseHolder() {}

	BaseHolder(BaseHolder& rhs) = delete;
	BaseHolder(BaseHolder&&) noexcept = delete;
	BaseHolder& operator=(const BaseHolder&) = delete;
	BaseHolder& operator=(BaseHolder&&) noexcept = delete;

	void ClearTemp() {
		data_map<reindexer::IdRelSet> tmp_data;
		tmp_data_.swap(tmp_data);
	}
	DIt end() { return data_.end(); }

	void Clear() {
		ClearTemp();
		data_.clear();
	}
	void SetConfig(const std::unique_ptr<reindexer::FtFuzzyConfig>& cfg) { cfg_ = *cfg.get(); }
	DIt GetData(const wchar_t* key);
	void SetSize(uint32_t size, reindexer::VDocIdType id, int filed);
	void AddData(const wchar_t* key, reindexer::VDocIdType id, unsigned pos, unsigned field, unsigned arrayIdx);
	void Commit();

public:
	data_map<reindexer::IdRelSet> tmp_data_;
	data_map<reindexer::AdvacedPackedVec> data_;
	word_size_map words_;
	reindexer::FtFuzzyConfig cfg_;
};

}  // namespace search_engine
