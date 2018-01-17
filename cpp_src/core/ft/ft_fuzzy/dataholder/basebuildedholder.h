#pragma once
#include <stdint.h>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/config/ftfuzzyconfig.h"
#include "core/ft/ft_fuzzy/advacedpackedvec.h"
#include "core/ft/idrelset.h"
#include "core/idset.h"
#include "datastruct.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "smardeque.h"
#include "tools/customhash.h"
namespace search_engine {

using std::unique_ptr;
using std::vector;
using std::shared_ptr;
using std::set;
using std::map;
using std::unordered_map;
using namespace reindexer;

#ifndef DEBUG_FT
struct DataStructHash {
	inline size_t operator()(const wstring &ent) const { return Hash(ent); }
};
struct DataStructEQ {
	inline bool operator()(const wstring &ent, const wstring &ent1) const { return ent == ent1; }
};
template <typename T1>
using data_map = fast_hash_map<wstring, T1, DataStructHash, DataStructEQ>;
typedef fast_hash_set<wstring, DataStructHash, DataStructEQ> data_set;

#else
struct DataStructHash {
	inline size_t operator()(const uint32_t ent) const { return ent; }
};

template <typename T1>
using data_map = fast_hash_map<uint32_t, T1, DataStructHash>;
typedef fast_hash_set<uint32_t, DataStructHash> data_set;
#endif
typedef data_map<AdvacedPackedVec>::iterator DIt;
typedef fast_hash_map<int, fast_hash_map<int, uint32_t>> word_size_map;

class BaseHolder {
public:
	typedef shared_ptr<BaseHolder> Ptr;
	BaseHolder() {}

	BaseHolder(BaseHolder &rhs) = delete;
	BaseHolder(BaseHolder &&) noexcept = delete;
	BaseHolder &operator=(const BaseHolder &) = delete;
	BaseHolder &operator=(BaseHolder &&) noexcept = delete;

	void ClearTemp() {
		data_map<IdRelSet> tmp_data;
		tmp_data_.swap(tmp_data);
	}
	DIt end() { return data_.end(); }

	void Clear() {
		ClearTemp();
		data_.clear();
	}
	void SetConfig(const unique_ptr<FtFuzzyConfig> &cfg) { cfg_ = *cfg.get(); }
	DIt GetData(const wchar_t *key);
	void SetSize(uint32_t size, VDocIdType id, int filed);
	void AddDada(const wchar_t *key, VDocIdType id, int pos, int field);
	void Commit();

public:
	data_map<IdRelSet> tmp_data_;
	data_map<AdvacedPackedVec> data_;
	word_size_map words_;
	FtFuzzyConfig cfg_;
};

}  // namespace search_engine
