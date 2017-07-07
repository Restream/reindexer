#pragma once
#include <stdint.h>
#include <map>
#include <memory>
#include <set>
#include <unordered_set>
#include "core/idset.h"
#include "datastruct.h"

namespace search_engine {

using std::vector;
using std::unordered_set;
using std::shared_ptr;
using std::set;
using std::map;

class BaseHolder {
public:
	typedef shared_ptr<BaseHolder> Ptr;
	typedef pair<IdType, ProcType> ResData;
	struct ResComp {
		bool operator()(const ResData &lhs, const ResData &rhs) const {
			if (abs(static_cast<int>((lhs.second - rhs.second) * 100)) < 10) {
				return lhs.first < rhs.first;
			}
			return lhs.second > rhs.second;
		}
	};
	typedef set<ResData, ResComp> SearchType;

	typedef shared_ptr<SearchType> SearchTypePtr;

	void AddData(const DataStruct &data);
	void Reinit(const Ptr base_holder, const map<IdType, string> &changed);

	BaseHolder() {}
	BaseHolder(const BaseHolder &rhs);

	BaseHolder &operator=(const BaseHolder &rhs);

	void AddReserve(size_t size);

	Info *GetInfo(uint32_t hash);
	Info *GetInfo(const DataStruct &data);

	const DataStruct *GetData(uint32_t hash);
	const DataStruct *GetData(const DataStruct &data);

	~BaseHolder();

private:
	struct DataStructHash {
		inline size_t operator()(const DataStruct &ent) const { return ent.hash; }
	};
	struct DataStructComparator {
		bool operator()(const DataStruct &ent, const DataStruct &rhs) const { return ent.hash == rhs.hash; }
	};
	unordered_set<Info *> Infos;
	typedef unordered_set<DataStruct, DataStructHash, DataStructComparator> data_type;

	data_type all_data_;
};
}  // namespace search_engine
