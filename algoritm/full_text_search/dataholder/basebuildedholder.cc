#include "basebuildedholder.h"
#include <math.h>
#include <algorithm>
#include <iterator>
#include <memory>
#include <unordered_map>

namespace search_engine {

using std::make_shared;
using std::pair;
using std::set_intersection;
using std::vector;
using std::map;
using std::unordered_map;

void BaseHolder::AddData(const DataStruct& data) {
	all_data_.insert(data);
	Infos.insert(data.info);
}

Info* BaseHolder::GetInfo(const DataStruct& data) {
	auto it = all_data_.find(data);

	if (it != all_data_.end()) {
		return it->info;
	}

	return nullptr;
}

Info* BaseHolder::GetInfo(uint32_t hash) {
	// Todo Fix here to find hash simple
	DataStruct temp;
	temp.hash = hash;
	return GetInfo(temp);
}

const DataStruct* BaseHolder::GetData(const DataStruct& data) {
	auto it = all_data_.find(data);

	if (it != all_data_.end()) {
		return &(*it);
	}

	return nullptr;
}

const DataStruct* BaseHolder::GetData(uint32_t hash) {
	// Todo Fix here to find hash simple
	DataStruct temp;
	temp.hash = hash;
	return GetData(temp);
}

BaseHolder::~BaseHolder() {
	for (auto info_ptr : Infos) {
		delete info_ptr;
	}
}

typedef pair<IdType, IdContext> pair_i;
typedef pair<IdType, string> pair_t;

struct comp {
	using is_transparent = void;
	bool operator()(const pair_i& p1, const pair_t& p2) const { return p1.first < p2.first; }
	bool operator()(const pair_t& p1, const pair_i& p2) const { return p1.first < p2.first; }
};

void BaseHolder::Reinit(const Ptr base_holder, const map<IdType, string>& changed) {
	vector<pair<IdType, string>> v_intersection;
	comp comparator;
	DataStruct new_data;
	unordered_map<Info*, Info*> old_to_new;
	for (auto& data : base_holder->all_data_) {
		v_intersection.clear();
		Info* info;
		auto it = old_to_new.find(data.info);
		if (it == old_to_new.end()) {
			info = new Info(*data.info);
			old_to_new[data.info] = info;
		} else {
			info = it->second;
		}
		info->ids_ = data.info->ids_;
		new_data.hash = data.hash;
		new_data.info = info;
		set_intersection(changed.begin(), changed.end(), info->ids_.begin(), info->ids_.end(), back_inserter(v_intersection), comparator);
		for (auto val : v_intersection) {
			info->ids_.erase(val.first);
		}
		if (info->ids_.size() > 0) {
			AddData(new_data);
		} else {
			delete info;
		}
	}
}
void BaseHolder::AddReserve(size_t size) {
	all_data_.reserve(all_data_.bucket_count() + size);
	Infos.reserve(Infos.bucket_count() + size * 3);
}

BaseHolder::BaseHolder(const BaseHolder& rhs) {
	DataStruct new_data;
	unordered_map<Info*, Info*> old_to_new;
	for (auto& data : rhs.all_data_) {
		Info* info;
		auto it = old_to_new.find(data.info);
		if (it == old_to_new.end()) {
			info = new Info(*data.info);
			old_to_new[data.info] = info;
		} else {
			info = it->second;
		}
		new_data.hash = data.hash;
		new_data.info = info;
		AddData(new_data);
	}
}

BaseHolder& BaseHolder::operator=(const BaseHolder& rhs) {
	if (this == &rhs) return *this;

	DataStruct new_data;
	unordered_map<Info*, Info*> old_to_new;
	for (auto& data : rhs.all_data_) {
		Info* info;
		auto it = old_to_new.find(data.info);
		if (it == old_to_new.end()) {
			info = new Info(*data.info);
			old_to_new[data.info] = info;
		} else {
			info = it->second;
		}
		new_data.hash = data.hash;
		new_data.info = info;
		AddData(new_data);
	}
	return *this;
}
}  // namespace search_engine
