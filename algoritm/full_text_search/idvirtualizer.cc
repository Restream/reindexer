#include "idvirtualizer.h"
#include <algorithm>

namespace search_engine {

using std::make_shared;

IdVirtualizer::IdVirtualizer(const IdVirtualizer &rhs) : next_free_(0) {
	for (auto &dt : rhs.data_) {
		Context *ctx = new Context;
		ctx->vir_id_ = dt.second->vir_id_;
		ctx->real_ids_ = make_shared<IdSet>();
		data_[dt.first] = ctx;
		contexts_.insert(ctx);
		changed_real_[ctx->vir_id_] = ctx;
		ctx->real_ids_->insert(ctx->real_ids_->begin(), dt.second->real_ids_->begin(), dt.second->real_ids_->end());
	}
	changed_virtual_ = rhs.changed_virtual_;
	next_free_ = rhs.next_free_;
	min_val_ = rhs.min_val_;
	max_val_ = rhs.max_val_;
}

IdVirtualizer &IdVirtualizer::operator=(const IdVirtualizer &rhs) {
	if (this == &rhs) return *this;

	for (auto &dt : rhs.data_) {
		Context *ctx = new Context;
		ctx->vir_id_ = dt.second->vir_id_;
		ctx->real_ids_ = make_shared<IdSet>();
		data_[dt.first] = ctx;
		contexts_.insert(ctx);
		changed_real_[ctx->vir_id_] = ctx;
		ctx->real_ids_->insert(ctx->real_ids_->begin(), dt.second->real_ids_->begin(), dt.second->real_ids_->end());
	}
	changed_virtual_ = rhs.changed_virtual_;
	next_free_ = rhs.next_free_;
	min_val_ = rhs.min_val_;
	max_val_ = rhs.max_val_;

	return *this;
}

IdVirtualizer::~IdVirtualizer() {
	for (auto ctx : contexts_) {
		delete ctx;
	}
}

void IdVirtualizer::AddData(const string &key, IdType id) {
	if (id > max_val_) {
		max_val_ = id;
	}
	if (id < min_val_) {
		min_val_ = id;
	}
	auto it = data_.find(key);
	if (it == data_.end()) {
		auto ctx = CreateContext(key, id);
		changed_real_[ctx->vir_id_] = ctx;
		return;
	}
	changed_real_[it->second->vir_id_] = it->second;
	it->second->real_ids_->push_back(id);
}
void IdVirtualizer::RemoveData(const string &key, IdType id) {
	auto it = data_.find(key);
	if (it == data_.end()) {
		return;
	}
	changed_real_[it->second->vir_id_] = it->second;
	it->second->real_ids_->erase(id);
	if (it->second->real_ids_->size() == 0) {
		DeleteContext(key, id);
	}
}
void IdVirtualizer::DeleteContext(const string &key, IdType id) {
	auto nit = changed_virtual_.find(id);
	if (nit == changed_virtual_.end()) {
		changed_virtual_[id] = "";
	} else if (nit->second != "") {
		// delete data added before next commit
		changed_virtual_.erase(nit);
	}
	auto it = data_.find(key);
	contexts_.erase(it->second);
	delete it->second;
	data_.erase(it);
}
string IdVirtualizer::GetDataByVirtualId(IdType id) const {
	Context ctx{nullptr, id};
	auto it = contexts_.find(&ctx);
	if (it == contexts_.end()) {
		return "";
	}
	for (auto &data : data_) {
		if (data.second == *it) {
			return data.first;
		}
	}
	return "";
}

IdVirtualizer::Context *IdVirtualizer::CreateContext(const string &key, IdType id) {
	Context *context = new Context;
	context->real_ids_ = make_shared<IdSet>();
	context->real_ids_->push_back(id);
	context->vir_id_ = next_free_;
	data_[key] = context;
	contexts_.insert(context);
	changed_virtual_[next_free_] = key;
	next_free_++;
	return context;
}

IdSet::Ptr IdVirtualizer::GetByVirtualId(IdType id) const {
	Context ctx{nullptr, id};
	auto it = contexts_.find(&ctx);
	if (it == contexts_.end()) {
		return make_shared<IdSet>();
	}
	return (*it)->real_ids_;
}

IdSet::Ptr IdVirtualizer::GetByVirtualId(BaseHolder::SearchTypePtr ids) const {
	auto mergedIds = make_shared<IdSet>();
	mergedIds->reserve(ids->size() * 2);
	std::vector<bool> kl(max_val_ - min_val_, false);

	for (auto it = ids->begin(); it != ids->end(); ++it) {
		auto id_set = GetByVirtualId(it->first);
		if (id_set) {
			for (auto val : *id_set) {
				if (!kl[val - min_val_]) {
					kl[val - min_val_] = true;
					mergedIds->push_back(val);
				}
			}
		}
	}
	return mergedIds;
}

map<IdType, string> IdVirtualizer::GetDataForBuild() {
	map<IdType, string> ret;
	ret.swap(changed_virtual_);
	return ret;
}

void IdVirtualizer::Commit(const CommitContext &ctx) {
	for (auto &data : changed_real_) {
		data.second->real_ids_->commit(ctx);
	}
}
}  // namespace search_engine
