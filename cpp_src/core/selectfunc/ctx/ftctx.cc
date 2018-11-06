#include "ftctx.h"
namespace reindexer {
FtCtx::FtCtx() {
	data_ = std::make_shared<Data>();
	this->type = BaseFunctionCtx::kFtCtx;
}

int16_t FtCtx::Proc(size_t pos) {
	if (pos >= data_->proc_.size()) return 0;
	return data_->proc_[pos];
}
void FtCtx::Reserve(size_t size) { data_->proc_.reserve(size); }

size_t FtCtx::Size() { return data_->proc_.size(); }

bool FtCtx::NeedArea() { return data_->need_area_; }

bool FtCtx::PrepareAreas(fast_hash_map<string, int> &fields, const string &name) {
	if (!fields.empty()) data_->is_composite_ = true;

	if (data_->is_composite_) {
		for (auto &field : fields) {
			data_->need_area_ = CheckFunction(field.first, {SelectFuncStruct::SelectFuncStruct::kSelectFuncSnippet,
															SelectFuncStruct::SelectFuncStruct::kSelectFuncHighlight});
			if (data_->need_area_) return true;
		}
	}
	data_->need_area_ = CheckFunction(
		name, {SelectFuncStruct::SelectFuncStruct::kSelectFuncSnippet, SelectFuncStruct::SelectFuncStruct::kSelectFuncHighlight});
	return data_->need_area_;
}
void FtCtx::SetData(Data::Ptr data) { data_ = data; }
FtCtx::Data::Ptr FtCtx::GetData() { return data_; }

AreaHolder::Ptr FtCtx::Area(IdType id) {
	auto it = data_->holders_.find(id);
	if (it == data_->holders_.end() || !it->second) {
		return {};
	};
	return it->second;
}
size_t FtCtx::GetSize() { return data_->proc_.size(); }

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, AreaHolder::UniquePtr &&holder) {
	AreaHolder::Ptr ptr;
	if (data_->need_area_ && holder) {
		ptr = move(holder);
	}
	for (; begin != end; ++begin) {
		data_->proc_.push_back(proc);
		if (data_->need_area_) {
			data_->holders_.emplace(*begin, ptr);
		}
	}
}

template void FtCtx::Add<span<IdType>::iterator>(span<IdType>::iterator begin, span<IdType>::iterator end, int16_t proc,
												 AreaHolder::UniquePtr &&holder);
}  // namespace reindexer
