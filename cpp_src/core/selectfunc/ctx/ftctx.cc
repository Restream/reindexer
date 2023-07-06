#include "ftctx.h"
#include "estl/span.h"
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

size_t FtCtx::Size() const noexcept { return data_->proc_.size(); }

bool FtCtx::NeedArea() const noexcept { return data_->need_area_; }

bool FtCtx::PrepareAreas(const RHashMap<std::string, int> &fields, const std::string &name) {
	if (!fields.empty()) data_->is_composite_ = true;

	if (data_->is_composite_) {
		for (auto &field : fields) {
			data_->need_area_ =
				CheckFunction(field.first, {SelectFuncStruct::SelectFuncType::Snippet, SelectFuncStruct::SelectFuncType::SnippetN,
											SelectFuncStruct::SelectFuncType::Highlight});
			if (data_->need_area_) return true;
		}
	}
	data_->need_area_ = CheckFunction(name, {SelectFuncStruct::SelectFuncType::Snippet, SelectFuncStruct::SelectFuncType::SnippetN,
											 SelectFuncStruct::SelectFuncType::Highlight});
	return data_->need_area_;
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, AreaHolder &&holder) {
	data_->area_.emplace_back(std::move(holder));
	for (; begin != end; ++begin) {
		data_->proc_.push_back(proc);
		if (data_->need_area_) {
			data_->holders_.emplace(*begin, data_->area_.size() - 1);
		}
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc) {
	for (; begin != end; ++begin) {
		data_->proc_.push_back(proc);
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask, AreaHolder &&holder) {
	data_->area_.emplace_back(std::move(holder));
	for (; begin != end; ++begin) {
		assertrx(static_cast<size_t>(*begin) < mask.size());
		if (!mask[*begin]) continue;
		data_->proc_.push_back(proc);
		if (data_->need_area_) {
			data_->holders_.emplace(*begin, data_->area_.size() - 1);
		}
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask) {
	for (; begin != end; ++begin) {
		assertrx(static_cast<size_t>(*begin) < mask.size());
		if (!mask[*begin]) continue;
		data_->proc_.push_back(proc);
	}
}

template void FtCtx::Add<span<IdType>::iterator>(span<IdType>::iterator begin, span<IdType>::iterator end, int16_t proc,
												 AreaHolder &&holder);
template void FtCtx::Add<span<IdType>::iterator>(span<IdType>::iterator begin, span<IdType>::iterator end, int16_t proc,
												 const std::vector<bool> &, AreaHolder &&holder);
template void FtCtx::Add<span<IdType>::iterator>(span<IdType>::iterator begin, span<IdType>::iterator end, int16_t proc);
template void FtCtx::Add<span<IdType>::iterator>(span<IdType>::iterator begin, span<IdType>::iterator end, int16_t proc,
												 const std::vector<bool> &);

}  // namespace reindexer
