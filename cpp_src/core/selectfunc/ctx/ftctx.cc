#include "ftctx.h"

namespace reindexer {

bool FtCtx::PrepareAreas(const RHashMap<std::string, int> &fields, const std::string &name) {
	assertrx_dbg(!NeedArea());
	auto &data = *data_;
	if (!fields.empty()) {
		data.isComposite_ = true;
	}

	bool needArea = false;
	if (data.isComposite_) {
		for (auto &field : fields) {
			needArea = CheckFunction(field.first, {SelectFuncType::Snippet, SelectFuncType::SnippetN, SelectFuncType::Highlight});
			if (needArea) {
				break;
			}
		}
	}
	needArea = needArea || CheckFunction(name, {SelectFuncType::Snippet, SelectFuncType::SnippetN, SelectFuncType::Highlight});
	if (needArea) {
		data.InitHolders();
	}
	return needArea;
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, AreaHolder &&holder) {
	auto &data = *data_;
	data.area_.emplace_back(std::move(holder));
	for (; begin != end; ++begin) {
		data.proc_.emplace_back(proc);
		if (data.holders_.has_value()) {
			data.holders_->emplace(*begin, data_->area_.size() - 1);
		}
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc) {
	auto &data = *data_;
	for (; begin != end; ++begin) {
		data.proc_.emplace_back(proc);
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask, AreaHolder &&holder) {
	auto &data = *data_;
	data.area_.emplace_back(std::move(holder));
	for (; begin != end; ++begin) {
		assertrx(static_cast<size_t>(*begin) < mask.size());
		if (!mask[*begin]) continue;
		data.proc_.emplace_back(proc);
		if (data.holders_.has_value()) {
			data.holders_->emplace(*begin, data.area_.size() - 1);
		}
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask) {
	auto &data = *data_;
	for (; begin != end; ++begin) {
		assertrx(static_cast<size_t>(*begin) < mask.size());
		if (!mask[*begin]) continue;
		data.proc_.emplace_back(proc);
	}
}

template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc,
													   AreaHolder &&holder);
template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc,
													   const std::vector<bool> &, AreaHolder &&holder);
template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc);
template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc,
													   const std::vector<bool> &);

}  // namespace reindexer
