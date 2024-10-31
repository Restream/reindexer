#include "ftctx.h"

namespace reindexer {

FtCtx::FtCtx(BaseFunctionCtx::CtxType t) : BaseFunctionCtx(t) {
	switch (t) {
		case BaseFunctionCtx::CtxType::kFtCtx:
			data_ = make_intrusive<FtCtxData>(t);
			break;
		case BaseFunctionCtx::CtxType::kFtArea:
			data_ = make_intrusive<FtCtxAreaData<Area>>(t);
			data_->holders.emplace();
			break;
		case BaseFunctionCtx::CtxType::kFtAreaDebug:
			data_ = make_intrusive<FtCtxAreaData<AreaDebug>>(t);
			data_->holders.emplace();
			break;
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc) {
	auto& data = *data_;
	for (; begin != end; ++begin) {
		data.proc.emplace_back(proc);
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool>& mask) {
	auto& data = *data_;
	for (; begin != end; ++begin) {
		assertrx(static_cast<size_t>(*begin) < mask.size());
		if (!mask[*begin]) {
			continue;
		}
		data.proc.emplace_back(proc);
	}
}

template <typename InputIterator, typename AreaType>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, AreasInDocument<AreaType>&& areas) {
	intrusive_ptr<FtCtxAreaData<AreaType>> dataArea = static_ctx_pointer_cast<FtCtxAreaData<AreaType>>(data_);
	assertrx_throw(dataArea);
	dataArea->area.emplace_back(std::move(areas));
	auto& data = *data_;
	if (data.holders.has_value()) {
		auto& holders = data.holders.value();
		for (; begin != end; ++begin) {
			data.proc.push_back(proc);
			holders.emplace(*begin, dataArea->area.size() - 1);
		}
	}
}

template <typename InputIterator, typename AreaType>
void FtCtx::Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool>& mask, AreasInDocument<AreaType>&& areas) {
	intrusive_ptr<FtCtxAreaData<AreaType>> dataArea = static_ctx_pointer_cast<FtCtxAreaData<AreaType>>(data_);
	assertrx_throw(dataArea);
	auto& data = *data_;
	dataArea->area.emplace_back(std::move(areas));
	if (data.holders.has_value()) {
		auto& holders = data.holders.value();
		for (; begin != end; ++begin) {
			assertrx_dbg(static_cast<size_t>(*begin) < mask.size());
			if (!mask[*begin]) {
				continue;
			}
			data.proc.push_back(proc);
			holders.emplace(*begin, dataArea->area.size() - 1);
		}
	}
}

template void FtCtx::Add<span<const IdType>::iterator, Area>(span<const IdType>::iterator begin, span<const IdType>::iterator end,
															 int16_t proc, AreasInDocument<Area>&& holder);
template void FtCtx::Add<span<const IdType>::iterator, Area>(span<const IdType>::iterator begin, span<const IdType>::iterator end,
															 int16_t proc, const std::vector<bool>&, AreasInDocument<Area>&& holder);

template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc,
													   AreasInDocument<AreaDebug>&& holder);
template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc,
													   const std::vector<bool>&, AreasInDocument<AreaDebug>&& holder);

template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc);
template void FtCtx::Add<span<const IdType>::iterator>(span<const IdType>::iterator begin, span<const IdType>::iterator end, int16_t proc,
													   const std::vector<bool>&);

}  // namespace reindexer
