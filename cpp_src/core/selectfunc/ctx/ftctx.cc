#include "ftctx.h"
#include <span>

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
		case BaseFunctionCtx::CtxType::kKnnCtx:
		case BaseFunctionCtx::CtxType::kNotSet:
			throw_as_assert;
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, RankT rank) {
	auto& data = *data_;
	for (; begin != end; ++begin) {
		data.rank.emplace_back(rank);
	}
}

template <typename InputIterator>
void FtCtx::Add(InputIterator begin, InputIterator end, RankT rank, const std::vector<bool>& mask) {
	auto& data = *data_;
	for (; begin != end; ++begin) {
		assertrx(static_cast<size_t>(*begin) < mask.size());
		if (!mask[*begin]) {
			continue;
		}
		data.rank.emplace_back(rank);
	}
}

template <typename InputIterator, typename AreaType>
void FtCtx::Add(InputIterator begin, InputIterator end, RankT rank, AreasInDocument<AreaType>&& areas) {
	intrusive_ptr<FtCtxAreaData<AreaType>> dataArea = static_ctx_pointer_cast<FtCtxAreaData<AreaType>>(data_);
	assertrx_throw(dataArea);
	dataArea->area.emplace_back(std::move(areas));
	auto& data = *data_;
	if (data.holders.has_value()) {
		auto& holders = data.holders.value();
		for (; begin != end; ++begin) {
			data.rank.push_back(rank);
			holders.emplace(*begin, dataArea->area.size() - 1);
		}
	}
}

template <typename InputIterator, typename AreaType>
void FtCtx::Add(InputIterator begin, InputIterator end, RankT rank, const std::vector<bool>& mask, AreasInDocument<AreaType>&& areas) {
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
			data.rank.push_back(rank);
			holders.emplace(*begin, dataArea->area.size() - 1);
		}
	}
}

template void FtCtx::Add<std::span<const IdType>::iterator, Area>(std::span<const IdType>::iterator begin, std::span<const IdType>::iterator end, RankT,
															 AreasInDocument<Area>&& holder);
template void FtCtx::Add<std::span<const IdType>::iterator, Area>(std::span<const IdType>::iterator begin, std::span<const IdType>::iterator end, RankT,
															 const std::vector<bool>&, AreasInDocument<Area>&& holder);

template void FtCtx::Add<std::span<const IdType>::iterator>(std::span<const IdType>::iterator begin, std::span<const IdType>::iterator end, RankT,
													   AreasInDocument<AreaDebug>&& holder);
template void FtCtx::Add<std::span<const IdType>::iterator>(std::span<const IdType>::iterator begin, std::span<const IdType>::iterator end, RankT,
													   const std::vector<bool>&, AreasInDocument<AreaDebug>&& holder);

template void FtCtx::Add<std::span<const IdType>::iterator>(std::span<const IdType>::iterator begin, std::span<const IdType>::iterator end, RankT);
template void FtCtx::Add<std::span<const IdType>::iterator>(std::span<const IdType>::iterator begin, std::span<const IdType>::iterator end, RankT,
													   const std::vector<bool>&);

}  // namespace reindexer
