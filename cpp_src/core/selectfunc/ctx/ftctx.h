#pragma once

#include <optional>
#include "basefunctionctx.h"
#include "core/ft/areaholder.h"
#include "core/ft/ft_fast/splitter.h"
#include "core/ft/usingcontainer.h"
#include "core/type_consts.h"

namespace reindexer {

struct FtCtxData : public intrusive_atomic_rc_base {
	FtCtxData(BaseFunctionCtx::CtxType t) noexcept : type(t) {}
	~FtCtxData() override = default;
	typedef intrusive_ptr<FtCtxData> Ptr;
	std::vector<RankT> rank;
	std::optional<RHashMap<IdType, size_t>> holders;
	bool isWordPositions = false;
	std::string extraWordSymbols;
	BaseFunctionCtx::CtxType type;
	intrusive_ptr<const ISplitter> splitter;
};

template <typename AreaType>
struct FtCtxAreaData : public FtCtxData {
	FtCtxAreaData(BaseFunctionCtx::CtxType t) noexcept : FtCtxData(t) {}
	std::vector<AreasInDocument<AreaType>> area;
};

class FtCtx : public BaseFunctionCtx {
public:
	typedef intrusive_ptr<FtCtx> Ptr;
	FtCtx(BaseFunctionCtx::CtxType t);
	RankT Rank(size_t pos) const noexcept override { return (pos < data_->rank.size()) ? data_->rank[pos] : 0.0; }

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, RankT);

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, RankT, const std::vector<bool>& mask);

	template <typename InputIterator, typename AreaType>
	void Add(InputIterator begin, InputIterator end, RankT, AreasInDocument<AreaType>&& holder);

	template <typename InputIterator, typename AreaType>
	void Add(InputIterator begin, InputIterator end, RankT, const std::vector<bool>& mask, AreasInDocument<AreaType>&& holder);

	void Reserve(size_t size) { data_->rank.reserve(size); }
	size_t Size() const noexcept { return data_->rank.size(); }

	void SetSplitter(intrusive_ptr<const ISplitter> s) noexcept { data_->splitter = std::move(s); }
	void SetWordPosition(bool v) noexcept { data_->isWordPositions = v; }

	const FtCtxData::Ptr& GetData() const noexcept { return data_; }
	void SetData(FtCtxData::Ptr data) noexcept { data_ = std::move(data); }

private:
	FtCtxData::Ptr data_;
};

}  // namespace reindexer
