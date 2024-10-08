#pragma once

#include <core/type_consts.h>
#include <memory>
#include "basefunctionctx.h"
#include "core/ft/areaholder.h"
#include "core/ft/usingcontainer.h"

namespace reindexer {

struct FtCtxData : public intrusive_atomic_rc_base {
	FtCtxData(BaseFunctionCtx::CtxType t) noexcept : type(t) {}
	virtual ~FtCtxData() = default;
	void InitHolders() {
		assertrx_dbg(!holders.has_value());
		holders.emplace();
	}
	typedef intrusive_ptr<FtCtxData> Ptr;
	std::vector<int16_t> proc;
	std::optional<RHashMap<IdType, size_t>> holders;
	bool isComposite = false;
	bool isWordPositions = false;
	std::string extraWordSymbols;
	BaseFunctionCtx::CtxType type;
};

template <typename AreaType>
struct FtCtxAreaData : public FtCtxData {
	FtCtxAreaData(BaseFunctionCtx::CtxType t) noexcept : FtCtxData(t) {}
	std::vector<AreasInDocument<AreaType>> area;
};
enum FtSortType { RankOnly, RankAndID, ExternalExpression };

class FtCtx : public BaseFunctionCtx {
public:
	typedef intrusive_ptr<FtCtx> Ptr;
	FtCtx(BaseFunctionCtx::CtxType t);
	int16_t Proc(size_t pos) const noexcept { return (pos < data_->proc.size()) ? data_->proc[pos] : 0; }

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc);

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool>& mask);

	template <typename InputIterator, typename AreaType>
	void Add(InputIterator begin, InputIterator end, int16_t proc, AreasInDocument<AreaType>&& holder);

	template <typename InputIterator, typename AreaType>
	void Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool>& mask, AreasInDocument<AreaType>&& holder);

	void Reserve(size_t size) { data_->proc.reserve(size); }
	size_t Size() const noexcept { return data_->proc.size(); }

	void SetExtraWordSymbols(const std::string& s) { data_->extraWordSymbols = s; }
	void SetWordPosition(bool v) { data_->isWordPositions = v; }

	FtCtxData::Ptr GetData() { return data_; }
	void SetData(FtCtxData::Ptr data) noexcept { data_ = std::move(data); }

private:
	FtCtxData::Ptr data_;
};

}  // namespace reindexer
