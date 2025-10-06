#pragma once

#include <optional>
#include <variant>
#include "core/ft/areaholder.h"
#include "core/ft/ft_fast/splitter.h"
#include "core/nsselecter/ranks_holder.h"
#include "core/type_consts.h"
#include "tools/rhashmap.h"

namespace reindexer {

class FuncNone;
class Snippet;
class Highlight;
class SnippetN;
class DebugRank;
class RankT;

template <class T, class U>
intrusive_ptr<T> static_ctx_pointer_cast(const intrusive_ptr<U>& r) noexcept {
	assertrx_dbg(dynamic_cast<T*>(r.get()) != nullptr);
	return intrusive_ptr<T>(static_cast<T*>(r.get()));
}

template <typename VariantType, typename T, std::size_t index = 0>
constexpr std::size_t variant_index() {
	static_assert(std::variant_size_v<VariantType> > index, "Type not found in variant");
	if constexpr (std::is_same_v<std::variant_alternative_t<index, VariantType>, T>) {
		return index;
	} else {
		return variant_index<VariantType, T, index + 1>();
	}
}

enum class [[nodiscard]] FtCtxType { kFtCtx = 1, kFtArea = 2, kFtAreaDebug = 3, kNotSet = 4 };

using FtFuncVariant = std::variant<FuncNone, Snippet, Highlight, SnippetN, DebugRank>;
enum class [[nodiscard]] FtFuncType : uint8_t {
	None = variant_index<FtFuncVariant, FuncNone>(),
	Snippet = variant_index<FtFuncVariant, Snippet>(),
	Highlight = variant_index<FtFuncVariant, Highlight>(),
	SnippetN = variant_index<FtFuncVariant, SnippetN>(),
	DebugRank = variant_index<FtFuncVariant, DebugRank>(),
};

struct [[nodiscard]] FtCtxData : public intrusive_atomic_rc_base {
	FtCtxData(FtCtxType t, RanksHolder::Ptr r) noexcept : ranks(std::move(r)), type(t) { assertrx_dbg(ranks); }
	~FtCtxData() override = default;
	typedef intrusive_ptr<FtCtxData> Ptr;
	RanksHolder::Ptr ranks;
	std::optional<RHashMap<IdType, size_t>> holders;
	bool isWordPositions = false;
	std::string extraWordSymbols;
	FtCtxType type;
	intrusive_ptr<const ISplitter> splitter;
};

template <typename AreaType>
struct [[nodiscard]] FtCtxAreaData : public FtCtxData {
	FtCtxAreaData(FtCtxType t, RanksHolder::Ptr r) noexcept : FtCtxData(t, std::move(r)) {}
	std::vector<AreasInDocument<AreaType>> area;
};

class [[nodiscard]] FtCtx : public intrusive_atomic_rc_base {
public:
	typedef intrusive_ptr<FtCtx> Ptr;
	FtCtx(FtCtxType, RanksHolder::Ptr);

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, RankT);

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, RankT, const std::vector<bool>& mask);

	template <typename InputIterator, typename AreaType>
	void Add(InputIterator begin, InputIterator end, RankT, AreasInDocument<AreaType>&& holder);

	template <typename InputIterator, typename AreaType>
	void Add(InputIterator begin, InputIterator end, RankT, const std::vector<bool>& mask, AreasInDocument<AreaType>&& holder);

	RanksHolder& Ranks() & noexcept { return *data_->ranks; }
	const RanksHolder& Ranks() const& noexcept { return *data_->ranks; }
	auto Ranks() const&& = delete;
	RanksHolder::Ptr RanksPtr() const noexcept { return data_->ranks; }

	void SetSplitter(intrusive_ptr<const ISplitter> s) noexcept { data_->splitter = std::move(s); }
	void SetWordPosition(bool v) noexcept { data_->isWordPositions = v; }

	const FtCtxData::Ptr& GetData() const& noexcept { return data_; }
	auto GetData() && = delete;
	void SetData(FtCtxData::Ptr data) noexcept { data_ = std::move(data); }

	FtCtxType Type() const noexcept { return data_->type; }

	void AddFunction(const std::string& name, FtFuncType functionIndx) {
		auto it = std::find_if(functions_.begin(), functions_.end(), [&name](const FuncData& data) { return data.name == name; });
		auto& ref = (it == functions_.end()) ? functions_.emplace_back(std::string(name)) : *it;
		ref.types[static_cast<unsigned>(functionIndx)] = true;
	}

private:
	FtCtxData::Ptr data_;
	struct [[nodiscard]] FuncData {
		using TypesArrayT = std::array<bool, std::variant_size_v<FtFuncVariant>>;

		explicit FuncData(std::string&& _name) noexcept : name(std::move(_name)) {}

		std::string name;

		TypesArrayT types = {};
	};
	h_vector<FuncData, 2> functions_;
};

}  // namespace reindexer
