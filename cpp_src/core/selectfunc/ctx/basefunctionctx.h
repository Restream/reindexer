#pragma once

#include <variant>
#include "core/rank_t.h"
#include "core/selectfunc/functions/debugrank.h"
#include "core/selectfunc/functions/highlight.h"
#include "core/selectfunc/functions/snippet.h"
#include "estl/intrusive_ptr.h"

namespace reindexer {

template <class T, class U>
intrusive_ptr<T> static_ctx_pointer_cast(const intrusive_ptr<U>& r) noexcept {
	assertrx_dbg(dynamic_cast<T*>(r.get()) != nullptr);
	return intrusive_ptr<T>(static_cast<T*>(r.get()));
}

class FuncNone {
public:
	bool Process(ItemRef&, PayloadType&, const SelectFuncStruct&, std::vector<key_string>&) noexcept { return false; }
};

template <typename VariantType, typename T, std::size_t index = 0>
constexpr std::size_t variant_index() {
	static_assert(std::variant_size_v<VariantType> > index, "Type not found in variant");
	if constexpr (std::is_same_v<std::variant_alternative_t<index, VariantType>, T>) {
		return index;
	} else {
		return variant_index<VariantType, T, index + 1>();
	}
}

using SelectFuncVariant = std::variant<FuncNone, Snippet, Highlight, SnippetN, DebugRank>;
enum class SelectFuncType {
	None = variant_index<SelectFuncVariant, FuncNone>(),
	Snippet = variant_index<SelectFuncVariant, Snippet>(),
	Highlight = variant_index<SelectFuncVariant, Highlight>(),
	SnippetN = variant_index<SelectFuncVariant, SnippetN>(),
	DebugRank = variant_index<SelectFuncVariant, DebugRank>(),
	Max	 // Max possible value
};

class BaseFunctionCtx : public intrusive_atomic_rc_base {
public:
	typedef intrusive_ptr<BaseFunctionCtx> Ptr;
	enum class [[nodiscard]] CtxType { kFtCtx = 1, kFtArea = 2, kFtAreaDebug = 3, kKnnCtx = 4, kNotSet = 5 };
	BaseFunctionCtx(CtxType type) noexcept : type_{type} {}
	virtual ~BaseFunctionCtx() = default;

	void AddFunction(const std::string& name, SelectFuncType functionIndx) {
		auto it = std::find_if(functions_.begin(), functions_.end(), [&name](const FuncData& data) { return data.name == name; });
		auto& ref = (it == functions_.end()) ? functions_.emplace_back(std::string(name)) : *it;
		ref.types[static_cast<unsigned>(functionIndx)] = true;
	}
	bool CheckFunction(const std::string& name, std::initializer_list<SelectFuncType> types) {
		auto it = std::find_if(functions_.begin(), functions_.end(), [&name](const FuncData& data) { return data.name == name; });
		if (it != functions_.end()) {
			for (auto t : types) {
				if (it->types[static_cast<unsigned>(t)]) {
					return true;
				}
			}
		}
		return false;
	}
	CtxType Type() const noexcept { return type_; }
	virtual RankT Rank(size_t pos) const noexcept = 0;

private:
	struct FuncData {
		using TypesArrayT = std::array<bool, static_cast<unsigned>(SelectFuncType::Max)>;

		FuncData(std::string&& _name) noexcept : name(std::move(_name)) {}

		std::string name;

		TypesArrayT types = {};
	};
	h_vector<FuncData, 2> functions_;
	CtxType type_ = CtxType::kNotSet;
};

}  // namespace reindexer
