#pragma once

#include "core/nsselecter/joins/items_processor.h"
#include "core/nsselecter/joins/preselect.h"
#include "core/nsselecter/joins/queryresults.h"
#include "core/queryresults/localqueryresults.h"
#include "estl/type_traits.h"
#include "tools/logger.h"

namespace reindexer {

template <typename Result, typename Ctx>
class [[nodiscard]] ResultHandler {
public:
	static constexpr bool IsJoinSelectResult = IsJoinSelectCtx<Ctx>;
	static constexpr bool IsJoinPreSelectResult = IsJoinPreSelectCtx<Ctx>;
	static constexpr bool IsMainSelectResult = IsMainSelectCtx<Ctx>;

	explicit ResultHandler(Result& result) noexcept : result_{result} {}

	void AddItem(Ctx& ctx, RankT rank, IdType rowId, IdType properRowId, const PayloadValue& item, TagsMatcher& tm, const PayloadType& pt,
				 const NamespaceName& nsName) {
		if constexpr (IsJoinPreSelectResult) {
			addItemForPreSelectBuild(ctx, rank, rowId, properRowId, item, tm, pt);
		} else if constexpr (IsJoinSelectResult || IsMainSelectResult) {
			addItem(ctx, rank, rowId, properRowId, item, tm, pt, nsName);
		} else {
			static_assert(always_false<Ctx>::value, "Unsupported Context type");
		}
	}

	RX_ALWAYS_INLINE void Reserve(Ctx& ctx, size_t capacity, IsRanked isRanked) {
		if constexpr (std::same_as<Result, LocalQueryResults>) {
			if constexpr (IsJoinPreSelectResult) {
				if (auto* values = std::get_if<joins::PreSelect::Values>(&ctx.preSelect.Result().payload); values) {
					values->Reserve(capacity);
				} else {
					result_.Items().Reserve(capacity, isRanked);
				}
			} else if constexpr (IsJoinSelectResult || IsMainSelectResult) {
				result_.Items().Reserve(capacity, isRanked);
			} else {
				static_assert(always_false<Ctx>::value, "Unsupported Context type");
			}
		} else {
			static_assert(std::same_as<Result, FtMergeStatuses>, "Unhandled ResultType");
		}
	}

	RX_ALWAYS_INLINE size_t GetSize(Ctx& ctx) {
		if constexpr (IsJoinPreSelectResult || IsJoinSelectResult) {
			if (auto* values = std::get_if<joins::PreSelect::Values>(&ctx.preSelect.Result().payload); values) {
				return values->Size();
			} else {
				return result_.Count();
			}
		} else if constexpr (IsMainSelectResult) {
			return result_.Count();
		} else {
			static_assert(always_false<Ctx>::value, "Unsupported Context type");
		}
	}

	RX_ALWAYS_INLINE void SetItemsValuesAfterSorting(std::span<const PayloadValue>&& items, size_t fromPos) {
		if constexpr (IsJoinSelectResult || IsMainSelectResult) {
			for (size_t i = fromPos; i < result_.Count(); ++i) {
				auto& itemRef = result_.Items().GetItemRef(i);
				if (!itemRef.ValueInitialized()) {
					itemRef.SetValue(items[itemRef.Id().ToNumber()]);
				}
			}
		} else if constexpr (IsJoinPreSelectResult) {
			// nop
		} else {
			static_assert(always_false<Ctx>::value, "Unsupported Context type");
		}
	}

private:
	void addItem(Ctx& ctx, RankT rank, IdType rowId, IdType properRowId, const PayloadValue& item, TagsMatcher& tm, const PayloadType& pt,
				 const NamespaceName& nsName) {
		if (!ctx.sortingContext.expressions.empty()) {
			if (result_.haveRank) {
				result_.AddItemRef(rank, properRowId, unsigned(ctx.sortingContext.exprResults[0].size()), ctx.nsid);
			} else {
				result_.AddItemRef(properRowId, unsigned(ctx.sortingContext.exprResults[0].size()), ctx.nsid);
			}
			calculateSortExpressions(ctx, rank, rowId, item, tm, pt);
		} else {
			if (result_.haveRank) {
				result_.AddItemRef(rank, properRowId, item, ctx.nsid);
			} else {
				result_.AddItemRef(properRowId, item, ctx.nsid);
			}
		}

		const int kLimitItems = 10000000;
		size_t sz = result_.Count();
		if (sz >= kLimitItems && !(sz % kLimitItems)) [[unlikely]] {
			logFmt(LogWarning, "Too big query results ns='{}',count='{}',rowId='{}',q='{}'", nsName, sz, properRowId, ctx.query.GetSQL());
		}
	}

	void addItemForPreSelectBuild(JoinPreSelectCtx& ctx, RankT rank, IdType rowId, IdType properRowId, const PayloadValue& item,
								  TagsMatcher& tm, const PayloadType& pt) {
		std::visit(overloaded{[rowId](IdSetPlain& ids) { ids.AddUnordered(rowId); },
							  [&](joins::PreSelect::Values& values) {
								  if (!ctx.sortingContext.expressions.empty()) {
									  if (result_.haveRank) {
										  values.EmplaceBack(rank, properRowId, unsigned(ctx.sortingContext.exprResults[0].size()),
															 ctx.nsid);
									  } else {
										  values.EmplaceBack(properRowId, unsigned(ctx.sortingContext.exprResults[0].size()), ctx.nsid);
									  }
									  calculateSortExpressions(ctx, rank, rowId, item, tm, pt);
								  } else {
									  if (result_.haveRank) {
										  values.EmplaceBack(rank, properRowId, item, ctx.nsid);
									  } else {
										  values.EmplaceBack(properRowId, item, ctx.nsid);
									  }
								  }
							  },
							  [](const SelectIteratorContainer&) { throw_as_assert; }},
				   ctx.preSelect.Result().payload);
	}

	void calculateSortExpressions(Ctx& ctx, RankT rank, IdType rowId, const PayloadValue& pv, TagsMatcher& tm, const PayloadType& pt) {
		const joins::ItemsProcessors emptyJoinItemsProcessors;
		const auto& exprs = ctx.sortingContext.expressions;
		auto& exprResults = ctx.sortingContext.exprResults;
		assertrx_throw(exprs.size() == exprResults.size());
		const ConstPayload item{pt, pv};
		const auto& joinItemsProcessors = ctx.joinItemsProcessors ? *ctx.joinItemsProcessors : emptyJoinItemsProcessors;
		const auto joinedResultPtr = ctx.nsid < result_.joined_.size() ? &result_.joined_[ctx.nsid] : nullptr;
		for (size_t i = 0; i < exprs.size(); ++i) {
			exprResults[i].push_back(exprs[i].Calculate(rowId, item, joinedResultPtr, joinItemsProcessors, rank, tm, 0));
		}
	}

private:
	Result& result_;
};

}  // namespace reindexer
