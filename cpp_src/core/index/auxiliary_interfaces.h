#pragma once

#include <cstdlib>
#include <vector>
#include "core/type_consts.h"

namespace reindexer::index {

constexpr size_t kCancelCheckFrequency = 32 * 1024;

class [[nodiscard]] ICancelable {
public:
	virtual bool IsCanceled() const noexcept = 0;
	virtual ~ICancelable() = default;
};

#define RX_RETURN_IF_CANCELED(cancelable)           \
	{                                               \
		if (cancelable.IsCanceled()) [[unlikely]] { \
			return WasCanceled_True;                \
		}                                           \
	}

class [[nodiscard]] IUpdateSortedContext {
public:
	virtual ~IUpdateSortedContext() = default;
	virtual unsigned GetSortedIdxCount() const noexcept = 0;
	virtual SortType GetCurSortId() const noexcept = 0;
	virtual const std::vector<SortType>& Ids2Sorts() const& noexcept = 0;
	virtual std::vector<SortType>& Ids2Sorts() & noexcept = 0;
};

constexpr inline bool IsOrderedCondition(CondType condition) noexcept {
	switch (condition) {
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
		case CondRange:
			return true;
		case CondAny:
		case CondEq:
		case CondSet:
		case CondAllSet:
		case CondLike:
		case CondEmpty:
		case CondDWithin:
		case CondKnn:
			return false;
		default:
			std::abort();
	}
}

}  // namespace reindexer::index
