#pragma once

#include <optional>
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/selectiteratorcontainer.h"
#include "core/queryresults/itemref.h"

namespace reindexer::joins {

struct [[nodiscard]] PreSelectProperties {
	PreSelectProperties(int64_t qresMaxIts, int64_t maxItersIdSetPreSelect) noexcept
		: qresMaxIterations{qresMaxIts}, maxIterationsIdSetPreSelect{maxItersIdSetPreSelect} {}

	bool isLimitExceeded = false;
	bool isUnorderedIndexSort = false;
	bool btreeIndexOptimizationEnabled = false;
	int64_t qresMaxIterations{0};
	const int64_t maxIterationsIdSetPreSelect;
};

struct [[nodiscard]] PreSelect {
	class [[nodiscard]] Values : public ItemRefVector {
	public:
		Values(const PayloadType& pt, const TagsMatcher& tm) noexcept : payloadType{pt}, tagsMatcher{tm} {}
		Values(Values&& other) noexcept
			: ItemRefVector(std::move(other)),
			  // NOLINTNEXTLINE (bugprone-use-after-move)
			  payloadType(std::move(other.payloadType)),
			  // NOLINTNEXTLINE (bugprone-use-after-move)
			  tagsMatcher(std::move(other.tagsMatcher)),
			  // NOLINTNEXTLINE (bugprone-use-after-move)
			  locked_(other.locked_) {
			// NOLINTNEXTLINE (bugprone-use-after-move)
			other.locked_ = false;
		}
		Values(const Values&) = delete;
		Values& operator=(const Values&) = delete;
		Values& operator=(Values&&) = delete;
		~Values() {
			if (locked_) {
				for (size_t i = 0; i < Size(); ++i) {
					Payload{payloadType, GetItemRef(i).Value()}.ReleaseStrings();
				}
			}
		}
		bool Locked() const noexcept { return locked_; }
		void Lock() {
			assertrx_throw(!locked_);
			for (size_t i = 0; i < Size(); ++i) {
				Payload{payloadType, GetItemRef(i).Value()}.AddRefStrings();
			}
			locked_ = true;
		}
		bool IsPreselectAllowed() const noexcept { return preselectAllowed_; }
		void PreselectAllowed(bool a) noexcept { preselectAllowed_ = a; }

		PayloadType payloadType;
		TagsMatcher tagsMatcher;
		NamespaceName nsName;

	private:
		bool locked_ = false;
		bool preselectAllowed_ = true;
	};

	struct [[nodiscard]] SortOrderContext {
		const Index* index = nullptr;  // main ordered index with built sort order mapping
		SortingEntry sortingEntry;	   // main sorting entry for the ordered index
	};

	static constexpr int MaxIterationsForValuesOptimization = 200;

	using PreSelectT = std::variant<IdSetPlain, SelectIteratorContainer, Values>;
	using Ptr = std::shared_ptr<PreSelect>;
	using CPtr = std::shared_ptr<const PreSelect>;
	PreSelectT payload;
	bool enableSortOrders = false;
	bool btreeIndexOptimizationEnabled = true;
	SortOrderContext sortOrder;
	StoredValuesOptimizationStatus storedValuesOptStatus = StoredValuesOptimizationStatus::Enabled;
	std::optional<PreSelectProperties> properties;
	std::string explainPreSelect;
};

enum class [[nodiscard]] PreSelectMode { Empty, Build, Execute, ForInsertion, InsertionRejected };

class [[nodiscard]] PreSelectBuildCtx {
public:
	explicit PreSelectBuildCtx(PreSelect::Ptr r) noexcept : result_{std::move(r)} {}
	PreSelect& Result() & noexcept { return *result_; }
	PreSelectMode Mode() const noexcept { return PreSelectMode::Build; }
	const PreSelect::Ptr& ResultPtr() const& noexcept { return result_; }
	auto ResultPtr() const&& = delete;

private:
	PreSelect::Ptr result_;
};

class [[nodiscard]] PreSelectExecuteCtx {
public:
	explicit PreSelectExecuteCtx(PreSelect::CPtr r) noexcept : result_{std::move(r)}, mode_{PreSelectMode::Execute} {}
	explicit PreSelectExecuteCtx(PreSelect::CPtr r, int maxIters) noexcept
		: result_{std::move(r)}, mode_{PreSelectMode::ForInsertion}, mainQueryMaxIterations_{maxIters} {}
	const PreSelect& Result() const& noexcept { return *result_; }
	PreSelectMode Mode() const noexcept { return mode_; }
	int MainQueryMaxIterations() const {
		assertrx_dbg(mode_ == PreSelectMode::ForInsertion);
		return mainQueryMaxIterations_;
	}
	const PreSelect::CPtr& ResultPtr() const& noexcept { return result_; }
	void Reject() {
		assertrx_dbg(mode_ == PreSelectMode::ForInsertion);
		mode_ = PreSelectMode::InsertionRejected;
	}

	auto Result() const&& = delete;
	auto ResultPtr() const&& = delete;

private:
	PreSelect::CPtr result_;
	PreSelectMode mode_;
	int mainQueryMaxIterations_{0};
};

template <typename T>
inline constexpr bool IsPreSelectBuildCtx = std::same_as<std::remove_cvref_t<T>, PreSelectBuildCtx>;

}  // namespace reindexer::joins
