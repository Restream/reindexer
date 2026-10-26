#pragma once

#include <variant>
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/explaincalc.h"
#include "core/nsselecter/joins/items_processor.h"
#include "sort/pdqsort.hpp"

namespace reindexer {

class FtFunctionsHolder;
class JoinedQuery;

class [[nodiscard]] RxSelector {
	class [[nodiscard]] NsLockerItem {
	public:
		explicit NsLockerItem(std::string_view searchName, NamespaceImpl::Ptr ins, Namespace::Ptr n) noexcept
			: searchName_{searchName}, nsImpl_(std::move(ins)), ns_(std::move(n)) {}
		NsLockerItem(NsLockerItem&&) = default;
		NsLockerItem& operator=(NsLockerItem&&) = default;
		~NsLockerItem() = default;
		NsLockerItem(const NsLockerItem&) = delete;
		NsLockerItem& operator=(const NsLockerItem&) = delete;

		uintptr_t Ordering() const noexcept { return uintptr_t(ns_.get()); }
		void RLock(const RdxContext& ctx) {
			assertrx_dbg(!nsLck_.owns_lock());
			nsLck_ = nsImpl_->rLock(ctx);
		}
		bool UnlockIfOwns() noexcept {
			bool owns = nsLck_.owns_lock();
			if (owns) {
				nsLck_.unlock();
			}
			return owns;
		}
		std::string_view SearchName() const noexcept { return searchName_; }
		std::string_view ImplName() const noexcept {
			assertrx_dbg(nsLck_.owns_lock());
			return nsImpl_->name_;
		}
		const NamespaceImpl::Ptr& NsImpl() const& noexcept { return nsImpl_; }
		auto NsImpl() && = delete;

		unsigned count = 1;

	private:
		std::string_view searchName_;
		NamespaceImpl::Ptr nsImpl_;
		Namespace::Ptr ns_;
		NamespaceImpl::Locker::RLockT nsLck_;
	};

public:
	template <typename Context>
	class [[nodiscard]] NsLocker : private h_vector<NsLockerItem, 4> {
	public:
		explicit NsLocker(const Context& context) noexcept : context_(context) {}
		~NsLocker() {
			// Unlock first
			for (auto it = rbegin(), re = rend(); it != re; ++it) {
				// Some namespaces may be in unlocked state in case of the exception during Lock() call
				if (!it->UnlockIfOwns()) {
					assertrx(!locked_);
				}
			}
			// Clean (ns may be released, if locker holds last ref)
		}

		void Add(std::string_view searchName, NamespaceImpl::Ptr&& nsImpl, Namespace::Ptr ns) {
			assertrx(!locked_);
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (it->NsImpl().get() == nsImpl.get()) {
					assertrx_dbg(it->count > 0);
					++(it->count);
					return;
				}
			}

			emplace_back(searchName, std::move(nsImpl), std::move(ns));
		}
		void Delete(const NamespaceImpl::Ptr& nsImpl) {
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (it->NsImpl().get() == nsImpl.get()) {
					assertrx_dbg(it->count > 0);
					if (!--(it->count)) {
						erase(it);
					}
					return;
				}
			}
			assertrx_throw(false);
		}
		void Lock() {
			boost::sort::pdqsort_branchless(
				begin(), end(), [](const NsLockerItem& lhs, const NsLockerItem& rhs) noexcept { return lhs.Ordering() < rhs.Ordering(); });
			for (auto it = begin(), e = end(); it != e; ++it) {
				it->RLock(context_);
			}
			locked_ = true;
		}
		// Releases the mutexes only, keeping NamespaceImpl::Ptr, which are still required by the caller (e.g. by perf counters).
		void Unlock() noexcept {
			locked_ = false;
			for (auto it = rbegin(), re = rend(); it != re; ++it) {
				it->UnlockIfOwns();
			}
		}

		NamespaceImpl::Ptr Get(std::string_view name) {
			assertrx_dbg(locked_);
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (iequals(it->SearchName(), name)) {
					if (!iequals(it->SearchName(), it->ImplName())) [[unlikely]] {
						throw Error(errConflict, "Unable to get namespace: '{}'/'{}' - conflicting rename is in progress", it->SearchName(),
									it->ImplName());
					}
					return it->NsImpl();
				}
			}
			throw Error(errParams, "Namespace '{}' does not exist in namespaces locker", name);
		}

	private:
		bool locked_ = false;
		const Context& context_;
	};

	class [[nodiscard]] NsLockerWItem {
	public:
		explicit NsLockerWItem(std::string_view searchName, Namespace::Ptr ns = {}, bool isWLock = false) noexcept
			: searchName_{searchName}, ns_{std::move(ns)}, isWLock_{isWLock} {}
		NsLockerWItem(NsLockerWItem&&) = default;
		NsLockerWItem& operator=(NsLockerWItem&&) = default;
		~NsLockerWItem() = default;
		NsLockerWItem(const NsLockerWItem&) = delete;
		NsLockerWItem& operator=(const NsLockerWItem&) = delete;

		uintptr_t Ordering() const noexcept { return uintptr_t(ns_.get()); }
		template <typename QueryStatCalculatorT>
		void Lock(QueryStatCalculatorT& statCalculator, const RdxContext& ctx) {
			assertrx_dbg(!std::visit(overloaded{[](auto& v) { return v.owns_lock(); }}, nsLck_));
			if (isWLock_) {
				Namespace::GetWrLockData wd = ns_->GetWrLock(statCalculator, ctx);
				nsLck_ = std::move(wd.lock);
				nsImpl_ = std::move(wd.nsImpl);
				nsImpl_->updateSelectTime();
			} else {
				nsImpl_ = ns_->GetMainNs();
				nsImpl_->updateSelectTime();
				nsLck_ = nsImpl_->rLock(ctx);
			}
		}
		// NOLINTNEXTLINE(bugprone-exception-escape)
		bool UnlockIfOwns() noexcept {
			return std::visit(overloaded{[](NamespaceImpl::Locker::RLockT& v) {
											 bool owns = v.owns_lock();
											 if (owns) {
												 v.unlock();
											 }
											 return owns;
										 },
										 []([[maybe_unused]] NamespaceImpl::Locker::WLockT& v) {
											 bool owns = v.owns_lock();
											 if (owns) {
												 v.unlock();
											 }
											 // Wlock could be extracted previously, so always returning true
											 return true;
										 }},
							  nsLck_);
		}
		std::string_view SearchName() const noexcept { return searchName_; }
		std::string_view ImplName() const noexcept {
			assertrx_dbg(std::visit(overloaded{[](auto& v) { return v.owns_lock(); }}, nsLck_));
			return nsImpl_->name_;
		}
		const NamespaceImpl::Ptr& NsImpl() const& noexcept { return nsImpl_; }
		auto NsImpl() && = delete;
		const Namespace::Ptr& Ns() const& noexcept { return ns_; }
		auto Ns() && = delete;
		std::optional<std::pair<NamespaceImpl::Locker::WLockT, NamespaceImpl::Ptr>> TryExtractWLock() noexcept {
			assertrx_dbg(isWLock_ == std::holds_alternative<NamespaceImpl::Locker::WLockT>(nsLck_));
			if (isWLock_ && std::holds_alternative<NamespaceImpl::Locker::WLockT>(nsLck_)) {
				assertrx_dbg(std::get_if<NamespaceImpl::Locker::WLockT>(&nsLck_)->owns_lock());
				return std::make_pair(std::move(*std::get_if<NamespaceImpl::Locker::WLockT>(&nsLck_)), nsImpl_);
			}
			return std::nullopt;
		}
		bool IsWLock() const noexcept { return isWLock_; }

		unsigned count = 1;

	private:
		std::string_view searchName_;
		Namespace::Ptr ns_;
		NamespaceImpl::Ptr nsImpl_;
		std::variant<NamespaceImpl::Locker::RLockT, NamespaceImpl::Locker::WLockT> nsLck_;
		bool isWLock_ = false;
	};

	class [[nodiscard]] NsLockerW {
	public:
		NsLockerW(const RdxContext& context) noexcept : context_(context) {}
		~NsLockerW() {
			// Unlock first
			for (auto it = nsList.rbegin(), re = nsList.rend(); it != re; ++it) {
				// Some namespaces may be in unlocked state in case of the exception during Lock() call
				if (!it->UnlockIfOwns()) {
					assertrx(!locked_);
				}
			}
			// Clean(ns may be released, if locker holds last ref)
		}

		NamespaceImpl::Ptr Get(std::string_view name) {
			for (auto it = nsList.cbegin(), e = nsList.cend(); it != e; ++it) {
				if (iequals(it->SearchName(), name)) {
					if (!iequals(it->SearchName(), it->ImplName())) [[unlikely]] {
						throw Error(errConflict, "Unable to get namespace: '{}'/'{}' - conflicting rename is in progress", it->SearchName(),
									it->ImplName());
					}
					return it->NsImpl();
				}
			}
			throw Error(errParams, "Namespace '{}' does not exist in namespaces locker", name);
		}

		void Add(std::string_view searchName, Namespace::Ptr&& ns, bool wLock) {
			assertrx(!locked_);
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				assertrx_dbg(!wLock || !it->IsWLock());	 // Can't hold multiple write locks
				if (it->Ns().get() == ns.get()) {
					assertrx_dbg(it->IsWLock() || !wLock);
					assertrx_dbg(it->count > 0);
					++(it->count);
					return;
				}
			}

			nsList.emplace_back(searchName, std::move(ns), wLock);
		}
		void Delete(const NamespaceImpl::Ptr& ns) {
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				if (it->NsImpl().get() == ns.get()) {
					assertrx_dbg(it->count > 0);
					if (!--(it->count)) {
						nsList.erase(it);
					}
					return;
				}
			}
			assertrx_throw(false);
		}
		template <typename QueryStatCalculatorT>
		void Lock(QueryStatCalculatorT& statCalculator) {
			assertrx(!locked_);
			boost::sort::pdqsort_branchless(nsList.begin(), nsList.end(), [](const NsLockerWItem& lhs, const NsLockerWItem& rhs) noexcept {
				return lhs.Ordering() < rhs.Ordering();
			});
			NamespaceImpl::Ptr mainNsImpl;
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				it->Lock(statCalculator, context_);
			}
			locked_ = true;
		}

		std::pair<NamespaceImpl::Locker::WLockT, NamespaceImpl::Ptr> ExtractWLock() {
			assertrx(locked_);
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				auto wlock = it->TryExtractWLock();
				if (wlock.has_value()) {
					return std::move(wlock.value());
				}
			}
			throw_as_assert;
		}

	private:
		NamespaceImpl::Ptr mainNs;
		h_vector<NsLockerWItem, 4> nsList;
		bool locked_ = false;
		const RdxContext& context_;
	};

	template <typename LockerType>
	static void DoSelect(ConstQueryImpl q, std::optional<Query>& queryCopy, LocalQueryResults& result, LockerType& locks,
						 FtFunctionsHolder& func, const RdxContext& ctx);
	static void DoPreSelectForUpdateDelete(ConstQueryImpl q, std::optional<Query>& queryCopy, LocalQueryResults& result, NsLockerW& locks,
										   FloatVectorsHolderMap* fvHolder, const RdxContext& ctx);

private:
	template <typename LockerT>
	static void preselectSubQueries(QueryImpl mainQuery, std::vector<LocalQueryResults>& queryResultsHolder, LockerT&, FtFunctionsHolder&,
									std::vector<SubQueryExplain>&, const RdxContext&);
	template <typename LockerT>
	static bool selectSubQuery(ConstQueryImpl subQuery, ConstQueryImpl mainQuery, LockerT&, FtFunctionsHolder&,
							   std::vector<SubQueryExplain>&, const RdxContext&);
	template <typename LockerT>
	static VariantArray selectSubQuery(ConstQueryImpl subQuery, ConstQueryImpl mainQuery, LockerT&, LocalQueryResults&, FtFunctionsHolder&,
									   std::variant<std::string, size_t> fieldOrKeys, std::vector<SubQueryExplain>&, const RdxContext&);

	template <typename LockerType>
	static void preselectSubQueriesMain(ConstQueryImpl q, std::optional<Query>& queryCopy, LockerType& locks, FtFunctionsHolder& func,
										std::vector<SubQueryExplain>& subQueryExplains, Explain::Duration& preselectTimeTotal,
										std::vector<LocalQueryResults>& queryResultsHolder, LogLevel logLevel, const RdxContext& ctx);
	template <typename LockerType>
	static void preselectSubQueriesInJoins(QueryImpl q, std::vector<LocalQueryResults>& queryResultsHolder, LockerType& locks,
										   FtFunctionsHolder& func, std::vector<SubQueryExplain>& subQueryExplains, const RdxContext& ctx);

	// Root sort is copied onto every merge subquery and bound again inside that namespace.
	static void copyParentSorting(JoinedQuery& dst, ConstQueryImpl parent);
	// Keeps the earlier row when the same namespace and the same fields filter return one item twice.
	// rowsPerNs is the number of rows each namespace appended. It only sizes the hash set.
	static void deduplicateMergedItems(ItemRefVector& items, std::span<NamespaceImpl* const> namespaces, std::span<const size_t> rowsPerNs,
									   const LocalQueryResults& result);
	struct [[nodiscard]] MergedSortColumn {
		bool isExpr = false;
		double expr = 0;
		// One array per compared field. A scalar, array, JSON path or sparse index has one.
		// A composite index has one array per constituent field, in index order.
		h_vector<VariantArray, 1> values;
		const CollateOpts* opts = nullptr;
		bool desc = false;
		bool inForced = false;
		ptrdiff_t forcedPos = 0;
	};
	// Sparse indexes have no payload field: their values live at the index JSON path.
	// Composite indexes are compared as their constituent fields, in index order.
	static h_vector<VariantArray, 1> mergedFieldValues(const NamespaceImpl& ns, int index, std::string_view column, const PayloadValue& pv);
	// Match forced values the same way as NsSelecter::applyForcedSortImpl, using this namespace's index.
	static void markMergedForced(MergedSortColumn& key, const NamespaceImpl& ns, int index, const PayloadValue& pv,
								 const VariantArray& forcedSort);
	// Equal columns fall through to nsid. Its order follows the first entry, the same way a single namespace breaks ties by id.
	static bool mergedSortLess(std::span<const MergedSortColumn> lhs, std::span<const MergedSortColumn> rhs,
							   std::span<const MergedSortColumn> collateRow, bool useForced, uint16_t lhsNs, uint16_t rhsNs);
	static void sortMergedItems(ItemRefVector& items, std::span<NamespaceImpl* const> namespaces,
								std::span<const std::vector<SortExpression>> parsedSort, const SortingEntries& entries,
								const VariantArray& forcedSort, std::span<const joins::ItemsProcessors> joinProcs,
								const joins::Results& joined);
	// Sum, count and count_cached were accumulated before duplicate rows were removed.
	static void adjustAggregationsAfterDedup(LocalQueryResults& result, std::span<NamespaceImpl* const> namespaces,
											 size_t itemsBeforeDedup);
	// Sum of one field over the rows left after dedup. Empty when none of those rows has the field.
	static std::optional<double> sumMergedField(const ItemRefVector& items, std::span<NamespaceImpl* const> namespaces,
												std::string_view field);
};

}  // namespace reindexer
