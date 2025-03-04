#pragma once

#include <variant>
#include "core/namespace/namespaceimpl.h"
#include "core/querystat.h"

namespace reindexer {

class SelectFunctionsHolder;

class RxSelector {
	struct NsLockerItem {
		explicit NsLockerItem(NamespaceImpl::Ptr ins = {}) noexcept : ns(std::move(ins)), count(1) {}
		NamespaceImpl::Ptr ns;
		NamespaceImpl::Locker::RLockT nsLck;
		unsigned count = 1;
	};

public:
	template <typename Context>
	class NsLocker : private h_vector<NsLockerItem, 4> {
	public:
		explicit NsLocker(const Context& context) noexcept : context_(context) {}
		~NsLocker() {
			// Unlock first
			for (auto it = rbegin(), re = rend(); it != re; ++it) {
				// Some namespaces may be in unlocked state in case of the exception during Lock() call
				if (it->nsLck.owns_lock()) {
					it->nsLck.unlock();
				} else {
					assertrx(!locked_);
				}
			}
			// Clean (ns may will release, if locker holds last ref)
		}

		void Add(NamespaceImpl::Ptr&& ns) {
			assertrx(!locked_);
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (it->ns.get() == ns.get()) {
					++(it->count);
					return;
				}
			}

			emplace_back(std::move(ns));
		}
		void Delete(const NamespaceImpl::Ptr& ns) noexcept {
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (it->ns.get() == ns.get()) {
					if (!--(it->count)) {
						erase(it);
					}
					return;
				}
			}
			assertrx(0);
		}
		void Lock() {
			boost::sort::pdqsort_branchless(
				begin(), end(), [](const NsLockerItem& lhs, const NsLockerItem& rhs) noexcept { return lhs.ns.get() < rhs.ns.get(); });
			for (auto it = begin(), e = end(); it != e; ++it) {
				it->nsLck = it->ns->rLock(context_);
			}
			locked_ = true;
		}

		NamespaceImpl::Ptr Get(const std::string& name) noexcept {
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (iequals(it->ns->name_, name)) {
					return it->ns;
				}
			}
			return NamespaceImpl::Ptr();
		}

	protected:
		bool locked_ = false;
		const Context& context_;
	};

	template <typename T, typename QueryType>
	static void DoSelect(const Query& q, LocalQueryResults& result, NsLocker<T>& locks, SelectFunctionsHolder& func, const RdxContext& ctx,
						 QueryStatCalculator<QueryType>& queryStatCalculator);

private:
	struct QueryResultsContext;

	template <typename T>
	static JoinedSelectors prepareJoinedSelectors(const Query& q, LocalQueryResults& result, NsLocker<T>& locks,
												  SelectFunctionsHolder& func, std::vector<QueryResultsContext>&, const RdxContext& ctx);
	template <typename T>
	[[nodiscard]] static std::vector<SubQueryExplain> preselectSubQueries(Query& mainQuery,
																		  std::vector<LocalQueryResults>& queryResultsHolder, NsLocker<T>&,
																		  SelectFunctionsHolder&, const RdxContext&);
	template <typename T>
	[[nodiscard]] static bool selectSubQuery(const Query& subQuery, const Query& mainQuery, NsLocker<T>&, SelectFunctionsHolder&,
											 std::vector<SubQueryExplain>&, const RdxContext&);
	template <typename T>
	[[nodiscard]] static VariantArray selectSubQuery(const Query& subQuery, const Query& mainQuery, NsLocker<T>&, LocalQueryResults&,
													 SelectFunctionsHolder&, std::variant<std::string, size_t> fieldOrKeys,
													 std::vector<SubQueryExplain>&, const RdxContext&);
	static StoredValuesOptimizationStatus isPreResultValuesModeOptimizationAvailable(const Query& jItemQ, const NamespaceImpl::Ptr& jns,
																					 const Query& mainQ);
};

}  // namespace reindexer
