#pragma once

#include "core/namespace/namespaceimpl.h"
#include "core/querystat.h"

namespace reindexer {

class SelectFunctionsHolder;

class RxSelector {
	struct NsLockerItem {
		NsLockerItem(NamespaceImpl::Ptr ins = {}) noexcept : ns(std::move(ins)), count(1) {}
		NamespaceImpl::Ptr ns;
		NamespaceImpl::Locker::RLockT nsLck;
		unsigned count = 1;
	};

public:
	template <typename Context>
	class NsLocker : private h_vector<NsLockerItem, 4> {
	public:
		NsLocker(const Context &context) : context_(context) {}
		~NsLocker() {
			// Unlock first
			for (auto it = rbegin(); it != rend(); ++it) {
				// Some of the namespaces may be in unlocked statet in case of the exception during Lock() call
				if (it->nsLck.owns_lock()) {
					it->nsLck.unlock();
				} else {
					assertrx(!locked_);
				}
			}
			// Clean (ns may releases, if locker holds last ref)
		}

		void Add(NamespaceImpl::Ptr ns) {
			assertrx(!locked_);
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					++(it->count);
					return;
				}
			}

			emplace_back(std::move(ns));
			return;
		}
		void Delete(const NamespaceImpl::Ptr &ns) {
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					if (!--(it->count)) erase(it);
					return;
				}
			}
			assertrx(0);
		}
		void Lock() {
			boost::sort::pdqsort_branchless(
				begin(), end(), [](const NsLockerItem &lhs, const NsLockerItem &rhs) noexcept { return lhs.ns.get() < rhs.ns.get(); });
			for (auto it = begin(), e = end(); it != e; ++it) {
				it->nsLck = it->ns->rLock(context_);
			}
			locked_ = true;
		}

		NamespaceImpl::Ptr Get(const std::string &name) {
			for (auto it = begin(); it != end(); it++) {
				if (iequals(it->ns->name_, name)) return it->ns;
			}
			return nullptr;
		}

	protected:
		bool locked_ = false;
		const Context &context_;
	};

	template <typename T, typename QueryType>
	static void DoSelect(const Query &q, LocalQueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func, const RdxContext &ctx,
						 QueryStatCalculator<QueryType> &queryStatCalculator);

private:
	struct QueryResultsContext;
	template <typename T>
	static JoinedSelectors prepareJoinedSelectors(const Query &q, LocalQueryResults &result, NsLocker<T> &locks,
												  SelectFunctionsHolder &func, std::vector<QueryResultsContext> &, const RdxContext &ctx);
	template <typename T>
	[[nodiscard]] static std::vector<SubQueryExplain> preselectSubQueries(Query &mainQuery, std::vector<LocalQueryResults> &queryResultsHolder,
																		  NsLocker<T> &, SelectFunctionsHolder &, const RdxContext &);
	template <typename T>
	[[nodiscard]] static bool selectSubQuery(const Query &subQuery, const Query &mainQuery, NsLocker<T> &, SelectFunctionsHolder &,
											 std::vector<SubQueryExplain> &, const RdxContext &);
	template <typename T>
	[[nodiscard]] static VariantArray selectSubQuery(const Query &subQuery, const Query &mainQuery, NsLocker<T> &, LocalQueryResults &,
													 SelectFunctionsHolder &, std::variant<std::string, size_t> fieldOrKeys,
													 std::vector<SubQueryExplain> &, const RdxContext &);
	static bool isPreResultValuesModeOptimizationAvailable(const Query &jItemQ, const NamespaceImpl::Ptr &jns, const Query &mainQ);
};

}  // namespace reindexer
