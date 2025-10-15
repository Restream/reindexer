#pragma once

#include <variant>
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "core/querystat.h"

namespace reindexer {

class FtFunctionsHolder;

class [[nodiscard]] RxSelector {
	struct [[nodiscard]] NsLockerItem {
		explicit NsLockerItem(NamespaceImpl::Ptr ins, Namespace::Ptr n) noexcept : nsImpl(std::move(ins)), ns(std::move(n)), count(1) {}
		NamespaceImpl::Ptr nsImpl;
		Namespace::Ptr ns;
		NamespaceImpl::Locker::RLockT nsLck;
		unsigned count = 1;
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
				if (it->nsLck.owns_lock()) {
					it->nsLck.unlock();
				} else {
					assertrx(!locked_);
				}
			}
			// Clean (ns may will release, if locker holds last ref)
		}

		void Add(NamespaceImpl::Ptr&& nsImpl, Namespace::Ptr ns) {
			assertrx(!locked_);
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (it->nsImpl.get() == nsImpl.get()) {
					++(it->count);
					return;
				}
			}

			emplace_back(std::move(nsImpl), std::move(ns));
		}
		void Delete(const NamespaceImpl::Ptr& nsImpl) {
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (it->nsImpl.get() == nsImpl.get()) {
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
				begin(), end(), [](const NsLockerItem& lhs, const NsLockerItem& rhs) noexcept { return lhs.ns.get() < rhs.ns.get(); });
			for (auto it = begin(), e = end(); it != e; ++it) {
				it->nsLck = it->nsImpl->rLock(context_);
			}
			locked_ = true;
		}

		NamespaceImpl::Ptr Get(const std::string& name) noexcept {
			for (auto it = begin(), e = end(); it != e; ++it) {
				if (iequals(it->nsImpl->name_, name)) {
					return it->nsImpl;
				}
			}
			return NamespaceImpl::Ptr();
		}

	protected:
		bool locked_ = false;
		const Context& context_;
	};

	class [[nodiscard]] NsLockerWItem {
	public:
		explicit NsLockerWItem(Namespace::Ptr ns = {}, bool isWLock = false) noexcept : ns_(std::move(ns)), count_(1), isWLock_(isWLock) {}
		Namespace::Ptr ns_;
		NamespaceImpl::Ptr nsImpl_;
		std::variant<NamespaceImpl::Locker::RLockT, NamespaceImpl::Locker::WLockT> nsLck_;
		unsigned count_ = 1;
		bool isWLock_ = false;
	};

	class [[nodiscard]] NsLockerW {
	public:
		NsLockerW(const RdxContext& context) noexcept : context_(context) {}
		~NsLockerW() {
			// Unlock first
			for (auto it = nsList.rbegin(), re = nsList.rend(); it != re; ++it) {
				// Some namespaces may be in unlocked state in case of the exception during Lock() call
				std::visit(overloaded{[this](NamespaceImpl::Locker::RLockT& v) {
										  if (v.owns_lock()) {
											  v.unlock();
										  } else {
											  assertrx(!locked_);
										  }
									  },
									  [](NamespaceImpl::Locker::WLockT&) {}},
						   it->nsLck_);
			}
			// Clean(ns may will release, if locker holds last ref)
		}

		NamespaceImpl::Ptr Get(std::string_view name) noexcept {
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				if (iequals(std::string_view(it->nsImpl_->name_), name)) {
					return it->nsImpl_;
				}
			}
			return NamespaceImpl::Ptr();
		}

		void Add(Namespace::Ptr&& ns, bool wLock) {
			assertrx(!locked_);
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				if (it->ns_.get() == ns.get()) {
					++(it->count_);
					return;
				}
			}

			nsList.emplace_back(std::move(ns), wLock);
		}
		void Delete(const NamespaceImpl::Ptr& ns) {
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				if (it->nsImpl_.get() == ns.get()) {
					if (!--(it->count_)) {
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
				return lhs.ns_.get() < rhs.ns_.get();
			});
			NamespaceImpl::Ptr mainNsImpl;
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				if (it->isWLock_) {
					Namespace::GetWrLockData wd = it->ns_->GetWrLock(statCalculator, context_);
					it->nsLck_ = std::move(wd.lock);
					it->nsImpl_ = std::move(wd.nsImpl);
					it->nsImpl_->updateSelectTime();
				} else {
					it->nsImpl_ = it->ns_->GetMainNs();
					it->nsImpl_->updateSelectTime();
					it->nsLck_ = it->nsImpl_->rLock(context_);
				}
			}
			locked_ = true;
		}

		std::pair<NamespaceImpl::Locker::WLockT, NamespaceImpl::Ptr> Unlock() {
			assertrx(locked_);
			for (auto it = nsList.begin(), e = nsList.end(); it != e; ++it) {
				if (it->isWLock_) {
					return {std::move(std::get<1>(it->nsLck_)), it->nsImpl_};
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

	template <typename LockerType, typename QueryType>
	static void DoSelect(const Query& q, std::optional<Query>& queryCopy, LocalQueryResults& result, LockerType& locks,
						 FtFunctionsHolder& func, const RdxContext& ctx, QueryStatCalculator<QueryType>& queryStatCalculator);

	static void DoPreSelectForUpdateDelete(const Query& q, std::optional<Query>& queryCopy, LocalQueryResults& result, NsLockerW& locks,
										   FloatVectorsHolderMap* fvHolder, const RdxContext& ctx);

private:
	struct QueryResultsContext;

	template <typename Locker>
	static JoinedSelectors prepareJoinedSelectors(const Query& q, LocalQueryResults& result, Locker& locks, FtFunctionsHolder& func,
												  std::vector<QueryResultsContext>&, SetLimit0ForChangeJoin limit0, const RdxContext& ctx);
	template <typename LockerT>
	static std::vector<SubQueryExplain> preselectSubQueries(Query& mainQuery, std::vector<LocalQueryResults>& queryResultsHolder, LockerT&,
															FtFunctionsHolder&, const RdxContext&);
	template <typename LockerT>
	static bool selectSubQuery(const Query& subQuery, const Query& mainQuery, LockerT&, FtFunctionsHolder&, std::vector<SubQueryExplain>&,
							   const RdxContext&);
	template <typename LockerT>
	static VariantArray selectSubQuery(const Query& subQuery, const Query& mainQuery, LockerT&, LocalQueryResults&, FtFunctionsHolder&,
									   std::variant<std::string, size_t> fieldOrKeys, std::vector<SubQueryExplain>&, const RdxContext&);
	static StoredValuesOptimizationStatus isPreResultValuesModeOptimizationAvailable(const Query& jItemQ, const NamespaceImpl::Ptr& jns,
																					 const Query& mainQ);

	template <typename LockerType>
	static void preselectSubQuriesMain(const Query& q, std::optional<Query>& queryCopy, LockerType& locks, FtFunctionsHolder& func,
									   std::vector<SubQueryExplain>& subQueryExplains, ExplainCalc::Duration& preselectTimeTotal,
									   std::vector<LocalQueryResults>& queryResultsHolder, LogLevel logLevel, const RdxContext& ctx);
};

}  // namespace reindexer
