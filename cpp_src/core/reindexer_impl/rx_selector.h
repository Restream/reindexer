#pragma once

#include <variant>
#include "core/namespace/namespace.h"
#include "core/namespace/namespaceimpl.h"
#include "core/querystat.h"

namespace reindexer {

class FtFunctionsHolder;

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
