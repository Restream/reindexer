#pragma once

#include <map>
#include <set>
#include "core/namespace/namespacename.h"
#include "estl/lock.h"
#include "estl/mutex.h"

namespace reindexer::cluster {

class [[nodiscard]] NamespacesSyncScheduler {
public:
	class [[nodiscard]] Guard {
	public:
		Guard() noexcept = default;
		Guard(const NamespaceName& ns, NamespacesSyncScheduler& owner) noexcept : ns_{ns}, owner_{&owner} { assertrx_dbg(!ns.empty()); }
		~Guard() {
			if (owner_ && !ns_.empty()) {
				[[maybe_unused]] bool removed = owner_->syncDone(ns_);
				assertrx_dbg(removed);
			}
		}
		Guard(const Guard&) = delete;
		Guard(Guard&& o) noexcept = default;
		Guard& operator=(const Guard&) = delete;
		Guard& operator=(Guard&&) noexcept = default;

		const NamespaceName& Name() const& noexcept { return ns_; }
		const NamespaceName& Name() && = delete;

	private:
		NamespaceName ns_;
		NamespacesSyncScheduler* owner_{nullptr};
	};

	~NamespacesSyncScheduler() { assertrx_dbg(syncing_.empty()); }

	Guard ScheduleNext(const std::set<NamespaceName>& suggestions) RX_REQUIRES(!mtx_) {
		assertrx_dbg(!suggestions.empty());

		lock_guard lck(mtx_);

		auto suggIt = suggestions.begin();
		auto syncIt = syncing_.begin();
		while (suggIt != suggestions.end() && syncIt != syncing_.end()) {
			std::string_view suggNs(*suggIt), syncNs(syncIt->first);
			if (suggNs < syncNs) {
				++suggIt;
			} else if (syncNs < suggNs) {
				++syncIt;
			} else {
				++syncIt->second;
				return Guard{syncIt->first, *this};
			}
		}
		syncing_[*suggestions.begin()] = 1;
		return Guard{*suggestions.begin(), *this};
	}

private:
	bool syncDone(const NamespaceName& ns) noexcept RX_REQUIRES(!mtx_) {
		assertrx_dbg(!ns.empty());

		lock_guard lck(mtx_);

		auto found = syncing_.find(ns);
		if (found == syncing_.end()) [[unlikely]] {
			return false;
		}
		assertrx_dbg(found->second);
		if (--found->second == 0) {
			syncing_.erase(found);
		}
		return true;
	}

	mutex mtx_;
	std::map<NamespaceName, uint32_t> syncing_ RX_GUARDED_BY(mtx_);
};

}  // namespace reindexer::cluster
