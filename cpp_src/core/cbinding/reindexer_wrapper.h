#pragma once

#include "core/reindexer.h"
#include "tools/logger.h"
#include "updatesobserver.h"

namespace reindexer_server {
class Server;
}

namespace reindexer {

constexpr size_t kBuiltinUpdatesBufSize = 8192;

struct [[nodiscard]] ReindexerWrapper {
	ReindexerWrapper(ReindexerConfig&& cfg) : rx(std::move(cfg)), builtinUpdatesObs(kBuiltinUpdatesBufSize) {}
	ReindexerWrapper(Reindexer&& rx) : rx(std::move(rx)), builtinUpdatesObs(kBuiltinUpdatesBufSize) {}
	~ReindexerWrapper() {
		if (auto err = rx.UnsubscribeUpdates(builtinUpdatesObs); !err.ok()) {
			logFmt(LogError, "Database destruction error (updates subscription): {}", err.what());
		}
	}

	Reindexer rx;
	BufferedUpdateObserver builtinUpdatesObs;
};

class [[nodiscard]] WrapersMap {
public:
	std::pair<ReindexerWrapper*, bool> try_emplace(reindexer_server::Server* sptr, Reindexer* rptr) {
		assertrx_dbg(rptr);
		assertrx_dbg(sptr);
		const auto uiRptr = reinterpret_cast<uintptr_t>(rptr);
		const auto rhash = HashT()(uiRptr);
		const auto uiSrvPtr = reinterpret_cast<uintptr_t>(sptr);
		const auto srvHash = HashT()(uiSrvPtr);

		lock_guard lck(mtx_);

		auto rmapPair = map_.try_emplace_prehashed(srvHash, uiSrvPtr);
		auto& rmap = rmapPair.first->second;

		if (auto it = rmap.find(uiRptr, rhash); it != rmap.end()) {
			return std::make_pair(it->second.get(), false);
		}
		auto [it, emplaced] =
			rmap.try_emplace_prehashed(rhash, uiRptr, make_intrusive<intrusive_rc_wrapper<ReindexerWrapper>>(Reindexer(*rptr)));
		return std::make_pair(it->second.get(), emplaced);
	}
	void erase(reindexer_server::Server* sptr) noexcept {
		const auto uiSptr = reinterpret_cast<uintptr_t>(sptr);
		assertrx_dbg(sptr);

		lock_guard lck(mtx_);
		map_.erase(uiSptr);
	}

private:
	// Using non-atomic intrusive wrapper, because internal map_ is the only actual owner.
	// It could be a simple unique_ptr, but MSVC complains on it
	using HashT = std::hash<uintptr_t>;
	using RMapT = fast_hash_map<uintptr_t, intrusive_ptr<intrusive_rc_wrapper<ReindexerWrapper>>, HashT>;

	mutable mutex mtx_;
	// Values of this map will be exposed outside of the class, so they must remain unchanged after map resizing
	fast_hash_map_l<uintptr_t, RMapT, HashT> map_;
};

}  // namespace reindexer
