#include "events/listener.h"
#include "tools/logger.h"

namespace reindexer {

constexpr double kUpdatesHandlingPeriod = 0.05;

EventsListener::EventsListener(const std::string& dbName, size_t maxUpdatesQueueSize)
	: updatesQueue_(maxUpdatesQueueSize), dbName_(dbName) {
	{
		using VecT = std::vector<std::string>;
		VecT container;

#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
		updatesQueue_.Init<VecT>(std::move(container), nullptr);  // TODO: Add some logger #1724
#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic pop
#endif
	}
	terminateAsync_.set(loop_);
	terminateAsync_.set([](net::ev::async& a) noexcept { a.loop.break_loop(); });
	terminateAsync_.start();
}

EventsListener::~EventsListener() {
	Stop();
	terminateAsync_.stop();
}

Error EventsListener::AddOrUpdate(IEventsObserver& observer, EventSubscriberConfig&& cfg) {
	if (!cfg.ActiveStreams()) {
		return Remove(observer);
	}

	lock_guard subUnsubLck(subUnsubMtx_);
	lock_guard lck(mtx_);

	if (auto it = subs_.find(&observer); it != subs_.end()) {
		it->second.cfg = std::move(cfg);
	} else {
		subs_.emplace(&observer, ObserverInfo{.nextUpdateId = updatesQueue_.GetNextUpdateID(), .cfg = std::move(cfg)});
		[[maybe_unused]] auto cnt = subsCount_.fetch_add(1, std::memory_order_release);
		assertrx_dbg(cnt == 0 || eventsThread_.joinable());
	}
	rebuildCommonFilter();
	if (!eventsThread_.joinable()) {
		eventsThread_ = std::thread([this] { eventsLoop(); });
	}
	return {};
}

Error EventsListener::Remove(IEventsObserver& observer) {
	lock_guard subUnsubLck(subUnsubMtx_);
	{
		lock_guard lck(mtx_);
		if (!subs_.erase(&observer)) {
			return {};
		}
		rebuildCommonFilter();
		subsCount_.fetch_sub(1, std::memory_order_release);
		if (!subs_.empty()) {
			return {};
		}
	}
	stop();
	updatesQueue_.EraseUntil(std::numeric_limits<uint64_t>::max());
	return {};
}

void EventsListener::Stop() {
	lock_guard subUnsubLck(subUnsubMtx_);
	stop();
}

void EventsListener::stop() {
	if (eventsThread_.joinable()) {
		terminate_ = true;
		terminateAsync_.send();
		eventsThread_.join();
		terminate_ = false;
	}
}

void EventsListener::rebuildCommonFilter() {
#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
	commonFilter_ = DBNamespaces();
#if !defined(__clang__) && !defined(_MSC_VER)
#pragma GCC diagnostic pop
#endif
	for (const auto& sub : subs_) {
		auto& streams = sub.second.cfg.Streams();
		for (auto& stream : streams) {
			commonFilter_.withConfigNamespace = commonFilter_.withConfigNamespace || stream.withConfigNamespace;
			if (stream.nss.empty()) {
				commonFilter_.nss = NSSetT();
			} else if (!commonFilter_.nss.has_value()) {
				commonFilter_.nss.emplace();
				for (auto& nsFilter : stream.nss) {
					commonFilter_.nss->emplace(nsFilter);
				}
			} else if (!commonFilter_.nss->empty()) {
				for (auto& nsFilter : stream.nss) {
					commonFilter_.nss->emplace(nsFilter);
				}
			}
		}
	}
}

void EventsListener::eventsLoop() noexcept {
	while (!terminate_.load(std::memory_order_acquire)) {
		try {
			net::ev::periodic sendTimer;
			sendTimer.set(loop_);
			sendTimer.set([this](net::ev::timer&, int) { handleUpdates(); });
			sendTimer.start(kUpdatesHandlingPeriod, kUpdatesHandlingPeriod);

			while (!terminate_.load(std::memory_order_acquire)) {
				loop_.run();
			}
		} catch (const Error& e) {
			logFmt(LogError, "[events_manager]: fatal error in the event loop: '{}'", e.what());
			assertrx_dbg(false);
		} catch (std::exception& e) {
			logFmt(LogError, "[events_manager]: fatal error in the event loop: '{}'", e.what());
			assertrx_dbg(false);
		} catch (...) {
			logFmt(LogError, "[events_manager]: fatal error in the event loop: <unknown>");
			assertrx_dbg(false);
		}
	}
}

void EventsListener::handleUpdates() noexcept {
	bool hasProgress;
	do {
		hasProgress = false;
		auto minNextUpdateId = std::numeric_limits<uint64_t>::max();
		{
			shared_lock lck(mtx_);
			for (auto& [observer, subInfo] : subs_) {
				const auto prevNextUpdateId = subInfo.nextUpdateId;
				try {
					sendUpdates(*observer, subInfo);
				} catch (std::exception& e) {
					logFmt(LogError, "[events_manager]: unable to send update: '{}'", e.what());
				} catch (...) {
					logFmt(LogError, "[events_manager]: unable to send update: <unknown>");
				}
				hasProgress = hasProgress || subInfo.nextUpdateId != prevNextUpdateId;
				minNextUpdateId = std::min(minNextUpdateId, subInfo.nextUpdateId);
			}
		}
		updatesQueue_.EraseUntil(minNextUpdateId);
	} while (hasProgress);
}

void EventsListener::sendUpdates(IEventsObserver& observer, ObserverInfo& subInfo) {
	if (!observer.AvailableEventsSpace()) {
		return;
	}
	const auto updatePtr = updatesQueue_.Read(subInfo.nextUpdateId, std::nullopt);
	if (!updatePtr) {
		return;
	}
	const auto opts = EventsSerializationOpts(kEventSerializationFormatVersion)
						  .WithDBName(dbName_, subInfo.cfg.WithDBName())
						  .WithData(subInfo.cfg.DataType())
						  .WithShardID(shardID_.load(std::memory_order_relaxed), subInfo.cfg.WithShardID())
						  .WithServerID(serverID_.load(std::memory_order_relaxed), subInfo.cfg.WithServerID())
						  .WithLSN(subInfo.cfg.WithLSN())
						  .WithTimestamp(subInfo.cfg.WithTimestamp());
	if (updatePtr->IsUpdatesDropBlock) {
		const EventRecord updatesLostEvent(updates::UpdateRecord::UpdatesDropT{});
		const auto mask = buildStreamsMask(subInfo, updatesLostEvent);
		assertrx_dbg(mask);
		observer.SendEvent(mask, opts, updatesLostEvent);
		subInfo.nextUpdateId = updatePtr->ID() + 1;
		return;
	}

	subInfo.nextUpdateId = std::max(subInfo.nextUpdateId, updatePtr->ID());
	for (uint16_t offset = subInfo.nextUpdateId - updatePtr->ID(); offset < updatePtr->Count(); ++offset) {
		if (updatePtr->IsInvalidated() || !observer.AvailableEventsSpace()) {
			break;
		}
		const auto& upd = updatePtr->GetUpdate(offset).Data();
		if (const auto mask = buildStreamsMask(subInfo, upd)) {
			observer.SendEvent(mask, opts, upd);
		}
		++subInfo.nextUpdateId;
	}
}

uint32_t EventsListener::buildStreamsMask(const ObserverInfo& observer, const EventRecord& rec) noexcept {
	auto& streams = observer.cfg.Streams();
	assertrx_dbg(streams.size() <= kMaxStreamsPerSub);

	if (rec.IsEmptyRecord()) {
		return 0;
	}
	uint32_t mask = 0;
	if (rec.IsResyncOnUpdatesDropRecord()) [[unlikely]] {
		// Updates drop affects the whole subscription, so it is not filtered
		for (const auto& s : streams) {
			mask |= (1 << s.id);
		}
		return mask;
	}
	if (!observer.cfg.Events().empty() && !observer.cfg.Events().count(rec.Type())) {
		return 0;
	}
	for (const auto& s : streams) {
		if (s.Check(rec)) {
			mask |= (1 << s.id);
		}
	}
	return mask;
}

}  // namespace reindexer
