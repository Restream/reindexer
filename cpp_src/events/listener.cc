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

	auto it = subs_.find(&observer);
	bool emplaced = false;
	if (it == subs_.end()) {
		if (subs_.size() == UpdatesQueueT::kMaxReplicas) {
			return Error(errParams, "Subscribers limit was reached: {}", UpdatesQueueT::kMaxReplicas);
		}
		emplaced = true;
		const auto uid = emptyUIDs_.empty() ? maxUID_++ : emptyUIDs_.back();
		if (!emptyUIDs_.empty()) {
			emptyUIDs_.pop_back();
		}
		it = subs_.emplace(&observer, uid).first;
	}
	it->second.cfg = std::move(cfg);
	if (emplaced) {
		[[maybe_unused]] auto cnt = subsCount_.fetch_add(1, std::memory_order_release);
		assertrx_dbg(cnt == 0 || eventsThread_.joinable());
		it->second.startUpdateId = updatesQueue_.GetNextUpdateID();
	}
	rebuildCommonFilter();
	if (!eventsThread_.joinable()) {
		eventsThread_ = std::thread([this] { eventsLoop(); });
	}
	return {};
}

Error EventsListener::Remove(IEventsObserver& observer) {
	lock_guard subUnsubLck(subUnsubMtx_);
	unique_lock lck(mtx_);
	if (auto found = subs_.find(&observer); found != subs_.end()) {
		std::optional<uint64_t> updatesDropBoundry;
		const auto removedNextUpdateId = found->second.nextUpdateId;
		const auto removedUID = found->second.uid;
		auto minNextUpdateId = removedNextUpdateId;
		auto secondMinNextUpdateId = std::numeric_limits<uint64_t>::max();
		const auto prevSubsCnt = subs_.size();
		subs_.erase(found);
		rebuildCommonFilter();
		subsCount_.fetch_sub(1, std::memory_order_release);
		if (subs_.size() == 0) {
			assertrx_dbg(eventsThread_.joinable());
			updatesDropBoundry.emplace(updatesQueue_.GetNextUpdateID());
		} else {
			for (auto& sub : subs_) {
				const auto& subInfo = sub.second;
				if (subInfo.nextUpdateId <= minNextUpdateId) {
					secondMinNextUpdateId = minNextUpdateId;
					minNextUpdateId = subInfo.nextUpdateId;
				} else if (subInfo.nextUpdateId < secondMinNextUpdateId) {
					secondMinNextUpdateId = subInfo.nextUpdateId;
				}
			}
			if (minNextUpdateId == removedNextUpdateId && secondMinNextUpdateId != std::numeric_limits<uint64_t>::max()) {
				updatesDropBoundry.emplace(secondMinNextUpdateId);
			}
		}

		// Handle updates under subUnsubMtx_ only to avoid locking on the 'hot' path (SendUpdates(), etc)
		lck.unlock();

		if (prevSubsCnt == 1) {
			stop();
		}
		if (updatesDropBoundry.has_value()) {
			// Drop full nodes
			eraseUpdatesOnUnsubscribe(removedUID, minNextUpdateId, *updatesDropBoundry, prevSubsCnt);
		}

		lck.lock();
		emptyUIDs_.emplace_back(removedUID);
	}
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
			if (!commonFilter_.nss.has_value()) {
				commonFilter_.nss.emplace();
			}
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

void EventsListener::handleUpdates() {
	bool updatesSeen;
	do {
		updatesSeen = false;

		shared_lock lck(mtx_);

		for (auto& sub : subs_) {
			auto& subInfo = sub.second;
			auto& subIface = sub.first;
			auto maxUpdates = subIface->AvailableEventsSpace();
			if (!maxUpdates) {
				continue;
			}
			auto updatePtr = updatesQueue_.Read(subInfo.nextUpdateId, std::nullopt);
			if (!updatePtr) {
				continue;
			}
			updatesSeen = true;
			const auto opts = EventsSerializationOpts(kEventSerializationFormatVersion)
								  .WithDBName(dbName_, subInfo.cfg.WithDBName())
								  .WithData(subInfo.cfg.DataType())
								  .WithShardID(shardID_.load(std::memory_order_relaxed), subInfo.cfg.WithShardID())
								  .WithServerID(serverID_.load(std::memory_order_relaxed), subInfo.cfg.WithServerID())
								  .WithLSN(subInfo.cfg.WithLSN())
								  .WithTimestamp(subInfo.cfg.WithTimestamp());
			if (updatePtr->IsUpdatesDropBlock) {
				static EventRecord updatesLostEvent(updates::UpdateRecord::UpdatesDropT{});
				updatesLostEvent.UpdateTimestamp();
				const auto mask = buildStreamsMask(subInfo, updatesLostEvent);
				assertrx_dbg(mask);
				subIface->SendEvent(mask, opts, updatesLostEvent);
				continue;
			}

			subInfo.nextUpdateId = updatePtr->ID() > subInfo.nextUpdateId ? updatePtr->ID() : subInfo.nextUpdateId;
			for (uint16_t offset = subInfo.nextUpdateId - updatePtr->ID(); offset < updatePtr->Count(); ++offset) {
				if (updatePtr->IsInvalidated()) {
					break;
				}
				maxUpdates = subIface->AvailableEventsSpace();
				if (!maxUpdates) {
					break;
				}
				if (subInfo.startUpdateId > subInfo.nextUpdateId++) {
					updatePtr->OnUpdateHandledSimple(subInfo.uid, subs_.size(), offset);
					continue;
				}
				auto& upd = updatePtr->GetUpdate(offset).Data();
				const auto mask = buildStreamsMask(subInfo, upd);
				if (mask) {
					try {
						subIface->SendEvent(mask, opts, upd);
					} catch (const Error& e) {
						--subInfo.nextUpdateId;
						logFmt(LogError, "[events_manager]: unable to send update: '{}'", e.what());
						break;
					} catch (std::exception& e) {
						--subInfo.nextUpdateId;
						logFmt(LogError, "[events_manager]: unable to send update: '{}'", e.what());
						break;
					} catch (...) {
						--subInfo.nextUpdateId;
						logFmt(LogError, "[events_manager]: unable to send update: <unknown>");
						break;
					}
				}
				updatePtr->OnUpdateHandledSimple(subInfo.uid, subs_.size(), offset);
			}
		}
	} while (updatesSeen);
}

void EventsListener::eraseUpdatesOnUnsubscribe(uint32_t uid, uint64_t from, uint64_t to, uint32_t replicas) {
	assertrx_dbg(from <= to);
	for (auto next = from; next < to;) {
		for (auto updatePtr = updatesQueue_.Read(next, std::nullopt); updatePtr; updatePtr = updatesQueue_.Read(next, std::nullopt)) {
			if (updatePtr->IsUpdatesDropBlock) {
				continue;
			}
			next = updatePtr->ID() > next ? updatePtr->ID() : next;
			for (uint16_t offset = next - updatePtr->ID(); offset < updatePtr->Count() && next < to; ++offset) {
				++next;
				updatePtr->OnUpdateHandledSimple(uid, replicas, offset);
			}
			assertrx_dbg(next >= to || updatePtr->IsFullyErased());
		}
	}
}

uint32_t EventsListener::buildStreamsMask(const ObserverInfo& observer, const EventRecord& rec) noexcept {
	auto& streams = observer.cfg.Streams();
	assertrx_dbg(streams.size() <= kMaxStreamsPerSub);

	if (rec.IsEmptyRecord()) {
		return 0;
	}
	if (!observer.cfg.Events().empty() && !observer.cfg.Events().count(rec.Type())) {
		return 0;
	}
	uint32_t mask = 0;
	if (!rec.IsResyncOnUpdatesDropRecord()) [[likely]] {
		for (const auto& s : streams) {
			if (s.Check(rec)) {
				mask |= (1 << s.id);
			}
		}
		return mask;
	}

	// On updates drop
	for (const auto& s : streams) {
		mask |= (1 << s.id);
	}
	return mask;
}

}  // namespace reindexer
